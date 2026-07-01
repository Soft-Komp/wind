# backend/app/services/webhook_service.py
"""
Serwis webhooka — przyjmowanie dokumentow push od zewnetrznych systemow.

Realizuje 6-krokowy przeplyw z dokumentacji Etapu 2 (sekcja 4.13):
  1. Pobierz zrodlo po webhook_token (404 jesli brak/nieaktywne)
  2. Sparsuj body zgodnie z formatem zrodla (JSON — XML do rozszerzenia w F7)
  3. Zastosuj mapowanie pol, zbuduj UnifiedDocument
  4. Sprawdz duplikaty (DuplicateDetectionService)
  5. Zapisz, status pending_dispatch (lub duplicate_pending)
  6. Skolejkuj auto_dispatch_task natychmiast (nie czeka na cykl cron)

Rate limiting: 100 req/min per token (Redis), konfigurowalny przez
SystemConfig WEBHOOK_RATE_LIMIT_PER_MINUTE.

Bezpieczenstwo:
  - Token weryfikowany przez constant-time compare (source_admin_service.verify_webhook_token)
  - Rate limit PRZED jakimkolwiek przetwarzaniem (ochrona przed DoS)
  - is_test_mode=True na zrodle: dokument zapisywany ale auto_dispatch_task
    NIE jest wywolywany (zgodnie z decyzja: zrodla testowe nie wchodza do realnego obiegu)

UWAGA: from __future__ import annotations — NIGDY w tym pliku (SQLAlchemy ORM).
"""

import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.approval.document_source import DocumentSource
from app.schemas.unified_document import UnifiedDocument

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"
_DEFAULT_RATE_LIMIT_PER_MINUTE = 100


class WebhookRateLimitError(Exception):
    """Przekroczono limit zadan dla tego tokenu."""


class WebhookPayloadError(Exception):
    """Body webhooka nie da sie sparsowac lub zmapowac na UnifiedDocument."""


# =============================================================================
# Glowna funkcja — przyjecie dokumentu
# =============================================================================

async def receive_document(
    db: AsyncSession,
    redis: Any,
    *,
    token: str,
    payload: dict[str, Any],
    client_ip: str | None = None,
) -> dict[str, Any]:
    """
    Przyjmuje dokument przesłany przez webhook.

    Returns:
        {"id_instance": int, "status": str, "is_duplicate": bool}

    Raises:
        HTTPException(404): token nie odpowiada zadnemu aktywnemu zrodlu push.
        HTTPException(429): przekroczono rate limit.
        HTTPException(422): payload nie da sie zmapowac na UnifiedDocument.
    """
    # ── Krok 1: Zweryfikuj token, znajdz zrodlo ──────────────────────────────
    from app.services.source_admin_service import verify_webhook_token

    source = await verify_webhook_token(db, token)
    if source is None:
        logger.warning(
            "webhook: token nieprawidlowy lub zrodlo nieaktywne | ip=%s token_prefix=%s",
            client_ip, token[:8] if token else "",
        )
        await _log_invalid_token_attempt(db, token=token, client_ip=client_ip)
        await db.commit()
        raise HTTPException(status_code=404, detail="Nieprawidlowy token webhooka.")

    # ── Rate limiting (PRZED jakimkolwiek przetwarzaniem) ────────────────────
    await _check_rate_limit(db, redis, source.id_source, token)

    # ── Krok 2+3: Sparsuj body, zbuduj UnifiedDocument ───────────────────────
    try:
        unified_doc = await _build_unified_document(db, source, payload)
    except WebhookPayloadError as exc:
        await _log_webhook_attempt(
            db, id_source=source.id_source, success=False,
            error_message=str(exc), client_ip=client_ip,
        )
        raise HTTPException(status_code=422, detail=str(exc))

    # ── Krok 4: Sprawdz duplikaty ─────────────────────────────────────────────
    # Najpierw zapisz instancje (potrzebny id_instance dla DuplicateDetectionService),
    # potem sprawdz i ewentualnie oznacz jako duplicate_pending — ten sam wzorzec
    # co w source_sync_task (dostarczony w F3).
    id_instance = await _insert_instance(db, source, unified_doc)

    from app.services.duplicate_detection_service import DuplicateDetectionService
    is_duplicate = await DuplicateDetectionService.check_and_mark(
        db,
        id_instance=id_instance,
        id_source=source.id_source,
        id_document=unified_doc.id_document,
    )

    await db.commit()

    # ── Krok 6: Auto-dispatch natychmiast (pomijamy gdy zrodlo testowe) ──────
    if not source.is_test_mode and not is_duplicate:
        await _trigger_immediate_dispatch(redis, id_instance)
    elif source.is_test_mode:
        logger.info(
            "webhook: zrodlo w is_test_mode — auto_dispatch_task pominiety | "
            "id_instance=%s id_source=%s",
            id_instance, source.id_source,
        )

    await _log_webhook_attempt(
        db, id_source=source.id_source, success=True,
        id_instance=id_instance, client_ip=client_ip,
    )
    await db.commit()

    final_status = "duplicate_pending" if is_duplicate else "pending_dispatch"
    logger.info(
        "webhook: dokument przyjety | id_instance=%s id_source=%s status=%s ip=%s",
        id_instance, source.id_source, final_status, client_ip,
    )

    return {
        "id_instance":  id_instance,
        "status":       final_status,
        "is_duplicate": is_duplicate,
    }


# =============================================================================
# Krok 2+3: Parsowanie i mapowanie
# =============================================================================

async def _build_unified_document(
    db: AsyncSession,
    source: DocumentSource,
    payload: dict[str, Any],
) -> UnifiedDocument:
    """
    Buduje UnifiedDocument z payloadu webhooka zgodnie z mapowaniem pol zrodla.

    Jesli zrodlo nie ma skonfigurowanych field_mappings — uzywa wbudowanego
    domyslnego mapowania (oczekuje standardowych nazw: id_document, doc_number,
    doc_date, amount_gross, contractor_name, nip).
    """
    if not isinstance(payload, dict):
        raise WebhookPayloadError("Body webhooka musi byc obiektem JSON.")

    id_document = _extract_str(payload, "id_document") or _extract_str(payload, "ksef_id")
    if not id_document:
        raise WebhookPayloadError(
            "Payload nie zawiera 'id_document' (lub 'ksef_id') — wymagany identyfikator dokumentu."
        )

    # Pobierz field_mappings dla tego zrodla (jesli skonfigurowane)
    mappings_result = await db.execute(
        text(
            f"SELECT [common_field], [source_field], [field_type] "
            f"FROM [{_SCHEMA}].[skw_document_source_field_mappings] "
            f"WHERE [id_source] = :s"
        ),
        {"s": source.id_source},
    )
    mappings = {r[0]: (r[1], r[2]) for r in mappings_result.fetchall()}

    def _get_mapped(common_field: str, default_key: str) -> Any:
        """Odczytuje wartosc z payloadu — przez mapowanie jesli skonfigurowane, inaczej domyslny klucz."""
        if common_field in mappings:
            source_field, _ = mappings[common_field]
            return _nested_get(payload, source_field)
        return payload.get(default_key)

    doc_number      = _extract_str(payload, "doc_number") or str(_get_mapped("doc_number", "doc_number") or "") or None
    contractor_name = _extract_str(payload, "contractor_name") or str(_get_mapped("contractor_name", "contractor_name") or "") or None
    nip             = _extract_str(payload, "nip") or str(_get_mapped("nip", "nip") or "") or None

    amount_gross = _to_decimal(payload.get("amount_gross") or _get_mapped("amount_gross", "amount_gross"))
    doc_date     = _to_date(payload.get("doc_date") or _get_mapped("doc_date", "doc_date"))

    return UnifiedDocument(
        id_document=id_document,
        id_source=source.id_source,
        source_name=source.source_name,
        doc_number=doc_number,
        doc_date=doc_date,
        amount_gross=amount_gross,
        contractor_name=contractor_name,
        nip=nip,
        raw_data=payload,
    )


def _extract_str(payload: dict, key: str) -> str | None:
    val = payload.get(key)
    return str(val) if val is not None else None


def _nested_get(payload: dict, dotted_key: str) -> Any:
    """Obsluga zagniezdzonego klucza 'address.city' -> payload['address']['city']."""
    parts = dotted_key.split(".")
    current: Any = payload
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _to_decimal(val: Any) -> Decimal | None:
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None


def _to_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    try:
        return datetime.fromisoformat(str(val)[:10]).date()
    except (ValueError, TypeError):
        return None


# =============================================================================
# Krok 5: Zapis instancji
# =============================================================================

async def _insert_instance(
    db: AsyncSession,
    source: DocumentSource,
    unified_doc: UnifiedDocument,
) -> int:
    """
    Wstawia nowa instancje obiegu ze statusem pending_dispatch.

    Uzywa OUTPUT INSERTED.id_instance — niezawodne z pyodbc/MSSQL
    (SCOPE_IDENTITY() ma problemy w niektorych konfiguracjach sterownika).
    """
    extra_data = unified_doc.to_extra_data_json()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    result = await db.execute(
        text(f"""
            INSERT INTO [{_SCHEMA}].[skw_document_approval_instances] (
                [id_source], [id_document], [status], [current_step],
                [document_title], [document_amount], [extra_data],
                [dispatch_attempts], [created_at], [updated_at], [is_urgent]
            )
            OUTPUT INSERTED.[id_instance]
            VALUES (
                :id_source, :id_document, N'pending_dispatch', 0,
                :document_title, :document_amount, :extra_data,
                0, :now, :now, 0
            )
        """),
        {
            "id_source":       source.id_source,
            "id_document":     unified_doc.id_document,
            "document_title":  unified_doc.doc_number or f"Dokument {unified_doc.id_document}",
            "document_amount": float(unified_doc.amount_gross) if unified_doc.amount_gross else None,
            "extra_data":      json.dumps(extra_data, ensure_ascii=False, default=str),
            "now":             now,
        },
    )
    row = result.fetchone()
    return int(row[0])


# =============================================================================
# Krok 6: Natychmiastowy auto-dispatch
# =============================================================================

async def _trigger_immediate_dispatch(redis: Any, id_instance: int) -> None:
    """Kolejkuje pojedyncze zadanie dispatch zamiast czekac na cykl cron (1 min)."""
    if not redis:
        return
    try:
        from arq.connections import ArqRedis
        arq_redis: ArqRedis = redis  # type: ignore[assignment]
        await arq_redis.enqueue_job("auto_dispatch_task_single", id_instance=id_instance)
    except Exception as exc:
        logger.warning(
            "webhook: nie udalo sie zakolejkowac natychmiastowego dispatch | "
            "id_instance=%s error=%s (cron i tak obsluzy w ciagu 1 min)",
            id_instance, exc,
        )


# =============================================================================
# Rate limiting
# =============================================================================

async def _check_rate_limit(
    db: AsyncSession,
    redis: Any,
    id_source: int,
    token: str,
) -> None:
    """
    Sprawdza rate limit przez Redis INCR z TTL 60s (sliding window uproszczony).

    Raises:
        HTTPException(429): limit przekroczony.
    """
    if not redis:
        return  # fail-open gdy Redis niedostepny — nie blokujemy webhooka

    limit = await _get_config_int(db, "WEBHOOK_RATE_LIMIT_PER_MINUTE", _DEFAULT_RATE_LIMIT_PER_MINUTE)
    rate_key = f"webhook_rate:{id_source}"

    try:
        current = await redis.incr(rate_key)
        if current == 1:
            await redis.expire(rate_key, 60)
        if current > limit:
            logger.warning(
                "webhook: rate limit przekroczony | id_source=%s current=%s limit=%s",
                id_source, current, limit,
            )
            raise HTTPException(
                status_code=429,
                detail={
                    "code":    "webhook.rate_limit_exceeded",
                    "message": f"Przekroczono limit {limit} zadan/minute dla tego tokenu.",
                },
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("webhook: blad sprawdzania rate limit (fail-open): %s", exc)


# =============================================================================
# Logowanie prob webhooka (diagnostyka)
# =============================================================================

async def _log_webhook_attempt(
    db: AsyncSession,
    *,
    id_source: int,
    success: bool,
    id_instance: int | None = None,
    error_message: str | None = None,
    client_ip: str | None = None,
) -> None:
    """
    Loguje probe webhooka do AuditLog (nie do skw_source_action_log —
    ta tabela wymaga id_instance NOT NULL, a probe nieudane go nie mają).
    """
    try:
        await db.execute(
            text(
                f"INSERT INTO [{_SCHEMA}].[skw_AuditLog] "
                f"([ID_USER], [Action], [EntityType], [EntityID], [NewValue], [Success], [Timestamp]) "
                f"VALUES (NULL, N'webhook.received', N'DocumentSource', :eid, :details, :success, SYSUTCDATETIME())"
            ),
            {
                "eid":     str(id_source),
                "details": json.dumps({
                    "id_instance":   id_instance,
                    "error_message": error_message,
                    "client_ip":     client_ip,
                }, ensure_ascii=False, default=str),
                "success": 1 if success else 0,
            },
        )
    except Exception as exc:
        logger.error("_log_webhook_attempt: blad zapisu: %s", exc)


async def _log_invalid_token_attempt(
    db: AsyncSession,
    *,
    token: str,
    client_ip: str | None,
) -> None:
    """
    Loguje proby webhooka z nieprawidlowym tokenem — KRYTYCZNE dla wykrywania
    brute-force/skanowania tokenow. Bez tego atak na tokeny jest niewidoczny.

    EntityID = NULL bo nie znamy id_source (token nie odpowiada zadnemu zrodlu).
    Token loguje sie tylko jako prefix (8 znakow) — nigdy caly token do logow.
    """
    try:
        await db.execute(
            text(
                f"INSERT INTO [{_SCHEMA}].[skw_AuditLog] "
                f"([ID_USER], [Action], [EntityType], [EntityID], [NewValue], [Success], [Timestamp]) "
                f"VALUES (NULL, N'webhook.invalid_token', N'DocumentSource', NULL, :details, 0, SYSUTCDATETIME())"
            ),
            {
                "details": json.dumps({
                    "token_prefix": token[:8] if token else "",
                    "client_ip":    client_ip,
                }, ensure_ascii=False, default=str),
            },
        )
    except Exception as exc:
        logger.error("_log_invalid_token_attempt: blad zapisu: %s", exc)


async def _get_config_int(db: AsyncSession, key: str, default: int) -> int:
    try:
        result = await db.execute(
            text(
                f"SELECT [ConfigValue] FROM [{_SCHEMA}].[skw_SystemConfig] "
                f"WHERE [ConfigKey] = :k AND [IsActive] = 1"
            ),
            {"k": key},
        )
        row = result.fetchone()
        return int(row[0]) if row else default
    except Exception:
        return default