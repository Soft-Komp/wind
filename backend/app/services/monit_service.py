"""
Serwis Monitów — System Windykacja
=====================================

Odpowiedzialność:
    - Masowa wysyłka monitów (email/sms/print) do dłużników
    - Kolejkowanie zadań do ARQ workera (nie wysyłamy tu — enqueue)
    - Zapis pending rekordów do dbo_ext.MonitHistory (per dłużnik)
    - Pobieranie historii monitów i statystyk
    - Aktualizacja statusu monitu (webhook callback od bramki SMS/email)
    - Ponowna próba wysyłki (retry) dla failed monitów
    - Publikacja SSE eventów po operacjach
    - Walidacja debtor_ids przez debtor_service przed kolejkowaniem

Architektura wysyłki (NIE implementujemy tu faktycznej wysyłki):
    ┌──────────────────────────────────────────────┐
    │  monit_service.send_bulk()                   │
    │  1. Walidacja debtor_ids → debtor_service    │
    │  2. Pobranie szablonu z Templates            │
    │  3. INSERT pending records → MonitHistory    │
    │  4. Enqueue do ARQ → workers/send_worker.py  │
    │  5. SSE event: task_completed                │
    └──────────────────────────────────────────────┘
              ↓ (asynchronicznie, po kolejkowaniu)
    ┌──────────────────────────────────────────────┐
    │  ARQ Worker                         │
    │  - Pobiera task z kolejki                    │
    │  - Faktyczna wysyłka (email/sms gateway)     │
    │  - UPDATE MonitHistory.Status = "sent"       │
    │  - Webhook callback → update_status()        │
    └──────────────────────────────────────────────┘

Zależności:
    - services/debtor_service.py (validate_ids)
    - services/event_service.py (SSE)
    - services/audit_service.py
    - db/models/monit_history.py
    - db/models/template.py (szablony)

Decyzje projektowe:
    - ARQ kolejka: Redis list (nie stream) — prostsze, wystarczające
    - Klucz kolejki ARQ: arq:queue:default (kompatybilny z domyślnym ARQ)
    - Enqueue = LPUSH JSON payload do kolejki Redis
    - Brak wysyłki bezpośredniej — separacja odpowiedzialności
    - Template: MessageBody jest renderowany przez ARQ worker (nie tutaj)
    - Retry: ręczne (endpoint /retry/{id}) — nie auto-retry w serwisie
    - Status update (webhook): idempotentny — ten sam status nie zmienia nic

"""

from __future__ import annotations

import logging
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from app import db
import orjson
from redis.asyncio import Redis
from sqlalchemy import and_, case, desc, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.monit_history import MonitHistory
from app.db.models.template import Template
from app.services import audit_service
from app.services import debtor_service
from app.services import event_service
from app.services.debtor_service import (
    DebtorBatchValidationError,
    DebtorValidationError,
    DebtorWaproError,
)
from app.db.wapro import get_kontrahent_names_batch
from app.core.config import get_settings as _get_app_settings
from sqlalchemy import text as sa_text

# ---------------------------------------------------------------------------
# Logger własny modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

# Dozwolone typy monitów
_VALID_MONIT_TYPES: frozenset[str] = frozenset({"email", "sms", "print"})

# Dozwolone statusy
_VALID_STATUSES: frozenset[str] = frozenset({
    "pending", "sent", "delivered", "bounced",
    "failed", "opened", "clicked",
})

# Przejścia statusów (state machine) — klucz: poprzedni status, wartość: dozwolone następne
_STATUS_TRANSITIONS: dict[str, frozenset[str]] = {
    "pending":   frozenset({"sent", "failed"}),
    "sent":      frozenset({"delivered", "bounced", "failed", "opened"}),
    "delivered": frozenset({"opened", "clicked"}),
    "opened":    frozenset({"clicked"}),
    "clicked":   frozenset(),
    "bounced":   frozenset({"failed"}),
    "failed":    frozenset({"pending"}),  # retry reset status do pending
}

# ARQ kolejka Redis
_ARQ_QUEUE_KEY = "arq:queue:default"

# Plik logów
_MONITS_LOG_FILE_PATTERN = "logs/monits_{date}.jsonl"

# Limity
_MAX_BULK_DEBTORS: int  = 500     # Maksymalna liczba dłużników w jednej wysyłce bulk
_DEFAULT_PAGE_SIZE: int = 50
_MAX_PAGE_SIZE: int     = 200
_MAX_RETRY_COUNT: int   = 3       # Maksymalna liczba prób ponownej wysyłki


# ===========================================================================
# Dataclassy wejściowe / wyjściowe
# ===========================================================================

@dataclass(frozen=True)
class MonitBulkRequest:
    """
    Parametry masowej wysyłki monitów.

    Attributes:
        debtor_ids:             Lista ID kontrahentów WAPRO.
        monit_type:             Kanał wysyłki: "email", "sms", "print".
        template_id:            ID szablonu (z tabeli Templates).
        scheduled_at:           Zaplanowany czas wysyłki. None = natychmiast.
        custom_subject:         Override tematu emaila (opcjonalne).
        invoice_ids_per_debtor: Mapa {debtor_id: [invoice_id, ...]} — tylko przeterminowane.
                                None = brak filtra per rozrachunek (tryb legacy).
    """
    debtor_ids:              list[int]
    monit_type:              str
    template_id:             Optional[int] = None
    scheduled_at:            Optional[datetime] = None
    custom_subject:          Optional[str] = None
    invoice_ids_per_debtor:  Optional[dict[int, list[int]]] = None  # NOWE

    def __post_init__(self) -> None:
        if not self.debtor_ids:
            raise MonitValidationError("Lista debtor_ids nie może być pusta.")
        if len(self.debtor_ids) > _MAX_BULK_DEBTORS:
            raise MonitValidationError(
                f"Maksymalna liczba dłużników w jednej wysyłce to {_MAX_BULK_DEBTORS}. "
                f"Podano: {len(self.debtor_ids)}"
            )
        monit_type = self.monit_type.strip().lower()
        if monit_type not in _VALID_MONIT_TYPES:
            raise MonitValidationError(
                f"Nieprawidłowy typ monitu: {self.monit_type!r}. "
                f"Dozwolone: {sorted(_VALID_MONIT_TYPES)}"
            )
        object.__setattr__(self, "monit_type", monit_type)

        if self.custom_subject is not None:
            subject = unicodedata.normalize("NFC", self.custom_subject.strip())
            object.__setattr__(self, "custom_subject", subject[:200] if subject else None)


@dataclass(frozen=True)
class MonitBulkResult:
    """Wynik masowej wysyłki — po kolejkowaniu do ARQ."""
    total_requested:   int
    valid_debtor_count: int
    invalid_debtor_ids: list[int]
    queued_count:      int
    monit_ids:         list[int]        # ID nowo utworzonych rekordów MonitHistory
    task_id:           Optional[str]    # ARQ task ID
    scheduled_at:      Optional[str]
    created_at:        str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass(frozen=True)
class StatusUpdateResult:
    """Wynik aktualizacji statusu monitu (webhook)."""
    monit_id:    int
    old_status:  str
    new_status:  str
    updated:     bool
    message:     str


# ===========================================================================
# Klasy wyjątków
# ===========================================================================

class MonitError(Exception):
    """Bazowy wyjątek serwisu monitów."""


class MonitValidationError(MonitError):
    """Błąd walidacji parametrów."""


class MonitNotFoundError(MonitError):
    """Monit ID nie istnieje."""


class MonitTemplateNotFoundError(MonitError):
    """Szablon ID nie istnieje lub jest nieaktywny."""


class MonitStatusTransitionError(MonitError):
    """
    Niedozwolone przejście statusu.

    Attributes:
        monit_id:   ID monitu.
        old_status: Aktualny status.
        new_status: Żądany nowy status.
    """
    def __init__(self, monit_id: int, old_status: str, new_status: str) -> None:
        self.monit_id   = monit_id
        self.old_status = old_status
        self.new_status = new_status
        super().__init__(
            f"Niedozwolone przejście statusu monitu #{monit_id}: "
            f"'{old_status}' → '{new_status}'"
        )


class MonitRetryError(MonitError):
    """Nie można ponowić wysyłki — limit prób lub zły status."""


class MonitIntervalBlockedError(MonitError):
    """
    Wysyłka zablokowana — naruszenie interwału monitów.

    Attributes:
        blocked_invoice_ids:  ID rozrachunków naruszających interwał.
        blocked_debtor_ids:   ID kontrahentów naruszających interwał.
        interval_days:        Skonfigurowany interwał w dniach.
        details:              Lista szczegółów per rozrachunek/kontrahent.
    """
    def __init__(
        self,
        blocked_invoice_ids: list[int],
        blocked_debtor_ids: list[int],
        interval_days: int,
        details: list[dict],
    ) -> None:
        self.blocked_invoice_ids = blocked_invoice_ids
        self.blocked_debtor_ids  = blocked_debtor_ids
        self.interval_days       = interval_days
        self.details             = details
        super().__init__(
            f"Wysylka zablokowana — naruszenie interwalu monitow. "
            f"Zablokowane rozrachunki: {blocked_invoice_ids}, "
            f"kontrahenci: {blocked_debtor_ids}"
        )


@dataclass
class IntervalCheckResult:
    """
    Wynik sprawdzenia interwału monitów przed wysyłką.

    Attributes:
        allowed:             True = można wysłać (lub warn mode z częściowym wysłaniem).
        block_mode:          'block' | 'warn' — z SystemConfig.
        interval_days:       Skonfigurowany interwał.
        blocked_invoice_ids: ID rozrachunków naruszających interwał.
        blocked_debtor_ids:  ID kontrahentów naruszających interwał.
        allowed_invoice_ids: ID rozrachunków które można wysłać (warn mode).
        skipped:             Lista pominiętych z powodem [{invoice_id, reason, ...}].
        details:             Szczegóły per blokada.
    """
    allowed:             bool
    block_mode:          str
    interval_days:       int
    blocked_invoice_ids: list[int] = field(default_factory=list)
    blocked_debtor_ids:  list[int] = field(default_factory=list)
    allowed_invoice_ids: list[int] = field(default_factory=list)
    skipped:             list[dict] = field(default_factory=list)
    details:             list[dict] = field(default_factory=list)


# ===========================================================================
# Funkcje pomocnicze — prywatne
# ===========================================================================

def _get_monits_log_file() -> Path:
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return log_dir / f"monits_{today}.jsonl"


def _append_to_file(filepath: Path, record: dict) -> None:
    try:
        line = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        with filepath.open("ab") as f:
            f.write(line)
    except OSError as exc:
        logger.warning(
            "Nie można zapisać do pliku logów monitów",
            extra={"filepath": str(filepath), "error": str(exc)}
        )


def _build_log_record(action: str, **kwargs) -> dict:
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": "monit_service",
        "action": action,
        **kwargs,
    }


def _monit_to_dict(monit: MonitHistory) -> dict:
    """Konwertuje obiekt MonitHistory na słownik bezpieczny do API."""
    return {
        "id_monit":        monit.id_monit,
        "id_kontrahenta":  monit.id_kontrahenta,
        "id_user":         monit.id_user,
        "monit_type":      monit.monit_type,
        "template_id":     monit.template_id,
        "status":          monit.status,
        "recipient":       monit.recipient,
        "subject":         monit.subject,
        "total_debt":      float(monit.total_debt) if monit.total_debt is not None else None,
        "invoice_numbers": monit.invoice_numbers,
        "pdf_path":        monit.pdf_path,
        "external_id":     monit.external_id,
        "scheduled_at":    monit.scheduled_at.isoformat() if monit.scheduled_at else None,
        "sent_at":         monit.sent_at.isoformat() if monit.sent_at else None,
        "delivered_at":    monit.delivered_at.isoformat() if monit.delivered_at else None,
        "opened_at":       monit.opened_at.isoformat() if monit.opened_at else None,
        "clicked_at":      monit.clicked_at.isoformat() if monit.clicked_at else None,
        "error_message":   monit.error_message,
        "retry_count":     monit.retry_count,
        "cost":            float(monit.cost) if monit.cost is not None else None,
        "is_active":       monit.is_active,
        "created_at":      monit.created_at.isoformat() if monit.created_at else None,
        "updated_at":      monit.updated_at.isoformat() if monit.updated_at else None,
    }


async def _get_template(
    db: AsyncSession,
    template_id: int,
) -> Template:
    """
    Pobiera szablon monitu.

    Args:
        db:          Sesja SQLAlchemy.
        template_id: ID szablonu.

    Returns:
        Obiekt Template.

    Raises:
        MonitTemplateNotFoundError: Szablon nie istnieje lub jest nieaktywny.
    """
    result = await db.execute(
        select(Template).where(
            and_(Template.id_template == template_id, Template.is_active == True)  # noqa: E712
        )
    )
    template = result.scalar_one_or_none()
    if template is None:
        raise MonitTemplateNotFoundError(
            f"Szablon ID={template_id} nie istnieje lub jest nieaktywny."
        )
    return template


def _is_valid_status_transition(old_status: str, new_status: str) -> bool:
    """
    Sprawdza czy przejście statusu jest dozwolone.

    Implementuje state machine statusów monitów.

    Args:
        old_status: Aktualny status.
        new_status: Nowy status.

    Returns:
        True jeśli przejście dozwolone.
    """
    if old_status == new_status:
        return True  # Idempotent — ten sam status OK
    allowed = _STATUS_TRANSITIONS.get(old_status, frozenset())
    return new_status in allowed


async def _enqueue_to_arq(
    redis: Redis,
    task_name: str,
    task_payload: dict,
) -> str:
    """
    Kolejkuje task do ARQ worker przez oficjalny ARQ pool.

    Używa get_arq_pool() z app.core.arq_pool — ARQ zapisuje
    zadanie jako ZSET (nie LIST). Stara implementacja z LPUSH
    była niezgodna z ARQ i powodowała WRONGTYPE error.

    Args:
        redis:        Klient Redis (nieużywany bezpośrednio — ARQ pool ma własne połączenie).
        task_name:    Nazwa funkcji workera (np. "send_bulk_emails").
        task_payload: Argumenty kwargs dla workera.

    Returns:
        job_id (str) wygenerowany przez ARQ.

    Raises:
        MonitError: Gdy kolejkowanie się nie powiodło.
    """
    try:
        from app.core.arq_pool import get_arq_pool
        arq = get_arq_pool()
        job = await arq.enqueue_job(task_name, **task_payload)
        task_id = str(job.job_id) if job else "unknown"

        logger.info(
            "Task kolejkowany do ARQ",
            extra={
                "task_id":   task_id,
                "task_name": task_name,
            }
        )
        return task_id

    except Exception as exc:
        logger.error(
            "Nie udało się kolejkować task do ARQ",
            extra={"task_name": task_name, "error": str(exc)}
        )
        raise MonitError(f"Błąd kolejkowania zadania '{task_name}': {exc}") from exc
    
# ===========================================================================
# Sprawdzenie interwału monitów — pre-flight check przed enqueue
# ===========================================================================

_INTERVAL_LOG_FILE_PATTERN = "logs/monit_interval_{date}.jsonl"
_CONFIG_CACHE_TTL = 300  # 5 minut — TTL cache konfiguracji interwału w Redis
_CONFIG_CACHE_KEY = "cfg:monit_interval"


async def _load_interval_config(db: AsyncSession, redis: Redis) -> dict:
    """
    Ładuje konfigurację interwału z Redis (cache) lub DB (fallback).

    Cache key: cfg:monit_interval, TTL: 5 minut.

    Returns:
        {"interval_days": int, "block_mode": str, "min_days_overdue": int}
    """
    # Próba odczytu z cache Redis
    try:
        cached = await redis.get(_CONFIG_CACHE_KEY)
        if cached:
            parsed = orjson.loads(cached)
            logger.debug(
                "Konfiguracja interwalu z cache Redis",
                extra={"config": parsed},
            )
            return parsed
    except Exception as exc:
        logger.warning(
            "Blad odczytu cache interwalu z Redis — fallback do DB",
            extra={"error": str(exc)},
        )

    # Fallback do DB
    defaults = {
        "interval_days":    14,
        "block_mode":       "block",
        "min_days_overdue": 1,
    }
    try:
        result = await db.execute(
            sa_text("""
                SELECT [ConfigKey], [ConfigValue]
                FROM [dbo_ext].[skw_SystemConfig]
                WHERE [ConfigKey] IN (
                    'monit.interval_days',
                    'monit.block_mode',
                    'monit.min_days_overdue'
                )
                AND [IsActive] = 1
            """)
        )
        rows = result.fetchall()
        config = dict(defaults)
        for row in rows:
            key, val = row[0], row[1]
            if key == "monit.interval_days" and val:
                try:
                    config["interval_days"] = int(val)
                except ValueError:
                    pass
            elif key == "monit.block_mode" and val in ("block", "warn"):
                config["block_mode"] = val
            elif key == "monit.min_days_overdue" and val:
                try:
                    config["min_days_overdue"] = int(val)
                except ValueError:
                    pass
    except Exception as exc:
        logger.error(
            "Blad odczytu konfiguracji interwalu z DB — uzywam wartosci domyslnych",
            extra={"error": str(exc), "defaults": defaults},
        )
        config = dict(defaults)

    # Zapisz do cache Redis
    try:
        await redis.setex(
            _CONFIG_CACHE_KEY,
            _CONFIG_CACHE_TTL,
            orjson.dumps(config),
        )
        logger.debug("Konfiguracja interwalu zapisana do cache Redis", extra={"config": config})
    except Exception as exc:
        logger.warning("Blad zapisu cache interwalu do Redis", extra={"error": str(exc)})

    return config


async def check_interval(
    db: AsyncSession,
    redis: Redis,
    debtor_id: int,
    invoice_ids: list[int],
    triggered_by_user_id: Optional[int] = None,
) -> IntervalCheckResult:
    """
    Sprawdza czy wysyłka monitu narusza skonfigurowany interwał.

    Logika (oba warunki sprawdzane niezależnie — OR):
        1. Per kontrahent: czy ostatni monit (skw_MonitHistory) był < interval_days temu
        2. Per rozrachunek: czy ostatni monit (skw_MonitHistory_Invoices) był < interval_days temu

    Tryby:
        block: jeśli naruszenie → IntervalCheckResult.allowed=False
               → caller powinien zwrócić HTTP 409
        warn:  jeśli naruszenie → IntervalCheckResult.allowed=True ale:
               - blocked_invoice_ids: lista naruszających
               - allowed_invoice_ids: lista do wysłania
               - skipped: lista z powodem

    Logowanie:
        Każde sprawdzenie → logs/monit_interval_YYYY-MM-DD.jsonl

    Args:
        db:                    Sesja SQLAlchemy.
        redis:                 Klient Redis.
        debtor_id:             ID kontrahenta WAPRO.
        invoice_ids:           Lista ID rozrachunków do sprawdzenia.
        triggered_by_user_id:  ID użytkownika (do logów).

    Returns:
        IntervalCheckResult z pełnymi informacjami o blokadzie.
    """
    # ── Krok 1: Załaduj konfigurację ────────────────────────────────────────
    config = await _load_interval_config(db, redis)
    interval_days: int = config["interval_days"]
    block_mode:    str = config["block_mode"]

    logger.info(
        "check_interval START",
        extra={
            "debtor_id":    debtor_id,
            "invoice_ids":  invoice_ids,
            "interval_days": interval_days,
            "block_mode":   block_mode,
            "user_id":      triggered_by_user_id,
        },
    )

    blocked_debtor_ids:  list[int] = []
    blocked_invoice_ids: list[int] = []
    details:             list[dict] = []
    skipped:             list[dict] = []

    # ── Krok 2: Sprawdź per kontrahent ──────────────────────────────────────
    debtor_blocked = False
    try:
        debtor_result = await db.execute(
            sa_text("""
                SELECT TOP 1
                    ISNULL(SentAt, CreatedAt)    AS OstatniMonit,
                    DATEDIFF(
                        DAY,
                        ISNULL(SentAt, CreatedAt),
                        GETDATE()
                    )                            AS DniTemu
                FROM [dbo_ext].[skw_MonitHistory]
                WHERE [ID_KONTRAHENTA] = :debtor_id
                  AND [IsActive] = 1
                ORDER BY ISNULL([SentAt], [CreatedAt]) DESC
            """).bindparams(debtor_id=debtor_id)
        )
        debtor_row = debtor_result.fetchone()

        if debtor_row is not None:
            dni_temu = debtor_row[1]  # DniTemu
            ostatni  = debtor_row[0]  # OstatniMonit

            if dni_temu is not None and dni_temu < interval_days:
                debtor_blocked = True
                blocked_debtor_ids.append(debtor_id)
                next_allowed = (
                    ostatni.isoformat()
                    if hasattr(ostatni, "isoformat") else str(ostatni)
                )
                details.append({
                    "type":               "debtor",
                    "debtor_id":          debtor_id,
                    "last_monit_date":    next_allowed,
                    "days_since_monit":   int(dni_temu),
                    "days_remaining":     interval_days - int(dni_temu),
                    "reason":             "interval_violated_debtor",
                })
                logger.warning(
                    "Interwał naruszony per kontrahent",
                    extra={
                        "debtor_id":        debtor_id,
                        "days_since_monit": dni_temu,
                        "interval_days":    interval_days,
                        "days_remaining":   interval_days - int(dni_temu),
                    },
                )

    except Exception as exc:
        logger.error(
            "Blad sprawdzenia interwalu per kontrahent",
            extra={"debtor_id": debtor_id, "error": str(exc)},
        )

    # ── Krok 3: Sprawdź per rozrachunek ─────────────────────────────────────
    allowed_invoice_ids: list[int] = []

    for inv_id in invoice_ids:
        invoice_blocked = False
        try:
            inv_result = await db.execute(
                sa_text("""
                    SELECT TOP 1
                        [CreatedAt]                         AS OstatniMonit,
                        DATEDIFF(DAY, [CreatedAt], GETDATE()) AS DniTemu
                    FROM [dbo_ext].[skw_MonitHistory_Invoices]
                    WHERE [ID_ROZRACHUNKU] = :inv_id
                    ORDER BY [CreatedAt] DESC
                """).bindparams(inv_id=inv_id)
            )
            inv_row = inv_result.fetchone()

            if inv_row is not None:
                dni_temu = inv_row[1]
                ostatni  = inv_row[0]

                if dni_temu is not None and dni_temu < interval_days:
                    invoice_blocked = True
                    blocked_invoice_ids.append(inv_id)
                    next_date = (
                        ostatni.isoformat()
                        if hasattr(ostatni, "isoformat") else str(ostatni)
                    )
                    detail = {
                        "type":             "invoice",
                        "invoice_id":       inv_id,
                        "last_monit_date":  next_date,
                        "days_since_monit": int(dni_temu),
                        "days_remaining":   interval_days - int(dni_temu),
                        "reason":           "interval_violated_invoice",
                    }
                    details.append(detail)
                    skipped.append({
                        "invoice_id":      inv_id,
                        "reason":          "interval_violated",
                        "last_monit_date": next_date,
                        "days_remaining":  interval_days - int(dni_temu),
                    })
                    logger.warning(
                        "Interwał naruszony per rozrachunek",
                        extra={
                            "invoice_id":       inv_id,
                            "days_since_monit": dni_temu,
                            "interval_days":    interval_days,
                        },
                    )

        except Exception as exc:
            logger.error(
                "Blad sprawdzenia interwalu per rozrachunek",
                extra={"invoice_id": inv_id, "error": str(exc)},
            )

        if not invoice_blocked and not debtor_blocked:
            allowed_invoice_ids.append(inv_id)

    # Jeśli kontrahent zablokowany → żaden rozrachunek nie przechodzi
    if debtor_blocked:
        allowed_invoice_ids = []
        for inv_id in invoice_ids:
            if not any(s["invoice_id"] == inv_id for s in skipped):
                debtor_detail = next(
                    (d for d in details if d.get("type") == "debtor"), {}
                )
                skipped.append({
                    "invoice_id":    inv_id,
                    "reason":        "interval_violated_debtor",
                    "last_monit_date": debtor_detail.get("last_monit_date"),
                    "days_remaining": debtor_detail.get("days_remaining"),
                })

    # ── Krok 4: Wyznacz wynik ────────────────────────────────────────────────
    has_any_block = bool(blocked_debtor_ids or blocked_invoice_ids)

    if block_mode == "block":
        allowed = not has_any_block
    else:
        # warn — dozwolone jeśli choć jeden invoice może przejść
        allowed = len(allowed_invoice_ids) > 0 or not has_any_block

    result = IntervalCheckResult(
        allowed=allowed,
        block_mode=block_mode,
        interval_days=interval_days,
        blocked_invoice_ids=blocked_invoice_ids,
        blocked_debtor_ids=blocked_debtor_ids,
        allowed_invoice_ids=allowed_invoice_ids if has_any_block else list(invoice_ids),
        skipped=skipped,
        details=details,
    )

    # ── Krok 5: Logowanie do JSONL ───────────────────────────────────────────
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = log_dir / f"monit_interval_{today}.jsonl"

    _append_to_file(
        log_file,
        {
            "ts":                  datetime.now(timezone.utc).isoformat(),
            "service":             "monit_service.check_interval",
            "debtor_id":           debtor_id,
            "invoice_ids":         invoice_ids,
            "interval_days":       interval_days,
            "block_mode":          block_mode,
            "allowed":             allowed,
            "blocked_invoice_ids": blocked_invoice_ids,
            "blocked_debtor_ids":  blocked_debtor_ids,
            "allowed_invoice_ids": result.allowed_invoice_ids,
            "skipped_count":       len(skipped),
            "user_id":             triggered_by_user_id,
        },
    )

    logger.info(
        "check_interval WYNIK: allowed=%s, blocked_inv=%d, blocked_deb=%d, allowed_inv=%d",
        allowed,
        len(blocked_invoice_ids),
        len(blocked_debtor_ids),
        len(result.allowed_invoice_ids),
        extra={
            "debtor_id":           debtor_id,
            "block_mode":          block_mode,
            "interval_days":       interval_days,
            "allowed":             allowed,
            "blocked_invoice_ids": blocked_invoice_ids,
            "allowed_invoice_ids": result.allowed_invoice_ids,
        },
    )

    return result

async def send_bulk(
    db: AsyncSession,
    redis: Redis,
    wapro,
    request: MonitBulkRequest,
    triggered_by_user_id: int,
    ip_address: Optional[str] = None,
) -> MonitBulkResult:
    """
    Masowa wysyłka monitów do dłużników.

    Przepływ:
        1. Walidacja debtor_ids → debtor_service.validate_ids()
           → Zbiera invalid_ids (nie blokuje dla tych które OK)
        2. Pobranie szablonu (jeśli template_id podany)
        3. INSERT pending records do MonitHistory (per dłużnik)
        4. Enqueue do ARQ worker (jeden task per typ wysyłki)
        5. AuditLog
        6. SSE event: task_completed (natychmiastowe powiadomienie)

    ⚠️  Faktyczna wysyłka następuje w ARQ worker .
    Ta funkcja tylko KOLEJKUJE i tworzy pending rekordy.

    Args:
        db:                    Sesja SQLAlchemy.
        redis:                 Klient Redis.
        wapro:                 Pula połączeń WAPRO (do validate_ids).
        request:               Parametry wysyłki.
        triggered_by_user_id:  ID użytkownika zlecającego wysyłkę.
        ip_address:            IP inicjatora.

    Returns:
        MonitBulkResult z ID kolejki i listą monit_ids.

    Raises:
        MonitTemplateNotFoundError: Gdy template_id nie istnieje.
        MonitValidationError:       Gdy żaden z debtor_ids nie jest prawidłowy.
    """

    # ── BLOKADA L1: Tryb demonstracyjny ──────────────────────────────────────
    # Sprawdzenie na najwcześniejszym możliwym etapie — przed jakimkolwiek
    # zapisem do DB czy kolejkowaniem do ARQ.
    _app_settings = _get_app_settings()
    if _app_settings.demo_mode:
        logger.warning(
            "Wysyłka zablokowana — demo_mode=true",
            extra={
                "triggered_by_user_id": triggered_by_user_id,
                "monit_type":           request.monit_type,
                "debtor_count":         len(request.debtor_ids),
                "ip_address":           ip_address,
                "demo_mode":            True,
            },
        )
        raise MonitError(
            "DEMO_MODE: Wysyłka monitów jest zablokowana w trybie demonstracyjnym. "
            "Skontaktuj się z administratorem systemu."
        )
    # ── koniec blokady DEMO_MODE ──────────────────────────────────────────────
    
    op_start = datetime.now(timezone.utc)

    logger.info(
        "Rozpoczynam masową wysyłkę monitów",
        extra={
            "debtor_count": len(request.debtor_ids),
            "monit_type": request.monit_type,
            "template_id": request.template_id,
            "triggered_by": triggered_by_user_id,
            "ip_address": ip_address,
        }
    )

    # Krok 1: Walidacja debtor_ids w WAPRO
    invalid_debtor_ids: list[int] = []
    try:
        valid_ids = await debtor_service.validate_ids(wapro, request.debtor_ids)
        invalid_debtor_ids = sorted(
            set(request.debtor_ids) - set(valid_ids)
        )
    except (DebtorValidationError, DebtorWaproError) as exc:
        logger.error(
            "Błąd walidacji debtor_ids — przerywam wysyłkę",
            extra={"error": str(exc), "triggered_by": triggered_by_user_id}
        )
        raise MonitValidationError(f"Błąd walidacji dłużników: {exc}") from exc

    if not valid_ids:
        raise MonitValidationError(
            "Żaden z podanych ID dłużników nie jest prawidłowy. Wysyłka przerwana."
        )

    if invalid_debtor_ids:
        logger.warning(
            "Część debtor_ids jest nieważnych — pomijamy",
            extra={
                "valid_count": len(valid_ids),
                "invalid_count": len(invalid_debtor_ids),
                "invalid_sample": invalid_debtor_ids[:20],
            }
        )

    # ── Krok 1b: Pre-flight check interwału monitów ──────────────────────────
    # Sprawdzamy PRZED tworzeniem rekordów MonitHistory i enqueueing do ARQ.
    # block_mode=block → zablokuj całą partię jeśli naruszenie (atomowo)
    # block_mode=warn  → wyślij tylko dozwolone, skipped w response

    _interval_skipped_all: list[dict] = []

    if request.invoice_ids_per_debtor:
        # Zbierz wyniki per kontrahent — atomowy check dla bulk
        _all_blocked_invoices: list[int] = []
        _all_blocked_debtors:  list[int] = []
        _all_details:          list[dict] = []
        _interval_config = await _load_interval_config(db, redis)

        # Normalizuj klucze do int — JSON przesyła jako stringi ("6" zamiast 6)
        _inv_map_check = {
            int(k): v
            for k, v in request.invoice_ids_per_debtor.items()
        }
        for _debtor_id in list(valid_ids):
            _inv_ids = _inv_map_check.get(_debtor_id, [])
            if not _inv_ids:
                continue
            _chk = await check_interval(
                db=db,
                redis=redis,
                debtor_id=_debtor_id,
                invoice_ids=_inv_ids,
                triggered_by_user_id=triggered_by_user_id,
            )
            _all_blocked_invoices.extend(_chk.blocked_invoice_ids)
            _all_blocked_debtors.extend(_chk.blocked_debtor_ids)
            _all_details.extend(_chk.details)
            _interval_skipped_all.extend(_chk.skipped)

        _has_any_block = bool(_all_blocked_invoices or _all_blocked_debtors)
        _block_mode = _interval_config.get("block_mode", "block")

        if _has_any_block and _block_mode == "block":
            # ATOMOWE ZABLOKOWANIE CAŁEJ PARTII
            logger.warning(
                "send_bulk ZABLOKOWANY — naruszenie interwalu (block_mode=block)",
                extra={
                    "debtor_count":          len(valid_ids),
                    "blocked_invoices":      _all_blocked_invoices,
                    "blocked_debtors":       _all_blocked_debtors,
                    "triggered_by":          triggered_by_user_id,
                },
            )
            raise MonitIntervalBlockedError(
                blocked_invoice_ids=_all_blocked_invoices,
                blocked_debtor_ids=_all_blocked_debtors,
                interval_days=_interval_config.get("interval_days", 14),
                details=_all_details,
            )

        elif _has_any_block and _block_mode == "warn":
            # WARN — odfiltruj zablokowane debtor_ids
            # (kontrahent zablokowany = usuń z valid_ids)
            _blocked_debtor_set = set(_all_blocked_debtors)
            valid_ids = [v for v in valid_ids if v not in _blocked_debtor_set]
            logger.warning(
                "send_bulk WARN — czesc kontrahentow pominieta (naruszenie interwalu)",
                extra={
                    "original_count":  len(request.debtor_ids),
                    "skipped_debtors": list(_blocked_debtor_set),
                    "remaining":       len(valid_ids),
                },
            )
            if not valid_ids:
                # Wszyscy zablokowania — zwróć pustą odpowiedź z info
                return MonitBulkResult(
                    total_requested=len(request.debtor_ids),
                    valid_debtor_count=0,
                    invalid_debtor_ids=invalid_debtor_ids,
                    queued_count=0,
                    monit_ids=[],
                    task_id=None,
                    scheduled_at=request.scheduled_at.isoformat() if request.scheduled_at else None,
                )

    # Krok 2: Pobranie szablonu (opcjonalne)
    template = None
    template_subject = None
    if request.template_id is not None:
        template = await _get_template(db, request.template_id)
        template_subject = template.subject
    # Krok 2b: Pobranie emaili / telefonów dłużników z WAPRO
    from app.db.wapro import get_debtor_by_id as _wapro_get_debtor
    debtor_contacts: dict[int, str] = {}
    debtor_contacts_raw: dict[int, dict] = {}
    for debtor_id in valid_ids:
        try:
            wapro_result = await _wapro_get_debtor(debtor_id)
            if wapro_result.rows:
                    row = wapro_result.rows[0]
                    debtor_contacts_raw[debtor_id] = row
                    email = row.get("Email") or ""
                    phone = row.get("Telefon") or ""
                    if request.monit_type == "email":
                        debtor_contacts[debtor_id] = email.strip()
                    elif request.monit_type == "sms":
                        debtor_contacts[debtor_id] = phone.strip()
                    else:
                        debtor_contacts[debtor_id] = ""
        except Exception as exc:
            logger.warning(
                "Nie udało się pobrać danych kontaktowych dłużnika",
                extra={"debtor_id": debtor_id, "error": str(exc)}
            )
            debtor_contacts[debtor_id] = ""
    # Krok 3: INSERT pending records do MonitHistory
    now = datetime.now(timezone.utc)
    monit_ids: list[int] = []
    for debtor_id in valid_ids:
        subject = request.custom_subject or template_subject
        new_monit = MonitHistory(
            id_kontrahenta=debtor_id,
            id_user=triggered_by_user_id,
            monit_type=request.monit_type,
            template_id=request.template_id,
            status="pending",
            recipient=debtor_contacts.get(debtor_id, ""),
            subject=subject,
            scheduled_at=request.scheduled_at,
            retry_count=0,
            is_active=True,
            created_at=now,
        )
        db.add(new_monit)
        await db.flush()  # Pobieramy ID przez flush
        monit_ids.append(new_monit.id_monit)

    await db.commit()

    logger.info(
        "Pending rekordy MonitHistory utworzone",
        extra={
            "count": len(monit_ids),
            "monit_type": request.monit_type,
            "monit_ids_sample": monit_ids[:10],
        }
    )

    # Krok 4: Enqueue do ARQ worker
    arq_payload = {
        "monit_ids":            monit_ids,
        "triggered_by_user_id": triggered_by_user_id,
    }

    _task_map = {"email": "send_bulk_emails", "sms": "send_bulk_sms", "print": "generate_pdf_task"}
    task_name = _task_map.get(request.monit_type, f"send_{request.monit_type}_task")
    task_id = None
    try:
        if request.monit_type == "print":
            # generate_pdf_task przyjmuje pojedynczy monit_id — enqueue per dłużnik
            for idx, debtor_id in enumerate(valid_ids):
                debtor_info = debtor_contacts_raw.get(debtor_id, {})
                arq_payload = {
                    "monit_id":             monit_ids[idx],
                    "debtor_name":          debtor_info.get("NazwaKontrahenta") or f"Dłużnik {debtor_id}",
                    "debtor_nip":           None,
                    "debtor_address":       None,
                    "invoices":             None,
                    "total_debt":           float(debtor_info.get("SumaDlugu") or 0.0),
                    "payment_deadline":     None,
                    "payment_account":      None,
                    "triggered_by_user_id": triggered_by_user_id,
                }
                task_id = await _enqueue_to_arq(redis, task_name, arq_payload)
        else:
            # Spłaszcz invoice_ids do płaskiej listy
            _all_invoice_ids: list[int] = []
            if request.invoice_ids_per_debtor:
                # Klucze z JSON przychodzą jako stringi — normalizuj do int
                _inv_map = {
                    int(k): v
                    for k, v in request.invoice_ids_per_debtor.items()
                }
                for _did in valid_ids:
                    _all_invoice_ids.extend(_inv_map.get(_did, []))

            arq_payload = {
                "monit_ids":            monit_ids,
                "triggered_by_user_id": triggered_by_user_id,
                "invoice_ids":          _all_invoice_ids,
            }
            task_id = await _enqueue_to_arq(redis, task_name, arq_payload)
    except MonitError as exc:
        logger.error(
            "Błąd kolejkowania — rekordy pending pozostają w DB",
            extra={
                "monit_ids": monit_ids,
                "error": str(exc),
            }
        )
        # Nie rollback — rekordy pending zostaną zebrane przez next ARQ scan
        task_id = None

    duration_ms = (datetime.now(timezone.utc) - op_start).total_seconds() * 1000

    _append_to_file(
        _get_monits_log_file(),
        _build_log_record(
            action="monit_bulk_queued",
            monit_type=request.monit_type,
            template_id=request.template_id,
            valid_debtors=len(valid_ids),
            invalid_debtors=len(invalid_debtor_ids),
            monit_count=len(monit_ids),
            task_id=task_id,
            triggered_by=triggered_by_user_id,
            ip_address=ip_address,
            duration_ms=round(duration_ms, 1),
        )
    )

    audit_service.log_crud(
        db=db,
        action="monit_bulk_sent",
        entity_type="Monit",
        details={
            "monit_type": request.monit_type,
            "template_id": request.template_id,
            "valid_debtors": len(valid_ids),
            "invalid_debtors": len(invalid_debtor_ids),
            "monit_ids": monit_ids,
            "task_id": task_id,
            "triggered_by": triggered_by_user_id,
            "ip_address": ip_address,
        },
        success=True,
    )

    # Krok 5: SSE event
    try:
        await event_service.publish_task_completed(
            redis=redis,
            task_name=task_name,
            success_count=len(valid_ids),
            failed_count=len(invalid_debtor_ids),
            message=(
                f"Zlecono wysyłkę {request.monit_type.upper()} do {len(valid_ids)} dłużników. "
                f"{f'Pominięto {len(invalid_debtor_ids)} nieważnych ID.' if invalid_debtor_ids else ''}"
            ),
            target_user_id=triggered_by_user_id,
            triggered_by_user_id=triggered_by_user_id,
            extra_data={
                "monit_type":    request.monit_type,
                "monit_ids":     monit_ids,
                "invalid_count": len(invalid_debtor_ids),
            },
        )
    except Exception as exc:
        logger.warning("Błąd publikacji SSE event po send_bulk", extra={"error": str(exc)})

    return MonitBulkResult(
        total_requested=len(request.debtor_ids),
        valid_debtor_count=len(valid_ids),
        invalid_debtor_ids=invalid_debtor_ids,
        queued_count=len(monit_ids),
        monit_ids=monit_ids,
        task_id=task_id,
        scheduled_at=request.scheduled_at.isoformat() if request.scheduled_at else None,
    )


# ===========================================================================
# Historia i statystyki
# ===========================================================================

async def get_history(
    db,                             # AsyncSession
    page: int = 1,
    page_size: int = 50,
    debtor_id=None,                 # Optional[int]
    user_id=None,                   # Optional[int]
    monit_type=None,                # Optional[str]
    status=None,                    # Optional[str]
) -> dict:
    """
    Pobiera paginowaną listę monitów z opcjonalnym wzbogaceniem o nazwę kontrahenta.

    Architektura dwuetapowa:
        1. SQLAlchemy (dbo_ext): pobierz stronę MonitHistory wg filtrów
        2. WAPRO (pyodbc, batch): pobierz NazwaKontrahenta dla unikalnych ID
           Jeden SELECT ... WHERE IN (...) — zero N+1 queries.

    Graceful degradation:
        Jeśli WAPRO niedostępne → nazwa_kontrahenta = None w każdym monit.
        Błąd wzbogacenia NIE przerywa odpowiedzi.

    Args:
        db:         Sesja SQLAlchemy (AsyncSession).
        page:       Numer strony (1-based).
        page_size:  Rozmiar strony (max 200).
        debtor_id:  Filtr po ID kontrahenta (None = wszystkie).
        user_id:    Filtr po ID operatora (None = wszyscy).
        monit_type: Filtr po typie: email | sms | print (None = wszystkie).
        status:     Filtr po statusie (None = wszystkie).

    Returns:
        Słownik z items[], total, page, page_size, total_pages.
        Każdy item zawiera pole "nazwa_kontrahenta" (str | None).
    """
    from sqlalchemy import and_, desc, func, select
    from app.db.models.monit_history import MonitHistory
    from app.db.wapro import get_kontrahent_names_batch

    page      = max(page, 1)
    page_size = min(max(page_size, 1), _MAX_PAGE_SIZE)

    # ── Krok 1: Buduj warunki WHERE ─────────────────────────────────────────
    conditions = [MonitHistory.is_active == True]  # noqa: E712

    if debtor_id is not None:
        conditions.append(MonitHistory.id_kontrahenta == debtor_id)
    if user_id is not None:
        conditions.append(MonitHistory.id_user == user_id)
    if monit_type is not None:
        mt = monit_type.strip().lower()
        if mt in _VALID_MONIT_TYPES:
            conditions.append(MonitHistory.monit_type == mt)
    if status is not None:
        st = status.strip().lower()
        if st in _VALID_STATUSES:
            conditions.append(MonitHistory.status == st)

    where = and_(*conditions)

    # ── Krok 2: COUNT ────────────────────────────────────────────────────────
    count_result = await db.execute(
        select(func.count(MonitHistory.id_monit)).where(where)
    )
    total = count_result.scalar_one() or 0

    if total == 0:
        return {
            "items": [], "total": 0,
            "page": page, "page_size": page_size, "total_pages": 0,
        }

    # ── Krok 3: Pobierz stronę danych (SQLAlchemy) ───────────────────────────
    data_result = await db.execute(
        select(MonitHistory)
        .where(where)
        .order_by(desc(MonitHistory.created_at))
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    monits = data_result.scalars().all()
    total_pages = (total + page_size - 1) // page_size

    # ── Krok 4: Batch WAPRO — nazwy kontrahentów ────────────────────────────
    # Zbierz unikalne ID z aktualnej strony (max page_size wartości)
    unique_debtor_ids = list({m.id_kontrahenta for m in monits if m.id_kontrahenta})

    kontrahent_names: dict[int, str | None] = {}
    if unique_debtor_ids:
        try:
            kontrahent_names = await get_kontrahent_names_batch(unique_debtor_ids)
        except Exception as exc:
            # Nie przerywaj odpowiedzi — degradacja graceful
            logger.warning(
                "Nie udało się pobrać nazw kontrahentów dla listy monitów — degradacja",
                extra={
                    "ids_count": len(unique_debtor_ids),
                    "error":     str(exc),
                    "page":      page,
                }
            )

    logger.debug(
        "get_history: total=%d, page=%d/%d, enriched=%d kontrahentów",
        total, page, total_pages, len(kontrahent_names),
        extra={
            "total":         total,
            "page":          page,
            "total_pages":   total_pages,
            "returned":      len(monits),
            "enriched_ids":  len(kontrahent_names),
            "debtor_filter": debtor_id,
            "status_filter": status,
        }
    )

    # ── Krok 5: Buduj wyniki z wzbogaceniem ─────────────────────────────────
    items = []
    for m in monits:
        d = _monit_to_dict(m)
        # Dodaj nazwę kontrahenta — None jeśli WAPRO nie odpowiedział lub ID nieznane
        d["nazwa_kontrahenta"] = kontrahent_names.get(m.id_kontrahenta)
        items.append(d)

    return {
        "items":       items,
        "total":       total,
        "page":        page,
        "page_size":   page_size,
        "total_pages": total_pages,
    }

async def get_by_id(
    db: AsyncSession,
    monit_id: int,
) -> dict:
    """
    Pobiera szczegóły pojedynczego monitu.

    Args:
        db:       Sesja SQLAlchemy.
        monit_id: ID monitu (BIGINT).

    Returns:
        Słownik z danymi monitu.

    Raises:
        MonitNotFoundError: Monit nie istnieje.
    """
    result = await db.execute(
        select(MonitHistory).where(
            and_(MonitHistory.id_monit == monit_id, MonitHistory.is_active == True)  # noqa: E712
        )
    )
    monit = result.scalar_one_or_none()
    if monit is None:
        raise MonitNotFoundError(f"Monit ID={monit_id} nie istnieje.")
    return _monit_to_dict(monit)


async def get_stats(
    db: AsyncSession,
    debtor_id: Optional[int] = None,
    user_id: Optional[int] = None,
    period: str = "month",            # ← DODANE
) -> dict:
    """
    Pobiera statystyki monitów (agregaty per status i per typ).
    Używana przez dashboard i widok dłużnika.

    Args:
        db:        Sesja SQLAlchemy.
        debtor_id: Filtr po dłużniku (None = wszystkie).
        user_id:   Filtr po użytkowniku (None = wszyscy).
        period:    Zakres czasowy: "week" | "month" | "year"
                   None lub inny string = brak filtra (wszystkie rekordy).

    Returns:
        Słownik ze statystykami:
        {
            "total": int,
            "by_status": {status: count},
            "by_type":   {type: count},
            "total_cost": float,
            "last_sent_at": ISO str | None,
            "period": str,
            "period_from": ISO str | None,
        }
    """
    from datetime import timedelta

    now = datetime.now(timezone.utc)

    # Oblicz datę graniczną dla okresu
    _PERIOD_MAP: dict[str, timedelta] = {
        "week":  timedelta(days=7),
        "month": timedelta(days=30),
        "year":  timedelta(days=365),
    }
    period_delta = _PERIOD_MAP.get(period)
    period_from = (now - period_delta) if period_delta else None

    conditions = [MonitHistory.is_active == True]  # noqa: E712
    if debtor_id is not None:
        conditions.append(MonitHistory.id_kontrahenta == debtor_id)
    if user_id is not None:
        conditions.append(MonitHistory.id_user == user_id)
    if period_from is not None:
        conditions.append(MonitHistory.created_at >= period_from)  # ← DODANE
    where = and_(*conditions)

    result = await db.execute(
        select(
            func.count(MonitHistory.id_monit).label("total"),
            func.sum(MonitHistory.cost).label("total_cost"),
            func.max(MonitHistory.sent_at).label("last_sent_at"),
            # By status
            func.sum(case((MonitHistory.status == "pending", 1), else_=0)).label("cnt_pending"),
            func.sum(case((MonitHistory.status == "sent", 1), else_=0)).label("cnt_sent"),
            func.sum(case((MonitHistory.status == "delivered", 1), else_=0)).label("cnt_delivered"),
            func.sum(case((MonitHistory.status == "bounced", 1), else_=0)).label("cnt_bounced"),
            func.sum(case((MonitHistory.status == "failed", 1), else_=0)).label("cnt_failed"),
            func.sum(case((MonitHistory.status == "opened", 1), else_=0)).label("cnt_opened"),
            func.sum(case((MonitHistory.status == "clicked", 1), else_=0)).label("cnt_clicked"),
            # By type
            func.sum(case((MonitHistory.monit_type == "email", 1), else_=0)).label("cnt_email"),
            func.sum(case((MonitHistory.monit_type == "sms", 1), else_=0)).label("cnt_sms"),
            func.sum(case((MonitHistory.monit_type == "print", 1), else_=0)).label("cnt_print"),
        ).where(where)
    )
    row = result.one()

    last_sent = None
    if row.last_sent_at and hasattr(row.last_sent_at, "isoformat"):
        last_sent = row.last_sent_at.isoformat()

    total_cost = float(row.total_cost) if row.total_cost else 0.0

    return {
        "total":        row.total or 0,
        "total_cost":   total_cost,
        "last_sent_at": last_sent,
        "period":       period,                                          # ← DODANE
        "period_from":  period_from.isoformat() if period_from else None,  # ← DODANE
        "by_status": {
            "pending":   row.cnt_pending   or 0,
            "sent":      row.cnt_sent      or 0,
            "delivered": row.cnt_delivered or 0,
            "bounced":   row.cnt_bounced   or 0,
            "failed":    row.cnt_failed    or 0,
            "opened":    row.cnt_opened    or 0,
            "clicked":   row.cnt_clicked   or 0,
        },
        "by_type": {
            "email": row.cnt_email or 0,
            "sms":   row.cnt_sms   or 0,
            "print": row.cnt_print or 0,
        },
    }


# ===========================================================================
# Aktualizacja statusu (webhook callback)
# ===========================================================================

async def update_status(
    db: AsyncSession,
    redis: Redis,
    monit_id: int,
    new_status: str,
    external_id: Optional[str] = None,
    extra_data: Optional[dict] = None,
) -> StatusUpdateResult:
    """
    Aktualizuje status monitu — wywoływana przez webhook od bramki.

    Idempotentna: ten sam status → brak zmiany → success=True, updated=False.
    Waliduje state machine: niedozwolone przejście → MonitStatusTransitionError.

    Aktualizuje odpowiednie timestamp:
        sent       → SentAt
        delivered  → DeliveredAt
        opened     → OpenedAt
        clicked    → ClickedAt

    Publikuje SSE event: monit_status_changed.

    Args:
        db:          Sesja SQLAlchemy.
        redis:       Klient Redis.
        monit_id:    ID monitu.
        new_status:  Nowy status z bramki.
        external_id: ID z zewnętrznego systemu (bramka email/sms).
        extra_data:  Dodatkowe dane z webhook (np. bounce reason).

    Returns:
        StatusUpdateResult.

    Raises:
        MonitNotFoundError:           Monit nie istnieje.
        MonitValidationError:         Nieprawidłowy status.
        MonitStatusTransitionError:   Niedozwolone przejście statusu.
    """
    new_status = new_status.strip().lower()
    if new_status not in _VALID_STATUSES:
        raise MonitValidationError(
            f"Nieprawidłowy status: {new_status!r}. Dozwolone: {sorted(_VALID_STATUSES)}"
        )

    result = await db.execute(
        select(MonitHistory).where(
            and_(MonitHistory.id_monit == monit_id, MonitHistory.is_active == True)  # noqa: E712
        )
    )
    monit = result.scalar_one_or_none()
    if monit is None:
        raise MonitNotFoundError(f"Monit ID={monit_id} nie istnieje.")

    old_status = monit.status

    # Idempotent
    if old_status == new_status:
        return StatusUpdateResult(
            monit_id=monit_id,
            old_status=old_status,
            new_status=new_status,
            updated=False,
            message=f"Status monitu #{monit_id} jest już '{new_status}' — brak zmiany.",
        )

    # Walidacja state machine
    if not _is_valid_status_transition(old_status, new_status):
        raise MonitStatusTransitionError(monit_id, old_status, new_status)

    # Aktualizacja
    now = datetime.now(timezone.utc)
    monit.status     = new_status
    monit.updated_at = now

    if external_id:
        monit.external_id = external_id

    # Timestampy per status
    if new_status == "sent" and monit.sent_at is None:
        monit.sent_at = now
    elif new_status == "delivered" and monit.delivered_at is None:
        monit.delivered_at = now
    elif new_status == "opened" and monit.opened_at is None:
        monit.opened_at = now
    elif new_status == "clicked" and monit.clicked_at is None:
        monit.clicked_at = now

    if extra_data and new_status in {"failed", "bounced"}:
        error_msg = extra_data.get("error") or extra_data.get("reason") or ""
        monit.error_message = str(error_msg)[:500]

    await db.flush()

    logger.info(
        "Status monitu zaktualizowany",
        extra={
            "monit_id": monit_id,
            "old_status": old_status,
            "new_status": new_status,
            "external_id": external_id,
            "debtor_id": monit.id_kontrahenta,
        }
    )

    _append_to_file(
        _get_monits_log_file(),
        _build_log_record(
            action="monit_status_updated",
            monit_id=monit_id,
            old_status=old_status,
            new_status=new_status,
            external_id=external_id,
            debtor_id=monit.id_kontrahenta,
        )
    )

    audit_service.log_crud(
        db=db,
        action="monit_status_updated",
        entity_type="Monit",
        entity_id=monit_id,
        old_value={"status": old_status},
        new_value={"status": new_status, "external_id": external_id},
        success=True,
    )

    # SSE event
    try:
        await event_service.publish_monit_status_changed(
            redis=redis,
            monit_id=monit_id,
            debtor_id=monit.id_kontrahenta,
            old_status=old_status,
            new_status=new_status,
            monit_type=monit.monit_type,
            target_user_id=monit.id_user,
        )
        # Invalidacja cache dłużnika (statystyki się zmieniły)
        await debtor_service.invalidate_debtor_cache(redis, monit.id_kontrahenta)
    except Exception as exc:
        logger.warning("Błąd post-update hooks", extra={"error": str(exc)})

    return StatusUpdateResult(
        monit_id=monit_id,
        old_status=old_status,
        new_status=new_status,
        updated=True,
        message=f"Status monitu #{monit_id} zmieniony: '{old_status}' → '{new_status}'",
    )


# ===========================================================================
# Retry (ponowna próba wysyłki)
# ===========================================================================

async def retry(
    db: AsyncSession,
    redis: Redis,
    monit_id: int,
    triggered_by_user_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Kolejkuje ponowną próbę wysyłki monitu.

    Dozwolone tylko dla monitów ze statusem "failed".
    Limit: max _MAX_RETRY_COUNT prób.

    Przepływ:
        1. Walidacja status == "failed" i retry_count < max
        2. Reset status → "pending", increment retry_count
        3. Enqueue do ARQ
        4. AuditLog + SSE

    Args:
        db:                    Sesja SQLAlchemy.
        redis:                 Klient Redis.
        monit_id:              ID monitu do ponowienia.
        triggered_by_user_id:  ID użytkownika zlecającego retry.
        ip_address:            IP inicjatora.

    Returns:
        Słownik z wynikiem operacji.

    Raises:
        MonitNotFoundError:  Monit nie istnieje.
        MonitRetryError:     Brak możliwości retry (zły status lub limit prób).
    """
    result = await db.execute(
        select(MonitHistory).where(
            and_(MonitHistory.id_monit == monit_id, MonitHistory.is_active == True)  # noqa: E712
        )
    )
    monit = result.scalar_one_or_none()
    if monit is None:
        raise MonitNotFoundError(f"Monit ID={monit_id} nie istnieje.")

    if monit.status != "failed":
        raise MonitRetryError(
            f"Monit #{monit_id} ma status '{monit.status}'. "
            f"Retry możliwe tylko dla statusu 'failed'."
        )

    current_retry = monit.retry_count or 0
    if current_retry >= _MAX_RETRY_COUNT:
        raise MonitRetryError(
            f"Monit #{monit_id} osiągnął limit prób ({_MAX_RETRY_COUNT}). "
            f"Retry nie jest możliwe."
        )

    # Reset do pending
    now = datetime.now(timezone.utc)
    monit.status      = "pending"
    monit.retry_count = current_retry + 1
    monit.error_message = None
    monit.updated_at  = now
    await db.flush()
    await db.commit()

    # Enqueue

    _task_map = {"email": "send_bulk_emails", "sms": "send_bulk_sms", "print": "generate_pdf_task"}
    task_name = _task_map.get(monit.monit_type, f"send_{monit.monit_type}_task")
    
    arq_payload = {
        "monit_ids":            [monit_id],
        "triggered_by_user_id": triggered_by_user_id,
    }
    try:
        task_id = await _enqueue_to_arq(redis, task_name, arq_payload)
    except MonitError as exc:
        task_id = None
        logger.error("Kolejkowanie retry nie powiodło się", extra={"error": str(exc)})

    logger.info(
        "Retry monitu zlecony",
        extra={
            "monit_id": monit_id,
            "retry_number": monit.retry_count,
            "triggered_by": triggered_by_user_id,
            "task_id": task_id,
        }
    )

    _append_to_file(
        _get_monits_log_file(),
        _build_log_record(
            action="monit_retry",
            monit_id=monit_id,
            retry_number=monit.retry_count,
            triggered_by=triggered_by_user_id,
            task_id=task_id,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="monit_retry",
        entity_type="Monit",
        entity_id=monit_id,
        details={
            "retry_number": monit.retry_count,
            "monit_type": monit.monit_type,
            "debtor_id": monit.id_kontrahenta,
            "triggered_by": triggered_by_user_id,
            "task_id": task_id,
        },
        success=True,
    )

    return {
        "monit_id":     monit_id,
        "retry_number": monit.retry_count,
        "task_id":      task_id,
        "new_status":   "pending",
        "message":      f"Monit #{monit_id} został ponownie zlecony do wysyłki (próba {monit.retry_count}/{_MAX_RETRY_COUNT}).",
    }


async def cancel(
    db: AsyncSession,
    monit_id: int,
    cancelled_by_user_id: int,
    reason: Optional[str] = None,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Anuluje zaplanowany monit (status: pending → failed).

    Możliwe tylko gdy status == "pending".

    Args:
        db:                     Sesja SQLAlchemy.
        monit_id:               ID monitu do anulowania.
        cancelled_by_user_id:   ID użytkownika.
        reason:                 Powód anulowania.
        ip_address:             IP inicjatora.

    Returns:
        Słownik z potwierdzeniem.

    Raises:
        MonitNotFoundError:  Monit nie istnieje.
        MonitRetryError:     Monit nie jest w statusie pending.
    """
    result = await db.execute(
        select(MonitHistory).where(
            and_(MonitHistory.id_monit == monit_id, MonitHistory.is_active == True)  # noqa: E712
        )
    )
    monit = result.scalar_one_or_none()
    if monit is None:
        raise MonitNotFoundError(f"Monit ID={monit_id} nie istnieje.")

    if monit.status != "pending":
        raise MonitRetryError(
            f"Monit #{monit_id} ma status '{monit.status}'. "
            f"Anulowanie możliwe tylko dla statusu 'pending'."
        )

    monit.status        = "failed"
    monit.error_message = f"Anulowany przez użytkownika: {reason or 'brak powodu'}"[:500]
    monit.updated_at    = datetime.now(timezone.utc)
    await db.flush()

    logger.info(
        "Monit anulowany",
        extra={
            "monit_id": monit_id,
            "cancelled_by": cancelled_by_user_id,
            "reason": reason,
        }
    )

    audit_service.log_crud(
        db=db,
        action="monit_cancelled",
        entity_type="Monit",
        entity_id=monit_id,
        old_value={"status": "pending"},
        new_value={"status": "failed"},
        details={"reason": reason, "cancelled_by": cancelled_by_user_id},
        success=True,
    )

    return {
        "monit_id": monit_id,
        "new_status": "failed",
        "message": f"Monit #{monit_id} został anulowany.",
        "reason": reason,
    }
async def get_queue_status(
    redis: Redis,
    db: AsyncSession,
) -> dict:
    """
    Zwraca status kolejki ARQ oraz podsumowanie monitów wg statusu.

    Źródła danych:
        - Redis: klucze ARQ (arq:queue:default) — liczba zadań w kolejce
        - DB:    agregaty MonitHistory wg statusu (pending / failed / sent)

    Returns:
        {
            "arq": {"queued": int, "worker_online": bool},
            "db_summary": {"pending": int, "failed": int, "sent_today": int},
            "checked_at": ISO str,
        }
    """
    from datetime import date

    now = datetime.now(timezone.utc)

    # ── 1. Redis — stan kolejki ARQ ──────────────────────────────────────────
    arq_queued = 0
    worker_online = False
    try:
        # ARQ trzyma zadania jako ZSET pod kluczem arq:queue:default
        arq_queued_raw = await redis.zcard("arq:queue:default")
        arq_queued = int(arq_queued_raw or 0)

        # Heartbeat workera — ARQ zapisuje: arq:health-check
        hc = await redis.get("arq:health-check")
        worker_online = hc is not None
    except Exception as exc:
        logger.warning(
            "Błąd odczytu kolejki ARQ z Redis",
            extra={"error": str(exc)},
        )

    # ── 2. DB — agregaty MonitHistory ────────────────────────────────────────
    today_start = datetime.combine(date.today(), datetime.min.time()).replace(
        tzinfo=timezone.utc
    )

    try:
        agg_result = await db.execute(
            select(
                func.sum(
                    case((MonitHistory.status == "pending", 1), else_=0)
                ).label("cnt_pending"),
                func.sum(
                    case((MonitHistory.status == "failed", 1), else_=0)
                ).label("cnt_failed"),
                func.sum(
                    case(
                        (
                            and_(
                                MonitHistory.status == "sent",
                                MonitHistory.sent_at >= today_start,
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("cnt_sent_today"),
            ).where(MonitHistory.is_active == True)  # noqa: E712
        )
        row = agg_result.one()
        db_summary = {
            "pending":    int(row.cnt_pending    or 0),
            "failed":     int(row.cnt_failed     or 0),
            "sent_today": int(row.cnt_sent_today or 0),
        }
    except Exception as exc:
        logger.error(
            "Błąd agregacji MonitHistory dla queue status",
            extra={"error": str(exc)},
        )
        db_summary = {"pending": 0, "failed": 0, "sent_today": 0}

    result = {
        "arq": {
            "queued":        arq_queued,
            "worker_online": worker_online,
            "note":          "Worker ARQ nie jest jeszcze uruchomiony ",
        },
        "db_summary": db_summary,
        "checked_at":  now.isoformat(),
    }

    logger.debug(
        "Queue status sprawdzony",
        extra={
            "arq_queued":     arq_queued,
            "worker_online":  worker_online,
            "db_pending":     db_summary["pending"],
        },
    )

    return result

async def get_pdf(
    db: AsyncSession,
    redis: Redis,
    monit_id: int,
) -> bytes:
    """
    Zwraca zawartość PDF dla monitu.

    Pobiera monit z DB, sprawdza PDFPath, odczytuje plik z dysku.

    Args:
        db:       Sesja SQLAlchemy.
        redis:    Klient Redis (reserved for future cache).
        monit_id: ID monitu.

    Returns:
        Zawartość pliku PDF jako bytes.

    Raises:
        MonitNotFoundError:    Monit nie istnieje.
        MonitValidationError:  Monit nie ma zapisanego PDF.
    """
    import os

    result = await db.execute(
        select(MonitHistory).where(
            and_(
                MonitHistory.id_monit == monit_id,
                MonitHistory.is_active == True,  # noqa: E712
            )
        )
    )
    monit = result.scalar_one_or_none()
    if monit is None:
        raise MonitNotFoundError(f"Monit ID={monit_id} nie istnieje.")

    if not monit.pdf_path:
        raise MonitValidationError(
            f"Monit ID={monit_id} nie ma zapisanego pliku PDF. "
            f"PDF jest generowany przez ARQ worker podczas wysyłki."
        )

    if not os.path.isfile(monit.pdf_path):
        logger.error(
            "PDFPath wskazuje na nieistniejący plik",
            extra={"monit_id": monit_id, "pdf_path": monit.pdf_path},
        )
        raise MonitValidationError(
            f"Plik PDF dla monitu ID={monit_id} nie istnieje na dysku. "
            f"Ścieżka: {monit.pdf_path}"
        )

    with open(monit.pdf_path, "rb") as f:
        return f.read()

    # ===========================================================================
# Podgląd PDF (GET /debtors/{id}/preview-pdf)
# ===========================================================================

async def generate_pdf_preview(
    db: AsyncSession,
    wapro: WaproConnectionPool,
    redis: Redis,
    debtor_id: int,
    template_id: int,
    channel: str = "email",
) -> bytes:
    """
    Generuje podgląd PDF monitu w pamięci (bez zapisu do MonitHistory).

    Pobiera dane dłużnika z WAPRO i szablon z dbo_ext.skw_Templates,
    generuje PDF przez ReportLab i zwraca jako bytes.

    Args:
        db:          Sesja SQLAlchemy.
        wapro:       Pula połączeń WAPRO.
        redis:       Klient Redis.
        debtor_id:   ID kontrahenta WAPRO.
        template_id: ID szablonu z skw_Templates.
        channel:     Kanał: email | sms | letter.

    Returns:
        Bajty PDF gotowe do StreamingResponse.

    Raises:
        MonitTemplateNotFoundError: Szablon nie istnieje.
        MonitDebtorNotFoundError:   Dłużnik nie istnieje w WAPRO.
    """
    from io import BytesIO
    from sqlalchemy import select as sa_select
    from app.core.config import get_settings
    settings = get_settings()

    # 1. Pobierz szablon z dbo_ext
    from app.db.models.monit_history import MonitHistory  # noqa (reuse session)
    from sqlalchemy import text

    tmpl_result = await db.execute(
        text(
            "SELECT ID_TEMPLATE, TemplateName, TemplateType, Subject, Body "
            "FROM dbo_ext.skw_Templates "
            "WHERE ID_TEMPLATE = :tid AND IsActive = 1"
        ),
        {"tid": template_id},
    )
    tmpl_row = tmpl_result.mappings().one_or_none()
    if tmpl_row is None:
        raise MonitTemplateNotFoundError(
            f"Szablon ID={template_id} nie istnieje lub jest nieaktywny."
        )

    # 2. Pobierz dłużnika z WAPRO (przez debtor_service cache)
    from app.services import debtor_service
    try:
        debtor_data = await debtor_service.get_by_id(
            wapro=wapro, db=db, redis=redis, debtor_id=debtor_id
        )
    except Exception as exc:
        raise MonitDebtorNotFoundError(
            f"Dłużnik ID={debtor_id} nie istnieje w WAPRO."
        ) from exc

    debtor = debtor_data.get("debtor", {})

    # 3. Generuj PDF przez ReportLab
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
    except ImportError as exc:
        raise RuntimeError(
            "ReportLab nie jest zainstalowany. "
            "Dodaj 'reportlab' do requirements.txt i przebuduj kontener."
        ) from exc

    # Rejestracja fontów DejaVu z obsługą polskich znaków
    _DEJAVU_DIR = "/usr/share/fonts/truetype/dejavu"
    try:
        pdfmetrics.registerFont(TTFont("DejaVu", f"{_DEJAVU_DIR}/DejaVuSans.ttf"))
        pdfmetrics.registerFont(TTFont("DejaVu-Bold", f"{_DEJAVU_DIR}/DejaVuSans-Bold.ttf"))
        pdfmetrics.registerFontFamily("DejaVu", normal="DejaVu", bold="DejaVu-Bold")
    except Exception as font_exc:
        logger.warning(
            "Nie można załadować fontu DejaVu — polskie znaki mogą nie działać",
            extra={"error": str(font_exc), "dejavu_dir": _DEJAVU_DIR},
        )

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=2 * cm,
        leftMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )

    # Style z fontem DejaVu (Unicode — pełna obsługa polskich znaków)
    base_styles = getSampleStyleSheet()
    style_normal = ParagraphStyle(
        "DejaVuNormal",
        parent=base_styles["Normal"],
        fontName="DejaVu",
        fontSize=10,
        leading=14,
    )
    style_title = ParagraphStyle(
        "DejaVuTitle",
        parent=base_styles["Title"],
        fontName="DejaVu-Bold",
        fontSize=14,
        leading=18,
    )
    style_italic = ParagraphStyle(
        "DejaVuItalic",
        parent=base_styles["Italic"],
        fontName="DejaVu",
        fontSize=9,
        leading=13,
    )

    # Inicjalizacja rendered_subject PRZED story — fallback na surowy Subject
    # jeśli Jinja2 jeszcze nie renderował (Python wymaga przypisania przed użyciem)
    rendered_subject = tmpl_row.get("Subject") or ""

    story = []
    # Nagłówek — nazwa firmy dłużnika
    nazwa = debtor.get("NazwaKontrahenta") or debtor.get("nazwa_kontrahenta") or f"ID {debtor_id}"
    story.append(Paragraph(f"<b>Podgląd monitu — {nazwa}</b>", style_title))
    story.append(Spacer(1, 0.5 * cm))
    # Metadane — temat renderowany po Jinja2, wstawiony niżej
    story.append(Paragraph(f"Szablon: {tmpl_row['TemplateName']}", style_normal))
    story.append(Paragraph(f"Kanał: {channel}", style_normal))
    story.append(Spacer(1, 0.5 * cm))

    # ── Krok 2.5: Pobierz numery faktur z WAPRO dla invoice_list ─────────────
    # Klucz "invoice_numbers" NIE istnieje w dict z skw_kontrahenci —
    # trzeba osobno odpytać skw_rozrachunki_faktur.
    invoice_list_str = "—"
    try:
        from app.db.wapro import get_invoices_for_debtor, InvoiceFilterParams
        _inv_params = InvoiceFilterParams(
            kontrahent_id=debtor_id,
            include_paid=False,
            limit=20,
            offset=0,
        )
        _inv_result = await get_invoices_for_debtor(_inv_params)
        if _inv_result.rows:
            _numbers = [
                str(row.get("NumerFaktury") or row.get("NR_DOK") or "").strip()
                for row in _inv_result.rows
                if row.get("NumerFaktury") or row.get("NR_DOK")
            ]
            if _numbers:
                invoice_list_str = ", ".join(_numbers)
        logger.debug(
            "Faktury pobrane dla podglądu PDF",
            extra={
                "debtor_id": debtor_id,
                "invoice_count": len(_inv_result.rows),
                "invoice_list_str": invoice_list_str,
            },
        )
    except Exception as _inv_exc:
        import traceback as _tb_inv
        logger.warning(
            "Nie udało się pobrać faktur dla podglądu PDF — używam myślnika",
            extra={
                "debtor_id": debtor_id,
                "error": str(_inv_exc),
                "traceback": _tb_inv.format_exc(),
            },
        )

    # ── Krok 3: Zbuduj kontekst Jinja2 i renderuj ────────────────────────────
    body_text = tmpl_row.get("Body") or "(brak treści szablonu)"
    rendered_subject = tmpl_row.get("Subject") or ""  # inicjalizacja przed try — fallback na surowy Subject
    rendered_subject = tmpl_row.get("Subject") or ""

    _jinja_context = {
        "debtor_name":  debtor.get("NazwaKontrahenta") or debtor.get("nazwa_kontrahenta") or "",
        "total_debt":   f"{float(debtor.get('SumaDlugu') or 0):.2f}",
        "invoice_list": invoice_list_str,
        "due_date":     _calc_preview_deadline(),
        "company_name": settings.COMPANY_NAME,
    }

    logger.debug(
        "Jinja2 render context",
        extra={
            "debtor_id":   debtor_id,
            "template_id": template_id,
            "debtor_keys": list(debtor.keys()),
            "context": {
                "debtor_name":  _jinja_context["debtor_name"],
                "total_debt":   _jinja_context["total_debt"],
                "invoice_list": _jinja_context["invoice_list"],
                "due_date":     _jinja_context["due_date"],
                "company_name": _jinja_context["company_name"],
            },
        },
    )

    try:
        from jinja2 import Environment, BaseLoader, Undefined

        class _SilentUndefined(Undefined):
            """Pusta wartość dla niezdefiniowanych zmiennych — nie rzuca wyjątku."""
            def __str__(self) -> str:
                return ""
            def __iter__(self):
                return iter([])
            def __bool__(self) -> bool:
                return False

        _env = Environment(loader=BaseLoader(), undefined=_SilentUndefined)

        # Renderuj body
        body_text = _env.from_string(body_text).render(**_jinja_context)

        # Renderuj subject (BUG FIX #2: poprzednio _subj był obliczany ale nigdy
        # trafiał do PDF — zamiast niego szło surowe tmpl_row['Subject'])
        if tmpl_row.get("Subject"):
            rendered_subject = _env.from_string(tmpl_row["Subject"]).render(**_jinja_context)

        logger.info(
            "Jinja2 render OK",
            extra={
                "debtor_id":         debtor_id,
                "template_id":       template_id,
                "body_len_before":   len(tmpl_row.get("Body") or ""),
                "body_len_after":    len(body_text),
                "subject_rendered":  rendered_subject[:120] if rendered_subject else None,
                "context_keys":      list(_jinja_context.keys()),
            },
        )

    except Exception as _jinja_exc:
        import traceback as _tb_j
        logger.error(
            "Błąd Jinja2 w podglądzie PDF — używam surowej treści bez podstawień",
            extra={
                "debtor_id":    debtor_id,
                "template_id":  template_id,
                "error":        str(_jinja_exc),
                "error_type":   type(_jinja_exc).__name__,
                "traceback":    _tb_j.format_exc(),
                "context_keys": list(_jinja_context.keys()),
            },
        )
        # NIE przerywamy — surowa treść trafi do PDF bez podstawień

    # Wstaw Temat PO renderowaniu Jinja2 — rendered_subject ma już podstawione zmienne
    if rendered_subject:
        story.append(Paragraph(f"Temat: {rendered_subject}", style_normal))
        story.append(Spacer(1, 0.2 * cm))

    # Usuń pełny HTML — ReportLab obsługuje tylko prosty tekst + <b><i><br/>
    body_text = _strip_html_for_reportlab(body_text)
    story.append(Paragraph(body_text, style_normal))

    story.append(Spacer(1, 1 * cm))
    story.append(Paragraph(
        "<i>Ten dokument to podgląd — nie został zapisany w historii monitów.</i>",
        style_italic,
    ))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()

    logger.info(
        "Wygenerowano podgląd PDF monitu",
        extra={
            "debtor_id": debtor_id,
            "template_id": template_id,
            "channel": channel,
            "pdf_size_bytes": len(pdf_bytes),
        },
    )

    return pdf_bytes

def _calc_preview_deadline(days: int = 7) -> str:
    """Termin płatności dla podglądu PDF (dziś + N dni)."""
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    dt = datetime.now(ZoneInfo("Europe/Warsaw")) + timedelta(days=days)
    return dt.strftime("%d.%m.%Y")

def _strip_html_for_reportlab(html: str) -> str:
    """
    Konwertuje HTML na tekst akceptowany przez ReportLab Paragraph.
    ReportLab obsługuje tylko: <b>, <i>, <u>, <br/>, <para> — reszta musi być usunięta.
    """
    import re

    # Zamień blokowe tagi na nowe linie
    html = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"</p>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"</tr>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"</div>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"</h[1-6]>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"<li[^>]*>", "• ", html, flags=re.IGNORECASE)

    # Zachowaj <b> i <i> — ReportLab je obsługuje
    html = re.sub(r"<strong[^>]*>", "<b>", html, flags=re.IGNORECASE)
    html = re.sub(r"</strong>", "</b>", html, flags=re.IGNORECASE)
    html = re.sub(r"<em[^>]*>", "<i>", html, flags=re.IGNORECASE)
    html = re.sub(r"</em>", "</i>", html, flags=re.IGNORECASE)

    # Usuń wszystkie pozostałe tagi HTML
    html = re.sub(r"<(?!b>|/b>|i>|/i>|u>|/u>)[^>]+>", "", html)

    # Zamień wielokrotne puste linie na jedną
    html = re.sub(r"\n{3,}", "\n\n", html)

    # Zamień newline na <br/> dla ReportLab
    html = html.replace("\n", "<br/>")

    # Usuń nadmiarowe spacje
    html = re.sub(r"[ \t]+", " ", html).strip()

    return html or "(brak treści szablonu)"