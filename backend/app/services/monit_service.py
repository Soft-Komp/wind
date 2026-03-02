"""
Serwis Monitów — System Windykacja
=====================================
Krok 16 / Faza 3 — services/monit_service.py

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
    │  ARQ Worker (Faza 6)                         │
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

Ścieżka docelowa: backend/app/services/monit_service.py
Autor: System Windykacja — Faza 3 Krok 16
Wersja: 1.0.0
Data: 2026-02-19
"""

from __future__ import annotations

import logging
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from backend.app import db
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
        debtor_ids:   Lista ID kontrahentów WAPRO.
        monit_type:   Kanał wysyłki: "email", "sms", "print".
        template_id:  ID szablonu (z tabeli Templates). None = brak szablonu (surowa treść).
        scheduled_at: Zaplanowany czas wysyłki. None = natychmiast.
        custom_subject: Override tematu emaila (opcjonalne).
    """
    debtor_ids:     list[int]
    monit_type:     str
    template_id:    Optional[int] = None
    scheduled_at:   Optional[datetime] = None
    custom_subject:  Optional[str] = None

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
    Kolejkuje task do ARQ worker.

    ARQ używa Redis LPUSH do listy arq:queue:default.
    Format payloadu: {"function": task_name, "args": [], "kwargs": {...}}

    Args:
        redis:        Klient Redis.
        task_name:    Nazwa funkcji workera (np. "send_email_task").
        task_payload: Argumenty dla workera.

    Returns:
        Wygenerowany task_id (UUID).

    Raises:
        MonitError: Gdy kolejkowanie się nie powiodło.
    """
    import uuid
    task_id = str(uuid.uuid4())
    arq_payload = {
        "function":  task_name,
        "args":      [],
        "kwargs":    task_payload,
        "task_id":   task_id,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        await redis.lpush(_ARQ_QUEUE_KEY, orjson.dumps(arq_payload))
        logger.info(
            "Task kolejkowany do ARQ",
            extra={
                "task_id": task_id,
                "task_name": task_name,
                "queue": _ARQ_QUEUE_KEY,
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
# Publiczne API serwisu — SEND
# ===========================================================================

async def send_bulk(
    db: AsyncSession,
    redis: Redis,
    wapro,                      # WaproConnectionPool — bez importu circular
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

    ⚠️  Faktyczna wysyłka następuje w ARQ worker (Faza 6).
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

    # Krok 2: Pobranie szablonu (opcjonalne)
    template = None
    template_subject = None
    if request.template_id is not None:
        template = await _get_template(db, request.template_id)
        template_subject = template.subject

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
        "monit_ids":     monit_ids,
        "debtor_ids":    valid_ids,
        "monit_type":    request.monit_type,
        "template_id":   request.template_id,
        "scheduled_at":  request.scheduled_at.isoformat() if request.scheduled_at else None,
        "triggered_by":  triggered_by_user_id,
    }

    task_name = f"send_{request.monit_type}_task"
    try:
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
    db: AsyncSession,
    debtor_id: Optional[int] = None,
    user_id: Optional[int] = None,
    monit_type: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = _DEFAULT_PAGE_SIZE,
) -> dict:
    """
    Pobiera paginowaną historię monitów.

    Obsługuje filtrowanie po dłużniku, użytkowniku, typie i statusie.
    Używana przez dwa endpointy:
        - GET /api/v1/debtors/{id}/monits → debtor_id podany
        - GET /api/v1/monits → brak filtrów (admin widzi wszystkie)

    Args:
        db:        Sesja SQLAlchemy.
        debtor_id: Filtr po ID kontrahenta (None = wszystkie).
        user_id:   Filtr po ID zlecającego (None = wszyscy).
        monit_type: Filtr po typie (None = wszystkie).
        status:    Filtr po statusie (None = wszystkie).
        page:      Numer strony.
        page_size: Rozmiar strony (max 200).

    Returns:
        Słownik z listą monitów i metadanymi paginacji.
    """
    page      = max(page, 1)
    page_size = min(max(page_size, 1), _MAX_PAGE_SIZE)

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

    count_result = await db.execute(
        select(func.count(MonitHistory.id_monit)).where(where)
    )
    total = count_result.scalar_one() or 0

    if total == 0:
        return {
            "items": [], "total": 0,
            "page": page, "page_size": page_size, "total_pages": 0,
        }

    data_result = await db.execute(
        select(MonitHistory)
        .where(where)
        .order_by(desc(MonitHistory.created_at))
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    monits = data_result.scalars().all()
    total_pages = (total + page_size - 1) // page_size

    return {
        "items":       [_monit_to_dict(m) for m in monits],
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
) -> dict:
    """
    Pobiera statystyki monitów (agregaty per status i per typ).

    Używana przez dashboard i widok dłużnika.

    Args:
        db:        Sesja SQLAlchemy.
        debtor_id: Filtr po dłużniku (None = wszystkie).
        user_id:   Filtr po użytkowniku (None = wszyscy).

    Returns:
        Słownik ze statystykami:
        {
            "total": int,
            "by_status": {status: count},
            "by_type":   {type: count},
            "total_cost": float,
            "last_sent_at": ISO str | None,
        }
    """
    conditions = [MonitHistory.is_active == True]  # noqa: E712
    if debtor_id is not None:
        conditions.append(MonitHistory.id_kontrahenta == debtor_id)
    if user_id is not None:
        conditions.append(MonitHistory.id_user == user_id)
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
        "total":       row.total or 0,
        "total_cost":  total_cost,
        "last_sent_at": last_sent,
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
    task_name = f"send_{monit.monit_type}_task"
    arq_payload = {
        "monit_ids":     [monit_id],
        "debtor_ids":    [monit.id_kontrahenta],
        "monit_type":    monit.monit_type,
        "template_id":   monit.template_id,
        "is_retry":      True,
        "retry_number":  monit.retry_count,
        "triggered_by":  triggered_by_user_id,
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