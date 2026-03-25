# Schematy Pydantic v2 dla Server-Sent Events (SSE).
#
# TYPY EVENTÓW (wg USTALENIA_PROJEKTU §13):
#   task_completed         — ARQ worker kończy task (email/SMS/PDF/snapshot)
#   permissions_updated    — Admin zmienia rolę użytkownika
#   new_invoices           — Synchronizacja WAPRO wykryła nowe faktury
#   debtor_updated         — Zmiana danych dłużnika
#   system_notification    — Ogólne powiadomienie (schema tamper, błędy krytyczne)
#
# MECHANIZM:
#   - Redis Pub/Sub: kanał per user `user:{user_id}`
#   - Endpoint: GET /api/v1/events/stream (sse-starlette EventSourceResponse)
#   - Każdy event zapisywany do logs/events_YYYY-MM-DD.jsonl (pliki nieusuwalne)
#   - Format zapisu: JSON Lines (jeden event = jedna linia JSON)
#
# UŻYCIE:
#   W event_service.py:
#     await publish_user_event(
#         user_id=1,
#         event=TaskCompletedEvent(task="send_emails", success=10, failed=2)
#     )
#
#   Frontend (EventSource):
#     const eventSource = new EventSource('/api/v1/events/stream');
#     eventSource.addEventListener('task_completed', (e) => {
#       const data = JSON.parse(e.data);
#       console.log(`Task ${data.task}: ${data.success} sukces, ${data.failed} błędów`);
#     });
#
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS
# =============================================================================

class NotificationLevel(str, Enum):
    """
    Poziom ważności powiadomienia systemowego.

    Używany w SystemNotificationEvent.level — określa jak frontend
    ma wyświetlić powiadomienie (kolor, dźwięk, czy blokować UI).

    INFO     — informacja neutralna (snapshot wykonany, config zmieniony)
    WARN     — ostrzeżenie — wymaga uwagi, ale system działa (degraded performance)
    CRITICAL — błąd krytyczny — system może nie działać poprawnie
               (schema tamper detected, baza niedostępna)
    """

    INFO = "info"
    WARN = "warn"
    CRITICAL = "critical"

    @property
    def is_critical(self) -> bool:
        """Czy powiadomienie wymaga natychmiastowej reakcji operatora."""
        return self == NotificationLevel.CRITICAL


# =============================================================================
# BAZOWY EVENT — wszystkie eventy dziedziczą po SSEEvent
# =============================================================================

class SSEEvent(BaseModel):
    """
    Bazowa klasa dla wszystkich SSE eventów.

    Wszystkie konkretne eventy (TaskCompletedEvent, PermissionsUpdatedEvent itp.)
    dziedziczą po SSEEvent i dodają swoje pola `data`.

    Wspólne pola:
        type       — typ eventu (task_completed, permissions_updated itp.)
        timestamp  — kiedy event został wygenerowany (UTC)
        user_id    — dla kogo event jest przeznaczony (opcjonalne dla broadcast)

    Format JSON w SSE stream (sse-starlette):
        event: task_completed
        data: {"task": "send_emails", "success": 10, "failed": 2, "message": "..."}

    Format JSON w pliku logs/events_YYYY-MM-DD.jsonl:
        {"type": "task_completed", "timestamp": "...", "user_id": 1,
         "data": {"task": "send_emails", "success": 10, ...}}
    """

    model_config = ConfigDict(
        populate_by_name=True,
        # Używamy orjson dla szybkiej serializacji (event_service.py)
        # SSE wymaga dużej przepustowości przy wielu klientach
    )

    type: str = Field(
        description=(
            "Typ eventu — używany jako 'event:' w SSE stream. "
            "Wartości: task_completed, permissions_updated, new_invoices, "
            "debtor_updated, system_notification"
        ),
        examples=["task_completed", "permissions_updated"],
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Czas wygenerowania eventu (UTC).",
    )
    user_id: int | None = Field(
        default=None,
        description=(
            "ID użytkownika dla którego event jest przeznaczony. "
            "None = broadcast do wszystkich użytkowników (rzadko używane)."
        ),
        ge=1,
    )

    def to_sse_dict(self) -> dict[str, Any]:
        """
        Konwertuje event do formatu dict dla sse-starlette.

        Zwraca:
            {"event": "task_completed", "data": "{...json...}"}

        Użycie w event_service.py:
            yield event.to_sse_dict()
        """
        import orjson

        return {
            "event": self.type,
            "data": orjson.dumps(self.model_dump(mode="json", exclude={"type", "timestamp", "user_id"})),
        }

    def to_log_line(self) -> str:
        """
        Konwertuje event do JSON Lines (jedna linia).

        Użycie w event_service.py:
            with open(f"logs/events_{date}.jsonl", "a") as f:
                f.write(event.to_log_line() + "\n")
        """
        import orjson

        return orjson.dumps(self.model_dump(mode="json")).decode("utf-8")


# =============================================================================
# TASK COMPLETED EVENT — worker ARQ kończy task
# =============================================================================

class TaskCompletedEvent(SSEEvent):
    """
    Event: ARQ worker zakończył task (email/SMS/PDF/snapshot).

    Kiedy: Po zakończeniu każdego z tasków:
        - send_bulk_emails
        - send_bulk_sms
        - generate_pdf
        - daily_snapshot

    Użycie w worker/tasks/email.py:
        await publish_user_event(
            user_id=user_id,
            event=TaskCompletedEvent(
                task="send_emails",
                success=150,
                failed=5,
                message="Wysłano 150 emaili, 5 błędów (bounce/timeout)",
                user_id=user_id,
            )
        )

    Frontend wyświetla toast notification: "Wysyłka zakończona: 150 emaili wysłano"
    """

    type: Literal["task_completed"] = Field(
        default="task_completed",
        description="Stały typ eventu — automatycznie ustawiony",
    )
    task: str = Field(
        description=(
            "Nazwa taska który się zakończył. "
            "Wartości: send_emails, send_sms, generate_pdf, daily_snapshot"
        ),
        examples=["send_emails", "send_sms", "daily_snapshot"],
        min_length=1,
        max_length=100,
    )
    success: int = Field(
        description="Liczba pomyślnie przetworzonych elementów.",
        ge=0,
        examples=[150, 0],
    )
    failed: int = Field(
        description="Liczba nieudanych elementów (błędy, bounce, timeout).",
        ge=0,
        examples=[5, 0],
    )
    message: str = Field(
        description=(
            "Komunikat opisujący wynik — po polsku, czytelny dla użytkownika. "
            "Wyświetlany w toast notification."
        ),
        examples=[
            "Wysłano 150 emaili, 5 błędów (bounce)",
            "Snapshot dzienny zakończony — 12 tabel zarchiwizowanych",
        ],
        min_length=1,
        max_length=500,
    )
    details: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Opcjonalne dodatkowe dane (np. lista błędów, ścieżki plików). "
            "Nie wyświetlane w toast — dostępne w szczegółach."
        ),
    )

    @property
    def total(self) -> int:
        """Łączna liczba przetworzonych elementów (success + failed)."""
        return self.success + self.failed

    @property
    def success_rate(self) -> float:
        """Procent sukcesu (0.0 - 1.0). 0.0 jeśli total == 0."""
        if self.total == 0:
            return 0.0
        return self.success / self.total


# =============================================================================
# PERMISSIONS UPDATED EVENT — admin zmienił rolę użytkownika
# =============================================================================

class PermissionsUpdatedEvent(SSEEvent):
    """
    Event: Admin zmienił rolę użytkownika → uprawnienia się zmieniły.

    Kiedy: PUT /api/v1/users/{user_id}/role

    Frontend po otrzymaniu tego eventu:
        1. Pobiera nowe uprawnienia (GET /api/v1/auth/me)
        2. Odświeża menu / przyciski / dostępne sekcje
        3. Opcjonalnie: wyloguje użytkownika i wymusi ponowne logowanie

    Użycie w api/users.py:
        await change_user_role(user_id, new_role_id)
        await publish_user_event(
            user_id=user_id,
            event=PermissionsUpdatedEvent(
                role_id=new_role_id,
                role_name="Manager",
                changed_by=current_user.id,
                user_id=user_id,
            )
        )
    """

    type: Literal["permissions_updated"] = Field(
        default="permissions_updated",
        description="Stały typ eventu",
    )
    role_id: int = Field(
        description="ID nowej roli przypisanej do użytkownika.",
        ge=1,
        examples=[2, 3],
    )
    role_name: str | None = Field(
        default=None,
        description="Nazwa nowej roli (opcjonalne, dla czytelności logów).",
        examples=["Manager", "User", "ReadOnly"],
        max_length=50,
    )
    changed_by: int | None = Field(
        default=None,
        description="ID admina który dokonał zmiany (opcjonalne).",
        ge=1,
    )


# =============================================================================
# NEW INVOICES EVENT — synchronizacja WAPRO wykryła nowe faktury
# =============================================================================

class NewInvoicesEvent(SSEEvent):
    """
    Event: Synchronizacja WAPRO wykryła nowe faktury / dłużników.

    Kiedy:
        - Cron task (np. co 15 min) sprawdza skw_kontrahenci
        - Wykrywa nowych dłużników lub nowe faktury
        - Publikuje event do użytkowników którzy mają widok listy dłużników

    Frontend po otrzymaniu:
        - Odświeża listę dłużników (GET /api/v1/debtors)
        - Wyświetla toast: "Wykryto 3 nowe faktury"

    Użycie w worker/tasks/sync_wapro.py:
        new_count = sync_invoices_from_wapro()
        if new_count > 0:
            await broadcast_to_admins(
                event=NewInvoicesEvent(count=new_count)
            )
    """

    type: Literal["new_invoices"] = Field(
        default="new_invoices",
        description="Stały typ eventu",
    )
    count: int = Field(
        description="Liczba nowych faktur wykrytych w synchronizacji.",
        ge=1,
        examples=[3, 15],
    )
    sync_date: datetime | None = Field(
        default=None,
        description="Czas synchronizacji z WAPRO (UTC). None = bieżący czas.",
    )


# =============================================================================
# DEBTOR UPDATED EVENT — zmiana danych dłużnika
# =============================================================================

class DebtorUpdatedEvent(SSEEvent):
    """
    Event: Dane dłużnika się zmieniły (WAPRO lub komentarz).

    Kiedy:
        - Synchronizacja WAPRO wykryła zmianę salda dłużnika
        - Ktoś dodał komentarz do dłużnika (POST /debtors/{id}/comments)
        - Zmieniono status monitu (wysłano email/SMS)

    Frontend po otrzymaniu:
        - Jeśli user ma otwarty widok szczegółów tego dłużnika → odśwież
        - Jeśli user ma listę dłużników → odśwież wiersz dla tego dłużnika

    Użycie w api/comments.py:
        await create_comment(debtor_id, comment_text, user_id)
        await publish_user_event(
            user_id=user_id,
            event=DebtorUpdatedEvent(
                debtor_id=debtor_id,
                change_type="comment_added",
                user_id=user_id,
            )
        )
    """

    type: Literal["debtor_updated"] = Field(
        default="debtor_updated",
        description="Stały typ eventu",
    )
    debtor_id: int = Field(
        description="ID kontrahenta (ID_KONTRAHENTA z WAPRO).",
        ge=1,
    )
    change_type: str | None = Field(
        default=None,
        description=(
            "Typ zmiany dla szczegółowego logowania. "
            "Wartości: comment_added, balance_updated, monit_sent, data_changed"
        ),
        examples=["comment_added", "balance_updated", "monit_sent"],
        max_length=50,
    )


# =============================================================================
# SYSTEM NOTIFICATION EVENT — powiadomienia systemowe
# =============================================================================

class SystemNotificationEvent(SSEEvent):
    """
    Event: Ogólne powiadomienie systemowe — błędy krytyczne, ostrzeżenia.

    Kiedy:
        - Schema tamper detected (schema_integrity.verify() wykrył niezgodność checksumów)
        - Baza danych niedostępna (health check failed)
        - Redis down
        - Snapshot dzienny zakończony
        - Config zmieniony przez admina

    Level CRITICAL → frontend wyświetla modal blokujący pracę:
        "SYSTEM OFFLINE: Wykryto nieautoryzowaną zmianę w bazie danych. Skontaktuj się z administratorem."

    Level WARN → toast ostrzeżenia:
        "Redis odpowiada wolno (>100ms) — powiadomienia real-time mogą być opóźnione"

    Level INFO → toast informacyjny:
        "Snapshot dzienny zakończony — dane zarchiwizowane"

    Użycie w core/schema_integrity.py:
        if checksum_mismatch:
            await broadcast_to_admins(
                event=SystemNotificationEvent(
                    message="SCHEMA TAMPER DETECTED: widok skw_kontrahenci został zmieniony poza Alembic",
                    level=NotificationLevel.CRITICAL,
                    details={"object": "skw_kontrahenci", "expected": 12345, "actual": 67890},
                )
            )
            sys.exit(1)  # BLOCK
    """

    type: Literal["system_notification"] = Field(
        default="system_notification",
        description="Stały typ eventu",
    )
    message: str = Field(
        description=(
            "Komunikat powiadomienia — po polsku, czytelny dla użytkownika. "
            "Dla level=CRITICAL: opisuje problem i co zrobić."
        ),
        examples=[
            "Snapshot dzienny zakończony — dane zarchiwizowane",
            "CRITICAL: Wykryto nieautoryzowaną zmianę w bazie danych",
        ],
        min_length=1,
        max_length=1000,
    )
    level: NotificationLevel = Field(
        description=(
            "Poziom ważności powiadomienia. "
            "INFO = informacja. WARN = ostrzeżenie. CRITICAL = błąd krytyczny."
        ),
    )
    details: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Dodatkowe dane techniczne (dla adminów/logów). "
            "Przykład: {'object': 'skw_kontrahenci', 'expected_checksum': 12345}"
        ),
    )

    @field_validator("message")
    @classmethod
    def validate_critical_message(cls, v: str, info) -> str:
        """
        Komunikaty CRITICAL muszą zaczynać się od słowa CRITICAL dla czytelności logów.
        Frontend parsuje to i wyświetla w czerwonym modaliu.
        """
        # info.data dostępne w Pydantic v2 — zawiera pozostałe pola
        # Sprawdzamy poziom jeśli został już ustawiony
        if "level" in info.data and info.data["level"] == NotificationLevel.CRITICAL:
            if not v.upper().startswith("CRITICAL"):
                # Nie wymuszamy — tylko sugerujemy w docstringu
                # Można odkomentować jeśli chcesz wymuszać:
                # raise ValueError("Komunikaty CRITICAL powinny zaczynać się od słowa CRITICAL")
                pass
        return v


# =============================================================================
# UNION TYPE — wszystkie możliwe eventy
# =============================================================================

# Type alias dla event_service.py — akceptuje dowolny konkretny event
AnySSEEvent = (
    TaskCompletedEvent
    | PermissionsUpdatedEvent
    | NewInvoicesEvent
    | DebtorUpdatedEvent
    | SystemNotificationEvent
)