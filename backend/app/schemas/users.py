"""
Schematy Pydantic v2 dla modułu zarządzania użytkownikami.

Pokrywa wszystkie endpointy:
  GET  /users                — lista z filtrowaniem i paginacją
  GET  /users/{id}           — szczegóły użytkownika
  POST /users                — tworzenie użytkownika
  PUT  /users/{id}           — aktualizacja użytkownika
  DELETE /users/{id}         — inicjacja soft-delete (202 + token)
  DELETE /users/{id}/confirm — wykonanie soft-delete
  POST /users/{id}/lock      — blokada konta
  POST /users/{id}/unlock    — odblokowanie konta

Każdy schemat INPUT:
  - extra='forbid'           — odrzucamy nieznane pola
  - Sanityzacja Unicode NFC  — ochrona przed homoglify
  - Walidatory na każdym polu — reguły biznesowe

Eksponowane dane (OUTPUT):
  - NIE eksponujemy: PasswordHash, FailedLoginAttempts
  - Wrażliwe pola operacyjne (LockedUntil) tylko dla Admin/Manager
    — kontrola na poziomie serwisu/endpointu, nie schematu

Konwencja nazw (aliasy):
  Modele SQLAlchemy używają oryginalnych nazw kolumn MSSQL (PascalCase).
  Schematy Pydantic eksponują snake_case przez aliasy.
  populate_by_name=True — możliwe oba.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from datetime import datetime
from typing import List, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)

# Reużywamy walidatora haseł z modułu auth
from app.schemas.auth import _sanitize_string, _validate_password_policy

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

# Regex: username — tylko litery, cyfry, kropki, podkreślniki, myślniki
_RE_USERNAME = re.compile(r"^[a-zA-Z0-9._\-]+$")

# Regex: uprawnienie w formacie `kategoria.akcja`
_RE_PERMISSION = re.compile(r"^[a-z_]+\.[a-z_]+$")

# Dozwolone kategorie uprawnień (zgodnie z dokumentacją projektu)
PERMISSION_CATEGORIES = frozenset({
    "auth", "users", "roles", "debtors", "monits",
    "comments", "pdf", "reports", "snapshots", "audit", "system",
})


# ---------------------------------------------------------------------------
# REQUEST: POST /users — tworzenie nowego użytkownika
# ---------------------------------------------------------------------------

class UserCreateRequest(BaseModel):
    """
    Dane nowego użytkownika tworzonego przez administratora.

    Wymagane uprawnienie: users.create
    Operacja logowana w AuditLog z OldValue=null, NewValue=json użytkownika.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    username: str = Field(
        ...,
        description="Unikalny login użytkownika. Małe litery, cyfry, kropki, podkreślniki, myślniki.",
        min_length=3,
        max_length=50,
        examples=["jan.kowalski", "anna_nowak"],
    )
    email: EmailStr = Field(
        ...,
        description="Adres email. Musi być unikalny w systemie.",
        max_length=100,
    )
    password: SecretStr = Field(
        ...,
        description="Hasło początkowe. Musi spełniać politykę: min. 8 znaków, cyfra, znak specjalny.",
    )
    full_name: Optional[str] = Field(
        default=None,
        description="Imię i nazwisko. Opcjonalne.",
        max_length=100,
    )
    role_id: int = Field(
        ...,
        description="ID roli z tabeli Roles. Istnienie roli weryfikowane w serwisie.",
        ge=1,
    )

    # ---- Walidatory ----

    @field_validator("username", mode="before")
    @classmethod
    def validate_username(cls, v: str) -> str:
        v = _sanitize_string(v, max_length=50)
        if not v:
            raise ValueError("Login nie może być pusty.")
        if not _RE_USERNAME.match(v):
            raise ValueError(
                "Login może zawierać tylko litery, cyfry, kropki, "
                "podkreślniki i myślniki."
            )
        # Normalizacja do lowercase — loginy są case-insensitive
        return v.lower()

    @field_validator("email", mode="before")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        if isinstance(v, str):
            v = _sanitize_string(v, max_length=100)
            return v.lower()
        return v

    @field_validator("password", mode="before")
    @classmethod
    def validate_password(cls, v) -> str:
        raw = v.get_secret_value() if isinstance(v, SecretStr) else str(v)
        return _validate_password_policy(raw)

    @field_validator("full_name", mode="before")
    @classmethod
    def sanitize_full_name(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = _sanitize_string(v, max_length=100)
        return v if v else None

    def __repr__(self) -> str:
        return (
            f"UserCreateRequest("
            f"username='{self.username}', "
            f"email='{self.email}', "
            f"password='**REDACTED**', "
            f"role_id={self.role_id})"
        )


# ---------------------------------------------------------------------------
# REQUEST: PUT /users/{id} — aktualizacja użytkownika
# ---------------------------------------------------------------------------

class UserUpdateRequest(BaseModel):
    """
    Dane aktualizacji użytkownika.

    Zgodnie z ustaleniami — CRUD standardowy, nie edytujemy pojedynczych pól
    osobnymi endpointami. Wszystkie pola opcjonalne (partial update).

    Wymagane uprawnienie: users.edit
    Operacja logowana w AuditLog z OldValue=stare_dane, NewValue=nowe_dane.

    UWAGA: Zmiana hasła przez ten endpoint jest niedozwolona.
    Do zmiany hasła służy POST /auth/change-password.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    email: Optional[EmailStr] = Field(
        default=None,
        description="Nowy adres email. Musi być unikalny.",
        max_length=100,
    )
    full_name: Optional[str] = Field(
        default=None,
        description="Nowe imię i nazwisko.",
        max_length=100,
    )
    role_id: Optional[int] = Field(
        default=None,
        description="Nowe ID roli.",
        ge=1,
    )
    is_active: Optional[bool] = Field(
        default=None,
        description=(
            "Czy konto aktywne. False = dezaktywacja (miększe niż lock). "
            "Dezaktywacja unieważnia wszystkie aktywne tokeny użytkownika."
        ),
    )

    @field_validator("email", mode="before")
    @classmethod
    def normalize_email(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return _sanitize_string(v, max_length=100).lower()

    @field_validator("full_name", mode="before")
    @classmethod
    def sanitize_full_name(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = _sanitize_string(v, max_length=100)
        return v if v else None

    @model_validator(mode="after")
    def at_least_one_field(self) -> "UserUpdateRequest":
        """Co najmniej jedno pole musi być przekazane."""
        if all(
            f is None
            for f in (self.email, self.full_name, self.role_id, self.is_active)
        ):
            raise ValueError(
                "Żadne pole nie zostało przekazane. "
                "Podaj co najmniej jedno pole do aktualizacji."
            )
        return self


# ---------------------------------------------------------------------------
# REQUEST: POST /users/{id}/lock — blokada konta
# ---------------------------------------------------------------------------

class UserLockRequest(BaseModel):
    """
    Blokada konta użytkownika do określonej daty/godziny.

    Wymagane uprawnienie: users.lock
    Blokada jest twarda — użytkownik nie może się zalogować nawet z prawidłowym hasłem.
    Przy logowaniu z zablokowanego konta: HTTP 423 Locked + czas odblokowania.
    """

    model_config = ConfigDict(extra="forbid")

    locked_until: datetime = Field(
        ...,
        description=(
            "Data i czas odblokowania (UTC). "
            "Musi być w przyszłości. "
            "Format: ISO 8601, np. '2026-03-01T12:00:00Z'."
        ),
    )
    reason: Optional[str] = Field(
        default=None,
        description="Powód blokady — zapisywany w AuditLog.",
        max_length=500,
    )

    @field_validator("locked_until", mode="after")
    @classmethod
    def locked_until_must_be_future(cls, v: datetime) -> datetime:
        from datetime import timezone
        now = datetime.now(tz=timezone.utc)
        # Normalizacja — upewniamy się że datetime jest timezone-aware
        if v.tzinfo is None:
            # Zakładamy UTC jeśli brak timezone info
            logger.warning(
                "locked_until bez informacji o strefie czasowej — zakładamy UTC."
            )
            v = v.replace(tzinfo=timezone.utc)
        if v <= now:
            raise ValueError(
                "Data blokady musi być w przyszłości."
            )
        return v

    @field_validator("reason", mode="before")
    @classmethod
    def sanitize_reason(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return _sanitize_string(v, max_length=500)


# ---------------------------------------------------------------------------
# REQUEST: POST /users/{id}/unlock — odblokowanie konta
# ---------------------------------------------------------------------------

class UserUnlockRequest(BaseModel):
    """Odblokowanie konta użytkownika. Wymagane uprawnienie: users.unlock."""

    model_config = ConfigDict(extra="forbid")

    reason: Optional[str] = Field(
        default=None,
        description="Powód odblokowania — zapisywany w AuditLog.",
        max_length=500,
    )

    @field_validator("reason", mode="before")
    @classmethod
    def sanitize_reason(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return _sanitize_string(v, max_length=500)


# ---------------------------------------------------------------------------
# QUERY PARAMS: GET /users — filtry listy użytkowników
# ---------------------------------------------------------------------------

class UserListFilters(BaseModel):
    """
    Parametry filtrowania i sortowania listy użytkowników.

    Wszystkie filtry są opcjonalne — brak filtrów zwraca wszystkich.
    Łączone przez AND.

    Użycie w endpointach FastAPI:
        @router.get("/users")
        async def list_users(
            filters: UserListFilters = Depends(),
            pagination: PaginationParams = Depends(),
        ):
    """

    model_config = ConfigDict(extra="forbid")

    # Filtry
    role_id: Optional[int] = Field(
        default=None,
        description="Filtr po ID roli.",
        ge=1,
    )
    is_active: Optional[bool] = Field(
        default=None,
        description="Filtr po statusie aktywności. Brak = wszyscy (aktywni + nieaktywni).",
    )
    search: Optional[str] = Field(
        default=None,
        description=(
            "Wyszukiwanie tekstowe po username, email i full_name. "
            "Min. 2 znaki. Zastosowane jako LIKE '%term%'."
        ),
        min_length=2,
        max_length=100,
    )
    is_locked: Optional[bool] = Field(
        default=None,
        description=(
            "Filtr po statusie blokady. "
            "True = tylko zablokowane (LockedUntil > NOW), "
            "False = tylko odblokowane."
        ),
    )

    # Sortowanie
    sort_by: str = Field(
        default="created_at",
        description="Kolumna sortowania.",
        pattern=r"^(created_at|username|email|full_name|last_login_at|role_id)$",
    )
    sort_order: str = Field(
        default="desc",
        description="Kierunek sortowania: asc (rosnąco) lub desc (malejąco).",
        pattern=r"^(asc|desc)$",
    )

    @field_validator("search", mode="before")
    @classmethod
    def sanitize_search(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = _sanitize_string(v, max_length=100)
        # Escapowanie znaków SQL LIKE — zapobiegamy injection przez wildcard
        v = v.replace("[", "[[]").replace("%", "[%]").replace("_", "[_]")
        return v if v else None


# ---------------------------------------------------------------------------
# RESPONSE DATA: Szczegóły użytkownika — GET /users/{id}
# ---------------------------------------------------------------------------

class RoleInUserResponse(BaseModel):
    """Zagnieżdżone dane roli w odpowiedzi użytkownika."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: int = Field(..., alias="ID_ROLE")
    name: str = Field(..., alias="RoleName")
    description: Optional[str] = Field(default=None, alias="Description")


class UserDetailResponse(BaseModel):
    """
    Szczegółowe dane użytkownika.

    EKSPONOWANE pola:
      - Podstawowe dane identyfikacyjne
      - Status konta i blokady
      - Dane roli i uprawnień
      - Timestamp aktywności

    NIE EKSPONOWANE (nigdy):
      - PasswordHash
      - FailedLoginAttempts (wrażliwe operacyjne)
      - LockedUntil eksponowany tylko gdy is_locked=True

    UWAGA: LockedUntil jest eksponowany — Admin musi wiedzieć do kiedy blokada.
    Można ograniczyć na poziomie endpointu dla roli User/ReadOnly.
    """

    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
        from_attributes=True,   # ← DODANE
    )

    id: int = Field(..., alias="id_user")          # ORM atrybut: id_user
    username: str = Field(...)                      # ORM atrybut: username
    email: str = Field(...)                         # ORM atrybut: email
    full_name: Optional[str] = Field(default=None) # ORM atrybut: full_name
    is_active: bool = Field(...)                    # ORM atrybut: is_active
    role: Optional["RoleInUserResponse"] = Field(default=None)
    permissions: List[str] = Field(default_factory=list)
    role_id: int = Field(...)                       # ORM atrybut: role_id
    created_at: datetime = Field(...)               # ORM atrybut: created_at
    updated_at: Optional[datetime] = Field(default=None)
    last_login_at: Optional[datetime] = Field(default=None)
    is_locked: bool = Field(default=False)
    locked_until: Optional[datetime] = Field(default=None)


class UserListItemResponse(BaseModel):
    """
    Skrócone dane użytkownika na liście — GET /users.

    Zawiera mniej pól niż UserDetailResponse — optymalizacja zapytania.
    Szczegóły ładowane osobno przez GET /users/{id}.
    """

    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
        from_attributes=True,   # ← DODANE
    )

    id: int = Field(..., alias="id_user")
    username: str = Field(...)
    email: str = Field(...)
    full_name: Optional[str] = Field(default=None)
    is_active: bool = Field(...)
    role_id: int = Field(...)
    role_name: Optional[str] = Field(default=None)
    is_locked: bool = Field(default=False)
    last_login_at: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(...)


# ---------------------------------------------------------------------------
# RESPONSE DATA: Tworzenie i aktualizacja użytkownika
# ---------------------------------------------------------------------------

class UserCreatedResponse(BaseModel):
    """Dane nowo utworzonego użytkownika — odpowiedź POST /users (201)."""

    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
        from_attributes=True,   # ← DODANE
    )

    id: int = Field(..., alias="id_user")
    username: str = Field(...)
    email: str = Field(...)
    full_name: Optional[str] = Field(default=None)
    role_id: int = Field(...)
    is_active: bool = Field(...)
    created_at: datetime = Field(...)


class UserUpdatedResponse(BaseModel):
    """Dane zaktualizowanego użytkownika — odpowiedź PUT /users/{id} (200)."""

    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
        from_attributes=True,   # ← DODANE
    )

    id: int = Field(..., alias="id_user")
    username: str = Field(...)
    email: str = Field(...)
    full_name: Optional[str] = Field(default=None)
    role_id: int = Field(...)
    is_active: bool = Field(...)
    updated_at: Optional[datetime] = Field(default=None)


# ---------------------------------------------------------------------------
# RESPONSE DATA: Lock/Unlock
# ---------------------------------------------------------------------------

class UserLockResponse(BaseModel):
    """Odpowiedź po zablokowaniu konta."""

    model_config = ConfigDict(extra="forbid")

    user_id: int = Field(..., description="ID zablokowanego użytkownika.")
    username: str = Field(..., description="Login zablokowanego użytkownika.")
    locked_until: datetime = Field(..., description="Data odblokowania (UTC).")
    locked_by: int = Field(..., description="ID administratora który wykonał blokadę.")
    message: str = Field(
        default="Konto zostało zablokowane.",
        description="Komunikat potwierdzający operację.",
    )


class UserUnlockResponse(BaseModel):
    """Odpowiedź po odblokowaniu konta."""

    model_config = ConfigDict(extra="forbid")

    user_id: int = Field(..., description="ID odblokowanego użytkownika.")
    username: str = Field(..., description="Login odblokowanego użytkownika.")
    unlocked_by: int = Field(..., description="ID administratora który wykonał odblokowanie.")
    message: str = Field(
        default="Konto zostało odblokowane.",
        description="Komunikat potwierdzający operację.",
    )


# ---------------------------------------------------------------------------
# REQUEST: DELETE /users/{id}/confirm — potwierdzenie soft-delete
# ---------------------------------------------------------------------------

class UserDeleteConfirmRequest(BaseModel):
    """
    Potwierdzenie soft-delete użytkownika (Krok 2 z 2).

    confirm_token pochodzi z odpowiedzi DELETE /users/{id} (202).
    Token jednorazowy — po użyciu trafia do Redis blacklist.

    Weryfikacja tokenu:
      - scope musi być 'confirm_delete'
      - entity_type musi być 'User'
      - entity_id musi zgadzać się z {id} w URL
      - requested_by musi być tym samym userem co Authorization header
      - exp nie może być przeszłością
    """

    model_config = ConfigDict(extra="forbid")

    confirm_token: SecretStr = Field(
        ...,
        description="JWT token potwierdzający DELETE. Jednorazowy, TTL: 60s.",
        min_length=10,
        max_length=1000,
    )

    def __repr__(self) -> str:
        return "UserDeleteConfirmRequest(confirm_token='**REDACTED**')"


# ---------------------------------------------------------------------------
# RESPONSE DATA: Inicjacja DELETE /users/{id} (202)
# ---------------------------------------------------------------------------

class UserDeleteInitData(BaseModel):
    """
    Dane odpowiedzi 202 przy inicjacji DELETE użytkownika.

    Zawiera confirm_token i podsumowanie — co zostanie usunięte,
    ostrzeżenia o powiązanych rekordach.
    """

    model_config = ConfigDict(extra="forbid")

    confirm_token: str = Field(
        ...,
        description="JWT token do potwierdzenia DELETE. TTL: 60s (z SystemConfig).",
    )
    expires_in: int = Field(
        ...,
        description="Czas ważności tokenu w sekundach.",
        ge=1,
    )
    summary: "UserDeleteSummary" = Field(
        ...,
        description="Podsumowanie operacji z ostrzeżeniami.",
    )


class UserDeleteSummary(BaseModel):
    """Podsumowanie operacji DELETE — wyświetlane użytkownikowi przed potwierdzeniem."""

    model_config = ConfigDict(extra="forbid")

    entity: str = Field(default="User")
    id: int = Field(..., description="ID użytkownika do usunięcia.")
    username: str = Field(..., description="Login użytkownika.")
    full_name: Optional[str] = Field(default=None, description="Imię i nazwisko.")
    email: str = Field(..., description="Email użytkownika.")
    monit_history_count: int = Field(
        default=0,
        description="Liczba powiązanych rekordów w MonitHistory.",
        ge=0,
    )
    audit_log_count: int = Field(
        default=0,
        description="Liczba powiązanych rekordów w AuditLog.",
        ge=0,
    )
    comment_count: int = Field(
        default=0,
        description="Liczba komentarzy napisanych przez użytkownika.",
        ge=0,
    )
    warning: Optional[str] = Field(
        default=None,
        description="Ostrzeżenie jeśli użytkownik ma powiązane rekordy.",
    )


# Aktualizacja forward reference
UserDeleteInitData.model_rebuild()


# ---------------------------------------------------------------------------
# RESPONSE DATA: Zakończenie DELETE /users/{id}/confirm (200)
# ---------------------------------------------------------------------------

class UserDeletedResponse(BaseModel):
    """Dane odpowiedzi po potwierdzeniu soft-delete użytkownika."""

    model_config = ConfigDict(extra="forbid")

    archived_at: datetime = Field(
        ...,
        description="Timestamp soft-delete (archiwizacji).",
    )
    entity_type: str = Field(default="User")
    entity_id: int = Field(..., description="ID usuniętego użytkownika.")
    username: str = Field(..., description="Login usuniętego użytkownika.")
    archive_path: Optional[str] = Field(
        default=None,
        description="Ścieżka do pliku archiwum JSON (dla administratora).",
    )