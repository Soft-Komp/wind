"""
Schematy Pydantic v2 dla modułu autoryzacji.

Każdy schemat INPUT (dane z frontendu) stosuje:
  1. extra='forbid'  — odrzucamy nieznane pola
  2. strip_whitespace — usuwamy białe znaki z granic stringów
  3. Sanityzacja     — normalizacja przed walidacją
  4. Walidatory      — reguły biznesowe (polityka haseł, format emaila itp.)
  5. Maskowanie      — hasła i tokeny nigdy nie pojawiają się w logach

Konwencja nazw:
  *Request  — dane przychodzące z frontendu (INPUT, rygorystyczna walidacja)
  *Response — dane wychodzące do frontendu (OUTPUT, ekspozycja pól)
  *Data     — payload zagnieżdżony wewnątrz BaseResponse.data

Polityka haseł (zgodna z ustaleniami projektu):
  - Minimum 8 znaków
  - Co najmniej jedna cyfra
  - Co najmniej jeden znak specjalny (!@#$%^&*...)
  - Historia haseł — weryfikowana w warstwie serwisu (schemat dostarcza hash)
"""

from __future__ import annotations

import logging
import re
import unicodedata
from datetime import datetime, timezone
from typing import Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe — polityka haseł i limity pól
# ---------------------------------------------------------------------------

# Minimalna długość hasła
PASSWORD_MIN_LENGTH: int = 8

# Maksymalna długość hasła — ochrona przed DoS przez długie hasła
PASSWORD_MAX_LENGTH: int = 128

# Regex: co najmniej jedna cyfra
_RE_HAS_DIGIT = re.compile(r"\d")

# Regex: co najmniej jeden znak specjalny
_RE_HAS_SPECIAL = re.compile(r"[!@#$%^&*()\-_=+\[\]{};:',.<>?/\\|`~\"§±]")

# Regex: username — tylko litery, cyfry, kropki, podkreślniki, myślniki
_RE_USERNAME = re.compile(r"^[a-zA-Z0-9._\-]+$")

# Regex: PIN master key — 4-6 cyfr
_RE_PIN = re.compile(r"^\d{4,6}$")

# Długość master key (64 znaki)
MASTER_KEY_LENGTH: int = 64

# Maksymalna długość OTP code — 6-8 cyfr
OTP_MIN_LENGTH: int = 6
OTP_MAX_LENGTH: int = 8


# ---------------------------------------------------------------------------
# Funkcje pomocnicze — sanityzacja i normalizacja
# ---------------------------------------------------------------------------

def _sanitize_string(value: str, *, max_length: int) -> str:
    """
    Podstawowa sanityzacja stringa:
      1. Strip whitespace
      2. Normalizacja Unicode (NFC) — zapobiega atakowi przez homoglify
      3. Usunięcie null bytes i innych znaków kontrolnych
      4. Obcięcie do max_length
    """
    if not isinstance(value, str):
        return value

    # Strip whitespace
    value = value.strip()

    # Normalizacja Unicode NFC — "é" (2 code points) → "é" (1 code point)
    value = unicodedata.normalize("NFC", value)

    # Usunięcie null bytes i znaków kontrolnych (poza tab i newline)
    value = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", value)

    # Obcięcie do maksymalnej długości
    if len(value) > max_length:
        logger.warning(
            "Pole przekroczyło maksymalną długość %d — obcięto do limitu.",
            max_length,
        )
        value = value[:max_length]

    return value


def _validate_password_policy(password: str) -> str:
    """
    Weryfikuje politykę haseł:
      - Min. 8 znaków
      - Co najmniej jedna cyfra
      - Co najmniej jeden znak specjalny

    Zwraca hasło bez zmian lub rzuca ValueError.
    Używana jako sub-validator — NIE loguje samego hasła.
    """
    if len(password) < PASSWORD_MIN_LENGTH:
        raise ValueError(
            f"Hasło musi mieć co najmniej {PASSWORD_MIN_LENGTH} znaków."
        )

    if len(password) > PASSWORD_MAX_LENGTH:
        raise ValueError(
            f"Hasło nie może przekraczać {PASSWORD_MAX_LENGTH} znaków."
        )

    if not _RE_HAS_DIGIT.search(password):
        raise ValueError("Hasło musi zawierać co najmniej jedną cyfrę.")

    if not _RE_HAS_SPECIAL.search(password):
        raise ValueError(
            "Hasło musi zawierać co najmniej jeden znak specjalny "
            "(!@#$%^&*()-_=+[]{};:',.<>?/\\|`~)."
        )

    return password


# ---------------------------------------------------------------------------
# REQUEST: POST /auth/login
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    """
    Dane logowania przesyłane z frontendu.

    Walidacja:
      - username: tylko dozwolone znaki, 3-50 znaków
      - password: jako SecretStr — nigdy nie serializowany do JSON/logów
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        # Nie logujemy tego modelu nigdy — zawiera hasło
    )

    username: str = Field(
        ...,
        description="Login użytkownika.",
        min_length=3,
        max_length=50,
        examples=["jan.kowalski"],
    )

    # SecretStr — Pydantic nie serializuje wartości, repr zwraca "**********"
    password: SecretStr = Field(
        ...,
        description="Hasło użytkownika. Nigdy nie pojawia się w logach.",
    )

    @field_validator("username", mode="before")
    @classmethod
    def validate_username(cls, v: str) -> str:
        v = _sanitize_string(v, max_length=50)
        if not _RE_USERNAME.match(v):
            raise ValueError(
                "Login może zawierać tylko litery, cyfry, kropki, podkreślniki i myślniki."
            )
        return v.lower()  # Normalizacja do lowercase — login case-insensitive

    def __repr__(self) -> str:
        """Bezpieczny repr — nie ujawnia hasła."""
        return f"LoginRequest(username='{self.username}', password='**REDACTED**')"


# ---------------------------------------------------------------------------
# RESPONSE DATA: POST /auth/login (sukces)
# ---------------------------------------------------------------------------

class TokenPair(BaseModel):
    """Para tokenów zwracana po udanym logowaniu."""

    model_config = ConfigDict(extra="ignore")

    access_token: str = Field(
        ...,
        description="JWT access token. Ważność: maksymalnie 24h (konfigurowalny).",
    )
    refresh_token: str = Field(
        ...,
        description="JWT refresh token. Ważność: 30 dni (konfigurowalny).",
    )
    token_type: str = Field(
        default="bearer",
        description="Typ tokenu — zawsze 'bearer'.",
    )
    expires_in: int = Field(
        ...,
        description="Czas ważności access tokenu w sekundach.",
        ge=1,
    )
    # Czy to jest sesja impersonacji
    is_impersonation: bool = Field(
        default=False,
        description="Czy token dotyczy sesji impersonacji.",
    )


# ---------------------------------------------------------------------------
# REQUEST: POST /auth/refresh
# ---------------------------------------------------------------------------

class RefreshTokenRequest(BaseModel):
    """Żądanie odświeżenia access tokenu."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    refresh_token: SecretStr = Field(
        ...,
        description="Aktywny refresh token. Jednorazowe użycie — po odświeżeniu stary jest unieważniany.",
        min_length=10,
        max_length=1000,
    )

    def __repr__(self) -> str:
        return "RefreshTokenRequest(refresh_token='**REDACTED**')"


# ---------------------------------------------------------------------------
# REQUEST: POST /auth/logout
# ---------------------------------------------------------------------------

class LogoutRequest(BaseModel):
    """
    Żądanie wylogowania.

    Refresh token jest potrzebny, żeby go unieważnić w bazie.
    Access token unieważniamy przez Redis blacklist (z Authorization header).
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    refresh_token: SecretStr = Field(
        ...,
        description="Refresh token do unieważnienia.",
        min_length=10,
        max_length=1000,
    )

    def __repr__(self) -> str:
        return "LogoutRequest(refresh_token='**REDACTED**')"


# ---------------------------------------------------------------------------
# RESPONSE DATA: GET /auth/me
# ---------------------------------------------------------------------------

class UserMeResponse(BaseModel):
    """
    Dane aktualnie zalogowanego użytkownika.

    UWAGA: Nigdy nie eksponujemy hash hasła, failed_login_attempts,
    locked_until w szczegółach — to dane wrażliwe operacyjne.
    """

    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
    )

    id: int = Field(..., alias="ID_USER", description="ID użytkownika.")
    username: str = Field(..., alias="Username")
    email: str = Field(..., alias="Email")
    full_name: Optional[str] = Field(default=None, alias="FullName")
    is_active: bool = Field(..., alias="IsActive")
    role_id: int = Field(..., alias="RoleID")
    role_name: Optional[str] = Field(
        default=None,
        description="Nazwa roli — ładowana przez JOIN w serwisie.",
    )
    permissions: list[str] = Field(
        default_factory=list,
        description="Lista uprawnień użytkownika w formacie 'kategoria.akcja'.",
    )
    last_login_at: Optional[datetime] = Field(
        default=None,
        alias="LastLoginAt",
    )
    created_at: datetime = Field(..., alias="CreatedAt")
    # Czy to sesja impersonacji
    is_impersonated: bool = Field(
        default=False,
        description="Czy bieżąca sesja jest sesją impersonacji.",
    )
    impersonated_by: Optional[int] = Field(
        default=None,
        description="ID administratora prowadzącego impersonację.",
    )


# ---------------------------------------------------------------------------
# REQUEST: POST /auth/change-password
# ---------------------------------------------------------------------------

class ChangePasswordRequest(BaseModel):
    """
    Zmiana hasła przez zalogowanego użytkownika.

    Zasady:
      - Stare hasło musi być prawidłowe
      - Nowe hasło musi spełniać politykę
      - Nowe hasło musi różnić się od starego
      - Historia haseł sprawdzana w serwisie (schemat nie ma dostępu do DB)
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    old_password: SecretStr = Field(
        ...,
        description="Aktualne hasło użytkownika — weryfikacja tożsamości.",
    )

    new_password: SecretStr = Field(
        ...,
        description="Nowe hasło. Musi spełniać politykę haseł.",
    )

    @field_validator("new_password", mode="before")
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        if isinstance(v, SecretStr):
            v = v.get_secret_value()
        return _validate_password_policy(v)

    @model_validator(mode="after")
    def passwords_must_differ(self) -> "ChangePasswordRequest":
        """Nowe hasło musi być inne niż stare."""
        old = self.old_password.get_secret_value()
        new = self.new_password.get_secret_value()
        if old == new:
            raise ValueError(
                "Nowe hasło musi różnić się od aktualnego."
            )
        return self

    def __repr__(self) -> str:
        return "ChangePasswordRequest(old_password='**REDACTED**', new_password='**REDACTED**')"


# ---------------------------------------------------------------------------
# REQUEST: POST /auth/forgot-password (Krok 1 OTP — inicjacja)
# ---------------------------------------------------------------------------

class ForgotPasswordRequest(BaseModel):
    """
    Inicjacja resetu hasła — wysłanie kodu OTP na email.

    Uwaga bezpieczeństwa: odpowiedź ZAWSZE jest identyczna niezależnie
    od tego czy email istnieje w bazie (zapobiega enumeration attack).
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    email: EmailStr = Field(
        ...,
        description="Adres email konta, dla którego resetujemy hasło.",
        max_length=100,
    )

    @field_validator("email", mode="before")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        """Normalizacja emaila do lowercase."""
        if isinstance(v, str):
            v = _sanitize_string(v, max_length=100)
            return v.lower()
        return v


# ---------------------------------------------------------------------------
# REQUEST: POST /auth/verify-otp (Krok 2 OTP — weryfikacja kodu)
# ---------------------------------------------------------------------------

class VerifyOtpRequest(BaseModel):
    """
    Weryfikacja kodu OTP wysłanego na email.

    Krok 2 z 3 procesu resetu hasła.
    Zwraca reset_token ważny przez krótki czas (konfigurowalne w SystemConfig).
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    email: EmailStr = Field(
        ...,
        description="Email użytkownika — identyfikacja w kombinacji z OTP.",
        max_length=100,
    )

    otp_code: str = Field(
        ...,
        description=f"Kod OTP z emaila. {OTP_MIN_LENGTH}-{OTP_MAX_LENGTH} cyfr.",
        min_length=OTP_MIN_LENGTH,
        max_length=OTP_MAX_LENGTH,
    )

    @field_validator("email", mode="before")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        if isinstance(v, str):
            return _sanitize_string(v, max_length=100).lower()
        return v

    @field_validator("otp_code", mode="before")
    @classmethod
    def validate_otp_code(cls, v: str) -> str:
        v = _sanitize_string(v, max_length=OTP_MAX_LENGTH)
        if not v.isdigit():
            raise ValueError("Kod OTP może zawierać wyłącznie cyfry.")
        return v

    def __repr__(self) -> str:
        return f"VerifyOtpRequest(email='{self.email}', otp_code='**REDACTED**')"


# ---------------------------------------------------------------------------
# RESPONSE DATA: POST /auth/verify-otp (sukces)
# ---------------------------------------------------------------------------

class OtpVerifiedData(BaseModel):
    """Dane zwracane po poprawnej weryfikacji OTP."""

    model_config = ConfigDict(extra="ignore")

    reset_token: str = Field(
        ...,
        description="Jednorazowy token do ustawienia nowego hasła. Krótki TTL.",
    )
    expires_in: int = Field(
        ...,
        description="Czas ważności reset_token w sekundach.",
        ge=1,
    )


# ---------------------------------------------------------------------------
# REQUEST: POST /auth/reset-password (Krok 3 OTP — nowe hasło)
# ---------------------------------------------------------------------------

class ResetPasswordRequest(BaseModel):
    """
    Ustawienie nowego hasła po weryfikacji OTP.

    Krok 3 z 3.
    reset_token pochodzi z odpowiedzi /auth/verify-otp.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    reset_token: SecretStr = Field(
        ...,
        description="Token otrzymany z /auth/verify-otp. Jednorazowy.",
        min_length=10,
        max_length=500,
    )

    new_password: SecretStr = Field(
        ...,
        description="Nowe hasło spełniające politykę.",
    )

    @field_validator("new_password", mode="before")
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        if isinstance(v, SecretStr):
            v = v.get_secret_value()
        return _validate_password_policy(v)

    def __repr__(self) -> str:
        return "ResetPasswordRequest(reset_token='**REDACTED**', new_password='**REDACTED**')"


# ---------------------------------------------------------------------------
# REQUEST: POST /auth/impersonate/{user_id}
# ---------------------------------------------------------------------------

class ImpersonateRequest(BaseModel):
    """
    Inicjacja impersonacji użytkownika przez administratora.

    Wymaga uprawnienia: auth.impersonate
    Wygasa po max 4 godzinach (konfigurowalny SystemConfig.impersonation.max_hours).
    Historia logowań docelowego użytkownika pozostaje czysta.
    """

    model_config = ConfigDict(extra="forbid")

    # Opcjonalny komentarz — dlaczego impersonacja (dla MasterAccessLog)
    reason: Optional[str] = Field(
        default=None,
        description="Uzasadnienie impersonacji — zapisywane w AuditLog.",
        max_length=500,
    )

    @field_validator("reason", mode="before")
    @classmethod
    def sanitize_reason(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return _sanitize_string(v, max_length=500)


# ---------------------------------------------------------------------------
# RESPONSE DATA: POST /auth/impersonate/{user_id}
# ---------------------------------------------------------------------------

class ImpersonationData(BaseModel):
    """Dane sesji impersonacji."""

    model_config = ConfigDict(extra="ignore")

    access_token: str = Field(
        ...,
        description="Access token z flagą is_impersonation=true w payload.",
    )
    token_type: str = Field(default="bearer")
    expires_in: int = Field(..., ge=1)
    target_user_id: int = Field(
        ...,
        description="ID użytkownika, którego konto jest impersonowane.",
    )
    target_username: str = Field(
        ...,
        description="Username impersonowanego użytkownika.",
    )
    impersonated_by: int = Field(
        ...,
        description="ID administratora prowadzącego impersonację.",
    )
    session_expires_at: datetime = Field(
        ...,
        description="Kiedy wygaśnie sesja impersonacji (UTC).",
    )


# ---------------------------------------------------------------------------
# RESPONSE DATA: GET /auth/impersonate/status
# ---------------------------------------------------------------------------

class ImpersonationStatusData(BaseModel):
    """Status aktywnej sesji impersonacji."""

    model_config = ConfigDict(extra="ignore")

    is_impersonation: bool = Field(
        ...,
        description="Czy aktualnie trwa sesja impersonacji.",
    )
    target_user_id: Optional[int] = Field(default=None)
    target_username: Optional[str] = Field(default=None)
    impersonated_by: Optional[int] = Field(default=None)
    session_expires_at: Optional[datetime] = Field(default=None)
    remaining_seconds: Optional[int] = Field(
        default=None,
        description="Ile sekund pozostało do wygaśnięcia sesji.",
    )


# ---------------------------------------------------------------------------
# REQUEST: POST /auth/master-access
# ---------------------------------------------------------------------------

class MasterAccessRequest(BaseModel):
    """
    Dostęp serwisowy przez Master Key.

    KRYTYCZNE ZASADY BEZPIECZEŃSTWA:
      - Ten schemat nigdy nie jest logowany w całości
      - master_key i pin są SecretStr — nigdy nie pojawiają się w logach
      - Weryfikacja: constant-time compare (zapobiega timing attacks)
      - Rate limiting: 3 próby / 15 min / IP → blokada na 1h
      - Zapis TYLKO do MasterAccessLog — nie do AuditLog, nie do UI
    """

    model_config = ConfigDict(extra="forbid")

    master_key: SecretStr = Field(
        ...,
        description=f"Master key ({MASTER_KEY_LENGTH} znaków) z pliku .env.",
        min_length=MASTER_KEY_LENGTH,
        max_length=MASTER_KEY_LENGTH,
    )

    pin: SecretStr = Field(
        ...,
        description="PIN 4-6 cyfr. Zahashowany w SystemConfig.",
    )

    target_user_id: int = Field(
        ...,
        description="ID użytkownika, do którego konta uzyskujemy dostęp.",
        ge=1,
    )

    @field_validator("pin", mode="before")
    @classmethod
    def validate_pin(cls, v: str) -> str:
        if isinstance(v, SecretStr):
            v = v.get_secret_value()
        v = str(v).strip()
        if not _RE_PIN.match(v):
            # UWAGA: celowo ogólny komunikat — nie ujawniamy formatu
            raise ValueError("Nieprawidłowe dane uwierzytelniające.")
        return v

    @field_validator("master_key", mode="before")
    @classmethod
    def validate_master_key_format(cls, v: str) -> str:
        if isinstance(v, SecretStr):
            v = v.get_secret_value()
        v = str(v).strip()
        if len(v) != MASTER_KEY_LENGTH:
            # Ogólny komunikat — nie ujawniamy oczekiwanej długości
            raise ValueError("Nieprawidłowe dane uwierzytelniające.")
        # Tylko znaki alfanumeryczne i myślniki — ochrona przed injection
        if not re.match(r"^[a-zA-Z0-9\-_]+$", v):
            raise ValueError("Nieprawidłowe dane uwierzytelniające.")
        return v

    def __repr__(self) -> str:
        """KRYTYCZNE: nigdy nie ujawniamy master_key ani pin w repr."""
        return (
            f"MasterAccessRequest("
            f"master_key='**REDACTED**', "
            f"pin='**REDACTED**', "
            f"target_user_id={self.target_user_id})"
        )


# ---------------------------------------------------------------------------
# RESPONSE DATA: POST /auth/master-access
# ---------------------------------------------------------------------------

class MasterAccessData(BaseModel):
    """Dane sesji master access — identyczne jak impersonacja."""

    model_config = ConfigDict(extra="ignore")

    impersonation_token: str = Field(
        ...,
        description="Token z flagą is_impersonation=true. Mechanizm taki sam jak impersonacja.",
    )
    expires_at: datetime = Field(
        ...,
        description="UTC timestamp wygaśnięcia sesji.",
    )
    target_user_id: int = Field(...)
    # UWAGA: nie zwracamy żadnych informacji o masterKey, pinie itp.


# ---------------------------------------------------------------------------
# Schematy odpowiedzi "tylko komunikat" — dla operacji bez payload
# ---------------------------------------------------------------------------

class MessageData(BaseModel):
    """Prosta odpowiedź z komunikatem — dla operacji bez konkretnego payload."""

    model_config = ConfigDict(extra="forbid")

    message: str = Field(
        ...,
        description="Komunikat sukcesu czytelny dla użytkownika.",
        min_length=1,
        max_length=500,
    )


# ---------------------------------------------------------------------------
# RESPONSE DATA: Odpowiedź dla /auth/forgot-password
# (celowo niezróżnicowana — anti-enumeration)
# ---------------------------------------------------------------------------

class ForgotPasswordData(BaseModel):
    """
    Odpowiedź dla /auth/forgot-password.

    ZAWSZE zwracamy ten sam komunikat niezależnie od istnienia emaila.
    Zapobiega email enumeration attack.
    """

    model_config = ConfigDict(extra="forbid")

    message: str = Field(
        default=(
            "Jeśli konto z podanym adresem email istnieje, "
            "kod weryfikacyjny został wysłany."
        ),
        description="Stały komunikat — nie ujawnia istnienia konta.",
    )
    expires_in: int = Field(
        default=300,
        description="Czas ważności kodu OTP w sekundach (informacyjny).",
        ge=60,
    )