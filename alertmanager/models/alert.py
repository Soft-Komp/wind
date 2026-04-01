# =============================================================================
# alertmanager/models/alert.py
# System Windykacja — Alert Manager — Modele danych
#
# Czyste dataclassy — brak zależności od FastAPI/SQLAlchemy.
# Używane przez wszystkie checkery i serwisy.
# =============================================================================

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


# =============================================================================
# ENUMY
# =============================================================================


class AlertLevel(str, Enum):
    """
    Poziom krytyczności alertu.

    Kolejność ważności (rosnąco):
        INFO < WARNING < SECURITY < CRITICAL
    """
    INFO = "INFO"
    WARNING = "WARNING"
    SECURITY = "SECURITY"
    CRITICAL = "CRITICAL"


class AlertType(str, Enum):
    """
    Typy alertów — unikalny identyfikator każdego rodzaju problemu.
    Używany jako klucz throttlingu w Redis: alert:cooldown:{alert_type}
    """
    # Infrastruktura
    DB_DOWN = "db_down"
    DB_HIGH_LATENCY = "db_high_latency"
    REDIS_DOWN = "redis_down"
    FAKIR_DOWN = "fakir_down"
    WORKER_DEAD = "worker_dead"

    # Bezpieczeństwo
    SCHEMA_TAMPER = "schema_tamper"
    BRUTE_FORCE = "brute_force"

    # Operacyjne
    DLQ_OVERFLOW = "dlq_overflow"
    SNAPSHOT_MISSING = "snapshot_missing"

    # Odzyskanie (recovery)
    RECOVERY = "recovery"


class CheckStatus(str, Enum):
    """Wynik pojedynczego sprawdzenia."""
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    ERROR = "error"          # błąd samego checkera (nie systemu)


# =============================================================================
# WYNIK SPRAWDZENIA
# =============================================================================


@dataclass
class CheckResult:
    """
    Wynik jednego sprawdzenia przez BaseChecker.

    Pola:
        alert_type  — unikalny typ alertu (klucz throttlingu)
        status      — ok / warning / critical / error
        level       — poziom gdy status != ok
        title       — krótki tytuł alertu (do emaila Subject)
        message     — opis problemu po polsku (do emaila Body)
        details     — słownik z danymi diagnostycznymi (do logu + emaila)
        checked_at  — timestamp UTC sprawdzenia
        duration_ms — czas wykonania sprawdzenia w ms
        checker_name— nazwa klasy checkera
    """
    alert_type: str
    status: CheckStatus
    level: AlertLevel
    title: str
    message: str
    details: dict[str, Any]
    checked_at: datetime
    duration_ms: float
    checker_name: str

    # Generowane automatycznie
    incident_id: str = field(
        default_factory=lambda: str(uuid.uuid4())
    )

    @property
    def is_ok(self) -> bool:
        return self.status == CheckStatus.OK

    @property
    def is_problem(self) -> bool:
        return self.status in (CheckStatus.WARNING, CheckStatus.CRITICAL)

    def to_dict(self) -> dict[str, Any]:
        """Serializacja do słownika — do logów JSON."""
        return {
            "incident_id": self.incident_id,
            "alert_type": self.alert_type,
            "status": self.status.value,
            "level": self.level.value,
            "title": self.title,
            "message": self.message,
            "details": self.details,
            "checked_at": self.checked_at.isoformat(),
            "duration_ms": round(self.duration_ms, 2),
            "checker_name": self.checker_name,
        }


# =============================================================================
# ALERT EMAIL
# =============================================================================


@dataclass
class AlertEmail:
    """
    Struktura emaila alertu gotowa do wysyłki przez smtp_alert_service.

    Pola:
        recipients      — lista adresów docelowych
        subject         — temat emaila
        html_body       — treść HTML
        text_body       — treść tekstowa (fallback)
        result          — oryginalny CheckResult (do logów)
        is_recovery     — True = email o powrocie do normy
    """
    recipients: list[str]
    subject: str
    html_body: str
    text_body: str
    result: CheckResult
    is_recovery: bool = False

    # Wynik wysyłki (wypełniany przez smtp_alert_service)
    sent: bool = False
    smtp_host_used: Optional[str] = None
    smtp_attempt: int = 0
    send_error: Optional[str] = None
    sent_at: Optional[datetime] = None


# =============================================================================
# STAN ALERTU (do śledzenia recovery)
# =============================================================================


@dataclass
class AlertState:
    """
    Stan alertu przechowywany w Redis.
    Pozwala wykryć przejście FIRING → OK (recovery).

    Klucz Redis: alert:state:{alert_type}
    TTL: brak (stan trwa do recovery lub restartu)
    """
    alert_type: str
    is_firing: bool
    first_fired_at: datetime
    last_fired_at: datetime
    fire_count: int
    last_incident_id: str

    def to_json(self) -> str:
        """Serializacja do JSON string (do Redis SET)."""
        import orjson
        return orjson.dumps({
            "alert_type": self.alert_type,
            "is_firing": self.is_firing,
            "first_fired_at": self.first_fired_at.isoformat(),
            "last_fired_at": self.last_fired_at.isoformat(),
            "fire_count": self.fire_count,
            "last_incident_id": self.last_incident_id,
        }).decode()

    @classmethod
    def from_json(cls, data: str) -> "AlertState":
        """Deserializacja z JSON string (z Redis GET)."""
        import orjson
        from dateutil.parser import parse as parse_dt
        d = orjson.loads(data)
        return cls(
            alert_type=d["alert_type"],
            is_firing=d["is_firing"],
            first_fired_at=parse_dt(d["first_fired_at"]),
            last_fired_at=parse_dt(d["last_fired_at"]),
            fire_count=d["fire_count"],
            last_incident_id=d["last_incident_id"],
        )


# =============================================================================
# KONFIGURACJA OPERACYJNA (z SystemConfig)
# =============================================================================


@dataclass
class RuntimeConfig:
    """
    Konfiguracja operacyjna ładowana z SystemConfig w bazie danych.
    Przeładowywana co config_reload_interval_seconds sekund.

    Wartości domyślne = wartości gdy baza niedostępna.
    """
    alerts_enabled: bool = True
    recipients: list[str] = field(default_factory=list)
    cooldown_minutes: int = 15
    brute_force_threshold: int = 10
    worker_heartbeat_timeout_seconds: int = 120
    db_latency_warn_ms: float = 500.0
    dlq_overflow_threshold: int = 10
    snapshot_expected_hour: int = 3    # godzina (UTC) o której ma być snapshot

    loaded_at: Optional[datetime] = None
    load_error: Optional[str] = None

    @property
    def is_stale(self) -> bool:
        """True jeśli konfiguracja nie była ładowana lub jest stara."""
        if self.loaded_at is None:
            return True
        age = (datetime.now(timezone.utc) - self.loaded_at).total_seconds()
        return age > 600  # > 10 minut = podejrzanie stara

    def to_dict(self) -> dict[str, Any]:
        return {
            "alerts_enabled": self.alerts_enabled,
            "recipients_count": len(self.recipients),
            "cooldown_minutes": self.cooldown_minutes,
            "brute_force_threshold": self.brute_force_threshold,
            "worker_heartbeat_timeout_seconds": self.worker_heartbeat_timeout_seconds,
            "db_latency_warn_ms": self.db_latency_warn_ms,
            "dlq_overflow_threshold": self.dlq_overflow_threshold,
            "snapshot_expected_hour": self.snapshot_expected_hour,
            "loaded_at": self.loaded_at.isoformat() if self.loaded_at else None,
            "load_error": self.load_error,
            "is_stale": self.is_stale,
        }