# =============================================================================
# alertmanager/checkers/base.py
# System Windykacja — Alert Manager — Bazowy checker
#
# ABC (Abstract Base Class) definiujący kontrakt dla wszystkich checkerów.
# Każdy checker MUSI zaimplementować metodę _perform_check().
#
# Wzorzec: Template Method Pattern
#   - BaseChecker.check() = szkielet algorytmu (timing, logging, error handling)
#   - _perform_check()    = konkretna logika checkera (w subklasie)
# =============================================================================

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from models.alert import AlertLevel, AlertType, CheckResult, CheckStatus

if TYPE_CHECKING:
    import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class BaseChecker(ABC):
    """
    Abstrakcyjna klasa bazowa dla wszystkich checkerów zdrowia systemu.

    Każdy checker:
        1. Ma unikalny alert_type (klucz throttlingu)
        2. Wykonuje jedno sprawdzenie: _perform_check()
        3. Mierzy czas wykonania
        4. Opakowuje wyniki w CheckResult
        5. Loguje wynik w strukturze JSON

    Użycie:
        checker = DbChecker(settings)
        result = await checker.check(redis_client)
    """

    # -----------------------------------------------------------------------
    # Atrybuty do nadpisania w subklasach
    # -----------------------------------------------------------------------
    alert_type: str = AlertType.DB_DOWN          # nadpisz w subklasie
    checker_name: str = "BaseChecker"             # nadpisz w subklasie
    default_level: AlertLevel = AlertLevel.CRITICAL
    timeout_seconds: float = 10.0                 # timeout sprawdzenia

    def __init__(self, settings: Any) -> None:
        """
        Args:
            settings: AlertManagerSettings — konfiguracja z .env
        """
        self._settings = settings
        self._logger = logging.getLogger(
            f"alertmanager.checkers.{self.checker_name}"
        )

    async def check(self, redis_client: "aioredis.Redis") -> CheckResult:
        """
        Główna metoda checkera — NIE nadpisuj w subklasach.

        Wykonuje _perform_check() z:
            - pomiarem czasu
            - timeoutem
            - pełnym logowaniem
            - obsługą nieoczekiwanych wyjątków
        """
        start = time.monotonic()
        checked_at = datetime.now(timezone.utc)

        self._logger.debug(
            "Checker START",
            extra={
                "checker": self.checker_name,
                "alert_type": self.alert_type,
                "ts": checked_at.isoformat(),
            }
        )

        try:
            result = await asyncio.wait_for(
                self._perform_check(redis_client),
                timeout=self.timeout_seconds,
            )
        except asyncio.TimeoutError:
            duration_ms = (time.monotonic() - start) * 1000
            result = self._make_error_result(
                checked_at=checked_at,
                duration_ms=duration_ms,
                title=f"Timeout sprawdzenia: {self.checker_name}",
                message=(
                    f"Checker '{self.checker_name}' nie odpowiedział "
                    f"w ciągu {self.timeout_seconds:.0f} sekund. "
                    f"Może to wskazywać na zawieszone połączenie."
                ),
                details={
                    "timeout_seconds": self.timeout_seconds,
                    "checker": self.checker_name,
                },
            )
            self._logger.error(
                "Checker TIMEOUT po %.0fs",
                self.timeout_seconds,
                extra=self._log_context(result),
            )
            return result

        except Exception as exc:
            duration_ms = (time.monotonic() - start) * 1000
            result = self._make_error_result(
                checked_at=checked_at,
                duration_ms=duration_ms,
                title=f"Nieoczekiwany błąd checkera: {self.checker_name}",
                message=f"Checker '{self.checker_name}' rzucił wyjątek: {type(exc).__name__}: {exc}",
                details={
                    "exception_type": type(exc).__name__,
                    "exception_str": str(exc),
                    "checker": self.checker_name,
                },
            )
            self._logger.critical(
                "Checker EXCEPTION: %s",
                exc,
                exc_info=True,
                extra=self._log_context(result),
            )
            return result

        # Uzupełnij czas jeśli checker sam nie ustawił
        if result.duration_ms == 0:
            result.duration_ms = (time.monotonic() - start) * 1000

        # Log wynika sprawdzenia
        log_fn = self._logger.info if result.is_ok else self._logger.warning
        log_fn(
            "Checker RESULT: %s — status=%s [%.1fms]",
            self.checker_name,
            result.status.value,
            result.duration_ms,
            extra=self._log_context(result),
        )

        return result

    @abstractmethod
    async def _perform_check(self, redis_client: "aioredis.Redis") -> CheckResult:
        """
        Konkretna logika sprawdzenia — ZAIMPLEMENTUJ w subklasie.

        Args:
            redis_client: klient Redis (shared z main loop)

        Returns:
            CheckResult z wynikiem sprawdzenia.
        """
        ...

    # -----------------------------------------------------------------------
    # Metody pomocnicze dla subklas
    # -----------------------------------------------------------------------

    def _make_ok_result(
        self,
        checked_at: datetime,
        duration_ms: float,
        message: str = "OK",
        details: dict | None = None,
    ) -> CheckResult:
        """Skrót do tworzenia wyniku pozytywnego."""
        return CheckResult(
            alert_type=self.alert_type,
            status=CheckStatus.OK,
            level=AlertLevel.INFO,
            title=f"{self.checker_name}: OK",
            message=message,
            details=details or {},
            checked_at=checked_at,
            duration_ms=duration_ms,
            checker_name=self.checker_name,
        )

    def _make_problem_result(
        self,
        checked_at: datetime,
        duration_ms: float,
        title: str,
        message: str,
        details: dict,
        level: AlertLevel | None = None,
        status: CheckStatus = CheckStatus.CRITICAL,
    ) -> CheckResult:
        """Skrót do tworzenia wyniku problemowego."""
        return CheckResult(
            alert_type=self.alert_type,
            status=status,
            level=level or self.default_level,
            title=title,
            message=message,
            details=details,
            checked_at=checked_at,
            duration_ms=duration_ms,
            checker_name=self.checker_name,
        )

    def _make_error_result(
        self,
        checked_at: datetime,
        duration_ms: float,
        title: str,
        message: str,
        details: dict,
    ) -> CheckResult:
        """Skrót do tworzenia wyniku błędu samego checkera."""
        return CheckResult(
            alert_type=self.alert_type,
            status=CheckStatus.ERROR,
            level=AlertLevel.CRITICAL,
            title=title,
            message=message,
            details=details,
            checked_at=checked_at,
            duration_ms=duration_ms,
            checker_name=self.checker_name,
        )

    @staticmethod
    def _log_context(result: CheckResult) -> dict:
        """Kontekst do logowania strukturalnego."""
        return {
            "incident_id": result.incident_id,
            "alert_type": result.alert_type,
            "status": result.status.value,
            "level": result.level.value,
            "duration_ms": round(result.duration_ms, 2),
            "checker": result.checker_name,
        }