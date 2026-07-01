# backend/app/services/hook_service.py
"""
HookService — silnik wykonywania hookow po akcjach obiegowych.

DECYZJA ARCHITEKTONICZNA F0.1 (rozwiazana):
  Hook krytyczny dziala PRZED db.commit().
  Implementacja: hook wykonuje sie w otwartej transakcji SQLAlchemy.
  Blad hooka krytycznego = wyjatek propagowany do callera = brak commit().
  Transakcja jest automatycznie rollbackowana przez SQLAlchemy na koncu
  bloku async with session.begin() lub przez jawne db.rollback().
  NIE uzywamy db.rollback() po commit() — nie ma kompensacji po fakcie.

Interfejs publiczny:
    await HookService.run_after(
        action="accepted",
        id_instance=1042,
        db=db,
        redis=redis,
    )

Kontrakt odpowiedzi zewnetrznego systemu (sql_procedure):
    SELECT status NVARCHAR, message NVARCHAR, refresh_document BIT
    Dokladnie 3 kolumny, dokladnie 1 wiersz.

Kontrakt odpowiedzi zewnetrznego systemu (api_call):
    HTTP 200 z JSON: {"status": "success|error|warning", "message": "...", "refresh_document": 0|1}
    HTTP != 200 → traktowane jako error.

Placeholdery w operation_config:
    {id_instance}, {id_document}, {doc_number}, {contractor_name},
    {amount_gross}, {doc_date}, {nip}, {action},
    {extra.DOWOLNE_POLE} — dowolne pole z extra_data dokumentu

Logowanie:
    Kazde wywolanie → skw_source_action_log niezaleznie od wyniku.
    request_payload, response_payload — kontrolowane przez SystemConfig:
    HOOK_LOG_REQUEST_PAYLOAD, HOOK_LOG_RESPONSE_PAYLOAD (domyslnie true).

UWAGA: from __future__ import annotations — NIGDY (SQLAlchemy ORM, FastAPI).
"""

import asyncio
import json
import logging
import re
import time
from typing import Any, NamedTuple

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_SCHEMA = "dbo"

# Walidacja trigger_action — zgodna z CHECK constraintem w DB (migracja 0039)
_VALID_TRIGGER_ACTIONS = frozenset({"accepted", "rejected"})

# Walidacja nazw placeholderow — tylko bezpieczne identyfikatory
_PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z0-9_.]+)\}")

# Ograniczenie dlugosci logowanych payloadow
_MAX_PAYLOAD_LOG_BYTES = 4096


# =============================================================================
# Typy wewnetrzne
# =============================================================================

class HookResult(NamedTuple):
    """Wynik wywolania hooka."""
    status:           str          # success | error | warning
    message:          str | None
    refresh_document: bool
    execution_ms:     int
    hook_id:          int


class HookError(Exception):
    """
    Wyjatek rzucany przez hook krytyczny przy statusie error lub timeout.
    Przekazywany do callera w approval_service — brak commit() = rollback.
    message trafia do uzytkownika jako detail HTTP 422.
    """
    def __init__(self, message: str, hook_id: int | None = None) -> None:
        super().__init__(message)
        self.user_message = message
        self.hook_id      = hook_id


# =============================================================================
# Glowna klasa serwisu
# =============================================================================

class HookService:
    """
    Silnik hookow. Stateless — wszystkie metody sa klasowe lub statyczne.
    """

    @classmethod
    async def run_after(
        cls,
        *,
        action: str,
        id_instance: int,
        db: AsyncSession,
        redis: Any,
        id_user: int | None = None,
    ) -> list[HookResult]:
        """
        Wykonuje wszystkie aktywne hooki dla zrodla instancji po danej akcji.

        WYWOLAJ wewnatrz approval_lock, przed db.commit().
        Hook krytyczny z bledem → rzuca HookError → brak commit() → rollback.
        Hook informacyjny z bledem → loguje, zwraca wynik, akcja przechodzi.

        Args:
            action:      Nazwa akcji: 'accepted' lub 'rejected'.
            id_instance: ID instancji obiegu.
            db:          Sesja SQLAlchemy — OTWARTA TRANSAKCJA.
            redis:       Klient Redis (do odczytu SystemConfig cache).
            id_user:     Kto wywolal akcje (None = systemowe).

        Returns:
            Lista wynikow wszystkich wykonanych hookow.

        Raises:
            HookError: gdy hook krytyczny zwroci status=error lub timeout.
        """
        if action not in _VALID_TRIGGER_ACTIONS:
            # Tylko accepted i rejected maja hooki — reszta przechodzi cicho
            logger.debug("HookService.run_after: action=%r nie ma hookow", action)
            return []

        # Pobierz hooki dla tego zrodla i akcji
        hooks = await cls._get_hooks(db, id_instance, action)
        if not hooks:
            logger.debug(
                "HookService.run_after: brak aktywnych hookow | instance=%s action=%s",
                id_instance, action,
            )
            return []

        # Pobierz dane dokumentu dla placeholderow
        doc_data = await cls._get_document_data(db, id_instance)

        results: list[HookResult] = []

        for hook in hooks:
            hook_id   = hook["id_hook"]
            severity  = hook["severity"]
            op_type   = hook["operation_type"]
            op_config = hook.get("operation_config") or "{}"
            timeout_s = cls._get_timeout(op_config)

            logger.info(
                "HookService: wykonuje hook | hook_id=%s action=%s severity=%s type=%s instance=%s",
                hook_id, action, severity, op_type, id_instance,
            )

            t_start = time.monotonic()
            try:
                result_raw = await asyncio.wait_for(
                    cls._execute_hook(
                        db=db,
                        op_type=op_type,
                        op_config_raw=op_config,
                        doc_data=doc_data,
                        action=action,
                        id_instance=id_instance,
                    ),
                    timeout=timeout_s,
                )
                execution_ms = round((time.monotonic() - t_start) * 1000)

            except asyncio.TimeoutError:
                execution_ms = round((time.monotonic() - t_start) * 1000)
                timeout_msg  = f"Timeout po {timeout_s} sekundach"

                await cls._log_execution(
                    db=db,
                    id_hook=hook_id,
                    id_instance=id_instance,
                    id_user=id_user,
                    status="error",
                    message=timeout_msg,
                    execution_ms=execution_ms,
                    request_payload=None,
                    response_payload=None,
                    redis=redis,
                )

                if severity == "critical":
                    raise HookError(
                        f"System zewnetrzny nie odpowiedzial w czasie {timeout_s} s. "
                        f"Akcja zostala cofnieta.",
                        hook_id=hook_id,
                    )
                else:
                    logger.warning(
                        "HookService: timeout hook informacyjny | hook_id=%s", hook_id
                    )
                    results.append(HookResult(
                        status="error",
                        message=timeout_msg,
                        refresh_document=False,
                        execution_ms=execution_ms,
                        hook_id=hook_id,
                    ))
                    continue

            except Exception as exc:
                execution_ms = round((time.monotonic() - t_start) * 1000)
                err_msg = f"Blad wywolania hooka: {type(exc).__name__}: {exc}"

                await cls._log_execution(
                    db=db,
                    id_hook=hook_id,
                    id_instance=id_instance,
                    id_user=id_user,
                    status="error",
                    message=err_msg[:500],
                    execution_ms=execution_ms,
                    request_payload=None,
                    response_payload=None,
                    redis=redis,
                )

                if severity == "critical":
                    raise HookError(
                        f"Blad systemu zewnetrznego: {exc}. Akcja zostala cofnieta.",
                        hook_id=hook_id,
                    ) from exc
                else:
                    logger.error(
                        "HookService: blad hooka informacyjnego | hook_id=%s: %s",
                        hook_id, exc,
                    )
                    results.append(HookResult(
                        status="error",
                        message=err_msg[:200],
                        refresh_document=False,
                        execution_ms=execution_ms,
                        hook_id=hook_id,
                    ))
                    continue

            # Parsuj wynik hooka
            hook_status       = result_raw.get("status", "error")
            hook_message      = result_raw.get("message") or ""
            hook_refresh      = bool(result_raw.get("refresh_document", False))
            req_payload_str   = result_raw.get("_request_payload")
            resp_payload_str  = result_raw.get("_response_payload")

            await cls._log_execution(
                db=db,
                id_hook=hook_id,
                id_instance=id_instance,
                id_user=id_user,
                status=hook_status,
                message=hook_message[:500] if hook_message else None,
                execution_ms=execution_ms,
                request_payload=req_payload_str,
                response_payload=resp_payload_str,
                redis=redis,
            )

            # Decyzja per wynik
            if hook_status == "error" and severity == "critical":
                raise HookError(
                    hook_message or "System zewnetrzny zwrocil blad. Akcja zostala cofnieta.",
                    hook_id=hook_id,
                )

            results.append(HookResult(
                status=hook_status,
                message=hook_message or None,
                refresh_document=hook_refresh,
                execution_ms=execution_ms,
                hook_id=hook_id,
            ))

            logger.info(
                "HookService: hook zakonczony | hook_id=%s status=%s ms=%s",
                hook_id, hook_status, execution_ms,
            )

        return results

    # ──────────────────────────────────────────────────────────────────────────
    # Wykonanie hooka per typ operacji
    # ──────────────────────────────────────────────────────────────────────────

    @classmethod
    async def _execute_hook(
        cls,
        *,
        db: AsyncSession,
        op_type: str,
        op_config_raw: str,
        doc_data: dict[str, Any],
        action: str,
        id_instance: int,
    ) -> dict[str, Any]:
        """
        Wykonuje hook zgodnie z operation_type.
        Zwraca dict: {status, message, refresh_document, _request_payload, _response_payload}.
        """
        try:
            op_config: dict = json.loads(op_config_raw)
        except json.JSONDecodeError:
            op_config = {}

        if op_type == "sql_procedure":
            return await cls._execute_sql_procedure(
                db=db,
                op_config=op_config,
                doc_data=doc_data,
                action=action,
                id_instance=id_instance,
            )

        if op_type == "api_call":
            return await cls._execute_api_call(
                op_config=op_config,
                doc_data=doc_data,
                action=action,
                id_instance=id_instance,
            )

        raise ValueError(f"Nieznany operation_type: {op_type!r}")

    @classmethod
    async def _execute_sql_procedure(
        cls,
        *,
        db: AsyncSession,
        op_config: dict[str, Any],
        doc_data: dict[str, Any],
        action: str,
        id_instance: int,
    ) -> dict[str, Any]:
        """
        Wykonuje procedure SQL i odczytuje wynik.

        Kontrakt procedury: SELECT status, message, refresh_document (1 wiersz).
        Parametry przekazywane przez placeholdery w procedure_name lub params.

        Przyklad op_config:
        {
            "procedure_name": "dbo.skw_AktualizujStatusFaktury",
            "params": {
                "ksef_id": "{extra.ksef_id}",
                "action":  "{action}"
            }
        }
        """
        proc_name = op_config.get("procedure_name", "")
        # Walidacja nazwy procedury — tylko bezpieczne znaki
        if not re.match(r'^[\w.]+$', proc_name):
            raise ValueError(f"Nieprawidlowa nazwa procedury: {proc_name!r}")

        params_template: dict = op_config.get("params", {})
        params_resolved: dict = {}

        for param_name, param_value in params_template.items():
            if isinstance(param_value, str):
                params_resolved[param_name] = cls._resolve_placeholder(
                    param_value, doc_data, action, id_instance
                )
            else:
                params_resolved[param_name] = param_value

        # Buduj EXEC z parametrami
        if params_resolved:
            param_sql_parts = ", ".join(f"@{k} = :{k}" for k in params_resolved)
            exec_sql = f"EXEC {proc_name} {param_sql_parts}"
        else:
            exec_sql = f"EXEC {proc_name}"

        request_payload = json.dumps({
            "procedure": proc_name,
            "params": params_resolved,
        }, ensure_ascii=False, default=str)

        result_row = await db.execute(text(exec_sql), params_resolved)
        row = result_row.fetchone()

        if not row:
            raise ValueError(
                f"Procedura {proc_name} nie zwrocila zadnego wiersza. "
                "Oczekiwano: SELECT status, message, refresh_document."
            )

        # Mapuj wynik na standardowy format
        cols = list(result_row.keys())
        row_dict = dict(zip(cols, row))

        status_val  = str(row_dict.get("status", "error")).lower()
        message_val = str(row_dict.get("message", "")) if row_dict.get("message") else ""
        refresh_val = bool(row_dict.get("refresh_document", 0))

        if status_val not in ("success", "error", "warning"):
            logger.warning(
                "_execute_sql_procedure: nieznany status %r, traktuje jako error", status_val
            )
            status_val = "error"

        response_payload = json.dumps(row_dict, ensure_ascii=False, default=str)

        return {
            "status":            status_val,
            "message":           message_val,
            "refresh_document":  refresh_val,
            "_request_payload":  request_payload,
            "_response_payload": response_payload,
        }

    @classmethod
    async def _execute_api_call(
        cls,
        *,
        op_config: dict[str, Any],
        doc_data: dict[str, Any],
        action: str,
        id_instance: int,
    ) -> dict[str, Any]:
        """
        Wykonuje HTTP call do zewnetrznego API.

        Kontrakt odpowiedzi: JSON {"status": "...", "message": "...", "refresh_document": 0|1}
        HTTP != 200 → status=error z kodem HTTP w message.

        Przyklad op_config:
        {
            "url": "https://erp.example.com/api/approval",
            "method": "POST",
            "headers": {"X-Api-Key": "secret"},
            "body": {"ksef_id": "{extra.ksef_id}", "action": "{action}"}
        }
        """
        url_template: str   = op_config.get("url", "")
        method: str         = op_config.get("method", "POST").upper()
        headers_template    = op_config.get("headers", {})
        body_template       = op_config.get("body", {})
        message_key: str    = op_config.get("message_key", "message")
        status_key: str     = op_config.get("status_key", "status")
        refresh_key: str    = op_config.get("refresh_key", "refresh_document")

        url = cls._resolve_placeholder(url_template, doc_data, action, id_instance)

        # Rozwiaz placeholdery w body
        body_resolved: dict = {}
        for k, v in body_template.items():
            body_resolved[k] = (
                cls._resolve_placeholder(v, doc_data, action, id_instance)
                if isinstance(v, str) else v
            )

        # Rozwiaz placeholdery w naglowkach (nie logujemy wartosci naglowkow)
        headers_resolved: dict = {}
        for k, v in headers_template.items():
            headers_resolved[k] = (
                cls._resolve_placeholder(v, doc_data, action, id_instance)
                if isinstance(v, str) else str(v)
            )

        request_payload = json.dumps({
            "url": url, "method": method, "body": body_resolved,
        }, ensure_ascii=False, default=str)

        async with httpx.AsyncClient(timeout=None) as client:
            # Timeout zarzadzany przez asyncio.wait_for w run_after — tu None
            resp = await client.request(
                method=method,
                url=url,
                json=body_resolved if body_resolved else None,
                headers=headers_resolved,
            )

        response_payload = resp.text[:_MAX_PAYLOAD_LOG_BYTES]

        if resp.status_code != 200:
            return {
                "status":            "error",
                "message":           f"HTTP {resp.status_code}: {resp.text[:200]}",
                "refresh_document":  False,
                "_request_payload":  request_payload,
                "_response_payload": response_payload,
            }

        try:
            resp_json = resp.json()
        except Exception:
            return {
                "status":            "error",
                "message":           f"Nieprawidlowy JSON w odpowiedzi: {resp.text[:200]}",
                "refresh_document":  False,
                "_request_payload":  request_payload,
                "_response_payload": response_payload,
            }

        status_val  = str(resp_json.get(status_key, "error")).lower()
        message_val = resp_json.get(message_key, "")
        refresh_val = bool(resp_json.get(refresh_key, False))

        if status_val not in ("success", "error", "warning"):
            status_val = "error"

        return {
            "status":            status_val,
            "message":           str(message_val) if message_val else "",
            "refresh_document":  refresh_val,
            "_request_payload":  request_payload,
            "_response_payload": json.dumps(resp_json, ensure_ascii=False, default=str),
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Pomocnicze — pobieranie danych
    # ──────────────────────────────────────────────────────────────────────────

    @classmethod
    async def _get_hooks(
        cls,
        db: AsyncSession,
        id_instance: int,
        action: str,
    ) -> list[dict[str, Any]]:
        """Pobiera aktywne hooki dla zrodla instancji i danej akcji."""
        result = await db.execute(
            text(f"""
                SELECT
                    h.[id_hook],
                    h.[trigger_action],
                    h.[operation_type],
                    h.[operation_config],
                    h.[severity]
                FROM [{_SCHEMA}].[skw_source_hooks] h
                JOIN [{_SCHEMA}].[skw_document_approval_instances] i
                  ON i.[id_source] = h.[id_source]
                WHERE i.[id_instance] = :inst
                  AND h.[trigger_action] = :action
                  AND h.[is_active] = 1
                ORDER BY h.[id_hook] ASC
            """),
            {"inst": id_instance, "action": action},
        )
        cols = list(result.keys())
        return [dict(zip(cols, r)) for r in result.fetchall()]

    @classmethod
    async def _get_document_data(
        cls,
        db: AsyncSession,
        id_instance: int,
    ) -> dict[str, Any]:
        """
        Pobiera dane dokumentu potrzebne do rozwiazania placeholderow.
        Laczy dane z instancji + widok WAPRO (dla pelnych pol wspólnych).
        """
        result = await db.execute(
            text(f"""
                SELECT
                    i.[id_instance],
                    i.[id_document],
                    i.[extra_data],
                    i.[document_amount],
                    i.[document_title],
                    -- Dane z widoku WAPRO (jesli dostepne)
                    fah.[NUMER]           AS doc_number,
                    fah.[NazwaKontrahenta] AS contractor_name,
                    fah.[WARTOSC_BRUTTO]  AS amount_gross,
                    fah.[DataWystawienia] AS doc_date,
                    NULL                  AS nip
                FROM [{_SCHEMA}].[skw_document_approval_instances] i
                LEFT JOIN [{_SCHEMA}].[skw_faktury_akceptacja_naglowek] fah
                       ON fah.[KSEF_ID] = i.[id_document]
                WHERE i.[id_instance] = :inst
            """),
            {"inst": id_instance},
        )
        cols = list(result.keys())
        row  = result.fetchone()
        if not row:
            return {"id_instance": id_instance, "id_document": ""}

        data = dict(zip(cols, row))

        # Parsuj extra_data JSON → dostepne jako extra.POLE
        extra: dict = {}
        if data.get("extra_data"):
            try:
                extra = json.loads(data["extra_data"])
            except Exception:
                pass
        data["_extra"] = extra

        return data

    @staticmethod
    def _get_timeout(op_config_raw: str) -> float:
        """
        Odczytuje timeout z operation_config (klucz timeout_seconds).
        Zakres: 5–120s. Domyslnie: 30s.
        """
        try:
            cfg = json.loads(op_config_raw) if op_config_raw else {}
            val = int(cfg.get("timeout_seconds", 30))
        except Exception:
            val = 30
        return float(max(5, min(120, val)))

    @staticmethod
    def _resolve_placeholder(
        template: str,
        doc_data: dict[str, Any],
        action: str,
        id_instance: int,
    ) -> str:
        """
        Podstawia placeholdery w stringu szablonowym.
        Bezpiecznie — tylko znane klucze, brak eval, brak format().
        """
        extra: dict = doc_data.get("_extra", {})

        known: dict[str, Any] = {
            "id_instance":     str(id_instance),
            "id_document":     str(doc_data.get("id_document", "")),
            "doc_number":      str(doc_data.get("doc_number") or ""),
            "contractor_name": str(doc_data.get("contractor_name") or ""),
            "amount_gross":    str(doc_data.get("amount_gross") or ""),
            "doc_date":        str(doc_data.get("doc_date") or ""),
            "nip":             str(doc_data.get("nip") or ""),
            "action":          action,
        }

        def _replace(match: re.Match) -> str:
            key = match.group(1)
            # Obsluga extra.POLE
            if key.startswith("extra."):
                extra_key = key[6:]
                return str(extra.get(extra_key, ""))
            return str(known.get(key, f"{{{key}}}"))

        return _PLACEHOLDER_RE.sub(_replace, template)

    # ──────────────────────────────────────────────────────────────────────────
    # Logowanie do skw_source_action_log
    # ──────────────────────────────────────────────────────────────────────────

    @classmethod
    async def _log_execution(
        cls,
        *,
        db: AsyncSession,
        id_hook: int,
        id_instance: int,
        id_user: int | None,
        status: str,
        message: str | None,
        execution_ms: int,
        request_payload: str | None,
        response_payload: str | None,
        redis: Any,
    ) -> None:
        """
        Zapisuje wynik wywolania hooka do skw_source_action_log.

        Logowanie payloadow kontrolowane przez SystemConfig:
            HOOK_LOG_REQUEST_PAYLOAD  (domyslnie true)
            HOOK_LOG_RESPONSE_PAYLOAD (domyslnie true)

        Blad zapisu logu NIE przerywa akcji — logujemy blad loggowania.
        """
        should_log_req  = await cls._get_config_bool(db, redis, "HOOK_LOG_REQUEST_PAYLOAD",  True)
        should_log_resp = await cls._get_config_bool(db, redis, "HOOK_LOG_RESPONSE_PAYLOAD", True)

        try:
            await db.execute(
                text(f"""
                    INSERT INTO [{_SCHEMA}].[skw_source_action_log] (
                        [id_hook], [id_action], [id_instance], [id_user],
                        [executed_at], [status], [message], [execution_ms],
                        [request_payload], [response_payload]
                    ) VALUES (
                        :id_hook, NULL, :id_instance, :id_user,
                        SYSUTCDATETIME(), :status, :message, :execution_ms,
                        :request_payload, :response_payload
                    )
                """),
                {
                    "id_hook":          id_hook,
                    "id_instance":      id_instance,
                    "id_user":          id_user,
                    "status":           status[:20],
                    "message":          (message or "")[:500],
                    "execution_ms":     execution_ms,
                    "request_payload":  (
                        request_payload[:_MAX_PAYLOAD_LOG_BYTES]
                        if should_log_req and request_payload else None
                    ),
                    "response_payload": (
                        response_payload[:_MAX_PAYLOAD_LOG_BYTES]
                        if should_log_resp and response_payload else None
                    ),
                },
            )
        except Exception as log_exc:
            # Blad zapisu logu — nie przerywamy akcji
            logger.error(
                "HookService._log_execution: blad zapisu do skw_source_action_log: %s",
                log_exc,
            )

    @staticmethod
    async def _get_config_bool(
        db: AsyncSession,
        redis: Any,
        key: str,
        default: bool,
    ) -> bool:
        """Odczytuje wartosc bool z SystemConfig. Fallback na default."""
        try:
            # Najpierw cache Redis
            if redis:
                cached = await redis.get(f"config:{key}")
                if cached is not None:
                    return str(cached).lower() == "true"
            # Potem DB
            result = await db.execute(
                text(
                    f"SELECT [ConfigValue] FROM [{_SCHEMA}].[skw_SystemConfig] "
                    f"WHERE [ConfigKey] = :k AND [IsActive] = 1"
                ),
                {"k": key},
            )
            row = result.fetchone()
            if row:
                return str(row[0]).lower() == "true"
        except Exception:
            pass
        return default