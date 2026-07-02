# backend/app/services/endpoint_registry_service.py
"""
Serwis rejestru wlacznikow endpointow — F (sekcja endpoint toggle).

Odpowiada za:
  - eager registration wszystkich endpointow przy starcie (register_all)
  - lazy registration przy pierwszym wywolaniu (ensure_registered)
  - sprawdzenie czy endpoint jest wlaczony (is_enabled) — hot path
  - toggle wlacznika (enable / disable) z logiem do skw_AuditLog
  - lista endpointow z filtrami dla panelu admina

Cache Redis: klucz "endpoint_toggle:{endpoint_key}" -> "1" (wlaczony) | "0" (wylaczony)
TTL: 30 sekund. Po toggle — natychmiastowa inwalidacja klucza.

UWAGA: from __future__ import annotations OK (nie ORM, nie router).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_SCHEMA    = "dbo"
_CACHE_PFX = "endpoint_toggle:"
_CACHE_TTL = 30  # sekund


# =============================================================================
# Hot path — sprawdzenie czy endpoint jest wlaczony
# =============================================================================

async def is_enabled(
    db: AsyncSession,
    redis: Any,
    endpoint_key: str,
) -> bool:
    """
    Sprawdza czy endpoint jest wlaczony. Wywolywane przez middleware
    przy KAZDYM requeście — musi byc szybkie.

    Kolejnosc:
      1. Redis cache (< 1ms) — L1
      2. Baza danych (lazy registration jesli brak wpisu)
      3. Domyslnie True (fail-open — brak wpisu = endpoint wlaczony)
    """
    cache_key = f"{_CACHE_PFX}{endpoint_key}"

    # L1: Redis
    if redis:
        try:
            cached = await redis.get(cache_key)
            if cached is not None:
                val = cached.decode() if isinstance(cached, bytes) else str(cached)
                return val == "1"
        except Exception as exc:
            logger.debug("endpoint_registry: Redis miss: %s", exc)

    # L2: baza
    try:
        result = await db.execute(
            text(f"""
                SELECT [is_enabled]
                FROM [{_SCHEMA}].[skw_EndpointRegistry]
                WHERE [endpoint_key] = :key
            """),
            {"key": endpoint_key},
        )
        row = result.fetchone()

        if row is None:
            # Lazy registration — pierwszy raz widzimy ten endpoint
            await _lazy_register(db, endpoint_key)
            await db.commit()
            enabled = True
        else:
            enabled = bool(row[0])

        # Zapisz do cache
        if redis:
            try:
                await redis.set(cache_key, "1" if enabled else "0", ex=_CACHE_TTL)
            except Exception:
                pass

        return enabled

    except Exception as exc:
        logger.error("endpoint_registry.is_enabled: blad DB: %s", exc)
        return True  # fail-open — nie blokuj requestu przy bledzie DB


async def _lazy_register(db: AsyncSession, endpoint_key: str, label: str | None = None) -> None:
    """Rejestruje endpoint przy pierwszym wywolaniu jesli jeszcze nie istnieje."""
    from app.core.endpoint_toggle import get_label
    effective_label = label or get_label(endpoint_key)

    await db.execute(
        text(f"""
            IF NOT EXISTS (
                SELECT 1 FROM [{_SCHEMA}].[skw_EndpointRegistry]
                WHERE [endpoint_key] = :key
            )
            INSERT INTO [{_SCHEMA}].[skw_EndpointRegistry]
                ([endpoint_key], [label], [is_enabled])
            VALUES (:key, :label, 1)
        """),
        {"key": endpoint_key, "label": effective_label},
    )
    logger.debug("endpoint_registry: lazy registered %s", endpoint_key)


# =============================================================================
# Eager registration przy starcie
# =============================================================================

async def register_all(db: AsyncSession, routes: dict[str, str | None]) -> int:
    """
    Rejestruje wszystkie endpointy przy starcie aplikacji.
    Wywolywane z on_startup po scan_and_register_routes().

    Uzywa MERGE — nie nadpisuje is_enabled dla juz istniejacych wpisow
    (admin mogl wylaczac endpointy recznie — nie resetujemy stanu).

    Args:
        db:     Sesja SQLAlchemy.
        routes: Slownik {endpoint_key: label} z scan_and_register_routes().

    Returns:
        Liczba nowo wstawionych wpisow.
    """
    inserted = 0
    for endpoint_key, label in routes.items():
        try:
            result = await db.execute(
                text(f"""
                    MERGE [{_SCHEMA}].[skw_EndpointRegistry] AS target
                    USING (SELECT :key AS endpoint_key, :label AS label) AS source
                    ON target.[endpoint_key] = source.[endpoint_key]
                    WHEN NOT MATCHED THEN
                        INSERT ([endpoint_key], [label], [is_enabled])
                        VALUES (source.[endpoint_key], source.[label], 1);
                """),
                {"key": endpoint_key, "label": label},
            )
            if result.rowcount and result.rowcount > 0:
                inserted += 1
        except Exception as exc:
            logger.warning("register_all: blad dla %s: %s", endpoint_key, exc)

    await db.commit()
    logger.info("endpoint_registry.register_all: wstawiono %d nowych endpointow", inserted)
    return inserted


# =============================================================================
# Toggle — wlacz / wylacz
# =============================================================================

async def disable_endpoint(
    db: AsyncSession,
    redis: Any,
    endpoint_key: str,
    *,
    actor_id: int,
    actor_username: str,
    reason: str,
) -> dict[str, Any]:
    """
    Wylacza endpoint. Wymaga podania powodu (reason).

    Zapisuje do skw_AuditLog: kto, kiedy, jaki endpoint, powod.
    Inwaliduje cache Redis natychmiast.

    Raises:
        ValueError: endpoint nie istnieje w rejestrze.
        ValueError: endpoint juz jest wylaczony.
    """
    row = await _get_or_404(db, endpoint_key)

    if not row["is_enabled"]:
        raise ValueError(f"Endpoint '{endpoint_key}' jest juz wylaczony.")

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    await db.execute(
        text(f"""
            UPDATE [{_SCHEMA}].[skw_EndpointRegistry]
            SET [is_enabled]      = 0,
                [disabled_by]     = :uid,
                [disabled_at]     = :now,
                [disabled_reason] = :reason,
                [updated_at]      = SYSUTCDATETIME()
            WHERE [endpoint_key] = :key
        """),
        {"uid": actor_id, "now": now, "reason": reason[:500], "key": endpoint_key},
    )

    await _audit(
        db, actor_id=actor_id, actor_username=actor_username,
        action="endpoint.disabled",
        endpoint_key=endpoint_key,
        old_value={"is_enabled": True},
        new_value={"is_enabled": False, "reason": reason},
    )

    await db.commit()
    await _invalidate_cache(redis, endpoint_key)

    logger.warning(
        "endpoint_registry: WYLACZONO endpoint | key=%s actor=%s reason=%s",
        endpoint_key, actor_username, reason,
    )

    return _row_to_dict(await _get_or_404(db, endpoint_key))


async def enable_endpoint(
    db: AsyncSession,
    redis: Any,
    endpoint_key: str,
    *,
    actor_id: int,
    actor_username: str,
) -> dict[str, Any]:
    """
    Wlacza endpoint. Czyści disabled_by/disabled_at/disabled_reason.

    Raises:
        ValueError: endpoint nie istnieje w rejestrze.
        ValueError: endpoint juz jest wlaczony.
    """
    row = await _get_or_404(db, endpoint_key)

    if row["is_enabled"]:
        raise ValueError(f"Endpoint '{endpoint_key}' jest juz wlaczony.")

    await db.execute(
        text(f"""
            UPDATE [{_SCHEMA}].[skw_EndpointRegistry]
            SET [is_enabled]      = 1,
                [disabled_by]     = NULL,
                [disabled_at]     = NULL,
                [disabled_reason] = NULL,
                [updated_at]      = SYSUTCDATETIME()
            WHERE [endpoint_key] = :key
        """),
        {"key": endpoint_key},
    )

    await _audit(
        db, actor_id=actor_id, actor_username=actor_username,
        action="endpoint.enabled",
        endpoint_key=endpoint_key,
        old_value={"is_enabled": False},
        new_value={"is_enabled": True},
    )

    await db.commit()
    await _invalidate_cache(redis, endpoint_key)

    logger.info(
        "endpoint_registry: WLACZONO endpoint | key=%s actor=%s",
        endpoint_key, actor_username,
    )

    return _row_to_dict(await _get_or_404(db, endpoint_key))


# =============================================================================
# Lista endpointow dla panelu admina
# =============================================================================

async def list_endpoints(
    db: AsyncSession,
    *,
    page: int = 1,
    per_page: int = 100,
    is_enabled: bool | None = None,
    search: str | None = None,
) -> dict[str, Any]:
    """
    Lista wszystkich zarejestrowanych endpointow z filtrami.

    Filtry:
      is_enabled: True = tylko wlaczone, False = tylko wylaczone, None = wszystkie
      search:     filtr po endpoint_key lub label (LIKE)
    """
    where: list[str] = []
    params: dict[str, Any] = {}

    if is_enabled is not None:
        where.append("[is_enabled] = :enabled")
        params["enabled"] = 1 if is_enabled else 0
    if search:
        safe = search.replace("'", "''")[:100]
        where.append("([endpoint_key] LIKE :search OR [label] LIKE :search)")
        params["search"] = f"%{safe}%"

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    count_result = await db.execute(
        text(f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_EndpointRegistry] {where_sql}"),
        params,
    )
    total = count_result.scalar() or 0

    params["offset"] = (page - 1) * per_page
    params["limit"] = per_page

    result = await db.execute(
        text(f"""
            SELECT
                er.[endpoint_key], er.[label], er.[is_enabled],
                er.[disabled_by], er.[disabled_at], er.[disabled_reason],
                er.[created_at], er.[updated_at],
                u.[Username] AS disabled_by_username
            FROM [{_SCHEMA}].[skw_EndpointRegistry] er
            LEFT JOIN [{_SCHEMA}].[skw_Users] u ON u.[ID_USER] = er.[disabled_by]
            {where_sql}
            ORDER BY er.[is_enabled] ASC, er.[endpoint_key] ASC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """),
        params,
    )
    cols = list(result.keys())
    items = [_row_to_dict(dict(zip(cols, r))) for r in result.fetchall()]

    return {"items": items, "total": total, "page": page, "per_page": per_page}


# =============================================================================
# Pomocnicze
# =============================================================================

async def _get_or_404(db: AsyncSession, endpoint_key: str) -> dict[str, Any]:
    result = await db.execute(
        text(f"""
            SELECT [endpoint_key], [label], [is_enabled],
                   [disabled_by], [disabled_at], [disabled_reason],
                   [created_at], [updated_at]
            FROM [{_SCHEMA}].[skw_EndpointRegistry]
            WHERE [endpoint_key] = :key
        """),
        {"key": endpoint_key},
    )
    cols = list(result.keys())
    row = result.fetchone()
    if row is None:
        raise ValueError(f"Endpoint '{endpoint_key}' nie istnieje w rejestrze.")
    return dict(zip(cols, row))


def _row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Konwertuje wiersz z bazy — ujednolicone typy."""
    return {
        "endpoint_key":       row.get("endpoint_key"),
        "label":              row.get("label") or row.get("endpoint_key"),
        "is_enabled":         bool(row.get("is_enabled", True)),
        "disabled_by":        row.get("disabled_by"),
        "disabled_by_username": row.get("disabled_by_username"),
        "disabled_at":        row.get("disabled_at"),
        "disabled_reason":    row.get("disabled_reason"),
        "created_at":         row.get("created_at"),
        "updated_at":         row.get("updated_at"),
    }


async def _invalidate_cache(redis: Any, endpoint_key: str) -> None:
    """Natychmiastowa inwalidacja cache po toggle."""
    if not redis:
        return
    try:
        await redis.delete(f"{_CACHE_PFX}{endpoint_key}")
    except Exception as exc:
        logger.debug("_invalidate_cache: %s", exc)


async def _audit(
    db: AsyncSession,
    *,
    actor_id: int,
    actor_username: str,
    action: str,
    endpoint_key: str,
    old_value: dict,
    new_value: dict,
) -> None:
    """Zapisuje zmiane wlacznika do skw_AuditLog."""
    try:
        await db.execute(
            text(f"""
                INSERT INTO [{_SCHEMA}].[skw_AuditLog]
                    ([ID_USER], [Username], [Action], [ActionCategory],
                     [EntityType], [EntityID], [OldValue], [NewValue], [Success], [Timestamp])
                VALUES
                    (:uid, :username, :action, N'System',
                     N'EndpointRegistry', NULL,
                     :old_value, :new_value, 1, SYSUTCDATETIME())
            """),
            {
                "uid":       actor_id,
                "username":  actor_username,
                "action":    action,
                "old_value": json.dumps({"endpoint_key": endpoint_key, **old_value}, ensure_ascii=False),
                "new_value": json.dumps({"endpoint_key": endpoint_key, **new_value}, ensure_ascii=False),
            },
        )
    except Exception as exc:
        logger.error("_audit: blad zapisu AuditLog: %s", exc)