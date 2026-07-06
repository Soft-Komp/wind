# backend/app/services/source_role_access_service.py
"""
Serwis dwupoziomowej kontroli dostepu do zrodel dokumentow — TODO-05, TODO-06.

Poziom 1 (ten serwis): skw_source_role_access
  → "Czy uzytkownik w ogole widzi to zrodlo?"
  → Kontrola per rola

Poziom 2 (istniejacy, documents_service._build_visibility_clause):
  → "Ktore dokumenty w ramach tego zrodla widzi?"
  → Kontrola per filtr (visibility_mode=restricted)

BYPASS: uprawnienia approval.supervise lub documents.view_all pomijaja oba poziomy.

Cache Redis TTL 300s:
  source_roles:{id_source} — lista id_role z dostepem
  user_sources:{id_user}   — lista id_source dostepnych dla usera

Invalidacja: przy kazdej zmianie przypisania scan_iter("user_sources:*")
(wszystkich userow, bo zmiana roli zrodla dotyka wielu userow jednoczesnie).

UWAGA: from __future__ import annotations OK (nie ORM, nie router).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_SCHEMA         = "dbo"
_CACHE_TTL      = 300  # sekund
_BYPASS_PERMS   = {"approval.supervise", "documents.view_all"}


# =============================================================================
# Sprawdzenie dostepu (hot path)
# =============================================================================

async def user_can_access_source(
    db: AsyncSession,
    redis: Any,
    *,
    id_user: int,
    id_role: int,
    id_source: int,
    user_permissions: set[str] | None = None,
) -> bool:
    """
    Sprawdza czy uzytkownik ma dostep do zrodla (poziom 1).

    Args:
        user_permissions: uprawnienia usera (z JWT lub cache) — jesli zawiera
                          approval.supervise lub documents.view_all, dostep zawsze True.

    Returns:
        True jesli dostep dozwolony.
    """
    # Bypass dla supervisorow
    if user_permissions and user_permissions & _BYPASS_PERMS:
        return True

    # Sprawdz cache per-user
    cache_key = f"user_sources:{id_user}"
    if redis:
        try:
            cached = await redis.get(cache_key)
            if cached is not None:
                allowed_sources = json.loads(
                    cached.decode() if isinstance(cached, bytes) else cached
                )
                return id_source in allowed_sources
        except Exception as exc:
            logger.debug("source_role_access cache miss: %s", exc)

    # Pobierz z bazy
    allowed_sources = await _get_accessible_sources_for_user(db, id_user, id_role)

    # Zapisz do cache
    if redis:
        try:
            await redis.set(
                cache_key,
                json.dumps(allowed_sources),
                ex=_CACHE_TTL,
            )
        except Exception:
            pass

    return id_source in allowed_sources


async def get_accessible_source_ids(
    db: AsyncSession,
    redis: Any,
    *,
    id_user: int,
    id_role: int,
    user_permissions: set[str] | None = None,
) -> list[int] | None:
    """
    Zwraca liste id_source dostepnych dla usera, lub None jesli ma pelny dostep.

    None oznacza brak filtru (supervisor/view_all) — uzywane w WHERE id_source IN (...).
    Pusta lista [] = uzytkownik nie ma dostepu do zadnego zrodla.
    """
    if user_permissions and user_permissions & _BYPASS_PERMS:
        return None  # brak filtru — widzi wszystko

    cache_key = f"user_sources:{id_user}"
    if redis:
        try:
            cached = await redis.get(cache_key)
            if cached is not None:
                return json.loads(
                    cached.decode() if isinstance(cached, bytes) else cached
                )
        except Exception:
            pass

    allowed = await _get_accessible_sources_for_user(db, id_user, id_role)

    if redis:
        try:
            await redis.set(cache_key, json.dumps(allowed), ex=_CACHE_TTL)
        except Exception:
            pass

    return allowed


async def _get_accessible_sources_for_user(
    db: AsyncSession,
    id_user: int,
    id_role: int,
) -> list[int]:
    """Pobiera z bazy liste id_source dostepnych dla roli usera."""
    try:
        result = await db.execute(
            text(f"""
                SELECT DISTINCT sra.[id_source]
                FROM [{_SCHEMA}].[skw_source_role_access] sra
                WHERE sra.[id_role] = :role_id
            """),
            {"role_id": id_role},
        )
        return [r[0] for r in result.fetchall()]
    except Exception as exc:
        logger.error("_get_accessible_sources_for_user blad: %s", exc)
        return []


# =============================================================================
# CRUD przypisania rol do zrodel
# =============================================================================

async def list_roles_for_source(db: AsyncSession, id_source: int) -> list[dict[str, Any]]:
    """Lista rol z dostepem do danego zrodla."""
    result = await db.execute(
        text(f"""
            SELECT sra.[id_role], r.[RoleName], sra.[created_at],
                   u.[Username] AS created_by_username
            FROM [{_SCHEMA}].[skw_source_role_access] sra
            JOIN [{_SCHEMA}].[skw_Roles] r ON r.[ID_ROLE] = sra.[id_role]
            LEFT JOIN [{_SCHEMA}].[skw_Users] u ON u.[ID_USER] = sra.[created_by]
            WHERE sra.[id_source] = :s
            ORDER BY r.[RoleName] ASC
        """),
        {"s": id_source},
    )
    cols = list(result.keys())
    return [dict(zip(cols, r)) for r in result.fetchall()]


async def add_role_to_source(
    db: AsyncSession,
    redis: Any,
    id_source: int,
    id_role: int,
    *,
    actor_id: int,
) -> dict[str, Any]:
    """
    Przypisuje role do zrodla. Idempotentne — brak bledu jesli juz przypisana.
    Invaliduje cache Redis dla wszystkich userow (scan_iter).
    """
    await db.execute(
        text(f"""
            IF NOT EXISTS (
                SELECT 1 FROM [{_SCHEMA}].[skw_source_role_access]
                WHERE [id_source] = :s AND [id_role] = :r
            )
            INSERT INTO [{_SCHEMA}].[skw_source_role_access]
                ([id_source], [id_role], [created_by])
            VALUES (:s, :r, :actor)
        """),
        {"s": id_source, "r": id_role, "actor": actor_id},
    )
    await db.commit()

    await _invalidate_user_sources_cache(redis)

    logger.info(
        "source_role_access: dodano role | id_source=%s id_role=%s actor=%s",
        id_source, id_role, actor_id,
    )
    roles = await list_roles_for_source(db, id_source)
    return next((r for r in roles if r["id_role"] == id_role), {})


async def remove_role_from_source(
    db: AsyncSession,
    redis: Any,
    id_source: int,
    id_role: int,
    *,
    actor_id: int,
) -> None:
    """
    Usuwa role z dostepem do zrodla.
    Invaliduje cache Redis dla wszystkich userow.
    """
    await db.execute(
        text(f"""
            DELETE FROM [{_SCHEMA}].[skw_source_role_access]
            WHERE [id_source] = :s AND [id_role] = :r
        """),
        {"s": id_source, "r": id_role},
    )
    await db.commit()

    await _invalidate_user_sources_cache(redis)

    logger.warning(
        "source_role_access: usunieto role | id_source=%s id_role=%s actor=%s",
        id_source, id_role, actor_id,
    )


async def list_accessible_sources_for_user(
    db: AsyncSession,
    redis: Any,
    *,
    id_user: int,
    id_role: int,
    user_permissions: set[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Lista zrodel dostepnych dla uzytkownika (GET /sources/my-accessible).
    Supervisor widzi wszystkie aktywne zrodla.
    """
    if user_permissions and user_permissions & _BYPASS_PERMS:
        result = await db.execute(
            text(f"""
                SELECT [id_source], [source_name], [source_type], [is_test_mode]
                FROM [{_SCHEMA}].[skw_document_sources]
                WHERE [is_active] = 1
                ORDER BY [source_name] ASC
            """)
        )
    else:
        result = await db.execute(
            text(f"""
                SELECT s.[id_source], s.[source_name], s.[source_type], s.[is_test_mode]
                FROM [{_SCHEMA}].[skw_document_sources] s
                JOIN [{_SCHEMA}].[skw_source_role_access] sra ON sra.[id_source] = s.[id_source]
                WHERE sra.[id_role] = :role_id AND s.[is_active] = 1
                ORDER BY s.[source_name] ASC
            """),
            {"role_id": id_role},
        )

    cols = list(result.keys())
    return [dict(zip(cols, r)) for r in result.fetchall()]


async def _invalidate_user_sources_cache(redis: Any) -> None:
    """Invaliduje cache user_sources:* dla wszystkich userow."""
    if not redis:
        return
    try:
        async for key in redis.scan_iter("user_sources:*"):
            await redis.delete(key)
        logger.debug("source_role_access: zinwalidowano cache user_sources:*")
    except Exception as exc:
        logger.warning("source_role_access: blad invalidacji cache: %s", exc)