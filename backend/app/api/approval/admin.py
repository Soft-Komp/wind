# backend/app/api/approval/admin.py
"""
2 endpointy administracyjne modulu Approval.

  GET  /approval/admin/status                — pelny status modulu dla admina
  POST /approval/admin/cleanup-attachments   — czyszczenie plikow orphaned

Oba wymagaja approval.supervise.

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import orjson
from fastapi import APIRouter, HTTPException, status
from sqlalchemy import text

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.services.approval_service import _check_module_enabled

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin")
_SCHEMA = "dbo"


# =============================================================================
# GET /approval/admin/status
# =============================================================================

@router.get(
    "/status",
    summary="Pelny status modulu Approval (panel admina)",
    description=(
        "Zwraca kompletny obraz stanu modulu: "
        "flagi konfiguracyjne, liczniki instancji per status, "
        "liczba grup/sciezek/filtrow/delegacji, "
        "rozmiar wolumenu zalacznikow, "
        "status crona deadline (ostatni wpis w approval_log). "
        "**Wymaga:** `approval.supervise`."
    ),
    dependencies=[require_permission("approval.supervise")],
)
async def get_admin_status(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # ── Flagi z Redis/SystemConfig ────────────────────────────────────────────
    flags_keys = [
        "APPROVAL_MODULE_ENABLED",
        "APPROVAL_COMMENTS_ENABLED",
        "APPROVAL_ATTACHMENTS_ENABLED",
        "APPROVAL_DELEGATIONS_ENABLED",
        "APPROVAL_URGENT_MARKING_ENABLED",
        "APPROVAL_AUTO_FILTERS_ENABLED",
        "APPROVAL_STATISTICS_ENABLED",
        "APPROVAL_EMAIL_NOTIFICATIONS_ENABLED",
        "APPROVAL_EMAIL_DEBOUNCE_MINUTES",
        "APPROVAL_ESCALATION_REMINDER_DAYS",
    ]
    flags: dict = {}
    for key in flags_keys:
        try:
            cached = await redis.get(f"syscfg:{key}")
            if cached is not None:
                val = cached.decode() if isinstance(cached, bytes) else cached
                flags[key] = val
            else:
                # Fallback do DB
                row = (await db.execute(
                    text(f"SELECT TOP 1 [value] FROM [{_SCHEMA}].[skw_SystemConfig] "
                         f"WHERE [key] = :k"),
                    {"k": key},
                )).fetchone()
                flags[key] = row[0] if row else None
        except Exception:
            flags[key] = None

    module_enabled = str(flags.get("APPROVAL_MODULE_ENABLED", "false")).lower() == "true"

    # ── Liczniki instancji per status ─────────────────────────────────────────
    instances: dict = {}
    if module_enabled:
        try:
            rows = await db.execute(
                text(
                    f"SELECT [status], COUNT(*) AS cnt "
                    f"FROM [{_SCHEMA}].[skw_document_approval_instances] "
                    f"GROUP BY [status]"
                )
            )
            instances = {r[0]: r[1] for r in rows.fetchall()}
        except Exception as exc:
            logger.warning("admin_status | instances query error: %s", exc)

    # ── Infrastruktura ────────────────────────────────────────────────────────
    infra: dict = {}
    if module_enabled:
        try:
            infra_rows = await db.execute(
                text(
                    f"SELECT "
                    f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_groups] WHERE [is_active]=1) AS groups_active, "
                    f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_groups])                     AS groups_total, "
                    f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_paths] WHERE [is_active]=1)  AS paths_active, "
                    f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_paths])                      AS paths_total, "
                    f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_filters] WHERE [is_active]=1) AS filters_active, "
                    f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_filters])                    AS filters_total, "
                    f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_document_categories] WHERE [is_active]=1) AS categories_active, "
                    f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_delegations] "
                    f"   WHERE [is_active]=1 AND [valid_to]>=:now)                                   AS delegations_active, "
                    f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_group_members])              AS total_members "
                ),
                {"now": now},
            )
            r = infra_rows.fetchone()
            if r:
                infra = {
                    "groups":      {"active": r[0], "total": r[1]},
                    "paths":       {"active": r[2], "total": r[3]},
                    "filters":     {"active": r[4], "total": r[5]},
                    "categories":  {"active": r[6]},
                    "delegations": {"active": r[7]},
                    "members":     {"total": r[8]},
                }
        except Exception as exc:
            logger.warning("admin_status | infra query error: %s", exc)

    # ── Ostatni cron deadline ─────────────────────────────────────────────────
    last_deadline_cron: dict | None = None
    if module_enabled:
        try:
            cron_row = (await db.execute(
                text(
                    f"SELECT TOP 1 [logged_at], [action] "
                    f"FROM [{_SCHEMA}].[skw_approval_log] "
                    f"WHERE [action] IN (N'deadline_expired', N'deadline_warning', N'deadline_escalated') "
                    f"ORDER BY [logged_at] DESC"
                )
            )).fetchone()
            if cron_row:
                last_deadline_cron = {
                    "logged_at": cron_row[0].isoformat() if cron_row[0] else None,
                    "action":    cron_row[1],
                }
        except Exception as exc:
            logger.warning("admin_status | cron query error: %s", exc)

    # ── Wolumen zalacznikow ───────────────────────────────────────────────────
    attachments_info: dict = {}
    try:
        attachments_dir = Path(
            os.environ.get("APPROVAL_ATTACHMENTS_DIR", "/data/approval_attachments")
        )
        if attachments_dir.exists():
            total_size   = sum(f.stat().st_size for f in attachments_dir.rglob("*") if f.is_file())
            total_files  = sum(1 for f in attachments_dir.rglob("*") if f.is_file())
            attachments_info = {
                "path":         str(attachments_dir),
                "total_files":  total_files,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
            }
        else:
            attachments_info = {
                "path":   str(attachments_dir),
                "exists": False,
            }
    except Exception as exc:
        attachments_info = {"error": str(exc)}

    # ── Orphaned pliki (soft-deleted w DB ale plik istnieje) ──────────────────
    orphaned_count = 0
    if module_enabled:
        try:
            orphaned_row = (await db.execute(
                text(
                    f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_attachments] "
                    f"WHERE [is_deleted]=1 AND [file_path] IS NOT NULL"
                )
            )).scalar()
            orphaned_count = orphaned_row or 0
        except Exception:
            pass

    logger.info(
        "admin_status | user=%d module_enabled=%s instances=%s",
        current_user.ID_USER, module_enabled, instances,
    )

    return {
        "ts":                now.isoformat(),
        "module_enabled":    module_enabled,
        "flags":             flags,
        "instances": {
            "by_status":     instances,
            "total":         sum(instances.values()),
            "in_progress":   instances.get("in_progress", 0),
            "pending":       instances.get("pending_dispatch", 0),
            "approved":      instances.get("approved", 0),
            "rejected":      instances.get("rejected", 0),
            "cancelled":     instances.get("cancelled", 0),
        },
        "infrastructure":    infra,
        "last_deadline_cron": last_deadline_cron,
        "attachments": {
            **attachments_info,
            "orphaned_db_records": orphaned_count,
        },
    }


# =============================================================================
# POST /approval/admin/cleanup-attachments
# =============================================================================

@router.post(
    "/cleanup-attachments",
    summary="Usun fizyczne pliki soft-deleted zalacznikow",
    description=(
        "Usuwa pliki z dysku dla rekordow w `skw_approval_attachments` "
        "gdzie `is_deleted=1`. Bezpieczne — nie usuwa rekordow z bazy, "
        "tylko pliki fizyczne. "
        "\n\nDomyslnie `dry_run=true` — zwraca liste plikow do usuniecia bez "
        "wykonywania operacji. Ustaw `dry_run=false` aby faktycznie usunac. "
        "\n\n**Wymaga:** `approval.supervise`."
    ),
    responses={
        200: {"description": "Wynik operacji cleanup"},
        503: {"description": "Modul wylaczony"},
    },
    dependencies=[require_permission("approval.supervise")],
)
async def cleanup_attachments(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    dry_run: bool = True,
):
    await _check_module_enabled(db, redis)

    # Pobierz rekordy soft-deleted z niepustym file_path
    rows = await db.execute(
        text(
            f"SELECT [id_attachment], [id_instance], [file_path], [file_name], "
            f"  [file_size], [deleted_at] "
            f"FROM [{_SCHEMA}].[skw_approval_attachments] "
            f"WHERE [is_deleted]=1 "
            f"  AND [file_path] IS NOT NULL "
            f"  AND LEN([file_path]) > 0 "
            f"ORDER BY [deleted_at] ASC"
        )
    )
    records = rows.fetchall()

    deleted   = []
    skipped   = []
    not_found = []
    errors    = []
    freed_bytes = 0

    for r in records:
        id_attachment, id_instance, file_path, file_name, file_size, deleted_at = r
        p = Path(file_path) if file_path else None

        entry = {
            "id_attachment": id_attachment,
            "id_instance":   id_instance,
            "file_name":     file_name,
            "file_path":     file_path,
            "file_size":     file_size,
            "deleted_at":    deleted_at.isoformat() if deleted_at else None,
        }

        if p is None or not p.exists():
            not_found.append(entry)
            # Wyczysc file_path w DB — plik i tak nie istnieje
            if not dry_run:
                try:
                    await db.execute(
                        text(f"UPDATE [{_SCHEMA}].[skw_approval_attachments] "
                             f"SET [file_path]=NULL WHERE [id_attachment]=:a"),
                        {"a": id_attachment},
                    )
                except Exception:
                    pass
            continue

        if dry_run:
            deleted.append(entry)
            freed_bytes += file_size or 0
            continue

        # Faktyczne usuniecie
        try:
            file_bytes = p.stat().st_size
            p.unlink()
            freed_bytes += file_bytes

            # Wyczysc file_path w DB po usunieciu
            await db.execute(
                text(f"UPDATE [{_SCHEMA}].[skw_approval_attachments] "
                     f"SET [file_path]=NULL WHERE [id_attachment]=:a"),
                {"a": id_attachment},
            )
            deleted.append({**entry, "freed_bytes": file_bytes})

        except PermissionError as exc:
            errors.append({**entry, "error": f"Brak uprawnien: {exc}"})
        except Exception as exc:
            errors.append({**entry, "error": str(exc)})

    if not dry_run and (deleted or not_found):
        await db.commit()

    logger.warning(
        orjson.dumps({
            "event":           "approval_cleanup_attachments",
            "dry_run":         dry_run,
            "deleted_count":   len(deleted),
            "not_found_count": len(not_found),
            "errors_count":    len(errors),
            "freed_mb":        round(freed_bytes / (1024 * 1024), 2),
            "executed_by":     current_user.ID_USER,
            "ts":              datetime.now(timezone.utc).isoformat(),
        }).decode()
    )

    return {
        "dry_run":       dry_run,
        "summary": {
            "total_records":   len(records),
            "deleted":         len(deleted),
            "not_found_on_disk": len(not_found),
            "skipped":         len(skipped),
            "errors":          len(errors),
            "freed_mb":        round(freed_bytes / (1024 * 1024), 2),
        },
        "files_deleted":   deleted   if not dry_run else [],
        "files_to_delete": deleted   if dry_run     else [],
        "not_found":       not_found,
        "errors":          errors,
        "message": (
            f"DRY RUN — {len(deleted)} plikow do usuniecia ({round(freed_bytes/1024/1024,2)} MB). "
            "Wywolaj z dry_run=false aby wykonac operacje."
            if dry_run else
            f"Usunieto {len(deleted)} plikow, zwolniono {round(freed_bytes/1024/1024,2)} MB."
        ),
    }