# backend/app/api/approval/stats.py
"""
5 endpointow — statystyki i raporty modulu obiegu.

  GET /approval/stats/overview
  GET /approval/stats/paths
  GET /approval/stats/groups
  GET /approval/stats/users
  GET /approval/reports/approved       — JSON lub CSV (Accept: text/csv)

503 jesli APPROVAL_STATISTICS_ENABLED=false.
Wszystkie endpointy uzywaja approval.supervise (approval.stats nie
istnieje w migracji 0028 — uzywamy najblizszego semantycznie uprawnienia).
UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import csv
import io
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.services.approval_service import _check_module_enabled, _check_feature_flag

logger = logging.getLogger(__name__)
router = APIRouter()
_SCHEMA = "dbo"


def _now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@router.get(
    "/stats/overview",
    summary="Podsumowanie globalne modulu obiegu",
    description=(
        "Liczniki: total instancji, per status, sredni czas (godziny), "
        "aktywne grupy/sciezki/delegacje. Zakres: ostatnie N dni. "
        "**503** jesli APPROVAL_STATISTICS_ENABLED=false."
    ),
    dependencies=[require_permission("approval.supervise")],
)
async def get_overview(
    current_user: CurrentUser, db: DB, redis: RedisClient,
    days: int = Query(30, ge=1, le=365),
):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_STATISTICS_ENABLED",
                               error_msg="Statystyki sa wylaczone.")
    since = _now_naive() - timedelta(days=days)

    status_counts = await db.execute(
        text(f"SELECT [status],COUNT(*) FROM [{_SCHEMA}].[skw_document_approval_instances] "
             f"WHERE [created_at]>=:s GROUP BY [status]"),
        {"s": since},
    )
    by_status = {r[0]: r[1] for r in status_counts.fetchall()}

    avg_hours = (await db.execute(
        text(
            f"SELECT AVG(DATEDIFF(HOUR,[dispatched_at],[completed_at])) "
            f"FROM [{_SCHEMA}].[skw_document_approval_instances] "
            f"WHERE [status]=N'approved' AND [completed_at] IS NOT NULL "
            f"  AND [created_at]>=:s"
        ),
        {"s": since},
    )).scalar()

    now = _now_naive()
    return {
        "period_days":           days,
        "since":                 since.isoformat(),
        "by_status":             by_status,
        "total":                 sum(by_status.values()),
        "avg_completion_hours":  float(avg_hours) if avg_hours else None,
        "active_groups":         (await db.execute(
            text(f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_groups] WHERE [is_active]=1")
        )).scalar(),
        "active_paths":          (await db.execute(
            text(f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_paths] WHERE [is_active]=1")
        )).scalar(),
        "active_delegations":    (await db.execute(
            text(f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_delegations] "
                 f"WHERE [is_active]=1 AND [valid_to]>=:now"),
            {"now": now},
        )).scalar(),
    }


@router.get(
    "/stats/paths",
    summary="Statystyki per sciezka akceptacyjna",
    description="Liczba uzyc, sredni czas, procent zaakceptowanych. **503** jesli APPROVAL_STATISTICS_ENABLED=false.",
    dependencies=[require_permission("approval.supervise")],
)
async def get_paths_stats(
    current_user: CurrentUser, db: DB, redis: RedisClient,
    days: int = Query(30, ge=1, le=365),
):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_STATISTICS_ENABLED",
                               error_msg="Statystyki sa wylaczone.")
    since = _now_naive() - timedelta(days=days)
    rows = await db.execute(
        text(
            f"SELECT p.[id_path],p.[path_name],"
            f"  COUNT(i.[id_instance]) AS total_uses,"
            f"  SUM(CASE WHEN i.[status]=N'approved' THEN 1 ELSE 0 END) AS approved_count,"
            f"  SUM(CASE WHEN i.[status]=N'rejected' THEN 1 ELSE 0 END) AS rejected_count,"
            f"  AVG(CASE WHEN i.[completed_at] IS NOT NULL "
            f"      THEN DATEDIFF(HOUR,i.[dispatched_at],i.[completed_at]) END) AS avg_hours "
            f"FROM [{_SCHEMA}].[skw_approval_paths] p "
            f"LEFT JOIN [{_SCHEMA}].[skw_document_approval_instances] i "
            f"  ON i.[id_path]=p.[id_path] AND i.[created_at]>=:s "
            f"GROUP BY p.[id_path],p.[path_name] ORDER BY total_uses DESC"
        ),
        {"s": since},
    )
    return {
        "period_days": days,
        "data": [
            {
                "id_path": r[0], "path_name": r[1],
                "total_uses": r[2] or 0, "approved": r[3] or 0, "rejected": r[4] or 0,
                "avg_hours": float(r[5]) if r[5] else None,
                "approval_rate": round(100*(r[3] or 0)/r[2], 1) if r[2] else None,
            }
            for r in rows.fetchall()
        ],
    }


@router.get(
    "/stats/groups",
    summary="Statystyki per grupa akceptacyjna",
    description="Liczba akceptacji, obsluzone instancje, liczba czlonkow. **503** jesli APPROVAL_STATISTICS_ENABLED=false.",
    dependencies=[require_permission("approval.supervise")],
)
async def get_groups_stats(
    current_user: CurrentUser, db: DB, redis: RedisClient,
    days: int = Query(30, ge=1, le=365),
):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_STATISTICS_ENABLED",
                               error_msg="Statystyki sa wylaczone.")
    since = _now_naive() - timedelta(days=days)
    rows = await db.execute(
        text(
            f"SELECT g.[id_group],g.[group_name],g.[consensus_type],"
            f"  COUNT(DISTINCT l.[id_instance]) AS instances_handled,"
            f"  COUNT(l.[id_log]) AS total_votes,"
            f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_group_members] m "
            f"   WHERE m.[id_group]=g.[id_group]) AS member_count "
            f"FROM [{_SCHEMA}].[skw_approval_groups] g "
            f"LEFT JOIN [{_SCHEMA}].[skw_approval_log] l "
            f"  ON l.[id_group_snapshot]=g.[id_group] AND l.[action]=N'accepted' "
            f"  AND l.[is_voided]=0 AND l.[logged_at]>=:s "
            f"WHERE g.[is_active]=1 "
            f"GROUP BY g.[id_group],g.[group_name],g.[consensus_type] "
            f"ORDER BY instances_handled DESC"
        ),
        {"s": since},
    )
    return {
        "period_days": days,
        "data": [
            {"id_group": r[0], "group_name": r[1], "consensus_type": r[2],
             "instances_handled": r[3] or 0, "total_votes": r[4] or 0,
             "member_count": r[5] or 0}
            for r in rows.fetchall()
        ],
    }


@router.get(
    "/stats/users",
    summary="Ranking uzytkownikow — aktywnosc akceptacyjna",
    description="Liczba akcji per user (accept, reject, rollback). **503** jesli APPROVAL_STATISTICS_ENABLED=false.",
    dependencies=[require_permission("approval.supervise")],
)
async def get_users_stats(
    current_user: CurrentUser, db: DB, redis: RedisClient,
    days: int = Query(30, ge=1, le=365),
    top:  int = Query(20, ge=1, le=100),
):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_STATISTICS_ENABLED",
                               error_msg="Statystyki sa wylaczone.")
    since = _now_naive() - timedelta(days=days)
    rows = await db.execute(
        text(
            f"SELECT TOP (:top) l.[id_user],l.[username_snapshot],"
            f"  SUM(CASE WHEN l.[action]=N'accepted' THEN 1 ELSE 0 END) AS accepted,"
            f"  SUM(CASE WHEN l.[action]=N'rejected' THEN 1 ELSE 0 END) AS rejected,"
            f"  SUM(CASE WHEN l.[action]=N'rollback' THEN 1 ELSE 0 END) AS rollbacks,"
            f"  COUNT(l.[id_log]) AS total_actions "
            f"FROM [{_SCHEMA}].[skw_approval_log] l "
            f"WHERE l.[logged_at]>=:s AND l.[id_user] IS NOT NULL AND l.[is_voided]=0 "
            f"GROUP BY l.[id_user],l.[username_snapshot] "
            f"ORDER BY total_actions DESC"
        ),
        {"s": since, "top": top},
    )
    return {
        "period_days": days,
        "data": [
            {"id_user": r[0], "username": r[1], "accepted": r[2],
             "rejected": r[3], "rollbacks": r[4], "total": r[5]}
            for r in rows.fetchall()
        ],
    }


@router.get(
    "/reports/approved",
    summary="Raport zaakceptowanych dokumentow",
    description=(
        "Lista zaakceptowanych dokumentow. "
        "Format: JSON (domyslnie) lub CSV (`Accept: text/csv` lub `?format=csv`). "
        "CSV: id_instance, id_document, source_name, document_title, document_amount, "
        "dispatched_by, dispatched_at, completed_at, path_name. "
        "**503** jesli APPROVAL_STATISTICS_ENABLED=false."
    ),
    dependencies=[require_permission("approval.supervise")],
)
async def report_approved(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request: Request,
    days:    int           = Query(30, ge=1, le=365),
    format:  Optional[str] = Query(None, description="csv lub json"),
):
    await _check_module_enabled(db, redis)
    await _check_feature_flag(db, redis, "APPROVAL_STATISTICS_ENABLED",
                               error_msg="Statystyki sa wylaczone.")
    since = _now_naive() - timedelta(days=days)

    rows = await db.execute(
        text(
            f"SELECT i.[id_instance],i.[id_document],ds.[source_name],"
            f"  i.[document_title],i.[document_amount],"
            f"  u.[Username],i.[dispatched_at],i.[completed_at],p.[path_name] "
            f"FROM [{_SCHEMA}].[skw_document_approval_instances] i "
            f"LEFT JOIN [{_SCHEMA}].[skw_document_sources] ds ON ds.[id_source]=i.[id_source] "
            f"LEFT JOIN [{_SCHEMA}].[skw_Users] u ON u.[ID_USER]=i.[dispatched_by] "
            f"LEFT JOIN [{_SCHEMA}].[skw_approval_paths] p ON p.[id_path]=i.[id_path] "
            f"WHERE i.[status]=N'approved' AND i.[completed_at]>=:s "
            f"ORDER BY i.[completed_at] DESC"
        ),
        {"s": since},
    )
    data = rows.fetchall()

    want_csv = (
        format == "csv"
        or request.headers.get("Accept", "").strip().lower() == "text/csv"
    )

    if want_csv:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "id_instance", "id_document", "source_name", "document_title",
            "document_amount", "dispatched_by", "dispatched_at", "completed_at", "path_name",
        ])
        for r in data:
            writer.writerow([
                r[0], r[1], r[2] or "", r[3] or "",
                str(r[4]) if r[4] is not None else "",
                r[5] or "",
                r[6].isoformat() if r[6] else "",
                r[7].isoformat() if r[7] else "",
                r[8] or "",
            ])
        output.seek(0)
        filename = f"approval_report_{since.strftime('%Y%m%d')}_{days}d.csv"
        return StreamingResponse(
            iter([output.read()]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return {
        "period_days": days,
        "total": len(data),
        "data": [
            {
                "id_instance":    r[0], "id_document":   r[1], "source_name":    r[2],
                "document_title": r[3],
                "document_amount": float(r[4]) if r[4] else None,
                "dispatched_by":  r[5],
                "dispatched_at":  r[6].isoformat() if r[6] else None,
                "completed_at":   r[7].isoformat() if r[7] else None,
                "path_name":      r[8],
            }
            for r in data
        ],
    }


@router.get(
    "/stats/my-performance",
    summary="Moje statystyki akceptacyjne",
    description=(
        "Statystyki zalogowanego uzytkownika: "
        "ile zaakceptowal/odrzucil, sredni czas reakcji (minuty), "
        "ranking w grupach do ktorych nalezy. "
        "**Nie wymaga** `approval.supervise` — kazdy user widzi swoje dane."
    ),
    dependencies=[require_permission("approval.accept")],
)
async def get_my_performance(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    days: int = Query(30, ge=1, le=365),
):
    await _check_module_enabled(db, redis)
    from datetime import datetime, timedelta, timezone
    since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)
    uid   = current_user.ID_USER
 
    # Akcje usera
    actions_row = (await db.execute(
        text(
            f"SELECT "
            f"  SUM(CASE WHEN l.[action]=N'accepted' THEN 1 ELSE 0 END) AS accepted, "
            f"  SUM(CASE WHEN l.[action]=N'rejected' THEN 1 ELSE 0 END) AS rejected, "
            f"  SUM(CASE WHEN l.[action]=N'rollback' THEN 1 ELSE 0 END) AS rollbacks, "
            f"  COUNT(DISTINCT l.[id_instance]) AS unique_instances "
            f"FROM [{_SCHEMA}].[skw_approval_log] l "
            f"WHERE l.[id_user]=:u AND l.[logged_at]>=:s AND l.[is_voided]=0"
        ),
        {"u": uid, "s": since},
    )).fetchone()
 
    # Sredni czas od pojawienia sie dokumentu w grupie do glosu usera (minuty)
    avg_response = (await db.execute(
        text(
            f"SELECT AVG(DATEDIFF(MINUTE, snap.[created_at], l.[logged_at])) "
            f"FROM [{_SCHEMA}].[skw_approval_log] l "
            f"JOIN [{_SCHEMA}].[skw_document_approval_snapshot_steps] snap "
            f"     ON snap.[id_instance]=l.[id_instance] "
            f"     AND snap.[step_order]=l.[step_order_snapshot] "
            f"WHERE l.[id_user]=:u AND l.[action]=N'accepted' "
            f"  AND l.[logged_at]>=:s AND l.[is_voided]=0"
        ),
        {"u": uid, "s": since},
    )).scalar()
 
    # Grupy do ktorych nalezy + liczba oczekujacych na akcje
    groups_rows = await db.execute(
        text(
            f"SELECT g.[id_group], g.[group_name], g.[consensus_type], "
            f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] s "
            f"   JOIN [{_SCHEMA}].[skw_document_approval_instances] i "
            f"        ON i.[id_instance]=s.[id_instance] "
            f"   WHERE s.[id_group]=g.[id_group] AND s.[status]=N'in_progress' "
            f"     AND i.[status]=N'in_progress') AS pending_in_group "
            f"FROM [{_SCHEMA}].[skw_approval_groups] g "
            f"JOIN [{_SCHEMA}].[skw_approval_group_members] m ON m.[id_group]=g.[id_group] "
            f"WHERE m.[id_user]=:u AND g.[is_active]=1 "
            f"ORDER BY pending_in_group DESC"
        ),
        {"u": uid},
    )
    groups = [
        {
            "id_group":      r[0],
            "group_name":    r[1],
            "consensus_type": r[2],
            "pending_in_group": r[3],
        }
        for r in groups_rows.fetchall()
    ]
 
    return {
        "period_days": days,
        "id_user":     uid,
        "accepted":    actions_row[0] or 0,
        "rejected":    actions_row[1] or 0,
        "rollbacks":   actions_row[2] or 0,
        "unique_instances": actions_row[3] or 0,
        "avg_response_minutes": float(avg_response) if avg_response else None,
        "my_groups": groups,
        "total_pending_for_me": sum(g["pending_in_group"] for g in groups),
    }
 
