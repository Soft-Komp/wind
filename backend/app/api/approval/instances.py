# backend/app/api/approval/instances.py
"""
Endpointy glownego obiegu dokumentow.

13 endpointow:
  GET  /approval/dispatch-queue
  GET  /approval/my-queue
  POST /approval/dispatch
  GET  /approval/instances/{id_instance}
  POST /approval/instances/{id_instance}/accept
  POST /approval/instances/{id_instance}/rollback
  POST /approval/instances/{id_instance}/reject
  POST /approval/instances/{id_instance}/cancel
  POST /approval/instances/{id_instance}/forward
  POST /approval/instances/{id_instance}/send-to-group
  POST /approval/instances/{id_instance}/mark-urgent
  GET  /approval/instances/{id_instance}/history
  GET  /approval/instances/{id_instance}/snapshot

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text
from app.schemas.common import BaseResponse, dt_utc

from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.services.approval_service import (
    _check_module_enabled,
    dispatch,
    accept,
    rollback,
    reject,
    cancel,
    mark_urgent,
)
from app.services import audit_service

from app.services.approval_service import rollback as do_rollback
from app.services.approval_service_ext import forward, send_to_group
from app.schemas.unified_document import get_adapter_by_source_id
from app.services.filter_engine import resolve_path

logger = logging.getLogger(__name__)
router = APIRouter()
_SCHEMA = "dbo"


# =============================================================================
# Schematy Pydantic
# =============================================================================

class DispatchBody(BaseModel):
    id_document:  str           = Field(..., min_length=1, max_length=100)
    id_source:    int           = Field(..., gt=0)
    id_path:      Optional[int] = Field(None, gt=0)
    id_category:  Optional[int] = Field(None, gt=0)
    comment:      Optional[str] = Field(None, max_length=1000)

    @field_validator("id_document")
    @classmethod
    def strip_doc(cls, v: str) -> str:
        return v.strip()


class AcceptBody(BaseModel):
    comment: Optional[str] = Field(None, max_length=1000)


class RollbackBody(BaseModel):
    comment: str = Field(..., min_length=10, max_length=1000,
                         description="Wymagany, min. 10 znakow")


class RejectBody(BaseModel):
    comment: str = Field(..., min_length=5, max_length=1000)


class CancelBody(BaseModel):
    comment: Optional[str] = Field(None, max_length=1000)


class ForwardBody(BaseModel):
    id_target_group: int           = Field(..., gt=0)
    comment:         str           = Field(..., min_length=5, max_length=1000)
    deadline_hours:  Optional[int] = Field(None, gt=0, le=8760)


class SendToGroupBody(BaseModel):
    id_target_group: int           = Field(..., gt=0)
    comment:         str           = Field(..., min_length=5, max_length=1000)
    deadline_hours:  Optional[int] = Field(None, gt=0, le=8760)


class MarkUrgentBody(BaseModel):
    is_urgent: bool


# =============================================================================
# DISPATCH QUEUE
# =============================================================================

@router.get(
    "/dispatch-queue",
    summary="Kolejka dyspozytora — dokumenty pending_dispatch",
    description=(
        "Zwraca dokumenty ze statusem `pending_dispatch`. "
        "Sortowanie: `is_urgent DESC, created_at ASC`. "
        "Dane z widoku `skw_v_approval_dispatch_queue`. "
        "**Wymaga:** `approval.dispatch`"
    ),
    responses={503: {"description": "Modul obiegu wylaczony"}},
    dependencies=[require_permission("approval.dispatch")],
)
async def get_dispatch_queue(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    page:      int          = Query(1, ge=1),
    per_page:  int          = Query(25, ge=1, le=100),
    is_urgent: Optional[bool] = Query(None),
    id_source: Optional[int]  = Query(None),
):
    await _check_module_enabled(db, redis)
    offset = (page - 1) * per_page
    where = ["1=1"]
    params: dict = {"limit": per_page, "offset": offset}
    if is_urgent is not None:
        where.append("v.[is_urgent] = :urgent")
        params["urgent"] = 1 if is_urgent else 0
    if id_source is not None:
        where.append("v.[id_source] = :src")
        params["src"] = id_source

    w = " AND ".join(where)

    total = (await db.execute(
        text(f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_v_approval_dispatch_queue] v WHERE {w}"),
        params,
    )).scalar() or 0

    rows = await db.execute(
        text(
            f"SELECT v.[id_instance], v.[id_document], v.[id_source], v.[source_name], "
            f"  v.[status], v.[is_urgent], v.[current_step], v.[document_title], "
            f"  v.[document_amount], v.[deadline_at], v.[instance_created_at], "
            f"  v.[fakir_numer], v.[fakir_wartosc_brutto], v.[fakir_kontrahent], "
            f"  v.[fakir_data_wystawienia], v.[fakir_termin_platnosci] "
            f"FROM [{_SCHEMA}].[skw_v_approval_dispatch_queue] v "
            f"WHERE {w} "
            f"ORDER BY v.[is_urgent] DESC, v.[instance_created_at] ASC "
            f"OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY"
        ),
        params,
    )
    items = []
    for r in rows.fetchall():
        items.append({
            "id_instance":      r[0], "id_document":      r[1],
            "id_source":        r[2], "source_name":      r[3],
            "status":           r[4], "is_urgent":        bool(r[5]),
            "current_step":     r[6], "document_title":   r[7],
            "document_amount":  float(r[8]) if r[8] is not None else None,
            "deadline_at":      dt_utc(r[9]),
            "created_at":       dt_utc(r[10]),
            "fakir_numer":      r[11],
            "fakir_brutto":     float(r[12]) if r[12] is not None else None,
            "fakir_kontrahent": r[13],
            "fakir_data_wyst":  str(r[14]) if r[14] else None,
            "fakir_termin":     str(r[15]) if r[15] else None,
        })
    return {"total": total, "page": page, "per_page": per_page, "data": items}


# =============================================================================
# MY QUEUE
# =============================================================================

@router.get(
    "/my-queue",
    summary="Moja kolejka akceptacyjna",
    description=(
        "Dokumenty czekajace na akcje zalogowanego uzytkownika. "
        "Sekcje: `own_queue` (bezposrednie czlonkostwo) i `delegated_queue` (delegacje). "
        "Sortowanie: `is_urgent DESC, created_at ASC`. "
        "Widok: `skw_v_approval_my_queue`."
    ),
    dependencies=[require_permission("approval.accept")],
)
async def get_my_queue(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    await _check_module_enabled(db, redis)
    user_id = current_user.id_user

    rows = await db.execute(
        text(
            f"SELECT v.[id_instance], v.[id_document], v.[id_source], v.[source_name], "
            f"  v.[status], v.[is_urgent], v.[current_step], v.[document_title], "
            f"  v.[document_amount], v.[deadline_at], v.[instance_created_at], "
            f"  v.[snapshot_id], v.[id_group], v.[group_name], v.[consensus_type], "
            f"  v.[votes_cast], v.[votes_required], v.[step_deadline], "
            f"  v.[via_delegation], v.[id_delegation], v.[delegated_from_id], "
            f"  v.[member_id_user], "
            f"  v.[fakir_numer], v.[fakir_wartosc_brutto], v.[fakir_kontrahent] "
            f"FROM [{_SCHEMA}].[skw_v_approval_my_queue] v "
            f"WHERE v.[authorized_id_user] = :uid "
            f"ORDER BY v.[is_urgent] DESC, v.[instance_created_at] ASC"
        ),
        {"uid": user_id},
    )
    own, delegated = [], []
    for r in rows.fetchall():
        item = {
            "id_instance":     r[0],  "id_document":     r[1],
            "id_source":       r[2],  "source_name":     r[3],
            "status":          r[4],  "is_urgent":       bool(r[5]),
            "current_step":    r[6],  "document_title":  r[7],
            "document_amount": float(r[8]) if r[8] is not None else None,
            "deadline_at":     dt_utc(r[9]),
            "created_at":      dt_utc(r[10]),
            "snapshot_id":     r[11], "id_group":        r[12],
            "group_name":      r[13], "consensus_type":  r[14],
            "votes_cast":      r[15], "votes_required":  r[16],
            "step_deadline":   dt_utc(r[17]),
            "fakir_numer":     r[22],
            "fakir_brutto":    float(r[23]) if r[23] is not None else None,
            "fakir_kontrahent": r[24],
        }
        if bool(r[18]):
            item["delegated_for_id"] = r[20]
            item["id_delegation"]    = r[19]
            delegated.append(item)
        else:
            own.append(item)
    return {
        "own_queue":       own,
        "delegated_queue": delegated,
        "total_own":       len(own),
        "total_delegated": len(delegated),
    }


# =============================================================================
# DISPATCH
# =============================================================================

@router.post(
    "/dispatch",
    status_code=status.HTTP_201_CREATED,
    summary="Przekaz dokument do obiegu akceptacyjnego",
    description=(
        "Tworzy instancje obiegu. Jesli `id_path` nie podany — silnik filtrow "
        "automatycznie dobiera sciezke. Brak dopasowania → `pending_dispatch`. "
        "**Blad 409:** aktywny obieg dla tego dokumentu juz istnieje."
    ),
    responses={
        201: {"description": "Instancja utworzona"},
        404: {"description": "Sciezka nie istnieje lub pusta"},
        409: {"description": "Aktywny obieg juz istnieje"},
        503: {"description": "Modul wylaczony"},
    },
    dependencies=[require_permission("approval.dispatch")],
)
async def dispatch_document(
    body:         DispatchBody,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
    request:      Request,
):
    await _check_module_enabled(db, redis)
    ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else None)

    resolved_path = body.id_path
    if resolved_path is None:
        adapter = await get_adapter_by_source_id(db, body.id_source)
        if adapter:
            doc = await adapter.get_document(db, body.id_document)
            if doc:
                flag = await redis.get("syscfg:APPROVAL_AUTO_FILTERS_ENABLED")
                auto_ok = (flag.decode() if isinstance(flag, bytes) else flag) if flag else "true"
                if auto_ok.lower() == "true":
                    resolved_path = await resolve_path(db, body.id_source, doc.to_filter_dict())

    instance = await dispatch(
        db, redis,
        id_document=body.id_document,
        id_source=body.id_source,
        id_path=resolved_path,
        id_category=body.id_category,
        dispatched_by_user_id=current_user.id_user,
        dispatched_by_username=current_user.username,
        ip_address=ip,
    )
    audit_service.log(
        db,
        action="approval_dispatched",
        category="Approval",
        entity_type="ApprovalInstance",
        entity_id=instance.id_instance,
        new_value={
            "id_document": body.id_document,
            "id_source":   body.id_source,
            "id_path":     instance.id_path,
            "status":      instance.status,
        },
        details={"auto_filter": body.id_path is None},
    )
    return {
        "id_instance":  instance.id_instance,
        "status":       instance.status,
        "id_path":      instance.id_path,
        "current_step": instance.current_step,
        "message":      "Dokument przekazany do obiegu.",
    }



# =============================================================================
# INSTANCE DETAIL
# =============================================================================

@router.get(
    "/instances/{id_instance}",
    summary="Szczegoly instancji obiegu",
    description=(
        "Dane z widoku `skw_v_approval_instance_detail`: dokument, "
        "biezacy krok, grupa, postep, dyspozytor, deadline."
    ),
    responses={404: {"description": "Instancja nie istnieje"}},
    dependencies=[require_permission("approval.view_queue")],
)
async def get_instance(
    id_instance:  int,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
):
    await _check_module_enabled(db, redis)
    row = await db.execute(
        text(f"SELECT * FROM [{_SCHEMA}].[skw_v_approval_instance_detail] "
             f"WHERE [id_instance] = :i"),
        {"i": id_instance},
    )
    r = row.fetchone()
    if not r:
        raise HTTPException(status_code=404, detail=f"Instancja {id_instance} nie istnieje.")
    keys = list(row.keys())
    data = dict(zip(keys, r))
    for k, v in data.items():
        if hasattr(v, "isoformat"):
            data[k] = dt_utc(v)
    return data


# =============================================================================
# ACCEPT
# =============================================================================

@router.post(
    "/instances/{id_instance}/accept",
    summary="Akceptacja biezacego etapu",
    description=(
        "Oddaje glos. AND: wszyscy musza zaakceptowac. OR: jeden wystarczy. "
        "Delegacje sprawdzane automatycznie. "
        "**409:** juz zaakceptowales w tej iteracji. "
        "**423:** lock zajety — sprobuj za chwile. "
        "**429:** rate limit (2s cooldown)."
    ),
    responses={
        200: {"description": "Glos oddany"},
        403: {"description": "Brak czlonkostwa w grupie"},
        409: {"description": "Juz zaakceptowales lub zly status"},
        423: {"description": "Lock zajety"},
        429: {"description": "Rate limit"},
    },
    dependencies=[require_permission("approval.accept")],
)
async def accept_instance(
    id_instance:  int,
    body:         AcceptBody,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
    request:      Request,
    bg:           BackgroundTasks,
):
    ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else None)

    from app.services.event_service import publish_document_approved, publish_document_waiting

    async def _notify(event_type: str, **kwargs):
        try:
            if event_type == "approved":
                await publish_document_approved(redis,
                    instance_id=id_instance,
                    dispatched_by=kwargs.get("dispatched_by"),
                    document_title=kwargs.get("document_title"),
                    triggered_by_user_id=current_user.id_user)
            elif event_type == "step_advanced":
                await publish_document_waiting(redis,
                    instance_id=id_instance,
                    id_group=kwargs.get("id_group", 0),
                    step_order=kwargs.get("step_order", 0),
                    document_title=kwargs.get("document_title"),
                    triggered_by_user_id=current_user.id_user)
        except Exception as exc:
            logger.error("accept notify error: %s", exc)


    return await accept(
        db, redis, bg,
        id_instance=id_instance,
        id_user=current_user.id_user,
        username=current_user.username,
        comment=body.comment,
        ip_address=ip,
        notify_fn=_notify,
    )


# =============================================================================
# ROLLBACK
# =============================================================================

@router.post(
    "/instances/{id_instance}/rollback",
    summary="Cofnij obieg o jeden etap",
    description=(
        "Cofa do poprzedniego etapu. Glosy unierwaznianie (is_voided=1). "
        "Jesli current_step=1 → status=`pending_dispatch`. "
        "**Komentarz wymagany (min. 10 znakow).** "
        "**Wymaga:** czlonkostwo w biezacej grupie LUB `approval.supervise`."
    ),
    responses={
        200: {"description": "Cofnieto. Zwraca from_step, to_step, new_status"},
        400: {"description": "Brak lub za krotki komentarz"},
        403: {"description": "Brak uprawnienia"},
        409: {"description": "Instancja nie jest in_progress"},
    },
    dependencies=[require_permission("approval.rollback")],
)
async def rollback_instance(
    id_instance:  int,
    body:         RollbackBody,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
    request:      Request,
):
    ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else None)
    from app.core.dependencies import _get_role_permissions
    perms = await _get_role_permissions(current_user.role_id, db, redis)
    return await do_rollback(
        db, redis,
        id_instance=id_instance,
        id_user=current_user.id_user,
        username=current_user.username,
        comment=body.comment,
        has_supervise="approval.supervise" in perms,
        ip_address=ip,
    )


# =============================================================================
# REJECT
# =============================================================================

@router.post(
    "/instances/{id_instance}/reject",
    summary="Odrzuc dokument (status terminal)",
    description=(
        "Odrzuca dokument — status=`rejected`. Komentarz wymagany. "
        "**Wymaga:** czlonkostwo LUB `approval.supervise`."
    ),
    responses={403: {"description": "Brak uprawnienia"}, 409: {"description": "Zly status"}},
    dependencies=[require_permission("approval.reject")],
)
async def reject_instance(
    id_instance:  int,
    body:         RejectBody,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
    request:      Request,
):
    ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else None)
    from app.core.dependencies import _get_role_permissions
    perms = await _get_role_permissions(current_user.role_id, db, redis)
    return await reject(
        db, redis,
        id_instance=id_instance,
        id_user=current_user.id_user,
        username=current_user.username,
        comment=body.comment,
        has_supervise="approval.supervise" in perms,
        ip_address=ip,
    )


# =============================================================================
# CANCEL
# =============================================================================

@router.post(
    "/instances/{id_instance}/cancel",
    summary="Anuluj obieg dokumentu",
    description=(
        "Status terminal `cancelled`. Dostepne dla dyspozytora lub `approval.supervise`. "
        "**409:** instancja juz approved lub cancelled."
    ),
    responses={403: {"description": "Nie jestes dyspozytorem"}, 409: {"description": "Juz zamknieta"}},
    dependencies=[require_permission("approval.dispatch")],
)
async def cancel_instance(
    id_instance:  int,
    body:         CancelBody,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
    request:      Request,
):
    ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else None)
    from app.core.dependencies import _get_role_permissions
    perms = await _get_role_permissions(current_user.role_id, db, redis)
    has_supervise = "approval.supervise" in perms

    inst = (await db.execute(
        text(f"SELECT [dispatched_by] FROM [{_SCHEMA}].[skw_document_approval_instances] "
             f"WHERE [id_instance] = :i"),
        {"i": id_instance},
    )).fetchone()
    if not inst:
        raise HTTPException(status_code=404, detail=f"Instancja {id_instance} nie istnieje.")
    if inst[0] != current_user.id_user and not has_supervise:
        raise HTTPException(status_code=403,
            detail="Tylko dyspozytor lub approval.supervise moze anulowac obieg.")

    return await cancel(
        db, redis,
        id_instance=id_instance,
        id_user=current_user.id_user,
        username=current_user.username,
        comment=body.comment,
        ip_address=ip,
    )


# =============================================================================
# FORWARD
# =============================================================================

@router.post(
    "/instances/{id_instance}/forward",
    summary="Przekaz do innej grupy (biezacy etap NIE zaliczony)",
    description=(
        "Wstawia grupe docelowa przed biezacym etapem. Biezacy przesuwa sie o +1. "
        "**Roznica od send-to-group:** nie unierwaznia glosow. "
        "**Wymaga:** `approval.forward` + czlonkostwo. "
        "**409:** ta sama grupa docelowa."
    ),
    responses={
        200: {"description": "Przekazano. Zwraca inserted_at_step, original_step_now"},
        403: {"description": "Brak uprawnienia"},
        409: {"description": "Ta sama grupa lub zly status"},
    },
    dependencies=[require_permission("approval.forward")],
)
async def forward_instance(
    id_instance:  int,
    body:         ForwardBody,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
    request:      Request,
):
    ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else None)
    return await forward(
        db, redis,
        id_instance=id_instance,
        id_target_group=body.id_target_group,
        id_user=current_user.id_user,
        username=current_user.username,
        comment=body.comment,
        deadline_hours=body.deadline_hours,
        has_forward_permission=True,
        ip_address=ip,
    )


# =============================================================================
# SEND TO GROUP
# =============================================================================

@router.post(
    "/instances/{id_instance}/send-to-group",
    summary="Wyslij do dodatkowej grupy akceptacyjnej",
    description=(
        "Wstawia grupe docelowa na biezaca pozycje. Biezacy etap przesuwa sie na N+1. "
        "**Roznica od forward:** glosy biezacej iteracji unierwaznianie — "
        "czlonek musi zaakceptowac ponownie. "
        "**Wymaga:** `approval.send_to_group` + czlonkostwo LUB `approval.supervise`."
    ),
    responses={
        200: {"description": "Wstawiono. Zwraca voided_votes"},
        403: {"description": "Brak uprawnienia"},
        409: {"description": "Ta sama grupa"},
    },
    dependencies=[require_permission("approval.send_to_group")],
)
async def send_to_group_instance(
    id_instance:  int,
    body:         SendToGroupBody,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
    request:      Request,
):
    ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else None)
    from app.core.dependencies import _get_role_permissions
    perms = await _get_role_permissions(current_user.role_id, db, redis)
    return await send_to_group(
        db, redis,
        id_instance=id_instance,
        id_target_group=body.id_target_group,
        id_user=current_user.id_user,
        username=current_user.username,
        comment=body.comment,
        deadline_hours=body.deadline_hours,
        has_send_to_group_permission=True,
        has_supervise="approval.supervise" in perms,
        ip_address=ip,
    )


# =============================================================================
# MARK URGENT
# =============================================================================

@router.post(
    "/instances/{id_instance}/mark-urgent",
    summary="Oznacz dokument jako pilny / cofnij oznaczenie",
    description=(
        "Ustawia lub zdejmuje flage `is_urgent`. "
        "Kolejki sortuja: `is_urgent DESC, created_at ASC`. "
        "**503:** APPROVAL_URGENT_MARKING_ENABLED=false. "
        "**Wymaga:** `approval.supervise`."
    ),
    responses={409: {"description": "Instancja zamknieta"}, 503: {"description": "Tryb pilny wylaczony"}},
    dependencies=[require_permission("approval.supervise")],
)
async def mark_urgent_instance(
    id_instance:  int,
    body:         MarkUrgentBody,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
    request:      Request,
):
    ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else None)
    return await mark_urgent(
        db, redis,
        id_instance=id_instance,
        is_urgent=body.is_urgent,
        id_user=current_user.id_user,
        username=current_user.username,
        ip_address=ip,
    )


# =============================================================================
# HISTORY
# =============================================================================

@router.get(
    "/instances/{id_instance}/history",
    summary="Historia obiegu z approval_log",
    description=(
        "Pelny log akcji. Kazdy wpis: action, action_display (polska nazwa dla UI), "
        "username_snapshot, votes_before/after, is_voided, details (JSON), ip, logged_at. "
        "is_voided=1 = uniewazniony przez rollback."
    ),
    responses={404: {"description": "Instancja nie istnieje"}},
    dependencies=[require_permission("approval.view_queue")],
)
async def get_instance_history(
    id_instance:  int,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
):
    await _check_module_enabled(db, redis)
    import json

    rows = await db.execute(
        text(
            f"SELECT [id_log],[id_user],[username_snapshot],[action],"
            f"  [step_order_snapshot],[id_group_snapshot],[consensus_snapshot],"
            f"  [votes_before],[votes_after],[is_voided],[details],[ip_address],[logged_at] "
            f"FROM [{_SCHEMA}].[skw_approval_log] "
            f"WHERE [id_instance] = :i ORDER BY [logged_at] ASC"
        ),
        {"i": id_instance},
    )
    _DISPLAY = {
        "dispatched": "Przekazano do obiegu",
        "accepted": "Zaakceptowano",
        "rejected": "Odrzucono",
        "rollback": "Cofnieto obieg",
        "approved": "Obieg zakonczony — zaakceptowany",
        "cancelled": "Anulowano",
        "forwarded": "Przekazano do innej grupy",
        "send_to_group": "Wyslano do grupy",
        "step_advanced": "Przejscie do kolejnego etapu",
        "marked_urgent": "Oznaczono jako pilny",
        "unmarked_urgent": "Usunieto oznaczenie pilny",
        "deadline_expired": "Termin przekroczony",
        "deadline_escalated": "Eskalacja",
    }
    entries = []
    for r in rows.fetchall():
        details = None
        if r[10]:
            try: details = json.loads(r[10])
            except Exception: details = r[10]
        entries.append({
            "id_log":            r[0], "id_user":           r[1],
            "username_snapshot": r[2], "action":            r[3],
            "action_display":    _DISPLAY.get(r[3], r[3]),
            "step_order":        r[4], "id_group":          r[5],
            "consensus":         r[6], "votes_before":      r[7],
            "votes_after":       r[8], "is_voided":         bool(r[9]),
            "details":           details, "ip_address":     r[11],
            "logged_at":         dt_utc(r[12]),
        })
    return {"id_instance": id_instance, "total": len(entries), "entries": entries}


# =============================================================================
# SNAPSHOT
# =============================================================================

@router.get(
    "/instances/{id_instance}/snapshot",
    summary="Kroki snapshot obiegu",
    description=(
        "Wszystkie kroki robocze instancji w kolejnosci. "
        "status: `approved` = zaliczony, `in_progress` = biezacy, "
        "`pending` = oczekujacy, `skipped` = pominiety."
    ),
    responses={404: {"description": "Instancja nie istnieje"}},
    dependencies=[require_permission("approval.view_queue")],
)
async def get_instance_snapshot(
    id_instance:  int,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
):
    await _check_module_enabled(db, redis)
    rows = await db.execute(
        text(
            f"SELECT s.[id_snapshot], s.[step_order], s.[id_group], "
            f"  g.[group_name], g.[consensus_type], "
            f"  s.[status], s.[votes_cast], s.[votes_required], "
            f"  s.[deadline_at], s.[completed_at] "
            f"FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] s "
            f"JOIN [{_SCHEMA}].[skw_approval_groups] g ON g.[id_group] = s.[id_group] "
            f"WHERE s.[id_instance] = :i ORDER BY s.[step_order] ASC"
        ),
        {"i": id_instance},
    )
    steps = []
    for r in rows.fetchall():
        steps.append({
            "id_snapshot":    r[0], "step_order":     r[1],
            "id_group":       r[2], "group_name":     r[3],
            "consensus_type": r[4], "status":         r[5],
            "votes_cast":     r[6], "votes_required": r[7],
            "deadline_at":    dt_utc(r[8]),
            "completed_at":   dt_utc(r[9]),
            "is_current":     r[5] == "in_progress",
            "is_complete":    r[5] == "approved",
        })
    return {"id_instance": id_instance, "total_steps": len(steps), "steps": steps}

# =============================================================================
# PATCH — backend/app/api/approval/instances.py
#
# Dodaj dwa nowe endpointy na koncu pliku, przed ostatnim endpointem snapshot.
# Kolejnosc w routerze nie ma znaczenia — zadna z tych sciezek nie koliduje
# z istniejacymi (sa bardziej szczegolowe lub inny prefix).
#
# STARY (koniec pliku — ostatnia funkcja get_instance_snapshot):
# =============================================================================

# -- NOWY endpoint 1: GET /approval/instances ----------------------------------
# Dodaj PRZED get_instance_snapshot (lub na koncu — nie ma konfliktu routingu)

@router.get(
    "/instances",
    summary="Lista wszystkich instancji obiegu (widok supervisora)",
    description=(
        "Paginowana lista wszystkich instancji z filtrami. "
        "Sortowanie domyslne: `is_urgent DESC, created_at DESC`. "
        "**Wymaga:** `approval.supervise`. "
        "\n\nDostepne filtry: `status`, `id_source`, `id_path`, `is_urgent`, "
        "`dispatched_by`, `date_from`, `date_to` (po `created_at`). "
        "\n\nKazdy element zawiera skrocone dane Fakir (numer, brutto, kontrahent). "
        "Dla pelnych danych uzyj `GET /instances/{id}`."
    ),
    responses={
        200: {"description": "Lista instancji z paginacja"},
        503: {"description": "Modul wylaczony"},
    },
    dependencies=[require_permission("approval.supervise")],
)
async def list_all_instances(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    page:         int           = Query(1, ge=1),
    per_page:     int           = Query(25, ge=1, le=100),
    status:       Optional[str] = Query(
        None,
        description="Filtr statusu: pending_dispatch|in_progress|approved|cancelled|rejected",
        pattern="^(pending_dispatch|in_progress|approved|cancelled|rejected)$",
    ),
    id_source:    Optional[int] = Query(None, gt=0, description="ID zrodla dokumentu"),
    id_path:      Optional[int] = Query(None, gt=0, description="ID sciezki akceptacyjnej"),
    is_urgent:    Optional[bool] = Query(None, description="Filtr: tylko pilne"),
    dispatched_by: Optional[int] = Query(None, gt=0, description="ID dyspozytora"),
    date_from:    Optional[str] = Query(
        None,
        description="Poczatek zakresu created_at (ISO 8601, np. 2026-01-01)",
    ),
    date_to:      Optional[str] = Query(
        None,
        description="Koniec zakresu created_at (ISO 8601, np. 2026-12-31)",
    ),
):
    await _check_module_enabled(db, redis)

    offset = (page - 1) * per_page
    where  = ["1=1"]
    params: dict = {"limit": per_page, "offset": offset}

    if status is not None:
        where.append("i.[status] = :status")
        params["status"] = status

    if id_source is not None:
        where.append("i.[id_source] = :src")
        params["src"] = id_source

    if id_path is not None:
        where.append("i.[id_path] = :pth")
        params["pth"] = id_path

    if is_urgent is not None:
        where.append("i.[is_urgent] = :urg")
        params["urg"] = 1 if is_urgent else 0

    if dispatched_by is not None:
        where.append("i.[dispatched_by] = :dby")
        params["dby"] = dispatched_by

    if date_from is not None:
        try:
            from datetime import datetime
            datetime.fromisoformat(date_from)
            where.append("i.[created_at] >= :dfrom")
            params["dfrom"] = date_from
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="date_from musi byc w formacie ISO 8601 (np. 2026-01-01)",
            )

    if date_to is not None:
        try:
            from datetime import datetime
            datetime.fromisoformat(date_to)
            where.append("i.[created_at] <= :dto")
            params["dto"] = date_to
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="date_to musi byc w formacie ISO 8601 (np. 2026-12-31)",
            )

    w = " AND ".join(where)

    total = (await db.execute(
        text(
            f"SELECT COUNT(*) "
            f"FROM [{_SCHEMA}].[skw_document_approval_instances] i "
            f"WHERE {w}"
        ),
        params,
    )).scalar() or 0

    rows = await db.execute(
        text(
            f"SELECT "
            f"  i.[id_instance], i.[id_document], i.[id_source], "
            f"  ds.[source_name], "
            f"  i.[id_path], p.[path_name], "
            f"  i.[id_category], dc.[category_name], "
            f"  i.[status], i.[current_step], i.[is_urgent], "
            f"  i.[document_title], i.[document_amount], "
            f"  i.[deadline_at], i.[dispatched_at], i.[completed_at], "
            f"  i.[created_at], i.[updated_at], "
            f"  i.[dispatched_by], "
            f"  u.[Username]   AS dispatched_by_username, "
            f"  u.[FullName]   AS dispatched_by_fullname, "
            f"  i.[is_urgent], i.[is_deadline_notified], "
            f"  fah.[NUMER]           AS fakir_numer, "
            f"  fah.[WARTOSC_BRUTTO]  AS fakir_brutto, "
            f"  fah.[NazwaKontrahenta] AS fakir_kontrahent "
            f"FROM [{_SCHEMA}].[skw_document_approval_instances] i "
            f"INNER JOIN [{_SCHEMA}].[skw_document_sources] ds "
            f"       ON ds.[id_source] = i.[id_source] "
            f"LEFT  JOIN [{_SCHEMA}].[skw_approval_paths] p "
            f"       ON p.[id_path] = i.[id_path] "
            f"LEFT  JOIN [{_SCHEMA}].[skw_document_categories] dc "
            f"       ON dc.[id_category] = i.[id_category] "
            f"LEFT  JOIN [{_SCHEMA}].[skw_Users] u "
            f"       ON u.[ID_USER] = i.[dispatched_by] "
            f"LEFT  JOIN [{_SCHEMA}].[skw_faktury_akceptacja_naglowek] fah "
            f"       ON ds.[source_name] = N'fakir' "
            f"       AND fah.[ID_BUF_DOKUMENT] = TRY_CAST(i.[id_document] AS INT) "
            f"WHERE {w} "
            f"ORDER BY i.[is_urgent] DESC, i.[created_at] DESC "
            f"OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY"
        ),
        params,
    )

    items = []
    for r in rows.fetchall():
        items.append({
            "id_instance":             r[0],
            "id_document":             r[1],
            "id_source":               r[2],
            "source_name":             r[3],
            "id_path":                 r[4],
            "path_name":               r[5],
            "id_category":             r[6],
            "category_name":           r[7],
            "status":                  r[8],
            "current_step":            r[9],
            "is_urgent":               bool(r[10]),
            "document_title":          r[11],
            "document_amount":         float(r[12]) if r[12] is not None else None,
            "deadline_at":             dt_utc(r[13]),
            "dispatched_at":           dt_utc(r[14]),
            "completed_at":            dt_utc(r[15]),
            "created_at":              dt_utc(r[16]),
            "updated_at":              dt_utc(r[17]),
            "dispatched_by":           r[18],
            "dispatched_by_username":  r[19],
            "dispatched_by_fullname":  r[20],
            "is_deadline_notified":    bool(r[22]),
            "fakir_numer":             r[23],
            "fakir_brutto":            float(r[24]) if r[24] is not None else None,
            "fakir_kontrahent":        r[25],
        })

    return {
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "pages":    max(1, -(-total // per_page)),  # ceil division
        "data":     items,
        "filters": {
            "status":        status,
            "id_source":     id_source,
            "id_path":       id_path,
            "is_urgent":     is_urgent,
            "dispatched_by": dispatched_by,
            "date_from":     date_from,
            "date_to":       date_to,
        },
    }


# -- NOWY endpoint 2: GET /approval/instances/{id}/votes -----------------------

@router.get(
    "/instances/{id_instance}/votes",
    summary="Aktualne glosy na biezacym etapie instancji",
    description=(
        "Zwraca szczegoly glosowania na biezacym kroku (etap `in_progress`). "
        "Dla kazdego czlonka grupy: czy zaglosowal, kiedy, jaki glos. "
        "Glosy `is_voided=1` (unierwaznionych przez rollback) sa wykluczone. "
        "Pole `consensus_reached` informuje czy etap jest juz zaliczony. "
        "\n\n**Dostep:** czlonek biezacej grupy LUB `approval.supervise` LUB dyspozytor."
    ),
    responses={
        200: {"description": "Szczegoly glosowania biezacego etapu"},
        404: {"description": "Instancja nie istnieje"},
        409: {"description": "Instancja nie jest in_progress — brak aktywnego etapu"},
    },
    dependencies=[require_permission("approval.view_queue")],
)
async def get_instance_votes(
    id_instance:  int,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
):
    await _check_module_enabled(db, redis)

    # Pobierz instancje i biezacy snapshot
    inst_row = (await db.execute(
        text(
            f"SELECT i.[status], i.[current_step], i.[dispatched_by], "
            f"  s.[id_snapshot], s.[id_group], s.[consensus_type], "
            f"  s.[votes_cast], s.[votes_required], s.[deadline_at], "
            f"  g.[group_name] "
            f"FROM [{_SCHEMA}].[skw_document_approval_instances] i "
            f"LEFT JOIN [{_SCHEMA}].[skw_document_approval_snapshot_steps] s "
            f"       ON s.[id_instance] = i.[id_instance] "
            f"       AND s.[step_order] = i.[current_step] "
            f"LEFT JOIN [{_SCHEMA}].[skw_approval_groups] g "
            f"       ON g.[id_group] = s.[id_group] "
            f"WHERE i.[id_instance] = :i"
        ),
        {"i": id_instance},
    )).fetchone()

    if not inst_row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Instancja {id_instance} nie istnieje.",
        )

    inst_status   = inst_row[0]
    current_step  = inst_row[1]
    dispatched_by = inst_row[2]
    id_snapshot   = inst_row[3]
    id_group      = inst_row[4]
    consensus_type = inst_row[5]
    votes_cast    = inst_row[6]
    votes_required = inst_row[7]
    step_deadline  = inst_row[8]
    group_name     = inst_row[9]

    if inst_status != "in_progress":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Instancja ma status '{inst_status}' — "
                "glosy sa dostepne tylko dla in_progress."
            ),
        )

    # Czlonkowie grupy z ich statusem glosu
    members_rows = await db.execute(
        text(
            f"SELECT "
            f"  gm.[id_user], "
            f"  u.[Username], "
            f"  u.[FullName], "
            f"  l.[action]      AS vote_action, "
            f"  l.[logged_at]   AS voted_at, "
            f"  l.[is_voided] "
            f"FROM [{_SCHEMA}].[skw_approval_group_members] gm "
            f"JOIN [{_SCHEMA}].[skw_Users] u "
            f"     ON u.[ID_USER] = gm.[id_user] "
            f"LEFT JOIN [{_SCHEMA}].[skw_approval_log] l "
            f"     ON l.[id_instance]       = :inst "
            f"     AND l.[id_user]          = gm.[id_user] "
            f"     AND l.[step_order_snapshot] = :step "
            f"     AND l.[action]           IN (N'accepted', N'rejected') "
            f"     AND l.[is_voided]        = 0 "
            f"WHERE gm.[id_group] = :grp "
            f"ORDER BY u.[FullName] ASC"
        ),
        {"inst": id_instance, "step": current_step, "grp": id_group},
    )

    members = []
    for r in members_rows.fetchall():
        has_voted = r[3] is not None
        members.append({
            "id_user":   r[0],
            "username":  r[1],
            "full_name": r[2],
            "has_voted": has_voted,
            "vote":      r[3],          # "accepted" | "rejected" | None
            "voted_at":  dt_utc(r[4]),
        })

    # Aktywne delegacje dla tej grupy
    from datetime import datetime, timezone
    now_naive = datetime.now(timezone.utc).replace(tzinfo=None)

    delegations_rows = await db.execute(
        text(
            f"SELECT "
            f"  d.[id_delegation], d.[id_user_from], d.[id_user_to], "
            f"  uf.[Username] AS from_username, "
            f"  ut.[Username] AS to_username, "
            f"  d.[valid_to] "
            f"FROM [{_SCHEMA}].[skw_approval_delegations] d "
            f"JOIN [{_SCHEMA}].[skw_Users] uf ON uf.[ID_USER] = d.[id_user_from] "
            f"JOIN [{_SCHEMA}].[skw_Users] ut ON ut.[ID_USER] = d.[id_user_to] "
            f"WHERE d.[is_active] = 1 "
            f"  AND d.[valid_from] <= :now "
            f"  AND d.[valid_to]   >= :now "
            f"  AND (d.[id_group] = :grp OR d.[id_group] IS NULL) "
            f"  AND d.[id_user_from] IN ("
            f"      SELECT [id_user] FROM [{_SCHEMA}].[skw_approval_group_members] "
            f"      WHERE [id_group] = :grp"
            f"  )"
        ),
        {"now": now_naive, "grp": id_group},
    )

    delegations = [
        {
            "id_delegation":  r[0],
            "id_user_from":   r[1],
            "id_user_to":     r[2],
            "from_username":  r[3],
            "to_username":    r[4],
            "valid_to":       dt_utc(r[5]),
        }
        for r in delegations_rows.fetchall()
    ]

    # Oblicz czy consensus osiagniety
    total_members   = len(members)
    voted_accepted  = sum(1 for m in members if m["vote"] == "accepted")
    voted_rejected  = sum(1 for m in members if m["vote"] == "rejected")

    if consensus_type == "OR":
        consensus_reached = voted_accepted >= 1
    else:  # AND
        consensus_reached = voted_accepted >= total_members and total_members > 0

    logger.info(
        "get_instance_votes | inst=%d step=%d group=%d members=%d voted=%d/%d",
        id_instance, current_step, id_group, total_members,
        voted_accepted + voted_rejected, total_members,
    )

    return {
        "id_instance":      id_instance,
        "current_step":     current_step,
        "id_group":         id_group,
        "group_name":       group_name,
        "consensus_type":   consensus_type,
        "votes_cast":       votes_cast or 0,
        "votes_required":   votes_required or total_members,
        "votes_accepted":   voted_accepted,
        "votes_rejected":   voted_rejected,
        "total_members":    total_members,
        "consensus_reached": consensus_reached,
        "step_deadline":    dt_utc(step_deadline),
        "members":          members,
        "active_delegations": delegations,
        "summary": {
            "pending":  total_members - voted_accepted - voted_rejected,
            "accepted": voted_accepted,
            "rejected": voted_rejected,
        },
    }

@router.get(
    "/instances/{id_instance}/can-act",
    summary="Czy zalogowany user moze wykonac akcje na instancji",
    description=(
        "Sprawdza uprawnienia bez wykonywania akcji — "
        "sluzy do wlaczania/wylaczania przyciskow w UI. "
        "\n\nZwraca obiekt z flagami: "
        "`can_accept`, `can_reject`, `can_rollback`, `can_forward`, "
        "`can_send_to_group`, `can_cancel`, `can_mark_urgent`. "
        "\n\nKazda flaga to bool. Jesli `can_accept=false`, przycisk Akceptuj "
        "powinien byc wyszarzony — user dostanie 403 jesli sprobuje. "
        "\n\n**Nie wymaga supervise** — kazdy user moze sprawdzic swoje uprawnienia."
    ),
    responses={
        200: {"description": "Obiekt z flagami uprawnien"},
        404: {"description": "Instancja nie istnieje"},
    },
    dependencies=[require_permission("approval.view_queue")],
)
async def get_instance_can_act(
    id_instance:  int,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
):
    await _check_module_enabled(db, redis)
    from app.core.dependencies import _get_role_permissions
    from datetime import datetime, timezone
 
    # Pobierz instancje z biezacym krokiem
    inst = (await db.execute(
        text(
            f"SELECT i.[status], i.[current_step], i.[dispatched_by], "
            f"  s.[id_group], s.[consensus_type], "
            f"  s.[votes_cast], s.[votes_required] "
            f"FROM [{_SCHEMA}].[skw_document_approval_instances] i "
            f"LEFT JOIN [{_SCHEMA}].[skw_document_approval_snapshot_steps] s "
            f"       ON s.[id_instance]=i.[id_instance] "
            f"       AND s.[step_order]=i.[current_step] "
            f"WHERE i.[id_instance]=:i"
        ),
        {"i": id_instance},
    )).fetchone()
 
    if not inst:
        raise HTTPException(status_code=404, detail=f"Instancja {id_instance} nie istnieje.")
 
    inst_status    = inst[0]
    current_step   = inst[1]
    dispatched_by  = inst[2]
    id_group       = inst[3]
    consensus_type = inst[4]
    votes_cast     = inst[5] or 0
    votes_required = inst[6] or 0
    is_in_progress = inst_status == "in_progress"
    is_dispatcher  = dispatched_by == current_user.id_user
 
    # Pobierz uprawnienia usera
    perms = await _get_role_permissions(current_user.role_id, db, redis)
    has_supervise        = "approval.supervise" in perms
    has_rollback_perm    = "approval.rollback" in perms
    has_forward_perm     = "approval.forward" in perms
    has_send_group_perm  = "approval.send_to_group" in perms
 
    # Czy user jest czlonkiem biezacej grupy?
    is_member = False
    if id_group:
        member_row = (await db.execute(
            text(
                f"SELECT 1 FROM [{_SCHEMA}].[skw_approval_group_members] "
                f"WHERE [id_group]=:g AND [id_user]=:u"
            ),
            {"g": id_group, "u": current_user.id_user},
        )).fetchone()
        is_member = member_row is not None
 
    # Czy user ma aktywna delegacje dla tej grupy?
    has_delegation = False
    if id_group and not is_member:
        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        deleg = (await db.execute(
            text(
                f"SELECT 1 FROM [{_SCHEMA}].[skw_approval_delegations] "
                f"WHERE [id_user_to]=:u AND [is_active]=1 "
                f"  AND [valid_from]<=:now AND [valid_to]>=:now "
                f"  AND ([id_group]=:g OR [id_group] IS NULL)"
            ),
            {"u": current_user.id_user, "g": id_group,
             "now": now_naive},
        )).fetchone()
        has_delegation = deleg is not None
 
    can_act_on_step = is_member or has_delegation or has_supervise
 
    # Czy juz zaglosowal w tej iteracji?
    already_voted = False
    if is_in_progress and id_group:
        voted = (await db.execute(
            text(
                f"SELECT 1 FROM [{_SCHEMA}].[skw_approval_log] "
                f"WHERE [id_instance]=:i AND [id_user]=:u "
                f"  AND [step_order_snapshot]=:step "
                f"  AND [action]=N'accepted' AND [is_voided]=0"
            ),
            {"i": id_instance, "u": current_user.id_user, "step": current_step},
        )).fetchone()
        already_voted = voted is not None
 
    return {
        "id_instance":   id_instance,
        "id_user":       current_user.id_user,
        "instance_status": inst_status,
        "is_member":     is_member,
        "has_delegation": has_delegation,
        "has_supervise": has_supervise,
        "already_voted": already_voted,
        "actions": {
            "can_accept":       is_in_progress and can_act_on_step and not already_voted,
            "can_reject":       is_in_progress and (can_act_on_step or has_supervise),
            "can_rollback":     is_in_progress and (can_act_on_step or has_supervise) and has_rollback_perm,
            "can_forward":      is_in_progress and can_act_on_step and has_forward_perm,
            "can_send_to_group": is_in_progress and (can_act_on_step or has_supervise) and has_send_group_perm,
            "can_cancel":       is_in_progress and (is_dispatcher or has_supervise),
            "can_mark_urgent":  is_in_progress and has_supervise,
        },
        "reason": {
            "not_member":       not is_member and not has_delegation and not has_supervise,
            "already_voted":    already_voted,
            "wrong_status":     not is_in_progress,
        },
    }
 
class ValidateDispatchBody(BaseModel):
    id_document: str           = Field(..., min_length=1, max_length=100)
    id_source:   int           = Field(..., gt=0)
    id_path:     Optional[int] = Field(None, gt=0)

    @field_validator("id_document")
    @classmethod
    def strip_doc(cls, v: str) -> str:
        return v.strip()


@router.post(
    "/dispatch/validate",
    summary="Walidacja dokumentu przed dispatch — preview bez wykonywania akcji",
    description=(
        "Sprawdza czy dokument moze byc wysłany do obiegu i jaka sciezka zostanie dobrana. "
        "Nie tworzy instancji — tylko zwraca informacje. "
        "\n\nSprawdza: "
        "czy dokument istnieje w zrodle, "
        "czy nie ma aktywnego obiegu dla tego dokumentu, "
        "jaka sciezka zostanie dobrana automatycznie (jesli id_path nie podane), "
        "ile krokow ma dobrana sciezka, "
        "jakie grupy sa w kolejnych etapach. "
        "\n\nUzywane przez frontend do podgladu przed klikniecieem Dispatch."
    ),
    responses={
        200: {"description": "Wynik walidacji (valid=true/false)"},
    },
    dependencies=[require_permission("approval.dispatch")],
)
async def validate_dispatch(
    body:         ValidateDispatchBody,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
):
    await _check_module_enabled(db, redis)

    result = {
        "valid":         True,
        "id_document":   body.id_document,
        "id_source":     body.id_source,
        "warnings":      [],
        "errors":        [],
        "active_instance": None,
        "resolved_path": None,
        "document_info": None,
    }

    # 1. Czy nie ma aktywnego obiegu?
    active = (await db.execute(
        text(
            f"SELECT [id_instance],[status] "
            f"FROM [{_SCHEMA}].[skw_document_approval_instances] "
            f"WHERE [id_document]=:d AND [id_source]=:s "
            f"  AND [status] NOT IN (N'approved',N'cancelled')"
        ),
        {"d": body.id_document, "s": body.id_source},
    )).fetchone()
    if active:
        result["valid"] = False
        result["errors"].append(
            f"Aktywny obieg juz istnieje (id_instance={active[0]}, status={active[1]})."
        )
        result["active_instance"] = {"id_instance": active[0], "status": active[1]}

    # 2. Pobierz info o dokumencie ze zrodla
    try:
        from app.schemas.unified_document import get_adapter_by_source_id
        adapter = await get_adapter_by_source_id(db, body.id_source)
        if adapter:
            doc = await adapter.get_document(db, body.id_document)
            if doc:
                result["document_info"] = {
                    "title":      doc.title,
                    "amount":     float(doc.amount) if doc.amount else None,
                    "contractor": doc.contractor_name,
                    "date":       str(doc.document_date) if doc.document_date else None,
                }
            else:
                result["warnings"].append(
                    f"Dokument {body.id_document} nie znaleziony w zrodle — "
                    "moze byc z innego systemu."
                )
    except Exception as exc:
        result["warnings"].append(f"Nie udalo sie pobrac danych dokumentu: {exc}")

    # 3. Resolve sciezki
    resolved_path_id = body.id_path
    if resolved_path_id is None:
        try:
            from app.services.filter_engine import resolve_path
            if result["document_info"]:
                filter_dict = {
                    "amount_gross":    result["document_info"].get("amount"),
                    "contractor_name": result["document_info"].get("contractor"),
                }
                resolved_path_id = await resolve_path(db, body.id_source, filter_dict)
        except Exception:
            pass

    if resolved_path_id:
        path_row = (await db.execute(
            text(
                f"SELECT p.[id_path],p.[path_name],p.[is_active],"
                f"  COUNT(s.[id_step]) AS step_count "
                f"FROM [{_SCHEMA}].[skw_approval_paths] p "
                f"LEFT JOIN [{_SCHEMA}].[skw_approval_path_steps] s "
                f"       ON s.[id_path]=p.[id_path] "
                f"WHERE p.[id_path]=:p "
                f"GROUP BY p.[id_path],p.[path_name],p.[is_active]"
            ),
            {"p": resolved_path_id},
        )).fetchone()

        if path_row:
            if not bool(path_row[2]):
                result["valid"] = False
                result["errors"].append(f"Sciezka '{path_row[1]}' jest nieaktywna.")
            elif path_row[3] == 0:
                result["valid"] = False
                result["errors"].append(f"Sciezka '{path_row[1]}' nie ma zadnych krokow.")
            else:
                # Pobierz kroki z grupami
                steps_rows = await db.execute(
                    text(
                        f"SELECT s.[step_order],g.[group_name],g.[consensus_type],"
                        f"  s.[deadline_hours] "
                        f"FROM [{_SCHEMA}].[skw_approval_path_steps] s "
                        f"JOIN [{_SCHEMA}].[skw_approval_groups] g ON g.[id_group]=s.[id_group] "
                        f"WHERE s.[id_path]=:p ORDER BY s.[step_order] ASC"
                    ),
                    {"p": resolved_path_id},
                )
                steps_preview = [
                    {
                        "step_order":     sr[0],
                        "group_name":     sr[1],
                        "consensus_type": sr[2],
                        "deadline_hours": sr[3],
                    }
                    for sr in steps_rows.fetchall()
                ]
                result["resolved_path"] = {
                    "id_path":    path_row[0],
                    "path_name":  path_row[1],
                    "step_count": path_row[3],
                    "steps":      steps_preview,
                    "auto_resolved": body.id_path is None,
                }
    else:
        result["warnings"].append(
            "Brak dopasowanej sciezki — dokument trafi do kolejki dyspozytora "
            "(status: pending_dispatch)."
        )

    return result


@router.get(
    "/instances/{id_instance}/timeline",
    summary="Chronologiczny timeline instancji (snapshot + log + komentarze)",
    description=(
        "Lacze w jeden posortowany feed: "
        "zdarzenia z approval_log, zmiany statusow krokow (snapshot), "
        "komentarze uzytkownikow. "
        "\n\nKazdy element ma: `ts` (timestamp), `type` "
        "(`log_entry`|`step_change`|`comment`), `actor` (username), `content`. "
        "\n\nZamiast 3 osobnych requestow (history + snapshot + comments) "
        "frontend dostaje gotowy feed w jednym wywolaniu."
    ),
    responses={
        200: {"description": "Chronologiczny feed zdarzen"},
        404: {"description": "Instancja nie istnieje"},
    },
    dependencies=[require_permission("approval.view_queue")],
)
async def get_instance_timeline(
    id_instance:  int,
    current_user: CurrentUser,
    db:           DB,
    redis:        RedisClient,
):
    await _check_module_enabled(db, redis)
    import json as _json
 
    # Sprawdz czy instancja istnieje
    inst = (await db.execute(
        text(f"SELECT [id_instance],[status] FROM [{_SCHEMA}].[skw_document_approval_instances] "
             f"WHERE [id_instance]=:i"),
        {"i": id_instance},
    )).fetchone()
    if not inst:
        raise HTTPException(status_code=404, detail=f"Instancja {id_instance} nie istnieje.")
 
    _ACTION_DISPLAY = {
        "dispatched":        "Przekazano do obiegu",
        "accepted":          "Zaakceptowano",
        "rejected":          "Odrzucono",
        "rollback":          "Cofnieto obieg",
        "approved":          "Obieg zakonczony",
        "cancelled":         "Anulowano",
        "forwarded":         "Przekazano do innej grupy",
        "send_to_group":     "Wyslano do grupy",
        "step_advanced":     "Przejscie do kolejnego etapu",
        "marked_urgent":     "Oznaczono jako pilny",
        "unmarked_urgent":   "Usunieto oznaczenie pilny",
        "deadline_expired":  "Termin przekroczony",
        "deadline_warning":  "Ostrzezenie o terminie",
        "deadline_escalated":"Eskalacja",
    }
 
    timeline = []
 
    # 1. Wpisy z approval_log
    log_rows = await db.execute(
        text(
            f"SELECT [id_log],[username_snapshot],[action],[step_order_snapshot],"
            f"  [id_group_snapshot],[votes_before],[votes_after],[is_voided],"
            f"  [details],[logged_at] "
            f"FROM [{_SCHEMA}].[skw_approval_log] "
            f"WHERE [id_instance]=:i ORDER BY [logged_at] ASC"
        ),
        {"i": id_instance},
    )
    for r in log_rows.fetchall():
        details = None
        if r[8]:
            try: details = _json.loads(r[8])
            except Exception: details = r[8]
        timeline.append({
            "ts":      dt_utc(r[9]),
            "type":    "log_entry",
            "id":      r[0],
            "actor":   r[1] or "system",
            "action":  r[2],
            "action_display": _ACTION_DISPLAY.get(r[2], r[2]),
            "step":    r[3],
            "is_voided": bool(r[7]),
            "details": details,
            "votes": {"before": r[5], "after": r[6]} if r[5] is not None else None,
        })
 
    # 2. Komentarze (jesli modul wlaczony)
    try:
        comment_rows = await db.execute(
            text(
                f"SELECT c.[id_comment],u.[Username],c.[content],c.[is_deleted],"
                f"  c.[created_at],c.[parent_id] "
                f"FROM [{_SCHEMA}].[skw_approval_comments] c "
                f"LEFT JOIN [{_SCHEMA}].[skw_Users] u ON u.[ID_USER]=c.[id_user] "
                f"WHERE c.[id_instance]=:i ORDER BY c.[created_at] ASC"
            ),
            {"i": id_instance},
        )
        for r in comment_rows.fetchall():
            is_del = bool(r[3])
            timeline.append({
                "ts":        r[4].isoformat() if r[4] else None,
                "type":      "comment",
                "id":        r[0],
                "actor":     r[1] or "?",
                "content":   "[usunieto]" if is_del else r[2],
                "is_deleted": is_del,
                "parent_id": r[5],
            })
    except Exception:
        pass  # Komentarze opcjonalne
 
    # 3. Sortuj chronologicznie
    timeline.sort(key=lambda x: x.get("ts") or "")
 
    return {
        "id_instance":  id_instance,
        "status":       inst[1],
        "total_events": len(timeline),
        "timeline":     timeline,
    }
 
