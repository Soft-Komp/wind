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
            "deadline_at":      r[9].isoformat() if r[9] else None,
            "created_at":       r[10].isoformat() if r[10] else None,
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
    user_id = current_user.ID_USER

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
            "deadline_at":     r[9].isoformat() if r[9] else None,
            "created_at":      r[10].isoformat() if r[10] else None,
            "snapshot_id":     r[11], "id_group":        r[12],
            "group_name":      r[13], "consensus_type":  r[14],
            "votes_cast":      r[15], "votes_required":  r[16],
            "step_deadline":   r[17].isoformat() if r[17] else None,
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
        dispatched_by_user_id=current_user.ID_USER,
        dispatched_by_username=current_user.Username,
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
            data[k] = v.isoformat()
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
                    triggered_by_user_id=current_user.ID_USER)
            elif event_type == "step_advanced":
                await publish_document_waiting(redis,
                    instance_id=id_instance,
                    id_group=kwargs.get("id_group", 0),
                    step_order=kwargs.get("step_order", 0),
                    document_title=kwargs.get("document_title"),
                    triggered_by_user_id=current_user.ID_USER)
        except Exception as exc:
            logger.error("accept notify error: %s", exc)


    return await accept(
        db, redis, bg,
        id_instance=id_instance,
        id_user=current_user.ID_USER,
        username=current_user.Username,
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
        id_user=current_user.ID_USER,
        username=current_user.Username,
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
        id_user=current_user.ID_USER,
        username=current_user.Username,
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
    if inst[0] != current_user.ID_USER and not has_supervise:
        raise HTTPException(status_code=403,
            detail="Tylko dyspozytor lub approval.supervise moze anulowac obieg.")

    return await cancel(
        db, redis,
        id_instance=id_instance,
        id_user=current_user.ID_USER,
        username=current_user.Username,
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
        id_user=current_user.ID_USER,
        username=current_user.Username,
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
        id_user=current_user.ID_USER,
        username=current_user.Username,
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
        id_user=current_user.ID_USER,
        username=current_user.Username,
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
            "logged_at":         r[12].isoformat() if r[12] else None,
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
            "deadline_at":    r[8].isoformat() if r[8] else None,
            "completed_at":   r[9].isoformat() if r[9] else None,
            "is_current":     r[5] == "in_progress",
            "is_complete":    r[5] == "approved",
        })
    return {"id_instance": id_instance, "total_steps": len(steps), "steps": steps}