# backend/app/api/approval/filters.py
"""
Endpointy zarzadzania filtrami automatycznymi.

Rozszerzenie 2026-07-31 / migracja 0073:
  * filtry standard obsluguja logic_operator=AND|OR,
  * AND pozostaje wartoscia domyslna i zachowuje kompatybilnosc wsteczna,
  * filtry universal zawsze uzywaja technicznej wartosci AND; operator nie
    zmienia sposobu wywolania funkcji SQL,
  * mutacje sa zapisywane przez istniejacy audit_service (AuditLog + JSONL),
    bez wprowadzania nowej tabeli historii filtrow.

KRYTYCZNE kolejnosci FastAPI:
  PATCH  /reorder          PRZED  PATCH /{id_filter}
  DELETE /{id_filter}/initiate i /{id_filter}/confirm — nie koliduja z innymi
  DELETE /{id_filter}/conditions/{id_condition} — bezposredni (sub-zasob)

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging
import orjson
import re
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text

from app.api.approval._delete_helpers import generate_delete_token, verify_delete_token
from app.core.dependencies import DB, CurrentUser, RedisClient, require_permission
from app.schemas.common import dt_utc
from app.services import audit_service
from app.services.approval_service import _check_module_enabled

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/filters")
_SCHEMA = "dbo"
_SCOPE = "delete_filter"
_VALID_OPERATORS = frozenset({"eq", "neq", "contains", "gt", "lt", "gte", "lte"})
_VALID_LOGIC_OPERATORS = frozenset({"AND", "OR"})
_DEFAULT_LOGIC_OPERATOR = "AND"
_FUNC_RE = re.compile(r"^[a-zA-Z0-9_]+$")


# =============================================================================
# Schematy requestow
# =============================================================================


def _normalize_logic_operator(value: Any) -> Any:
    """Normalizuje wartosc z API przed walidacja Pydantic."""
    if isinstance(value, str):
        return value.strip().upper()
    return value


class FilterCreateBody(BaseModel):
    filter_name:        str           = Field(..., min_length=2, max_length=200)
    filter_type:        str           = Field(..., pattern="^(standard|universal)$")
    id_path:            int           = Field(..., gt=0)
    id_source:          Optional[int] = Field(None, gt=0)
    priority:           int           = Field(100, ge=1, le=9999)
    universal_function: Optional[str] = Field(None, max_length=128)
    logic_operator:     str           = Field(_DEFAULT_LOGIC_OPERATOR, pattern="^(AND|OR)$")

    @field_validator("universal_function")
    @classmethod
    def validate_func(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not _FUNC_RE.match(v):
            raise ValueError("Nazwa funkcji SQL: ^[a-zA-Z0-9_]+$")
        return v

    @field_validator("logic_operator", mode="before")
    @classmethod
    def validate_logic_operator(cls, v: Any) -> Any:
        return _normalize_logic_operator(v)


class FilterPatchBody(BaseModel):
    filter_name:        Optional[str]  = Field(None, min_length=2, max_length=200)
    priority:           Optional[int]  = Field(None, ge=1, le=9999)
    is_active:          Optional[bool] = None
    universal_function: Optional[str]  = Field(None, max_length=128)
    logic_operator:     Optional[str]  = Field(None, pattern="^(AND|OR)$")

    @field_validator("universal_function")
    @classmethod
    def validate_func(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not _FUNC_RE.match(v):
            raise ValueError("Niepoprawna nazwa funkcji SQL.")
        return v

    @field_validator("logic_operator", mode="before")
    @classmethod
    def validate_logic_operator(cls, v: Any) -> Any:
        return _normalize_logic_operator(v)


class ReorderItem(BaseModel):
    id_filter: int = Field(..., gt=0)
    priority:  int = Field(..., ge=1, le=9999)


class ConditionCreateBody(BaseModel):
    field_name:  str = Field(..., min_length=1, max_length=100)
    operator:    str = Field(...)
    field_value: str = Field(..., max_length=500)

    @field_validator("operator")
    @classmethod
    def validate_op(cls, v: str) -> str:
        if v not in _VALID_OPERATORS:
            raise ValueError(f"Operator musi byc jednym z: {sorted(_VALID_OPERATORS)}")
        return v


class ConfirmDeleteBody(BaseModel):
    delete_token: str = Field(..., min_length=1)


# =============================================================================
# Pomocnicze
# =============================================================================


def _filter_row_to_dict(row: Any) -> dict[str, Any]:
    """Mapuje wiersz SELECT filtra na pelny stan audytowy/API."""
    return {
        "id_filter": row[0],
        "filter_name": row[1],
        "filter_type": row[2],
        "id_path": row[3],
        "id_source": row[4],
        "priority": row[5],
        "is_active": bool(row[6]),
        "universal_function": row[7],
        "logic_operator": row[8] or _DEFAULT_LOGIC_OPERATOR,
        "created_at": dt_utc(row[9]) if len(row) > 9 else None,
        "updated_at": dt_utc(row[10]) if len(row) > 10 else None,
    }


async def _get_filter_row(db: DB, id_filter: int) -> Any:
    return (await db.execute(
        text(
            f"SELECT [id_filter],[filter_name],[filter_type],[id_path],[id_source],"
            f"[priority],[is_active],[universal_function],[logic_operator],"
            f"[created_at],[updated_at] "
            f"FROM [{_SCHEMA}].[skw_approval_filters] WHERE [id_filter]=:f"
        ),
        {"f": id_filter},
    )).fetchone()


def _audit_crud(
    db: DB,
    *,
    action: str,
    entity_type: str,
    entity_id: Optional[int],
    current_user: CurrentUser,
    old_value: Optional[dict[str, Any]] = None,
    new_value: Optional[dict[str, Any]] = None,
    details: Optional[dict[str, Any]] = None,
) -> None:
    """Kanoniczny audit projektu: dbo_ext.AuditLog + dzienny plik JSONL."""
    audit_service.log_crud(
        db,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        old_value=old_value,
        new_value=new_value,
        user_id=current_user.id_user,
        username=getattr(current_user, "username", None),
        details=details,
    )


# =============================================================================
# Filtry
# =============================================================================


@router.get(
    "",
    summary="Lista filtrow automatycznych",
    description="Zwraca filtry posortowane priority DESC.",
    dependencies=[require_permission("approval.manage_filters")],
)
async def list_filters(
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    id_source: Optional[int] = None,
    active_only: bool = True,
):
    await _check_module_enabled(db, redis)
    where = ["1=1"]
    params: dict[str, Any] = {}
    if active_only:
        where.append("f.[is_active]=1")
    if id_source is not None:
        where.append("(f.[id_source]=:src OR f.[id_source] IS NULL)")
        params["src"] = id_source

    rows = await db.execute(
        text(
            f"SELECT f.[id_filter],f.[filter_name],f.[filter_type],f.[id_path],"
            f"  f.[id_source],f.[priority],f.[is_active],f.[universal_function],"
            f"  f.[logic_operator],p.[path_name],"
            f"  (SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_filter_conditions] c "
            f"   WHERE c.[id_filter]=f.[id_filter]) AS condition_count "
            f"FROM [{_SCHEMA}].[skw_approval_filters] f "
            f"LEFT JOIN [{_SCHEMA}].[skw_approval_paths] p ON p.[id_path]=f.[id_path] "
            f"WHERE {' AND '.join(where)} ORDER BY f.[priority] DESC"
        ),
        params,
    )
    return {
        "data": [
            {
                "id_filter": r[0],
                "filter_name": r[1],
                "filter_type": r[2],
                "id_path": r[3],
                "id_source": r[4],
                "priority": r[5],
                "is_active": bool(r[6]),
                "universal_function": r[7],
                "logic_operator": r[8] or _DEFAULT_LOGIC_OPERATOR,
                "path_name": r[9],
                "condition_count": r[10],
            }
            for r in rows.fetchall()
        ]
    }


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Utworz filtr automatyczny",
    dependencies=[require_permission("approval.manage_filters")],
)
async def create_filter(
    body: FilterCreateBody,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    await _check_module_enabled(db, redis)

    if body.filter_type == "universal" and not body.universal_function:
        raise HTTPException(status_code=422, detail="Dla universal wymagane universal_function.")
    if body.filter_type == "universal" and body.logic_operator != _DEFAULT_LOGIC_OPERATOR:
        raise HTTPException(
            status_code=422,
            detail="logic_operator='OR' jest dostepny wylacznie dla filtra typu standard.",
        )

    logic_operator = (
        body.logic_operator if body.filter_type == "standard" else _DEFAULT_LOGIC_OPERATOR
    )

    result = await db.execute(
        text(
            f"INSERT INTO [{_SCHEMA}].[skw_approval_filters] "
            f"([filter_name],[filter_type],[id_path],[id_source],[priority],"
            f" [universal_function],[logic_operator],[is_active]) "
            f"OUTPUT INSERTED.[id_filter] "
            f"VALUES (:fn,:ft,:ip,:is,:pr,:uf,:lo,:ia)"
        ),
        {
            "fn": body.filter_name,
            "ft": body.filter_type,
            "ip": body.id_path,
            "is": body.id_source,
            "pr": body.priority,
            "uf": body.universal_function,
            "lo": logic_operator,
            # NAPRAWA (2026-08-18, incydent id_instance=2930, FORTIS/UISA):
            # filtr ZAWSZE powstaje jako nieaktywny. FilterCreateBody celowo
            # nie ma pola is_active — nie ma go czym "przyslac", wiec nie ma
            # czego tu ignorowac. Aktywacja WYLACZNIE przez oddzielny
            # POST /{id_filter}/activate (patrz ZMIANA 3/4), ktory wymusza
            # obecnosc >=1 warunku dla filtra standard. Eliminuje to u
            # zrodla okno "aktywny, pusty filtr = catch-all" — auto_dispatch
            # (worker) dopasowal dokument 2930 266ms po utworzeniu filtra
            # 18, zanim jego pierwszy warunek zdazyl sie zapisac.
            "ia": 0,
        },
    )
    new_id = int(result.fetchone()[0])
    await db.commit()

    created_row = await _get_filter_row(db, new_id)
    created_state = _filter_row_to_dict(created_row)
    _audit_crud(
        db,
        action="approval_filter_created",
        entity_type="ApprovalFilter",
        entity_id=new_id,
        current_user=current_user,
        new_value=created_state,
        details={
            "logic_operator": logic_operator,
            "is_active": False,
            "activation_required_via": f"POST /approval/filters/{new_id}/activate",
        },
    )

    return {
        "id_filter": new_id,
        "filter_name": body.filter_name,
        "filter_type": body.filter_type,
        "priority": body.priority,
        "logic_operator": logic_operator,
        "is_active": False,
        "message": (
            "Filtr utworzony jako NIEAKTYWNY. Dodaj warunki, a nastepnie "
            f"aktywuj przez POST /approval/filters/{new_id}/activate."
        ),
    }


# KRYTYCZNE: /reorder PRZED /{id_filter}
@router.patch(
    "/reorder",
    summary="Zmien kolejnosc filtrow (bulk priority)",
    dependencies=[require_permission("approval.manage_filters")],
)
async def reorder_filters(
    items: list[ReorderItem],
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    await _check_module_enabled(db, redis)
    if not items:
        raise HTTPException(status_code=422, detail="Lista nie moze byc pusta.")

    ids = [item.id_filter for item in items]
    if len(ids) != len(set(ids)):
        raise HTTPException(status_code=422, detail="id_filter nie moze sie powtarzac.")

    before: list[dict[str, int]] = []
    after: list[dict[str, int]] = []
    updated = 0

    for item in items:
        row = (await db.execute(
            text(
                f"SELECT [priority] FROM [{_SCHEMA}].[skw_approval_filters] "
                f"WHERE [id_filter]=:f"
            ),
            {"f": item.id_filter},
        )).fetchone()
        if not row:
            await db.rollback()
            raise HTTPException(
                status_code=404,
                detail=f"Filtr id_filter={item.id_filter} nie istnieje.",
            )

        before.append({"id_filter": item.id_filter, "priority": int(row[0])})
        result = await db.execute(
            text(
                f"UPDATE [{_SCHEMA}].[skw_approval_filters] "
                f"SET [priority]=:pr,[updated_at]=SYSUTCDATETIME() "
                f"WHERE [id_filter]=:f"
            ),
            {"pr": item.priority, "f": item.id_filter},
        )
        updated += result.rowcount
        after.append({"id_filter": item.id_filter, "priority": item.priority})

    await db.commit()
    _audit_crud(
        db,
        action="approval_filters_reordered",
        entity_type="ApprovalFilter",
        entity_id=None,
        current_user=current_user,
        old_value={"items": before},
        new_value={"items": after},
        details={"updated": updated},
    )
    return {"updated": updated}


@router.get(
    "/{id_filter}",
    summary="Szczegoly filtru z warunkami",
    dependencies=[require_permission("approval.manage_filters")],
)
async def get_filter(
    id_filter: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    await _check_module_enabled(db, redis)
    row = await _get_filter_row(db, id_filter)
    if not row:
        raise HTTPException(status_code=404, detail="Filtr nie istnieje.")

    cond_rows = await db.execute(
        text(
            f"SELECT [id_condition],[field_name],[operator],[field_value] "
            f"FROM [{_SCHEMA}].[skw_approval_filter_conditions] "
            f"WHERE [id_filter]=:f ORDER BY [id_condition] ASC"
        ),
        {"f": id_filter},
    )
    result = _filter_row_to_dict(row)
    result["conditions"] = [
        {
            "id_condition": cr[0],
            "field_name": cr[1],
            "operator": cr[2],
            "field_value": cr[3],
        }
        for cr in cond_rows.fetchall()
    ]
    return result


@router.patch(
    "/{id_filter}",
    summary="Aktualizacja filtru",
    dependencies=[require_permission("approval.manage_filters")],
)
async def update_filter(
    id_filter: int,
    body: FilterPatchBody,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    await _check_module_enabled(db, redis)

    existing_row = await _get_filter_row(db, id_filter)
    if not existing_row:
        raise HTTPException(status_code=404, detail="Filtr nie istnieje.")
    old_state = _filter_row_to_dict(existing_row)

    if (
        body.logic_operator is not None
        and old_state["filter_type"] != "standard"
        and body.logic_operator != _DEFAULT_LOGIC_OPERATOR
    ):
        raise HTTPException(
            status_code=422,
            detail="logic_operator='OR' jest dostepny wylacznie dla filtra typu standard.",
        )

    sets: list[str] = []
    params: dict[str, Any] = {"f": id_filter}
    if body.filter_name is not None:
        sets.append("[filter_name]=:fn")
        params["fn"] = body.filter_name
    if body.priority is not None:
        sets.append("[priority]=:pr")
        params["pr"] = body.priority
    if body.is_active is not None:
        if body.is_active:
            # NAPRAWA (2026-08-18): aktywacja WYLACZNIE przez dedykowany
            # POST /{id_filter}/activate — jedyne miejsce, ktore wymusza
            # obecnosc co najmniej jednego warunku przed przejsciem
            # is_active 0 -> 1. Gdyby to pole zostalo dostepne rowniez tu,
            # zwykly PATCH omijalby zabezpieczenie calkowicie.
            raise HTTPException(
                status_code=422,
                detail=(
                    "Aktywacja filtra jest mozliwa wylacznie przez "
                    f"POST /approval/filters/{id_filter}/activate "
                    "(wymaga co najmniej jednego warunku dla filtra typu standard)."
                ),
            )
        sets.append("[is_active]=:a")
        params["a"] = 0
    if body.universal_function is not None:
        sets.append("[universal_function]=:uf")
        params["uf"] = body.universal_function
    if body.logic_operator is not None:
        sets.append("[logic_operator]=:lo")
        params["lo"] = (
            body.logic_operator
            if old_state["filter_type"] == "standard"
            else _DEFAULT_LOGIC_OPERATOR
        )

    if not sets:
        raise HTTPException(status_code=422, detail="Brak pol.")

    sets.append("[updated_at]=SYSUTCDATETIME()")
    await db.execute(
        text(
            f"UPDATE [{_SCHEMA}].[skw_approval_filters] "
            f"SET {','.join(sets)} WHERE [id_filter]=:f"
        ),
        params,
    )
    await db.commit()

    updated_row = await _get_filter_row(db, id_filter)
    new_state = _filter_row_to_dict(updated_row)
    changed_fields = sorted(
        key for key in new_state if old_state.get(key) != new_state.get(key)
    )
    _audit_crud(
        db,
        action="approval_filter_updated",
        entity_type="ApprovalFilter",
        entity_id=id_filter,
        current_user=current_user,
        old_value=old_state,
        new_value=new_state,
        details={
            "changed_fields": changed_fields,
            "logic_operator_changed": (
                old_state["logic_operator"] != new_state["logic_operator"]
            ),
        },
    )

    return {
        "id_filter": id_filter,
        "updated": True,
        "logic_operator": new_state["logic_operator"],
    }

@router.post(
    "/{id_filter}/activate",
    summary="Aktywuj filtr (jedyna droga is_active 0 -> 1)",
    description=(
        "Dla filtra typu standard wymaga >=1 zapisanego warunku — inaczej "
        "422. Dla filtra typu universal warunek nie jest wymagany "
        "(uzywa universal_function, obowiazkowej juz przy tworzeniu). "
        "NAPRAWA 2026-08-18 po incydencie id_instance=2930 (FORTIS/UISA, "
        "id_filter=18): eliminuje mozliwosc istnienia aktywnego, pustego "
        "filtra AND, ktory _evaluate_standard_filter() traktuje jako "
        "catch-all (dopasowuje kazdy dokument)."
    ),
    responses={
        404: {"description": "Filtr nie istnieje"},
        422: {"description": "Filtr typu standard bez zadnego warunku"},
    },
    dependencies=[require_permission("approval.manage_filters")],
)
async def activate_filter(
    id_filter: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    await _check_module_enabled(db, redis)

    row = await _get_filter_row(db, id_filter)
    if not row:
        raise HTTPException(status_code=404, detail="Filtr nie istnieje.")
    old_state = _filter_row_to_dict(row)

    condition_count = 0
    if old_state["filter_type"] == "standard":
        count_row = (await db.execute(
            text(
                f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_filter_conditions] "
                f"WHERE [id_filter]=:f"
            ),
            {"f": id_filter},
        )).fetchone()
        condition_count = int(count_row[0]) if count_row else 0
        if condition_count == 0:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Filtr typu standard bez zadnego warunku nie moze zostac "
                    "aktywowany — dzialalby jako catch-all (dopasowuje "
                    "kazdy dokument). Dodaj co najmniej jeden warunek przez "
                    f"POST /approval/filters/{id_filter}/conditions."
                ),
            )

    await db.execute(
        text(
            f"UPDATE [{_SCHEMA}].[skw_approval_filters] "
            f"SET [is_active]=1,[updated_at]=SYSUTCDATETIME() "
            f"WHERE [id_filter]=:f"
        ),
        {"f": id_filter},
    )
    await db.commit()

    updated_row = await _get_filter_row(db, id_filter)
    new_state = _filter_row_to_dict(updated_row)
    _audit_crud(
        db,
        action="approval_filter_activated",
        entity_type="ApprovalFilter",
        entity_id=id_filter,
        current_user=current_user,
        old_value=old_state,
        new_value=new_state,
        details={"condition_count": condition_count},
    )

    return {
        "id_filter": id_filter,
        "is_active": True,
        "condition_count": condition_count,
    }

@router.delete(
    "/{id_filter}/initiate",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Krok 1/2 — Inicjacja usuniecia filtru",
    description="Generuje token JWT (TTL 60s). Uzyj w DELETE /{id_filter}/confirm.",
    responses={202: {"description": "Token wygenerowany"}, 404: {"description": "Nie istnieje"}},
    dependencies=[require_permission("approval.manage_filters")],
)
async def initiate_delete_filter(
    id_filter: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request: Request,
):
    await _check_module_enabled(db, redis)
    row = await _get_filter_row(db, id_filter)
    if not row:
        raise HTTPException(status_code=404, detail="Filtr nie istnieje.")
    state = _filter_row_to_dict(row)

    token, ttl = await generate_delete_token(
        redis,
        entity_id=id_filter,
        scope=_SCOPE,
        initiated_by=current_user.id_user,
        extra={"filter_name": state["filter_name"]},
    )
    logger.warning(
        orjson.dumps({
            "event": "approval_filter_delete_initiated",
            "id_filter": id_filter,
            "filter_name": state["filter_name"],
            "initiated_by": current_user.id_user,
            "ip": request.headers.get(
                "X-Forwarded-For", getattr(request.client, "host", None)
            ),
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )
    return {
        "delete_token": token,
        "expires_in": ttl,
        "id_filter": id_filter,
        "filter_name": state["filter_name"],
        "message": (
            f"Token wazny {ttl}s. Uzyj w "
            f"DELETE /approval/filters/{id_filter}/confirm."
        ),
    }


@router.delete(
    "/{id_filter}/confirm",
    status_code=status.HTTP_200_OK,
    summary="Krok 2/2 — Potwierdzenie usuniecia filtru",
    description="Usuwa filtr (hard delete). Wymaga `delete_token` w body JSON.",
    responses={400: {"description": "Nieprawidlowy lub wygasly token"}},
    dependencies=[require_permission("approval.manage_filters")],
)
async def confirm_delete_filter(
    id_filter: int,
    body: ConfirmDeleteBody,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
    request: Request,
):
    await _check_module_enabled(db, redis)
    await verify_delete_token(
        redis,
        token=body.delete_token,
        entity_id=id_filter,
        scope=_SCOPE,
    )

    existing_row = await _get_filter_row(db, id_filter)
    if not existing_row:
        raise HTTPException(status_code=404, detail="Filtr nie istnieje.")
    old_state = _filter_row_to_dict(existing_row)

    conditions = (await db.execute(
        text(
            f"SELECT [id_condition],[field_name],[operator],[field_value] "
            f"FROM [{_SCHEMA}].[skw_approval_filter_conditions] "
            f"WHERE [id_filter]=:f ORDER BY [id_condition] ASC"
        ),
        {"f": id_filter},
    )).fetchall()
    old_state["conditions"] = [
        {
            "id_condition": r[0],
            "field_name": r[1],
            "operator": r[2],
            "field_value": r[3],
        }
        for r in conditions
    ]

    await db.execute(
        text(f"DELETE FROM [{_SCHEMA}].[skw_approval_filters] WHERE [id_filter]=:f"),
        {"f": id_filter},
    )
    await db.commit()

    _audit_crud(
        db,
        action="approval_filter_deleted",
        entity_type="ApprovalFilter",
        entity_id=id_filter,
        current_user=current_user,
        old_value=old_state,
        new_value={"deleted": True},
    )
    logger.warning(
        orjson.dumps({
            "event": "approval_filter_deleted",
            "id_filter": id_filter,
            "deleted_by": current_user.id_user,
            "ip": request.headers.get(
                "X-Forwarded-For", getattr(request.client, "host", None)
            ),
            "ts": datetime.now(timezone.utc).isoformat(),
        }).decode()
    )
    return {"id_filter": id_filter, "deleted": True}


# =============================================================================
# Warunki filtra
# =============================================================================


@router.post(
    "/{id_filter}/conditions",
    status_code=status.HTTP_201_CREATED,
    summary="Dodaj warunek do filtru standard",
    responses={422: {"description": "Niepoprawny operator"}},
    dependencies=[require_permission("approval.manage_filters")],
)
async def add_condition(
    id_filter: int,
    body: ConditionCreateBody,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    await _check_module_enabled(db, redis)

    filter_row = await _get_filter_row(db, id_filter)
    if not filter_row:
        raise HTTPException(status_code=404, detail="Filtr nie istnieje.")
    filter_state = _filter_row_to_dict(filter_row)
    if filter_state["filter_type"] != "standard":
        raise HTTPException(
            status_code=422,
            detail="Warunki mozna dodawac wylacznie do filtra typu standard.",
        )

    result = await db.execute(
        text(
            f"INSERT INTO [{_SCHEMA}].[skw_approval_filter_conditions] "
            f"([id_filter],[field_name],[operator],[field_value]) "
            f"OUTPUT INSERTED.[id_condition] VALUES (:f,:fn,:op,:fv)"
        ),
        {
            "f": id_filter,
            "fn": body.field_name,
            "op": body.operator,
            "fv": body.field_value,
        },
    )
    new_id = int(result.fetchone()[0])
    await db.commit()

    new_condition = {
        "id_condition": new_id,
        "id_filter": id_filter,
        "field_name": body.field_name,
        "operator": body.operator,
        "field_value": body.field_value,
    }
    _audit_crud(
        db,
        action="approval_filter_condition_created",
        entity_type="ApprovalFilterCondition",
        entity_id=new_id,
        current_user=current_user,
        new_value=new_condition,
        details={"id_filter": id_filter},
    )
    return new_condition


@router.delete(
    "/{id_filter}/conditions/{id_condition}",
    summary="Usun warunek filtru",
    dependencies=[require_permission("approval.manage_filters")],
)
async def delete_condition(
    id_filter: int,
    id_condition: int,
    current_user: CurrentUser,
    db: DB,
    redis: RedisClient,
):
    await _check_module_enabled(db, redis)

    condition_row = (await db.execute(
        text(
            f"SELECT [id_condition],[id_filter],[field_name],[operator],[field_value] "
            f"FROM [{_SCHEMA}].[skw_approval_filter_conditions] "
            f"WHERE [id_condition]=:c AND [id_filter]=:f"
        ),
        {"c": id_condition, "f": id_filter},
    )).fetchone()
    if not condition_row:
        raise HTTPException(status_code=404, detail="Warunek nie istnieje.")

    old_condition = {
        "id_condition": condition_row[0],
        "id_filter": condition_row[1],
        "field_name": condition_row[2],
        "operator": condition_row[3],
        "field_value": condition_row[4],
    }
    await db.execute(
        text(
            f"DELETE FROM [{_SCHEMA}].[skw_approval_filter_conditions] "
            f"WHERE [id_condition]=:c AND [id_filter]=:f"
        ),
        {"c": id_condition, "f": id_filter},
    )

    # NAPRAWA (2026-08-18, incydent id_instance=2930): jesli to byl OSTATNI
    # warunek aktywnego filtra standard — automatyczna dezaktywacja, zeby
    # nie zostawic aktywnego, pustego AND-a (catch-all).
    # ZALOZENIE (Claude, do potwierdzenia z Michalem — front napisal
    # "albo blokuje operacje, albo automatycznie dezaktywuje", bez
    # jednoznacznego wyboru): wybrano auto-dezaktywacje — mniej zaskakujaca
    # dla wywolujacego (200 zamiast 409/422), filtr i tak przestaje
    # dzialac zgodnie z oczekiwaniem. Jesli wolisz twarda blokade zamiast
    # tego, przenies ponizszy warunek PRZED DELETE powyzej i zamien
    # UPDATE is_active=0 na "raise HTTPException(409, ...)" bez wykonywania
    # samego DELETE.
    auto_deactivated = False
    remaining_row = (await db.execute(
        text(
            f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_filter_conditions] "
            f"WHERE [id_filter]=:f"
        ),
        {"f": id_filter},
    )).fetchone()
    remaining_count = int(remaining_row[0]) if remaining_row else 0

    filter_state_row = await _get_filter_row(db, id_filter)
    filter_state = _filter_row_to_dict(filter_state_row) if filter_state_row else None

    if (
        remaining_count == 0
        and filter_state is not None
        and filter_state["filter_type"] == "standard"
        and filter_state["is_active"]
    ):
        await db.execute(
            text(
                f"UPDATE [{_SCHEMA}].[skw_approval_filters] "
                f"SET [is_active]=0,[updated_at]=SYSUTCDATETIME() "
                f"WHERE [id_filter]=:f"
            ),
            {"f": id_filter},
        )
        auto_deactivated = True

    await db.commit()

    _audit_crud(
        db,
        action="approval_filter_condition_deleted",
        entity_type="ApprovalFilterCondition",
        entity_id=id_condition,
        current_user=current_user,
        old_value=old_condition,
        new_value={"deleted": True},
        details={"id_filter": id_filter, "auto_deactivated": auto_deactivated},
    )
    if auto_deactivated:
        _audit_crud(
            db,
            action="approval_filter_auto_deactivated",
            entity_type="ApprovalFilter",
            entity_id=id_filter,
            current_user=current_user,
            old_value={"is_active": True},
            new_value={"is_active": False},
            details={
                "reason": "last_condition_removed",
                "id_condition_removed": id_condition,
            },
        )
        logger.warning(
            orjson.dumps({
                "event": "approval_filter_auto_deactivated",
                "id_filter": id_filter,
                "reason": "last_condition_removed",
                "id_condition_removed": id_condition,
                "by_user": current_user.id_user,
                "ts": datetime.now(timezone.utc).isoformat(),
            }).decode()
        )

    return {
        "id_condition": id_condition,
        "deleted": True,
        "filter_auto_deactivated": auto_deactivated,
    }
