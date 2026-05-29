# =============================================================================
# PLIK 1: backend/app/services/approval_service_ext.py
#
# Rozszerzenie approval_service.py o:
#   - forward()
#   - send_to_group()
#   - mark_urgent()
#   - invalidate_group_cache()  — wywoluj przy zmianach grup/delegacji
#   - validate_delegation_create()
#
# INTEGRACJA z approval_service.py:
#   Skopiuj te funkcje na koniec istniejacego pliku approval_service.py.
#   Nie twórz osobnego pliku — silnik importow jest jednorodny.
#
# UWAGA: from __future__ import annotations — NIGDY w tym pliku.
# =============================================================================

import json
import logging
import re
from datetime import datetime, timezone

from fastapi import HTTPException, status
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.redis_lock import approval_lock

logger = logging.getLogger(__name__)
_SCHEMA = "dbo"
_CACHE_MEMBERS_TTL = 300
_CACHE_DELEG_TTL   = 300


# =============================================================================
# FORWARD — przekaz dalej (biezacy etap NIE jest zaliczany)
# =============================================================================

async def forward(
    db: AsyncSession,
    redis: Redis,
    *,
    id_instance: int,
    id_target_group: int,
    id_user: int,
    username: str,
    comment: str,
    deadline_hours: int | None = None,
    has_forward_permission: bool,
    ip_address: str | None = None,
) -> dict:
    """
    Przekazuje dokument do innej grupy bez zaliczania biezacego etapu.

    "To nie moj dokument — niech ta grupa sie tym zajmie."
    Biezacy krok NIE jest oznaczany jako zaakceptowany.
    Nowa grupa wstawiana przed biezaca pozycja (current_step),
    biezaca pozycja przesuwa sie o +1.

    Sprawdzenia:
      - approval.forward (has_forward_permission=True)
      - czlonkostwo w biezacej grupie
      - id_target_group rozny od biezacej grupy (409)
      - komentarz wymagany
      - status = in_progress

    Kroki:
      1. Walidacja uprawnien, statusu, grupy docelowej
      2. insert_group_into_snapshot(current_step, id_target_group)
      3. approval_log action=forwarded
      4. Commit
      5. JSONL

    Raises:
        HTTPException(403): Brak uprawnienia lub czlonkostwa
        HTTPException(409): Bledny status lub ta sama grupa docelowa
        HTTPException(404): Instancja nie istnieje
        HTTPException(423): Lock niedostepny
    """
    from app.services.approval_service import (
        _check_module_enabled, _check_ratelimit, _get_group_members_cached,
        _get_group_delegations_cached, _resolve_effective_users,
        _insert_approval_log, insert_group_into_snapshot, _jsonl_log,
    )

    if not comment or not comment.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Komentarz jest wymagany przy przekazywaniu dokumentu.",
        )
    if not has_forward_permission:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Brak uprawnienia approval.forward.",
        )

    await _check_ratelimit(redis, id_user, id_instance)
    await _check_module_enabled(db, redis)

    async with approval_lock(redis, id_instance):
        # Pobierz instancje
        inst_row = await db.execute(
            text(
                f"SELECT [status], [current_step] "
                f"FROM [{_SCHEMA}].[skw_document_approval_instances] "
                f"WHERE [id_instance] = :i"
            ),
            {"i": id_instance},
        )
        inst = inst_row.fetchone()
        if not inst:
            raise HTTPException(status_code=404, detail=f"Instancja {id_instance} nie istnieje.")
        if inst[0] != "in_progress":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Instancja {id_instance} ma status '{inst[0]}' — forward niedostepny.",
            )

        current_step = inst[1]

        # Pobierz biezaca grupe
        snap_row = await db.execute(
            text(
                f"SELECT [id_group] "
                f"FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                f"WHERE [id_instance] = :i AND [step_order] = :s"
            ),
            {"i": id_instance, "s": current_step},
        )
        snap = snap_row.fetchone()
        if not snap:
            raise HTTPException(status_code=500, detail="Blad wewnetrzny: brak kroku snapshotu.")

        current_group = snap[0]

        # Sprawdz ze docelowa rozna od biezacej
        if id_target_group == current_group:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Grupa docelowa ({id_target_group}) jest taka sama jak biezaca. "
                    "Wybierz inna grupe."
                ),
            )

        # Sprawdz czlonkostwo w biezacej grupie
        members = await _get_group_members_cached(db, redis, current_group)
        delegations = await _get_group_delegations_cached(db, redis, current_group)
        effective = _resolve_effective_users(members, delegations)
        is_member = id_user in members
        is_delegate = any(d == id_user for d in effective.values() if d is not None)
        if not is_member and not is_delegate:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "Brak uprawnienia do przekazania dokumentu. "
                    "Wymagane czlonkostwo w grupie biezacego etapu."
                ),
            )

        now = datetime.now(timezone.utc).replace(tzinfo=None)

        # Substytucja — oblicz votes_required dla nowej grupy
        group_row = (await db.execute(
            text(
                f"SELECT [consensus_type] "
                f"FROM [{_SCHEMA}].[skw_approval_groups] "
                f"WHERE [id_group] = :g"
            ),
            {"g": id_target_group},
        )).fetchone()
        if not group_row:
            raise HTTPException(
                status_code=404,
                detail=f"Grupa docelowa {id_target_group} nie istnieje.",
            )

        target_consensus = group_row[0]
        if target_consensus == "AND":
            target_members = await _get_group_members_cached(db, redis, id_target_group)
            votes_required = max(1, len(target_members))
        else:
            votes_required = 1

        step_deadline = (
            now + timedelta(hours=deadline_hours) if deadline_hours else None
        )

        # Podmień grupę w bieżącym kroku snapshotu — bez przesuwania step_order
        await db.execute(
            text(
                f"UPDATE [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                f"SET [id_group]       = :tg, "
                f"    [votes_required] = :vr, "
                f"    [votes_cast]     = 0, "
                f"    [status]         = 'in_progress', "
                f"    [deadline_at]    = :dl, "
                f"    [completed_at]   = NULL, "
                f"    [updated_at]     = :now "
                f"WHERE [id_instance] = :i AND [step_order] = :s"
            ),
            {
                "tg":  id_target_group,
                "vr":  votes_required,
                "dl":  step_deadline,
                "now": now,
                "i":   id_instance,
                "s":   current_step,
            },
        )

        # Unieważnij ewentualne głosy starej grupy dla tego etapu
        await db.execute(
            text(
                f"UPDATE [{_SCHEMA}].[skw_approval_log] "
                f"SET [is_voided] = 1 "
                f"WHERE [id_instance] = :i "
                f"  AND [step_order]  = :s "
                f"  AND [action]      = 'accepted' "
                f"  AND [is_voided]   = 0"
            ),
            {"i": id_instance, "s": current_step},
        )

        await _insert_approval_log(
            db,
            id_instance=id_instance,
            id_user=id_user,
            username_snapshot=username,
            action="forwarded",
            step_order_snapshot=current_step,
            id_group_snapshot=current_group,
            details={
                "comment":         comment.strip(),
                "id_target_group": id_target_group,
                "deadline_hours":  deadline_hours,
            },
            ip_address=ip_address,
        )

        await db.commit()

    _jsonl_log("forwarded", {
        "id_instance": id_instance,
        "id_user":     id_user,
        "from_group":  current_group,
        "to_group":    id_target_group,
        "at_step":     current_step,
    })

    logger.info(
        "forward | SUBSTITUTED | id_instance=%d step=%d from=%d to=%d",
        id_instance, current_step, current_group, id_target_group,
    )

    return {
        "id_instance":      id_instance,
        "substituted_step": current_step,
        "from_group":       current_group,
        "id_target_group":  id_target_group,
    }


# =============================================================================
# SEND TO GROUP — wyslij do dodatkowej grupy (biezacy etap kontynuuje po)
# =============================================================================

async def send_to_group(
    db: AsyncSession,
    redis: Redis,
    *,
    id_instance: int,
    id_target_group: int,
    id_user: int,
    username: str,
    comment: str,
    deadline_hours: int | None = None,
    has_send_to_group_permission: bool,
    has_supervise: bool = False,
    ip_address: str | None = None,
) -> dict:
    """
    Kieruje dokument do dodatkowej grupy akceptacyjnej przed biezacym etapem.

    "Zanim pojdzie dalej, niech ta konkretna grupa to zatwierdzi."
    Po akceptacji przez wstawiona grupe dokument wraca na BIEZACY etap (N+1).
    Roznica od forward: biezacy step_order po operacji pozostaje N
    (nowa grupa wstawiona pod N, oryginalny etap przesuniety na N+1).

    WAZNE: Poprzednie glosy biezacego etapu sa void-owane — czlonek ktory
    wykonal send_to_group musi zaakceptowac ponownie po powrocie dokumentu.

    Sprawdzenia:
      - approval.send_to_group (has_send_to_group_permission=True)
      - czlonkostwo w biezacej grupie LUB approval.supervise
      - id_target_group rozny od biezacej grupy (409)
      - komentarz wymagany
      - status = in_progress
    """
    from app.services.approval_service import (
        _check_module_enabled, _check_ratelimit, _get_group_members_cached,
        _get_group_delegations_cached, _resolve_effective_users,
        _insert_approval_log, insert_group_into_snapshot, _jsonl_log,
        _void_votes_for_step,
    )

    if not comment or not comment.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Komentarz jest wymagany przy przekazywaniu do grupy.",
        )
    if not has_send_to_group_permission:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Brak uprawnienia approval.send_to_group.",
        )

    await _check_ratelimit(redis, id_user, id_instance)
    await _check_module_enabled(db, redis)

    async with approval_lock(redis, id_instance):
        # Pobierz instancje
        inst_row = await db.execute(
            text(
                f"SELECT [status], [current_step] "
                f"FROM [{_SCHEMA}].[skw_document_approval_instances] "
                f"WHERE [id_instance] = :i"
            ),
            {"i": id_instance},
        )
        inst = inst_row.fetchone()
        if not inst:
            raise HTTPException(status_code=404, detail=f"Instancja {id_instance} nie istnieje.")
        if inst[0] != "in_progress":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Instancja {id_instance} ma status '{inst[0]}'.",
            )

        current_step = inst[1]

        # Pobierz biezaca grupe
        snap_row = await db.execute(
            text(
                f"SELECT [id_group] "
                f"FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                f"WHERE [id_instance] = :i AND [step_order] = :s"
            ),
            {"i": id_instance, "s": current_step},
        )
        snap = snap_row.fetchone()
        if not snap:
            raise HTTPException(status_code=500, detail="Blad wewnetrzny: brak kroku snapshotu.")

        current_group = snap[0]

        if id_target_group == current_group:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Grupa docelowa jest taka sama jak biezaca.",
            )

        # Sprawdz czlonkostwo lub supervise
        if not has_supervise:
            members = await _get_group_members_cached(db, redis, current_group)
            delegations = await _get_group_delegations_cached(db, redis, current_group)
            effective = _resolve_effective_users(members, delegations)
            is_member = id_user in members
            is_delegate = any(d == id_user for d in effective.values() if d is not None)
            if not is_member and not is_delegate:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Brak uprawnienia. Wymagane czlonkostwo lub approval.supervise.",
                )

        # Wstaw nowa grupe na BIEZACA pozycje (oryginalny krok -> N+1)
        await insert_group_into_snapshot(
            db, redis,
            id_instance=id_instance,
            position=current_step,
            id_group=id_target_group,
            id_user=id_user,
            deadline_hours=deadline_hours,
        )

        # Void wcześniejszych glosow biezacego etapu (teraz na pozycji N+1)
        # WAZNE: current_step wciaz wskazuje na nowo wstawiona grupe (N)
        # Glosy dla oryginalnego kroku (teraz N+1) trzeba void-owac
        # zeby czlonek musial zaakceptowac ponownie.
        voided = await _void_votes_for_step(db, id_instance, current_step + 1)

        now = datetime.now(timezone.utc).replace(tzinfo=None)

        await _insert_approval_log(
            db,
            id_instance=id_instance,
            id_user=id_user,
            username_snapshot=username,
            action="send_to_group",
            step_order_snapshot=current_step,
            id_group_snapshot=current_group,
            details={
                "comment":          comment.strip(),
                "id_target_group":  id_target_group,
                "deadline_hours":   deadline_hours,
                "voided_votes":     voided,
                "original_step_now": current_step + 1,
            },
            ip_address=ip_address,
        )

        await db.commit()

    _jsonl_log("send_to_group", {
        "id_instance":    id_instance,
        "id_user":        id_user,
        "current_group":  current_group,
        "target_group":   id_target_group,
        "current_step":   current_step,
        "voided":         voided,
    })

    return {
        "id_instance":       id_instance,
        "inserted_at_step":  current_step,
        "id_target_group":   id_target_group,
        "original_step_now": current_step + 1,
        "voided_votes":      voided,
    }


# =============================================================================
# MARK URGENT — oznacz dokument jako pilny / cofnij oznaczenie
# =============================================================================

async def mark_urgent(
    db: AsyncSession,
    redis: Redis,
    *,
    id_instance: int,
    is_urgent: bool,
    id_user: int,
    username: str,
    ip_address: str | None = None,
) -> dict:
    """
    Ustawia lub usuwa flage is_urgent na instancji obiegu.

    Wymaga feature flagi APPROVAL_URGENT_MARKING_ENABLED=true.
    Dostepne w trakcie dowolnego statusu (poza terminal approved/cancelled).
    Kolejka dispatch-queue i my-queue sortuje: is_urgent DESC, created_at ASC.

    Raises:
        HTTPException(503): Flaga wylaczona
        HTTPException(404): Instancja nie istnieje
        HTTPException(409): Instancja w statusie terminal
    """
    from app.services.approval_service import (
        _check_module_enabled, _check_feature_flag, _insert_approval_log, _jsonl_log,
    )

    await _check_module_enabled(db, redis)
    await _check_feature_flag(
        db, redis,
        key="APPROVAL_URGENT_MARKING_ENABLED",
        expected="true",
        error_msg="Tryb pilny jest wylaczony przez administratora.",
    )

    inst_row = await db.execute(
        text(
            f"SELECT [status], [is_urgent] "
            f"FROM [{_SCHEMA}].[skw_document_approval_instances] "
            f"WHERE [id_instance] = :i"
        ),
        {"i": id_instance},
    )
    inst = inst_row.fetchone()
    if not inst:
        raise HTTPException(status_code=404, detail=f"Instancja {id_instance} nie istnieje.")
    if inst[0] in ("approved", "cancelled"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Instancja {id_instance} jest zamknieta (status='{inst[0]}').",
        )

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    await db.execute(
        text(
            f"UPDATE [{_SCHEMA}].[skw_document_approval_instances] "
            f"SET [is_urgent] = :u, [updated_at] = :now "
            f"WHERE [id_instance] = :i"
        ),
        {"u": 1 if is_urgent else 0, "now": now, "i": id_instance},
    )

    action = "marked_urgent" if is_urgent else "unmarked_urgent"
    await _insert_approval_log(
        db,
        id_instance=id_instance,
        id_user=id_user,
        username_snapshot=username,
        action=action,
        details={"is_urgent": is_urgent, "previous": bool(inst[1])},
        ip_address=ip_address,
    )

    await db.commit()

    _jsonl_log(action, {
        "id_instance": id_instance, "id_user": id_user, "is_urgent": is_urgent,
    })

    return {"id_instance": id_instance, "is_urgent": is_urgent}


# =============================================================================
# INVALIDATE GROUP CACHE — wywolaj przy kazdej zmianie grupy / delegacji
# =============================================================================

async def invalidate_group_cache(redis: Redis, id_group: int) -> None:
    """
    Natychmiastowe usuniecie cache skladu grupy i jej delegacji z Redis.
    Wywolaj po:
      - dodaniu/usunieciu czlonka grupy (ApprovalGroupMember CREATE/DELETE)
      - utworzeniu/anulowaniu delegacji dla tej grupy (ApprovalDelegation)

    Nie rzuca wyjatku jesli klucz nie istnieje.
    """
    keys = [
        f"group_members:{id_group}",
        f"group_delegations:{id_group}",
    ]
    for key in keys:
        try:
            await redis.delete(key)
        except Exception as exc:
            logger.warning("invalidate_group_cache | Blad DEL klucza %s: %s", key, exc)

    logger.debug("invalidate_group_cache | id_group=%d — cache wyczyszczony", id_group)


# =============================================================================
# VALIDATE DELEGATION CREATE — walidacja przed zapisem delegacji
# =============================================================================

async def validate_delegation_create(
    db: AsyncSession,
    *,
    id_user_from: int,
    id_user_to: int,
    id_group: int | None,
    valid_from: datetime,
    valid_to: datetime,
) -> None:
    """
    Waliduje parametry nowej delegacji przed zapisem do bazy.

    Sprawdzenia:
      1. valid_to > valid_from (HTTP 422)
      2. id_user_from != id_user_to (HTTP 422)
      3. id_user_from jest czlonkiem id_group jesli id_group podany (HTTP 422)
      4. id_user_to NIE jest juz czlonkiem id_group (HTTP 409) — podwojne glosowanie AND
      5. Brak aktywnej nakładajacej sie delegacji for user_from → user_to w tej samej grupie (HTTP 409)
      6. id_user_to nie ma aktywnej delegacji z innego zrodla (brak sub-delegacji) (HTTP 422)

    Raises:
        HTTPException(422): Blad walidacji danych
        HTTPException(409): Konflikt z istniejacymi danymi
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Sprawdzenie 1 i 2
    if valid_to <= valid_from:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="valid_to musi byc pozniejsze niz valid_from.",
        )
    if id_user_from == id_user_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Delegacja do samego siebie jest niedozwolona.",
        )

    if id_group is not None:
        # Sprawdzenie 3: user_from musi byc czlonkiem grupy
        member_check = await db.execute(
            text(
                f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_group_members] "
                f"WHERE [id_group] = :g AND [id_user] = :u"
            ),
            {"g": id_group, "u": id_user_from},
        )
        if member_check.scalar() == 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Uzytkownik {id_user_from} nie jest czlonkiem grupy {id_group}. "
                    "Mozna delegowac tylko w obrębie wlasnych grup."
                ),
            )

        # Sprawdzenie 4: user_to NIE moze byc czlonkiem tej grupy (blokada AND)
        delegate_member_check = await db.execute(
            text(
                f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_group_members] "
                f"WHERE [id_group] = :g AND [id_user] = :u"
            ),
            {"g": id_group, "u": id_user_to},
        )
        if delegate_member_check.scalar() > 0:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Uzytkownik {id_user_to} jest juz czlonkiem grupy {id_group}. "
                    "Delegacja do czlonka tej samej grupy powoduje podwojne glosowanie — niedozwolone."
                ),
            )

    # Sprawdzenie 5: brak nakładajacych sie delegacji
    overlap_check = await db.execute(
        text(
            f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_delegations] "
            f"WHERE [id_user_from] = :uf "
            f"  AND [id_user_to]   = :ut "
            f"  AND [is_active]    = 1 "
            f"  AND [valid_from]  < :vt "
            f"  AND [valid_to]    > :vf "
            f"  AND ([id_group] = :g OR ([id_group] IS NULL AND :g IS NULL))"
        ),
        {
            "uf": id_user_from, "ut": id_user_to,
            "vf": valid_from,   "vt": valid_to,
            "g": id_group,
        },
    )
    if overlap_check.scalar() > 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Istnieje juz aktywna, nakladajaca sie delegacja dla tych uzytkownikow i grupy.",
        )

    # Sprawdzenie 6: user_to nie jest delegatem (brak sub-delegacji)
    sub_deleg_check = await db.execute(
        text(
            f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_delegations] "
            f"WHERE [id_user_to] = :u "
            f"  AND [is_active]  = 1 "
            f"  AND [valid_from] <= :now "
            f"  AND [valid_to]   >= :now"
        ),
        {"u": id_user_to, "now": now},
    )
    if sub_deleg_check.scalar() > 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Uzytkownik {id_user_to} jest juz aktywnym delegatem innej osoby. "
                "Sub-delegacje (delegowanie delegata) sa niedozwolone."
            ),
        )