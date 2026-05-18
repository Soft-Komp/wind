# backend/app/services/approval_service.py
"""
Serwis logiki biznesowej Modulu Obiegu Dokumentow i Akceptacji.

Odpowiedzialnosc:
    dispatch()       — przekazanie dokumentu do obiegu (tworzy snapshot)
    accept()         — akceptacja biezacego etapu (AND/OR, delegacje, koniec)
    rollback()       — cofniecie obiegu do poprzedniego etapu
    reject()         — odrzucenie dokumentu (terminal)
    cancel()         — anulowanie obiegu przez dyspozytora (terminal)
    _insert_approval_log()         — raw SQL INSERT (tabela APPEND-ONLY)
    _get_group_members_cached()    — sklad grupy z Redis (TTL 5 min)
    _get_group_delegations_cached() — aktywne delegacje z Redis (TTL 5 min)
    _check_module_enabled()        — feature flag APPROVAL_MODULE_ENABLED
    _check_feature_flag()          — generyczny feature flag check
    insert_group_into_snapshot()   — wstawienie grupy do snapshotu (forward/send_to_group)

Zasady operacyjne (wg specyfikacji i aneksu Redis):
    1. Kolejnosc: zapis do DB -> commit -> 200 klientowi -> background notifications
       NIGDY nie czekaj na powiadomienia przed odpowiedzia klientowi.
    2. Distributed lock (approval_lock) przed kazdym mutowalnym krokiem
       dotyczacym tej samej instancji.
    3. Rate limiting: approval_ratelimit:{id_user}:{id_instance} TTL 2s
       Sprawdzany przed lockiem — uzupelnienie, nie zamiennik locka.
    4. approval_log: wylacznie raw SQL text() INSERT — tabela ma trigger DENY.
    5. Cache Redis:
       group_members:{id_group}     TTL 5 min
       group_delegations:{id_group} TTL 5 min
       notif_unread:{id_user}       TTL 24h (INCR)
    6. Filtrowany unique index w DB: (id_document, id_source)
       WHERE status <> 'approved' AND status <> 'cancelled'
       Backend uzywa ACTIVE_STATUSES z modelu — musi byc zsynchronizowane.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, HTTPException, status
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.redis_lock import approval_lock
from app.db.models.approval.document_approval_instance import (
    ACTIVE_STATUSES,
    DocumentApprovalInstance,
)
from app.db.models.approval.document_approval_snapshot_step import (
    DocumentApprovalSnapshotStep,
)
from app.db.models.approval.approval_group import ApprovalGroup
from app.db.models.approval.approval_group_member import ApprovalGroupMember
from app.db.models.approval.approval_path import ApprovalPath
from app.db.models.approval.approval_path_step import ApprovalPathStep

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stale
# ---------------------------------------------------------------------------
_SCHEMA = "dbo"

# Klucze Redis
_KEY_LOCK          = "approval_lock:{id_instance}"
_KEY_RATELIMIT     = "approval_ratelimit:{id_user}:{id_instance}"
_KEY_MEMBERS       = "group_members:{id_group}"
_KEY_DELEGATIONS   = "group_delegations:{id_group}"
_KEY_NOTIF_UNREAD  = "notif_unread:{id_user}"

_CACHE_MEMBERS_TTL     = 300   # 5 minut
_CACHE_DELEG_TTL       = 300   # 5 minut
_CACHE_NOTIF_TTL       = 86400  # 24 godziny
_RATELIMIT_TTL         = 2     # 2 sekundy

# Plik JSONL do logowania akcji
_LOG_DIR = Path(os.environ.get("LOG_DIR", "/app/logs"))


# =============================================================================
# Funkcje pomocnicze — prywatne
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_naive() -> datetime:
    """MSSQL DATETIME wymaga datetime bez tzinfo."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _jsonl_log(action: str, data: dict[str, Any]) -> None:
    """
    Zapisuje wpis do dziennego pliku JSONL.
    Non-blocking — blad zapisu tylko loguje, nie rzuca wyjatku.
    """
    try:
        log_file = _LOG_DIR / f"approval_{_utcnow().strftime('%Y-%m-%d')}.jsonl"
        entry = {
            "ts":     _utcnow().isoformat(timespec="milliseconds"),
            "action": action,
            **data,
        }
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        logger.error("approval_jsonl_log | Blad zapisu | action=%s error=%s", action, exc)


async def _check_ratelimit(redis: Redis, id_user: int, id_instance: int) -> None:
    """
    Sprawdza rate limit przed akcja. HTTP 429 jesli przekroczony.
    Klucz: approval_ratelimit:{id_user}:{id_instance} TTL 2s.
    """
    key = f"approval_ratelimit:{id_user}:{id_instance}"
    exists = await redis.exists(key)
    if exists:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Zbyt szybko. Poczekaj chwile przed kolejna akcja na tym dokumencie.",
        )
    await redis.set(key, "1", ex=_RATELIMIT_TTL)


async def _check_feature_flag(
    db: AsyncSession,
    redis: Redis,
    key: str,
    expected: str = "true",
    error_msg: str = "Funkcja jest wylaczona.",
) -> None:
    """
    Sprawdza wartosc klucza w SystemConfig przez Redis cache (TTL 5 min).
    Rzuca HTTP 503 jesli wartosc rozna od expected.
    """
    cache_key = f"syscfg:{key}"
    cached = await redis.get(cache_key)

    if cached is None:
        row = await db.execute(
            text(
                f"SELECT [ConfigValue] FROM [{_SCHEMA}].[skw_SystemConfig] "
                f"WHERE [ConfigKey] = :k AND [IsActive] = 1"
            ),
            {"k": key},
        )
        result = row.fetchone()
        value = result[0] if result else "false"
        await redis.set(cache_key, value, ex=300)
    else:
        value = cached.decode() if isinstance(cached, bytes) else cached

    if value.lower() != expected.lower():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=error_msg,
        )


async def _check_module_enabled(db: AsyncSession, redis: Redis) -> None:
    """Weryfikuje flage APPROVAL_MODULE_ENABLED."""
    await _check_feature_flag(
        db, redis,
        key="APPROVAL_MODULE_ENABLED",
        expected="true",
        error_msg=(
            "Modul obiegu dokumentow jest wylaczony. "
            "Skontaktuj sie z administratorem."
        ),
    )


async def _get_group_members_cached(
    db: AsyncSession, redis: Redis, id_group: int
) -> list[int]:
    """
    Zwraca liste id_user czlonkow grupy z Redis cache (TTL 5 min).
    Cache miss: odpytaj baze, zapisz, zwroc.
    """
    cache_key = f"group_members:{id_group}"
    cached = await redis.get(cache_key)
    if cached:
        return json.loads(cached)

    rows = await db.execute(
        text(
            f"SELECT [id_user] FROM [{_SCHEMA}].[skw_approval_group_members] "
            f"WHERE [id_group] = :g"
        ),
        {"g": id_group},
    )
    members = [r[0] for r in rows.fetchall()]
    await redis.set(cache_key, json.dumps(members), ex=_CACHE_MEMBERS_TTL)
    return members


async def _get_group_delegations_cached(
    db: AsyncSession, redis: Redis, id_group: int
) -> list[dict]:
    """
    Zwraca aktywne delegacje dla grupy z Redis cache (TTL 5 min).
    Delegacja globalna (id_group IS NULL) rowniez zwracana.
    Format: [{"id_user_from": X, "id_user_to": Y, "id_delegation": Z}]
    """
    cache_key = f"group_delegations:{id_group}"
    cached = await redis.get(cache_key)
    if cached:
        return json.loads(cached)

    now = _utcnow_naive()
    rows = await db.execute(
        text(
            f"SELECT [id_user_from], [id_user_to], [id_delegation] "
            f"FROM [{_SCHEMA}].[skw_approval_delegations] "
            f"WHERE [is_active] = 1 "
            f"  AND [valid_from] <= :now "
            f"  AND [valid_to]   >= :now "
            f"  AND ([id_group] = :g OR [id_group] IS NULL)"
        ),
        {"now": now, "g": id_group},
    )
    delegations = [
        {"id_user_from": r[0], "id_user_to": r[1], "id_delegation": r[2]}
        for r in rows.fetchall()
    ]
    await redis.set(cache_key, json.dumps(delegations), ex=_CACHE_DELEG_TTL)
    return delegations


def _resolve_effective_users(
    members: list[int], delegations: list[dict]
) -> dict[int, int | None]:
    """
    Buduje mape: id_user_from -> id_user_to (delegat) lub None (bez delegacji).
    Uzytkownicy bez delegacji maja wartosc None.
    Delegaci (id_user_to) nie sa samodzielnymi czlonkami — reprezentuja czlonka.

    Zwraca: {id_user_from: id_user_to_or_None}
    """
    deleg_map = {d["id_user_from"]: d["id_user_to"] for d in delegations}
    result: dict[int, int | None] = {}
    for uid in members:
        result[uid] = deleg_map.get(uid)
    return result


async def _insert_approval_log(
    db: AsyncSession,
    *,
    id_instance: int,
    id_user: int | None,
    username_snapshot: str | None,
    action: str,
    step_order_snapshot: int | None = None,
    id_group_snapshot: int | None = None,
    consensus_snapshot: str | None = None,
    votes_before: int | None = None,
    votes_after: int | None = None,
    is_voided: bool = False,
    details: dict | None = None,
    ip_address: str | None = None,
) -> None:
    """
    Wstawia wpis do skw_approval_log przez raw SQL text().
    Tabela ma trigger DENY UPDATE/DELETE — tylko INSERT jest dozwolony.
    NIE uzywaj ORM dla tej tabeli.
    """
    details_json = json.dumps(details, ensure_ascii=False, default=str) if details else None
    now = _utcnow_naive()

    await db.execute(
        text(
            f"INSERT INTO [{_SCHEMA}].[skw_approval_log] "
            f"([id_instance], [id_user], [username_snapshot], [action], "
            f"[step_order_snapshot], [id_group_snapshot], [consensus_snapshot], "
            f"[votes_before], [votes_after], [is_voided], [details], [ip_address], [logged_at]) "
            f"VALUES "
            f"(:id_instance, :id_user, :username_snapshot, :action, "
            f":step_order_snapshot, :id_group_snapshot, :consensus_snapshot, "
            f":votes_before, :votes_after, :is_voided, :details, :ip_address, :logged_at)"
        ),
        {
            "id_instance":        id_instance,
            "id_user":            id_user,
            "username_snapshot":  username_snapshot,
            "action":             action,
            "step_order_snapshot": step_order_snapshot,
            "id_group_snapshot":  id_group_snapshot,
            "consensus_snapshot": consensus_snapshot,
            "votes_before":       votes_before,
            "votes_after":        votes_after,
            "is_voided":          1 if is_voided else 0,
            "details":            details_json,
            "ip_address":         ip_address,
            "logged_at":          now,
        },
    )


async def _void_votes_for_step(
    db: AsyncSession, id_instance: int, step_order: int
) -> int:
    """
    Oznacza is_voided=1 dla wszystkich glosow (akcja accepted) biezacego
    step_order i instancji. Zwraca liczbe uniewaznonych wpisow.
    Uzywane przy rollback — głosy kasowane logicznie, rekordy pozostaja.
    """
    result = await db.execute(
        text(
            f"UPDATE [{_SCHEMA}].[skw_approval_log] "
            f"SET [is_voided] = 1 "
            f"WHERE [id_instance] = :inst "
            f"  AND [step_order_snapshot] = :step "
            f"  AND [action] = 'accepted' "
            f"  AND [is_voided] = 0"
        ),
        {"inst": id_instance, "step": step_order},
    )
    return result.rowcount


async def _increment_notif_unread(redis: Redis, id_user: int) -> None:
    """Inkrementuje licznik nieprzeczytanych powiadomien w Redis."""
    key = f"notif_unread:{id_user}"
    await redis.incr(key)
    await redis.expire(key, _CACHE_NOTIF_TTL)


# =============================================================================
# DISPATCH — przekazanie dokumentu do obiegu
# =============================================================================

async def dispatch(
    db: AsyncSession,
    redis: Redis,
    *,
    id_document: str,
    id_source: int,
    id_path: int,
    id_category: int | None,
    dispatched_by_user_id: int,
    dispatched_by_username: str,
    document_title: str | None = None,
    document_amount: float | None = None,
    extra_data: dict | None = None,
    ip_address: str | None = None,
) -> DocumentApprovalInstance:
    """
    Przekazuje dokument do obiegu akceptacyjnego.

    Kroki:
      1. Sprawdz APPROVAL_MODULE_ENABLED
      2. Sprawdz brak aktywnego obiegu dla tego dokumentu (409 jesli istnieje)
      3. Pobierz sciezke i jej kroki (404 jesli nie istnieje lub pusta)
      4. Oblicz deadline_at dla calego obiegu (opcjonalne — z pierwszego kroku)
      5. Utworz DocumentApprovalInstance (status = in_progress, current_step = 1)
      6. Snapshot: kopiuj kroki sciezki do skw_document_approval_snapshot_steps
         Krok 1 dostaje status = in_progress, reszta = pending
      7. Ustaw votes_required per krok (OR=1, AND=liczba_czlonkow)
      8. INSERT do approval_log (action = dispatched)
      9. Commit
      10. JSONL log

    Raises:
        HTTPException(503): Modul wylaczony
        HTTPException(404): Sciezka nie istnieje lub jest pusta
        HTTPException(409): Aktywny obieg juz istnieje dla tego dokumentu
    """
    await _check_module_enabled(db, redis)

    # ── Krok 2: Sprawdz duplikat aktywnego obiegu ─────────────────────────────
    existing = await db.execute(
        text(
            f"SELECT [id_instance] FROM [{_SCHEMA}].[skw_document_approval_instances] "
            f"WHERE [id_document] = :doc "
            f"  AND [id_source]   = :src "
            f"  AND [status] <> N'approved' "
            f"  AND [status] <> N'cancelled'"
        ),
        {"doc": id_document, "src": id_source},
    )
    if existing.fetchone():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Dokument '{id_document}' ma juz aktywny obieg. "
                "Zakoncz lub anuluj istniejacy obieg przed ponownym przekazaniem."
            ),
        )

    # ── Krok 3: Pobierz sciezke i kroki ──────────────────────────────────────
    path_row = await db.execute(
        text(
            f"SELECT [id_path], [path_name] FROM [{_SCHEMA}].[skw_approval_paths] "
            f"WHERE [id_path] = :p AND [is_active] = 1"
        ),
        {"p": id_path},
    )
    path_data = path_row.fetchone()
    if not path_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sciezka akceptacyjna id={id_path} nie istnieje lub jest nieaktywna.",
        )

    steps_rows = await db.execute(
        text(
            f"SELECT s.[step_order], s.[id_group], s.[deadline_hours], "
            f"       g.[consensus_type] "
            f"FROM [{_SCHEMA}].[skw_approval_path_steps] s "
            f"JOIN [{_SCHEMA}].[skw_approval_groups] g ON g.[id_group] = s.[id_group] "
            f"WHERE s.[id_path] = :p "
            f"ORDER BY s.[step_order] ASC"
        ),
        {"p": id_path},
    )
    steps = steps_rows.fetchall()
    if not steps:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sciezka id={id_path} nie ma zdefiniowanych krokow.",
        )

    now = _utcnow_naive()

    # ── Krok 4: Oblicz globalny deadline (z pierwszego kroku) ─────────────────
    first_step_hours = steps[0][2]  # deadline_hours pierwszego kroku
    global_deadline = (
        now + timedelta(hours=first_step_hours)
        if first_step_hours else None
    )

    # ── Krok 5: Utworz instancje ──────────────────────────────────────────────
    instance = DocumentApprovalInstance(
        id_document=id_document,
        id_source=id_source,
        id_path=id_path,
        id_category=id_category,
        status="in_progress",
        current_step=1,
        is_urgent=False,
        dispatched_by=dispatched_by_user_id,
        dispatched_at=now,
        deadline_at=global_deadline,
        document_title=document_title,
        document_amount=document_amount,
        extra_data=json.dumps(extra_data, ensure_ascii=False) if extra_data else None,
        created_at=now,
        updated_at=now,
    )
    db.add(instance)
    await db.flush()  # pobierz id_instance przed snapshot

    logger.info(
        "dispatch | Nowa instancja id=%d | doc=%s source=%d path=%d user=%d",
        instance.id_instance, id_document, id_source, id_path, dispatched_by_user_id,
    )

    # ── Krok 6+7: Snapshot krokow + votes_required ────────────────────────────
    for step_order, id_group, deadline_hours, consensus_type in steps:
        # votes_required: OR=1, AND=liczba_czlonkow_grupy
        if consensus_type == "AND":
            members = await _get_group_members_cached(db, redis, id_group)
            votes_required = max(1, len(members))
        else:
            votes_required = 1

        snap_status = "in_progress" if step_order == 1 else "pending"
        step_deadline: datetime | None = None
        if step_order == 1 and deadline_hours:
            step_deadline = now + timedelta(hours=deadline_hours)

        snap = DocumentApprovalSnapshotStep(
            id_instance=instance.id_instance,
            step_order=step_order,
            id_group=id_group,
            status=snap_status,
            votes_required=votes_required,
            votes_cast=0,
            deadline_at=step_deadline,
            created_at=now,
            updated_at=now,
        )
        db.add(snap)

    # ── Krok 8: approval_log ──────────────────────────────────────────────────
    await _insert_approval_log(
        db,
        id_instance=instance.id_instance,
        id_user=dispatched_by_user_id,
        username_snapshot=dispatched_by_username,
        action="dispatched",
        step_order_snapshot=1,
        id_group_snapshot=steps[0][1],
        consensus_snapshot=steps[0][3],
        details={
            "id_path":        id_path,
            "path_name":      path_data[1],
            "steps_count":    len(steps),
            "document_title": document_title,
        },
        ip_address=ip_address,
    )

    # ── Krok 9: Commit ────────────────────────────────────────────────────────
    await db.commit()

    # ── Krok 10: JSONL (po commit) ────────────────────────────────────────────
    _jsonl_log("dispatched", {
        "id_instance":    instance.id_instance,
        "id_document":    id_document,
        "id_source":      id_source,
        "id_path":        id_path,
        "steps_count":    len(steps),
        "dispatched_by":  dispatched_by_user_id,
    })

    logger.info(
        "dispatch | OK | id_instance=%d steps=%d",
        instance.id_instance, len(steps),
    )
    return instance


# =============================================================================
# ACCEPT — akceptacja biezacego etapu
# =============================================================================

async def accept(
    db: AsyncSession,
    redis: Redis,
    background_tasks: BackgroundTasks,
    *,
    id_instance: int,
    id_user: int,
    username: str,
    comment: str | None = None,
    ip_address: str | None = None,
    notify_fn: Any = None,  # callback: publish_document_waiting / approved
) -> dict:
    """
    Akceptacja dokumentu na biezacym etapie.

    Kroki:
      1. Rate limit check (429 jesli < 2s od ostatniej akcji)
      2. Feature flag + distributed lock na instancji
      3. Pobierz instancje (404/409 walidacja statusu)
      4. Pobierz biezacy krok snapshotu
      5. Sprawdz czlonkostwo / delegacje → HTTP 403 jesli brak uprawnienia
      6. INSERT approval_log (action=accepted, id_user = delegat lub wlasny)
      7. INCREMENT votes_cast w snapshocie
      8. Sprawdz konsensus (AND/OR):
         - OR: votes_cast >= 1 → zaliczone
         - AND: votes_cast >= votes_required → zaliczone (uwzgledniaj delegacje)
      9. Jesli krok zaliczony:
         a. Czy jest nastepny krok?
            TAK: current_step += 1, nowy krok status=in_progress, oblicz deadline
            NIE: instancja status=approved, completed_at=now
     10. Commit
     11. Background: powiadomienia SSE + notif_unread INCR

    Raises:
        HTTPException(429): Rate limit
        HTTPException(503): Modul wylaczony
        HTTPException(404): Instancja nie istnieje
        HTTPException(409): Instancja nie jest in_progress
        HTTPException(403): Uzytkownik nie jest czlonkiem grupy ani delegatem
        HTTPException(423): Lock niedostepny
    """
    await _check_ratelimit(redis, id_user, id_instance)
    await _check_module_enabled(db, redis)

    async with approval_lock(redis, id_instance):
        # ── Krok 3: Instancja ─────────────────────────────────────────────────
        inst_row = await db.execute(
            text(
                f"SELECT [id_instance], [status], [current_step], [id_path], "
                f"       [dispatched_by], [document_title] "
                f"FROM [{_SCHEMA}].[skw_document_approval_instances] "
                f"WHERE [id_instance] = :i"
            ),
            {"i": id_instance},
        )
        inst = inst_row.fetchone()
        if not inst:
            raise HTTPException(status_code=404, detail=f"Instancja {id_instance} nie istnieje.")
        if inst[1] != "in_progress":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Instancja {id_instance} ma status '{inst[1]}' — akcja niedostepna.",
            )

        current_step = inst[2]
        document_title = inst[5]

        # ── Krok 4: Biezacy krok snapshotu ───────────────────────────────────
        snap_row = await db.execute(
            text(
                f"SELECT [id_snapshot], [id_group], [votes_cast], [votes_required], "
                f"       [deadline_at] "
                f"FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                f"WHERE [id_instance] = :i AND [step_order] = :s"
            ),
            {"i": id_instance, "s": current_step},
        )
        snap = snap_row.fetchone()
        if not snap:
            raise HTTPException(status_code=500, detail="Blad wewnetrzny: brak kroku snapshotu.")

        id_snapshot, id_group, votes_cast, votes_required, _ = snap

        # ── Krok 5: Sprawdz czlonkostwo / delegacje ───────────────────────────
        members = await _get_group_members_cached(db, redis, id_group)
        delegations = await _get_group_delegations_cached(db, redis, id_group)
        effective = _resolve_effective_users(members, delegations)

        # Ustal: czy user jest czlonkiem (bezposrednim lub delegatem)?
        acting_for: int | None = None   # id czlonka w imieniu ktorego dzialamy
        id_delegation: int | None = None

        if id_user in members:
            # Bezposredni czlonek
            acting_for = id_user
        else:
            # Sprawdz czy jest delegatem
            for member_id, delegate_id in effective.items():
                if delegate_id == id_user:
                    acting_for = member_id
                    # Znajdz id_delegation
                    for d in delegations:
                        if d["id_user_from"] == member_id and d["id_user_to"] == id_user:
                            id_delegation = d["id_delegation"]
                            break
                    break

        if acting_for is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Uzytkownik {id_user} nie jest czlonkiem grupy {id_group} "
                    "ani aktywnym delegatem. Brak uprawnienia do akceptacji."
                ),
            )

        # Sprawdz czy ten czlonek (lub delegat) juz glosowal w tej iteracji
        already_voted = await db.execute(
            text(
                f"SELECT COUNT(*) FROM [{_SCHEMA}].[skw_approval_log] "
                f"WHERE [id_instance] = :i "
                f"  AND [step_order_snapshot] = :s "
                f"  AND [id_user] = :u "
                f"  AND [action] = 'accepted' "
                f"  AND [is_voided] = 0"
            ),
            {"i": id_instance, "s": current_step, "u": acting_for},
        )
        if already_voted.scalar() > 0:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Juz zaakceptowales ten etap. Czekaj na pozostalych czlonkow grupy.",
            )

        now = _utcnow_naive()

        # ── Krok 6: approval_log ──────────────────────────────────────────────
        log_details: dict = {
            "acting_for":     acting_for,
            "id_delegation":  id_delegation,
        }
        if comment:
            log_details["comment"] = comment

        await _insert_approval_log(
            db,
            id_instance=id_instance,
            id_user=acting_for,  # zawsze czlonek (nie delegat)
            username_snapshot=username,
            action="accepted",
            step_order_snapshot=current_step,
            id_group_snapshot=id_group,
            votes_before=votes_cast,
            votes_after=votes_cast + 1,
            details=log_details,
            ip_address=ip_address,
        )

        # ── Krok 7: INCREMENT votes_cast ──────────────────────────────────────
        new_votes = votes_cast + 1
        await db.execute(
            text(
                f"UPDATE [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                f"SET [votes_cast] = :v, [updated_at] = :now "
                f"WHERE [id_snapshot] = :snap"
            ),
            {"v": new_votes, "now": now, "snap": id_snapshot},
        )

        # ── Krok 8: Konsensus ─────────────────────────────────────────────────
        step_complete = new_votes >= votes_required
        approved_terminal = False
        next_step: int | None = None

        if step_complete:
            # Zamknij biezacy krok
            await db.execute(
                text(
                    f"UPDATE [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                    f"SET [status] = 'approved', [completed_at] = :now, [updated_at] = :now "
                    f"WHERE [id_snapshot] = :snap"
                ),
                {"now": now, "snap": id_snapshot},
            )

            # Czy jest nastepny krok?
            next_row = await db.execute(
                text(
                    f"SELECT [id_snapshot], [id_group], [step_order], [deadline_at] "
                    f"FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                    f"WHERE [id_instance] = :i AND [step_order] = :s"
                ),
                {"i": id_instance, "s": current_step + 1},
            )
            next_snap = next_row.fetchone()

            if next_snap:
                # ── Krok 9a: Przejdz do nastepnego kroku ─────────────────────
                next_step = current_step + 1
                next_snap_id, next_group, _, _ = next_snap

                # Pobierz deadline_hours nastepnego kroku ze sciezki
                next_deadline_row = await db.execute(
                    text(
                        f"SELECT [deadline_hours] FROM [{_SCHEMA}].[skw_approval_path_steps] "
                        f"WHERE [id_path] = :p AND [step_order] = :s"
                    ),
                    {"p": inst[3], "s": next_step},
                )
                next_deadline_hours_row = next_deadline_row.fetchone()
                next_deadline_hours = next_deadline_hours_row[0] if next_deadline_hours_row else None

                next_step_deadline = (
                    now + timedelta(hours=next_deadline_hours)
                    if next_deadline_hours else None
                )

                await db.execute(
                    text(
                        f"UPDATE [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                        f"SET [status] = 'in_progress', "
                        f"    [deadline_at] = :dl, "
                        f"    [updated_at] = :now "
                        f"WHERE [id_snapshot] = :snap"
                    ),
                    {"dl": next_step_deadline, "now": now, "snap": next_snap_id},
                )
                await db.execute(
                    text(
                        f"UPDATE [{_SCHEMA}].[skw_document_approval_instances] "
                        f"SET [current_step] = :step, [updated_at] = :now "
                        f"WHERE [id_instance] = :i"
                    ),
                    {"step": next_step, "now": now, "i": id_instance},
                )

                await _insert_approval_log(
                    db,
                    id_instance=id_instance,
                    id_user=acting_for,
                    username_snapshot=username,
                    action="step_advanced",
                    step_order_snapshot=next_step,
                    id_group_snapshot=next_group,
                    details={"from_step": current_step, "to_step": next_step},
                    ip_address=ip_address,
                )

            else:
                # ── Krok 9b: Ostatni etap — obieg zakonczony ─────────────────
                approved_terminal = True
                await db.execute(
                    text(
                        f"UPDATE [{_SCHEMA}].[skw_document_approval_instances] "
                        f"SET [status] = 'approved', "
                        f"    [completed_at] = :now, "
                        f"    [updated_at] = :now "
                        f"WHERE [id_instance] = :i"
                    ),
                    {"now": now, "i": id_instance},
                )

                await _insert_approval_log(
                    db,
                    id_instance=id_instance,
                    id_user=acting_for,
                    username_snapshot=username,
                    action="approved",
                    step_order_snapshot=current_step,
                    details={"document_title": document_title},
                    ip_address=ip_address,
                )

        # ── Krok 10: Commit ───────────────────────────────────────────────────
        await db.commit()

    # ── Krok 11: Powiadomienia (background, poza lockiem) ─────────────────────
    _jsonl_log("accepted", {
        "id_instance":   id_instance,
        "id_user":       id_user,
        "acting_for":    acting_for,
        "step":          current_step,
        "votes_before":  votes_cast,
        "votes_after":   new_votes,
        "step_complete": step_complete,
        "approved":      approved_terminal,
    })

    if step_complete and notify_fn and background_tasks:
        if approved_terminal:
            background_tasks.add_task(
                notify_fn,
                "approved",
                id_instance=id_instance,
                dispatched_by=inst[4],
                document_title=document_title,
            )
        elif next_step is not None:
            background_tasks.add_task(
                notify_fn,
                "step_advanced",
                id_instance=id_instance,
                id_group=next_snap[1] if next_snap else None,
                step_order=next_step,
                document_title=document_title,
            )

    return {
        "id_instance":    id_instance,
        "step":           current_step,
        "votes_cast":     new_votes,
        "votes_required": votes_required,
        "step_complete":  step_complete,
        "approved":       approved_terminal,
        "next_step":      next_step,
    }


# =============================================================================
# ROLLBACK — cofniecie obiegu do poprzedniego etapu
# =============================================================================

async def rollback(
    db: AsyncSession,
    redis: Redis,
    *,
    id_instance: int,
    id_user: int,
    username: str,
    comment: str,  # wymagany przy rollback — nie opcjonalny
    has_supervise: bool = False,
    ip_address: str | None = None,
) -> dict:
    """
    Cofa obieg dokumentu o jeden etap wstecz.

    Kroki:
      1. Rate limit + feature flag + lock
      2. Pobierz instancje (walidacja: in_progress, current_step >= 1)
      3. Sprawdz uprawnienia: czlonek grupy biezacego etapu LUB approval.supervise
      4. void_votes: is_voided=1 dla wszystkich 'accepted' biezacego etapu
      5. Wyczysc deadline_at biezacego kroku snapshotu
      6a. Jesli current_step > 1: current_step -= 1, poprzedni krok = in_progress
      6b. Jesli current_step == 1: status = pending_dispatch (wraca do dyspozytora)
      7. INSERT approval_log (action=rollback)
      8. Commit
      9. JSONL log

    Raises:
        HTTPException(400): Brak komentarza
        HTTPException(403): Brak uprawnienia
        HTTPException(409): Bledny status instancji
    """
    if not comment or not comment.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Komentarz jest wymagany przy cofaniu obiegu.",
        )

    await _check_ratelimit(redis, id_user, id_instance)
    await _check_module_enabled(db, redis)

    async with approval_lock(redis, id_instance):
        # ── Krok 2: Instancja ─────────────────────────────────────────────────
        inst_row = await db.execute(
            text(
                f"SELECT [status], [current_step], [dispatched_by], [document_title] "
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
                detail=f"Instancja {id_instance} ma status '{inst[0]}' — rollback niedostepny.",
            )

        current_step = inst[1]

        # ── Krok 3: Uprawnienia ───────────────────────────────────────────────
        # Sprawdz czlonkostwo w biezacej grupie
        snap_row = await db.execute(
            text(
                f"SELECT [id_snapshot], [id_group] "
                f"FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                f"WHERE [id_instance] = :i AND [step_order] = :s"
            ),
            {"i": id_instance, "s": current_step},
        )
        snap = snap_row.fetchone()
        if not snap:
            raise HTTPException(status_code=500, detail="Blad wewnetrzny: brak kroku snapshotu.")

        id_snapshot, id_group = snap

        if not has_supervise:
            members = await _get_group_members_cached(db, redis, id_group)
            delegations = await _get_group_delegations_cached(db, redis, id_group)
            effective = _resolve_effective_users(members, delegations)
            is_member = id_user in members
            is_delegate = any(d == id_user for d in effective.values() if d is not None)
            if not is_member and not is_delegate:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        "Brak uprawnienia do cofniecia obiegu. "
                        "Wymagane czlonkostwo w grupie biezacego etapu "
                        "lub uprawnienie approval.supervise."
                    ),
                )

        now = _utcnow_naive()

        # ── Krok 4: Void votes ────────────────────────────────────────────────
        voided_count = await _void_votes_for_step(db, id_instance, current_step)

        # ── Krok 5: Wyczysc deadline_at biezacego kroku ───────────────────────
        await db.execute(
            text(
                f"UPDATE [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                f"SET [deadline_at] = NULL, "
                f"    [status] = 'pending', "
                f"    [votes_cast] = 0, "
                f"    [updated_at] = :now "
                f"WHERE [id_snapshot] = :snap"
            ),
            {"now": now, "snap": id_snapshot},
        )

        # ── Krok 6: Dekrementuj lub wroc do pending_dispatch ──────────────────
        if current_step > 1:
            new_step = current_step - 1

            # Poprzedni krok wraca do in_progress (reset deadline)
            prev_snap_row = await db.execute(
                text(
                    f"SELECT [id_snapshot], [id_group] "
                    f"FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                    f"WHERE [id_instance] = :i AND [step_order] = :s"
                ),
                {"i": id_instance, "s": new_step},
            )
            prev_snap = prev_snap_row.fetchone()
            if prev_snap:
                await db.execute(
                    text(
                        f"UPDATE [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                        f"SET [status] = 'in_progress', "
                        f"    [votes_cast] = 0, "
                        f"    [updated_at] = :now "
                        f"WHERE [id_snapshot] = :snap"
                    ),
                    {"now": now, "snap": prev_snap[0]},
                )

            await db.execute(
                text(
                    f"UPDATE [{_SCHEMA}].[skw_document_approval_instances] "
                    f"SET [current_step] = :step, [updated_at] = :now "
                    f"WHERE [id_instance] = :i"
                ),
                {"step": new_step, "now": now, "i": id_instance},
            )
            new_status = "in_progress"
        else:
            # current_step == 1 → wroc do dyspozytora
            new_step = 0
            new_status = "pending_dispatch"
            await db.execute(
                text(
                    f"UPDATE [{_SCHEMA}].[skw_document_approval_instances] "
                    f"SET [status] = 'pending_dispatch', "
                    f"    [current_step] = 0, "
                    f"    [updated_at] = :now "
                    f"WHERE [id_instance] = :i"
                ),
                {"now": now, "i": id_instance},
            )

        # ── Krok 7: approval_log ──────────────────────────────────────────────
        await _insert_approval_log(
            db,
            id_instance=id_instance,
            id_user=id_user,
            username_snapshot=username,
            action="rollback",
            step_order_snapshot=current_step,
            id_group_snapshot=id_group,
            details={
                "comment":       comment.strip(),
                "voided_votes":  voided_count,
                "rolled_to":     new_step,
                "new_status":    new_status,
                "has_supervise": has_supervise,
            },
            ip_address=ip_address,
        )

        # ── Krok 8: Commit ────────────────────────────────────────────────────
        await db.commit()

    # ── Krok 9: JSONL ────────────────────────────────────────────────────────
    _jsonl_log("rollback", {
        "id_instance":  id_instance,
        "id_user":      id_user,
        "from_step":    current_step,
        "to_step":      new_step,
        "new_status":   new_status,
        "voided_votes": voided_count,
    })

    logger.info(
        "rollback | OK | id_instance=%d step %d->%d status=%s voided=%d",
        id_instance, current_step, new_step, new_status, voided_count,
    )

    return {
        "id_instance":  id_instance,
        "from_step":    current_step,
        "to_step":      new_step,
        "new_status":   new_status,
        "voided_votes": voided_count,
    }


# =============================================================================
# REJECT — odrzucenie dokumentu (status terminal)
# =============================================================================

async def reject(
    db: AsyncSession,
    redis: Redis,
    *,
    id_instance: int,
    id_user: int,
    username: str,
    comment: str,
    has_supervise: bool = False,
    ip_address: str | None = None,
) -> dict:
    """
    Odrzuca dokument — status terminal 'rejected'.
    Wymaga czlonkostwa w biezacej grupie lub approval.supervise.
    Komentarz obowiazkowy.
    """
    if not comment or not comment.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Komentarz jest wymagany przy odrzucaniu dokumentu.",
        )

    await _check_ratelimit(redis, id_user, id_instance)
    await _check_module_enabled(db, redis)

    async with approval_lock(redis, id_instance):
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
                detail=f"Instancja {id_instance} ma status '{inst[0]}' — odrzucenie niedostepne.",
            )

        current_step = inst[1]

        # Sprawdz uprawnienia (czlonek grupy lub supervise)
        if not has_supervise:
            snap_row = await db.execute(
                text(
                    f"SELECT [id_group] "
                    f"FROM [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
                    f"WHERE [id_instance] = :i AND [step_order] = :s"
                ),
                {"i": id_instance, "s": current_step},
            )
            snap = snap_row.fetchone()
            if snap:
                members = await _get_group_members_cached(db, redis, snap[0])
                delegations = await _get_group_delegations_cached(db, redis, snap[0])
                effective = _resolve_effective_users(members, delegations)
                is_member = id_user in members
                is_delegate = any(d == id_user for d in effective.values() if d is not None)
                if not is_member and not is_delegate:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail="Brak uprawnienia do odrzucenia dokumentu.",
                    )

        now = _utcnow_naive()

        await db.execute(
            text(
                f"UPDATE [{_SCHEMA}].[skw_document_approval_instances] "
                f"SET [status] = 'rejected', [completed_at] = :now, [updated_at] = :now "
                f"WHERE [id_instance] = :i"
            ),
            {"now": now, "i": id_instance},
        )

        await _insert_approval_log(
            db,
            id_instance=id_instance,
            id_user=id_user,
            username_snapshot=username,
            action="rejected",
            step_order_snapshot=current_step,
            details={"comment": comment.strip(), "has_supervise": has_supervise},
            ip_address=ip_address,
        )

        await db.commit()

    _jsonl_log("rejected", {
        "id_instance": id_instance, "id_user": id_user, "step": current_step,
    })
    return {"id_instance": id_instance, "new_status": "rejected"}


# =============================================================================
# CANCEL — anulowanie przez dyspozytora
# =============================================================================

async def cancel(
    db: AsyncSession,
    redis: Redis,
    *,
    id_instance: int,
    id_user: int,
    username: str,
    comment: str | None = None,
    ip_address: str | None = None,
) -> dict:
    """
    Anuluje obieg. Dostepne dla dyspozytora i approval.supervise.
    Status terminal: 'cancelled'.
    """
    await _check_module_enabled(db, redis)

    async with approval_lock(redis, id_instance):
        inst_row = await db.execute(
            text(
                f"SELECT [status], [dispatched_by] "
                f"FROM [{_SCHEMA}].[skw_document_approval_instances] "
                f"WHERE [id_instance] = :i"
            ),
            {"i": id_instance},
        )
        inst = inst_row.fetchone()
        if not inst:
            raise HTTPException(status_code=404, detail=f"Instancja {id_instance} nie istnieje.")

        current_status = inst[0]
        if current_status in ("approved", "cancelled"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Instancja {id_instance} jest juz w statusie '{current_status}'.",
            )

        now = _utcnow_naive()

        await db.execute(
            text(
                f"UPDATE [{_SCHEMA}].[skw_document_approval_instances] "
                f"SET [status] = 'cancelled', [completed_at] = :now, [updated_at] = :now "
                f"WHERE [id_instance] = :i"
            ),
            {"now": now, "i": id_instance},
        )

        await _insert_approval_log(
            db,
            id_instance=id_instance,
            id_user=id_user,
            username_snapshot=username,
            action="cancelled",
            details={"comment": comment, "previous_status": current_status},
            ip_address=ip_address,
        )

        await db.commit()

    _jsonl_log("cancelled", {
        "id_instance": id_instance, "id_user": id_user,
        "previous_status": current_status,
    })
    return {"id_instance": id_instance, "new_status": "cancelled"}


# =============================================================================
# INSERT GROUP INTO SNAPSHOT — wspolna metoda dla forward / send_to_group
# =============================================================================

async def insert_group_into_snapshot(
    db: AsyncSession,
    redis: Redis,
    *,
    id_instance: int,
    position: int,         # numer kroku do wstawienia (step_order po wstawieniu)
    id_group: int,
    id_user: int,
    deadline_hours: int | None = None,
) -> DocumentApprovalSnapshotStep:
    """
    Wstawia nowa grupe do snapshotu instancji na pozycje N.
    Przesuwa wszystkie istniejace kroki >= N o +1 (UPDATE step_order).
    Uzywa locka — wywoluj zawsze wewnatrz sekcji krytycznej.

    Operacja:
      1. UPDATE snapshot_steps SET step_order = step_order + 1 WHERE step_order >= N
      2. INSERT nowy krok: step_order=N, id_group, status=in_progress
      3. votes_required: consensus_type grupy (AND=len(members), OR=1)
      4. deadline_at: jezeli deadline_hours podany

    UWAGA: nie modyfikuje globalnej sciezki (approval_path_steps).
    UWAGA: current_step_order instancji NIE jest zmieniany — to zadanie forward/send_to_group.
    Kolumna added_by (INT) — zapisujemy id_user ktory wstawil krok.

    Zwraca: nowo utworzony DocumentApprovalSnapshotStep
    """
    now = _utcnow_naive()

    # Przesuniecie istniejacych krokow >= position
    await db.execute(
        text(
            f"UPDATE [{_SCHEMA}].[skw_document_approval_snapshot_steps] "
            f"SET [step_order] = [step_order] + 1, [updated_at] = :now "
            f"WHERE [id_instance] = :i AND [step_order] >= :pos"
        ),
        {"now": now, "i": id_instance, "pos": position},
    )

    # votes_required
    group_row = await db.execute(
        text(
            f"SELECT [consensus_type] FROM [{_SCHEMA}].[skw_approval_groups] "
            f"WHERE [id_group] = :g"
        ),
        {"g": id_group},
    )
    group = group_row.fetchone()
    if not group:
        raise HTTPException(status_code=404, detail=f"Grupa {id_group} nie istnieje.")

    consensus_type = group[0]
    if consensus_type == "AND":
        members = await _get_group_members_cached(db, redis, id_group)
        votes_required = max(1, len(members))
    else:
        votes_required = 1

    step_deadline = now + timedelta(hours=deadline_hours) if deadline_hours else None

    new_snap = DocumentApprovalSnapshotStep(
        id_instance=id_instance,
        step_order=position,
        id_group=id_group,
        status="in_progress",
        votes_required=votes_required,
        votes_cast=0,
        deadline_at=step_deadline,
        created_at=now,
        updated_at=now,
    )
    db.add(new_snap)
    await db.flush()

    logger.info(
        "insert_group_into_snapshot | id_instance=%d pos=%d id_group=%d by=%d",
        id_instance, position, id_group, id_user,
    )
    return new_snap