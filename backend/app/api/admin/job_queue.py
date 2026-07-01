# backend/app/api/admin/job_queue.py
"""
Rejestr zadan ARQ — panel admina — F7.

NOWY plik, dolaczany do app/api/admin/__init__.py jako kolejny include_router
(ten sam wzorzec co sources.py i source_hooks_actions.py).

3 endpointy:
  GET /admin/job-queue          — lista z filtrami (task_name, status, daty)
  GET /admin/job-queue/{id_job} — szczegoly jednego zadania
  GET /admin/job-queue/summary  — podsumowanie ostatnich N godzin, "co padlo"

Uzycie typowe (Twoj scenariusz): "czy wysylka maili dzisiaj nie padla"
  GET /admin/job-queue/summary?hours=24
  -> failed_tasks pokazuje liste z licznikiem, bez logowania na serwer.

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging

from fastapi import APIRouter, HTTPException, Query

from app.core.dependencies import DB, CurrentUser, require_permission
from app.schemas.common import BaseResponse
from app.services import job_registry_service as svc

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/job-queue")


# =============================================================================
# GET /admin/job-queue/summary — PRZED /{id_job} (literal musi byc pierwszy)
# =============================================================================

@router.get(
    "/summary",
    summary="Podsumowanie zadan ARQ z ostatnich N godzin",
    description=(
        "Szybki przeglad 'czy wszystko OK' bez logowania na serwer. "
        "Zwraca liczby zadan per status oraz liste task_name ktore mialy "
        "co najmniej jeden blad w danym oknie czasowym (np. 'send_bulk_emails: 2 failed'). "
        "**Wymaga:** `system.view_job_queue`."
    ),
    dependencies=[require_permission("system.view_job_queue")],
)
async def get_job_queue_summary(
    current_user: CurrentUser,
    db: DB,
    hours: int = Query(24, ge=1, le=168, description="Okno czasowe w godzinach (max 7 dni)."),
):
    result = await svc.get_summary(db, hours=hours)
    return BaseResponse.ok(data=result, app_code="job_queue.summary")


# =============================================================================
# GET /admin/job-queue — lista z filtrami
# =============================================================================

@router.get(
    "",
    summary="Lista zadan ARQ z filtrami",
    description=(
        "Wszystkie zadania ARQ w systemie (monity, synchronizacja zrodel, "
        "auto-dispatch, hooki, akcje, OCR) — jeden uniwersalny rejestr. "
        "Sortowanie: najnowsze pierwsze (enqueued_at DESC). "
        "**Wymaga:** `system.view_job_queue`."
    ),
    dependencies=[require_permission("system.view_job_queue")],
)
async def list_job_queue(
    current_user: CurrentUser,
    db: DB,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    task_name: str | None = Query(None, description="Filtr po nazwie taska, np. 'send_bulk_emails'."),
    status: str | None = Query(None, pattern=r"^(queued|running|success|failed)$"),
    date_from: str | None = Query(None, description="Format: YYYY-MM-DD"),
    date_to: str | None = Query(None, description="Format: YYYY-MM-DD"),
):
    result = await svc.list_jobs(
        db, page=page, per_page=per_page,
        task_name=task_name, status=status,
        date_from=date_from, date_to=date_to,
    )
    return BaseResponse.ok(data=result, app_code="job_queue.list")


# =============================================================================
# GET /admin/job-queue/{id_job} — szczegoly
# =============================================================================

@router.get(
    "/{id_job}",
    summary="Szczegoly jednego zadania ARQ",
    responses={404: {"description": "Zadanie nie istnieje w rejestrze"}},
    dependencies=[require_permission("system.view_job_queue")],
)
async def get_job_detail(id_job: int, current_user: CurrentUser, db: DB):
    job = await svc.get_job(db, id_job)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Zadanie ID={id_job} nie istnieje w rejestrze.")
    return BaseResponse.ok(data=job, app_code="job_queue.get")