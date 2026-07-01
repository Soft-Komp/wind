# backend/app/api/webhooks.py
"""
Webhook endpoint — przyjmowanie dokumentow push od systemow zewnetrznych.

PUBLICZNY ENDPOINT — brak wymagania JWT. Zabezpieczenie wylacznie przez
token w URL (constant-time compare) + rate limiting Redis.

To NOWY plik, NOWY router — rejestrowany pod prefixem /webhooks w
backend/app/api/router.py (sekcja 19, po admin).

1 endpoint:
  POST /webhooks/sources/{token} — przyjmuje dokument, 202 Accepted

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging

from fastapi import APIRouter, Body, HTTPException, Request, status

from app.core.dependencies import DB, RedisClient
from app.schemas.common import BaseResponse
from app.services import webhook_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhooks")


@router.post(
    "/sources/{token}",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Przyjmij dokument przez webhook (publiczny, server-to-server)",
    description=(
        "**Endpoint publiczny — brak wymagania JWT.** Zabezpieczenie przez "
        "unikalny token w URL (porownanie constant-time, ochrona przed "
        "timing attack) oraz rate limiting Redis "
        "(domyslnie 100 zadan/minute na token, konfigurowalny przez "
        "SystemConfig WEBHOOK_RATE_LIMIT_PER_MINUTE). "
        "\n\nPayload JSON musi zawierac co najmniej 'id_document' "
        "(lub 'ksef_id' jako alias). Pozostale pola mapowane wedlug "
        "konfiguracji zrodla (skw_document_source_field_mappings) lub "
        "wbudowanego domyslnego mapowania. "
        "\n\nPrzetwarzanie jest ASYNCHRONICZNE — odpowiedz 202 Accepted "
        "z id_instance, dispatch do sciezki obiegu nastepuje w tle "
        "(natychmiast jesli mozliwe, w przeciwnym razie w ciagu 1 minuty "
        "przez cykliczny worker). "
        "\n\nJesli zrodlo ma is_test_mode=true — dokument jest zapisywany "
        "ale NIE wchodzi do automatycznego dispatch (zgodnie z zasada "
        "izolacji srodowisk testowych)."
    ),
    responses={
        404: {"description": "Nieprawidlowy token lub zrodlo nieaktywne"},
        422: {"description": "Payload nie da sie zmapowac na dokument (brak id_document)"},
        429: {"description": "Przekroczono rate limit dla tego tokenu"},
    },
)
async def receive_webhook_document(
    token: str,
    db: DB,
    redis: RedisClient,
    request: Request,
    payload: dict = Body(..., description="JSON z danymi dokumentu"),
):
    client_ip = request.headers.get(
        "X-Forwarded-For", request.client.host if request.client else None
    )

    result = await webhook_service.receive_document(
        db, redis,
        token=token,
        payload=payload,
        client_ip=client_ip,
    )

    return BaseResponse.ok(data=result, app_code="webhook.received")