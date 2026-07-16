# backend/app/api/webhooks.py
"""
Webhook endpoint — przyjmowanie dokumentow push od systemow zewnetrznych.

PUBLICZNY ENDPOINT — brak wymagania JWT. Zabezpieczenie wylacznie przez
token w URL (constant-time compare) + rate limiting Redis.

To NOWY plik, NOWY router — rejestrowany pod prefixem /webhooks w
backend/app/api/router.py (sekcja 19, po admin).

1 endpoint:
  POST /webhooks/sources/{token} — przyjmuje dokument
      202 Accepted — nowy dokument, dispatch zakolejkowany
      200 OK       — idempotentny retry (Tier 1a), dokument juz istnial,
                     payload zignorowany

UWAGA: from __future__ import annotations NIGDY w tym pliku.
"""
import logging

from fastapi import APIRouter, Body, HTTPException, Request, Response, status

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
        "wbudowanego domyslnego mapowania. Opcjonalne pole 'items' "
        "(tablica obiektow JSON, dynamiczne pola) — pozycje dokumentu, "
        "limit domyslnie 500 (SystemConfig WEBHOOK_MAX_ITEMS_PER_DOCUMENT). "
        "\n\n**Idempotencja (Tier 1a):** ponowne wyslanie tego samego "
        "id_document dla tego samego zrodla, dopoki istnieje aktywna "
        "instancja (status NOT IN approved/cancelled), zwraca HTTP 200 "
        "z idempotent_hit=true i ISTNIEJACYM id_instance — nowy payload "
        "jest wtedy calkowicie ignorowany, nie ma aktualizacji ani konfliktu. "
        "\n\nPrzetwarzanie nowego dokumentu jest ASYNCHRONICZNE — odpowiedz "
        "202 Accepted z id_instance, dispatch do sciezki obiegu nastepuje "
        "w tle (natychmiast jesli mozliwe, w przeciwnym razie w ciagu "
        "1 minuty przez cykliczny worker). "
        "\n\nJesli zrodlo ma is_test_mode=true — dokument jest zapisywany "
        "ale NIE wchodzi do automatycznego dispatch (zgodnie z zasada "
        "izolacji srodowisk testowych)."
    ),
    responses={
        200: {"description": "Idempotentny retry — dokument juz istnial (idempotent_hit=true)"},
        404: {"description": "Nieprawidlowy token lub zrodlo nieaktywne"},
        422: {"description": "Payload nie da sie zmapowac na dokument, lub 'items' ma zly ksztalt/przekracza limit"},
        429: {"description": "Przekroczono rate limit dla tego tokenu"},
    },
)
async def receive_webhook_document(
    token: str,
    db: DB,
    redis: RedisClient,
    request: Request,
    response: Response,
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

    # NAPRAWA 2026-07-16 (Tier 1a, Rozstrzygniecia Koncowe #1):
    # Dekorator @router.post ustawia domyslny status_code=202 (poziom
    # routingu FastAPI) — dla idempotentnego trafienia nadpisujemy go na
    # 200 przez wstrzykniety obiekt Response. Jednoczesnie przekazujemy
    # jawnie code= do BaseResponse.ok(), zeby tresc JSON ("code": ...)
    # byla spojna z realnym naglowkiem HTTP — PRZED ta poprawka body
    # zawsze mowilo "code": 200 (wartosc domyslna BaseResponse.ok) mimo
    # ze realny naglowek HTTP byl 202 z dekoratora — niespojnosc obecna
    # w kodzie od poczatku tego endpointu, naprawiona przy okazji.
    if result.get("idempotent_hit"):
        response.status_code = status.HTTP_200_OK
        http_code = 200
    else:
        response.status_code = status.HTTP_202_ACCEPTED
        http_code = 202

    return BaseResponse.ok(data=result, code=http_code, app_code="webhook.received")