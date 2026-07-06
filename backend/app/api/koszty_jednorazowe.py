@router.post(
    "/monits/{monit_id}/koszt-jednorazowy",
    summary="Dodaj jednorazowy koszt do monitu",
    dependencies=[require_permission("monits.manage")],
)
async def dodaj_koszt(
    monit_id: int,
    body: dict,
    current_user: CurrentUser, db: DB, ip: ClientIP, request_id: RequestID,
):
    from app.services.koszt_jednorazowy_service import (
        KosztJednorazowyInput,
        KosztJednorazowyValidationError,
        KosztJednorazowyMonitZamknietyError,   # ← NOWY import
        dodaj_koszt_jednorazowy,
    )

    # ── BLOK 1: walidacja i sanityzacja wejścia (bez zmian) ─────────────────
    try:
        dane = KosztJednorazowyInput(
            id_monit=monit_id,
            opis=str(body.get("opis", "")),
            kwota=body.get("kwota"),
        )
    except KosztJednorazowyValidationError as exc:
        logger.warning(
            "dodaj_koszt: błąd walidacji wejścia",
            extra={
                "event": "koszt_jednorazowy.walidacja_blad",
                "monit_id": monit_id,
                "body_raw": body,
                "error": str(exc),
                "user_id": current_user.id_user,
                "ip": ip,
                "request_id": request_id,
            },
        )
        raise HTTPException(status_code=422, detail={
            "code": "koszt_jednorazowy.validation_error", "message": str(exc),
        })

    # ── BLOK 2: zapis do bazy — TU wchodzi nowy except ──────────────────────
    try:
        result = await dodaj_koszt_jednorazowy(
            db=db, dane=dane,
            id_user_dodal=current_user.id_user, ip_address=ip, request_id=request_id,
        )
    except KosztJednorazowyMonitZamknietyError as exc:
        # MUSI być PRZED KosztJednorazowyValidationError — to jej podklasa,
        # więc kolejność except decyduje, który blok faktycznie coś złapie.
        logger.warning(
            "dodaj_koszt: monit nie jest już 'pending' — odrzucono",
            extra={
                "event": "koszt_jednorazowy.monit_zamkniety",
                "monit_id": monit_id,
                "error": str(exc),
                "user_id": current_user.id_user,
                "ip": ip,
                "request_id": request_id,
            },
        )
        raise HTTPException(status_code=409, detail={
            "code": "koszt_jednorazowy.monit_closed", "message": str(exc),
        })
    except KosztJednorazowyValidationError as exc:
        # Łapie np. "Monit ID=... nie istnieje lub jest nieaktywny"
        logger.warning(
            "dodaj_koszt: błąd walidacji przy zapisie",
            extra={
                "event": "koszt_jednorazowy.zapis_blad",
                "monit_id": monit_id,
                "error": str(exc),
                "user_id": current_user.id_user,
                "ip": ip,
                "request_id": request_id,
            },
        )
        raise HTTPException(status_code=422, detail={
            "code": "koszt_jednorazowy.validation_error", "message": str(exc),
        })

    return BaseResponse.ok(data=result, app_code="koszt_jednorazowy.dodano")