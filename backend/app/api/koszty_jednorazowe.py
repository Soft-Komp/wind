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
        KosztJednorazowyInput, KosztJednorazowyValidationError, dodaj_koszt_jednorazowy,
    )
    try:
        dane = KosztJednorazowyInput(
            id_monit=monit_id,
            opis=str(body.get("opis", "")),
            kwota=body.get("kwota"),
        )
    except KosztJednorazowyValidationError as exc:
        raise HTTPException(status_code=422, detail={
            "code": "koszt_jednorazowy.validation_error", "message": str(exc),
        })

    result = await dodaj_koszt_jednorazowy(
        db=db, dane=dane,
        id_user_dodal=current_user.id_user, ip_address=ip, request_id=request_id,
    )
    return BaseResponse.ok(data=result, app_code="koszt_jednorazowy.dodano")