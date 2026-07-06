"""
koszt_jednorazowy_service.py
Ręcznie wpisywane, jednorazowe koszty dodatkowe przypisane do monitu.
Append-only — patrz migracja 0049 (trigger TRG_skw_MonitKosztyJednorazowe_AppendOnly).
"""
from __future__ import annotations
import logging
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

_MAX_OPIS_LEN: int = 200
_MAX_KWOTA: Decimal = Decimal("100000.00")


class KosztJednorazowyValidationError(Exception):
    pass


@dataclass(frozen=True)
class KosztJednorazowyInput:
    id_monit: int
    opis: str
    kwota: Decimal

    def __post_init__(self) -> None:
        if self.id_monit <= 0:
            raise KosztJednorazowyValidationError("id_monit musi być dodatnią liczbą całkowitą.")

        sanitized = unicodedata.normalize("NFC", (self.opis or "").strip())
        if not sanitized:
            raise KosztJednorazowyValidationError("opis kosztu nie może być pusty.")
        if len(sanitized) > _MAX_OPIS_LEN:
            raise KosztJednorazowyValidationError(
                f"opis kosztu przekracza {_MAX_OPIS_LEN} znaków ({len(sanitized)})."
            )
        object.__setattr__(self, "opis", sanitized)

        try:
            kwota_dec = Decimal(str(self.kwota)).quantize(Decimal("0.01"))
        except (InvalidOperation, ValueError) as exc:
            raise KosztJednorazowyValidationError(f"kwota nieprawidłowa: {self.kwota!r}") from exc
        if kwota_dec <= 0 or kwota_dec > _MAX_KWOTA:
            raise KosztJednorazowyValidationError(
                f"kwota musi być w zakresie (0, {_MAX_KWOTA}], otrzymano: {kwota_dec}"
            )
        object.__setattr__(self, "kwota", kwota_dec)


async def dodaj_koszt_jednorazowy(
    db: AsyncSession,
    dane: KosztJednorazowyInput,
    id_user_dodal: int,
    ip_address: str | None,
    request_id: str | None,
) -> dict:
    """
    Dopisuje jednorazowy koszt do monitu. Append-only — brak metody edycji,
    tylko `unieważnij_koszt_jednorazowy` (is_voided=1).
    """
    result = await db.execute(
        text("""
            INSERT INTO dbo.skw_MonitKosztyJednorazowe
                (id_monit, opis, kwota, id_user_dodal, ip_address, request_id)
            OUTPUT INSERTED.id_koszt, INSERTED.created_at
            VALUES (:id_monit, :opis, :kwota, :id_user_dodal, :ip_address, :request_id)
        """),
        {
            "id_monit": dane.id_monit,
            "opis": dane.opis,
            "kwota": dane.kwota,
            "id_user_dodal": id_user_dodal,
            "ip_address": ip_address,
            "request_id": request_id,
        },
    )
    row = result.fetchone()
    await db.commit()

    logger.info(
        "koszt_jednorazowy: dodano nowy koszt do monitu",
        extra={
            "event": "koszt_jednorazowy.dodano",
            "id_koszt": row.id_koszt,
            "id_monit": dane.id_monit,
            "kwota": str(dane.kwota),
            "opis": dane.opis,
            "id_user_dodal": id_user_dodal,
            "ip_address": ip_address,
            "request_id": request_id,
            "created_at": row.created_at.isoformat(),
        },
    )
    return {
        "id_koszt": row.id_koszt,
        "id_monit": dane.id_monit,
        "opis": dane.opis,
        "kwota": float(dane.kwota),
        "created_at": row.created_at.isoformat(),
    }


async def unieważnij_koszt_jednorazowy(
    db: AsyncSession, id_koszt: int, id_user: int, powod: str,
) -> None:
    """Jedyny dozwolony 'zapis' po utworzeniu — ustawienie is_voided=1 (trigger pilnuje reszty)."""
    sanitized_powod = unicodedata.normalize("NFC", (powod or "").strip())[:200]
    if not sanitized_powod:
        raise KosztJednorazowyValidationError("powod unieważnienia jest wymagany.")

    await db.execute(
        text("""
            UPDATE dbo.skw_MonitKosztyJednorazowe
            SET is_voided = 1, voided_at = SYSUTCDATETIME(),
                voided_by = :id_user, voided_reason = :powod
            WHERE id_koszt = :id_koszt AND is_voided = 0
        """),
        {"id_koszt": id_koszt, "id_user": id_user, "powod": sanitized_powod},
    )
    await db.commit()
    logger.warning(
        "koszt_jednorazowy: unieważniono koszt",
        extra={"event": "koszt_jednorazowy.uniewazniono", "id_koszt": id_koszt,
               "id_user": id_user, "powod": sanitized_powod},
    )