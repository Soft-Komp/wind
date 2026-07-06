# backend/app/schemas/common_validators.py — DOPISAĆ (lub nowy plik, jeśli nie istnieje)
from datetime import date
from pydantic import field_validator

_DATA_WYDRUKU_MIN: date = date(1990, 1, 1)
_DATA_WYDRUKU_MAX: date = date(2100, 12, 31)


def validate_data_wydruku(v: date | None) -> date | None:
    """
    Walidacja techniczna (NIE biznesowa — decyzja: brak limitu tolerancji).
    Blokuje wyłącznie wartości niemożliwe/uszkodzone (overflow, literówka roku).
    """
    if v is None:
        return None
    if not (_DATA_WYDRUKU_MIN <= v <= _DATA_WYDRUKU_MAX):
        raise ValueError(
            f"data_wydruku poza dopuszczalnym zakresem technicznym "
            f"[{_DATA_WYDRUKU_MIN} … {_DATA_WYDRUKU_MAX}]: {v}"
        )
    return v