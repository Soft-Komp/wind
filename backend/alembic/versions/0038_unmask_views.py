# backend/alembic/versions/0038_unmask_views.py
"""0038_unmask_views

Przywrócenie danych oryginalnych we wszystkich trzech widokach.

Cofa efekt migracji 0036 i 0037 — przywraca oryginalne wartości
NazwaKontrahenta i NumerFaktury we wszystkich widokach które były
maskowane.

WIDOKI OBJĘTE:
    1. dbo.skw_faktury_akceptacja_naglowek  (v4 masked → v3 oryginalne)
    2. dbo.skw_rozrachunki_faktur           (masked → oryginalne, wersja 0020)
    3. dbo.skw_kontrahenci                  (masked → oryginalne, wersja 0020)

MECHANIZM:
    Wywołuje downgrade() z 0037 i 0036 — definicje widoków zakodowane
    tam na stałe. Nie duplikujemy SQL — importujemy bezpośrednio.

DOWNGRADE tej migracji:
    Przywraca maskowanie (wywołuje upgrade() z 0036 i 0037).

UWAGA PO WYKONANIU:
    Wyczyść Redis cache:
    docker exec windykacja_redis redis-cli FLUSHDB

Revision ID: 0038
Revises:     0037
Create Date: 2026-06-08
"""

from __future__ import annotations

import importlib.util
import os
import logging
from typing import Final

from alembic import op

revision:      str = "0038"
down_revision: str = "0037"
branch_labels       = None
depends_on          = None

logger = logging.getLogger(f"alembic.migration.{revision}")

SCHEMA_EXT: Final[str] = "dbo"


def _load(rev: str):
    """
    Laduje modul migracji bezposrednio z pliku — bez zaleznosci od sys.path.
    Katalog alembic/versions/ nie jest pakietem Python (brak __init__.py),
    wiec importlib.import_module nie dziala. Uzywamy spec_from_file_location.
    """
    suffix = (
        "mask_view_faktury_akceptacja_naglowek"
        if rev == "0036"
        else "mask_views_kontrahenci_rozrachunki"
    )
    filename = f"{rev}_{suffix}.py"

    # Ścieżka względem tego pliku — oba pliki są w tym samym katalogu
    versions_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(versions_dir, filename)

    if not os.path.isfile(filepath):
        raise FileNotFoundError(
            f"[0038] Nie znaleziono pliku migracji: {filepath}"
        )

    spec = importlib.util.spec_from_file_location(f"migration_{rev}", filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def upgrade() -> None:
    """
    Przywraca oryginalne dane — cofa maskowanie ze wszystkich widoków.

    Kolejność (odwrotna do maskowania):
        1. skw_rozrachunki_faktur + skw_kontrahenci  (downgrade 0037)
        2. skw_faktury_akceptacja_naglowek           (downgrade 0036)
    """
    logger.info("[%s] upgrade START — przywracanie danych oryginalnych (cofanie maskowania)", revision)

    logger.info("[%s] 1/2 Przywracam skw_rozrachunki_faktur + skw_kontrahenci …", revision)
    _load("0037").downgrade()
    logger.info("[%s] 2/2 Przywracam skw_faktury_akceptacja_naglowek …", revision)
    _load("0036").downgrade()

    logger.info("[%s] upgrade ZAKOŃCZONY — wszystkie widoki przywrócone do danych oryginalnych", revision)
    logger.info("[%s] PAMIĘTAJ: docker exec windykacja_redis redis-cli FLUSHDB", revision)


def downgrade() -> None:
    """
    Przywraca maskowanie — cofa unmask (powrót do stanu po 0037).

    Kolejność (taka sama jak maskowanie):
        1. skw_faktury_akceptacja_naglowek           (upgrade 0036)
        2. skw_rozrachunki_faktur + skw_kontrahenci  (upgrade 0037)
    """
    logger.warning("[%s] downgrade START — przywracanie maskowania danych", revision)

    _load("0036").upgrade()
    _load("0037").upgrade()

    logger.warning("[%s] downgrade ZAKOŃCZONY — maskowanie przywrócone", revision)
    logger.warning("[%s] PAMIĘTAJ: docker exec windykacja_redis redis-cli FLUSHDB", revision)