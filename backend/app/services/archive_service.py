"""
Serwis Archiwizacji — System Windykacja
=========================================

Odpowiedzialność:
    - Archiwizacja rekordów bazy danych przed soft-delete (JSON.gz)
    - Obsługa zarówno obiektów SQLAlchemy (ORM) jak i zwykłych słowników
    - Odczyt archiwum do diagnostyki lub restore
    - PLIKI NIEUSUWALNE — aplikacja tylko tworzy, nigdy nie kasuje

Format pliku:
    /app/archives/{YYYY-MM-DD}/archive_{table}_{id}_{ts}.json.gz

Schemat wewnątrz archiwum (JSON po dekompresji):
    {
        "archived_at":  "ISO datetime UTC",
        "archive_type": "soft_delete" | "manual" | "snapshot_row",
        "table":        "dbo_ext.Users",
        "record_id":    42,
        "record":       { ...pola rekordu... }
    }

Decyzje projektowe:
    - Nazwy plików zawierają timestamp z dokładnością do sekundy
      → wiele archiwów tego samego rekordu nie nadpisuje się nawzajem
    - Jeśli plik o tej samej nazwie już istnieje → dodajemy suffix _N
    - orjson do serializacji (najszybszy, obsługuje Decimal, datetime)
    - gzip poziom 6 (dobry kompromis szybkość/kompresja)
    - Błąd archiwizacji → log ERROR + zwróć None (nie blokuje głównej operacji)

"""

from __future__ import annotations

import gzip
import inspect
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional, Union

import orjson

# ---------------------------------------------------------------------------
# Logger własny modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

_ARCHIVE_BASE_DIR: str = "archives"
_GZIP_LEVEL: int = 6          # 1–9; 6 = dobry balans szybkość/rozmiar
_MAX_FILENAME_SUFFIX: int = 999  # Maksymalny suffix anty-kolizyjny

# Dozwolone typy archiwizacji
_VALID_ARCHIVE_TYPES: frozenset[str] = frozenset({
    "soft_delete",
    "manual",
    "snapshot_row",
    "bulk_delete",
})


# ===========================================================================
# Klasy wyjątków
# ===========================================================================

class ArchiveError(Exception):
    """Bazowy wyjątek serwisu archiwizacji."""


class ArchiveSerializationError(ArchiveError):
    """Nie udało się serializować rekordu do JSON."""


class ArchiveWriteError(ArchiveError):
    """Nie udało się zapisać pliku archiwum."""


class ArchiveReadError(ArchiveError):
    """Nie udało się odczytać pliku archiwum."""


# ===========================================================================
# Funkcje pomocnicze — prywatne
# ===========================================================================

def _get_archive_dir(date_utc: Optional[datetime] = None) -> Path:
    """
    Zwraca i tworzy dzienny katalog archiwum.

    Format: archives/YYYY-MM-DD/

    Args:
        date_utc: Data UTC do użycia jako prefiks katalogu.
                  Domyślnie: datetime.now(UTC).

    Returns:
        Ścieżka do istniejącego katalogu archiwum.
    """
    if date_utc is None:
        date_utc = datetime.now(timezone.utc)
    date_str = date_utc.strftime("%Y-%m-%d")
    path = Path(_ARCHIVE_BASE_DIR) / date_str
    path.mkdir(parents=True, exist_ok=True)
    return path


def _build_archive_filename(
    table_name: str,
    record_id: Any,
    timestamp: datetime,
) -> str:
    """
    Buduje nazwę pliku archiwum.

    Format: archive_{table}_{id}_{YYYYMMDD_HHMMSS}.json.gz

    Sanityzacja table_name: tylko alfanumeryczne i podkreślenia.

    Args:
        table_name: Nazwa tabeli (np. "Users", "dbo_ext.Comments").
        record_id:  ID rekordu.
        timestamp:  Timestamp operacji.

    Returns:
        Nazwa pliku archiwum (bez ścieżki).
    """
    import re
    # Sanityzacja nazwy tabeli — wyciągamy tylko ostatni człon (po ostatniej kropce)
    clean_table = table_name.rsplit(".", 1)[-1]
    clean_table = re.sub(r"[^a-zA-Z0-9_]", "_", clean_table).lower()
    ts_str = timestamp.strftime("%Y%m%d_%H%M%S")
    return f"archive_{clean_table}_{record_id}_{ts_str}.json.gz"


def _resolve_unique_filepath(dirpath: Path, filename: str) -> Path:
    """
    Zwraca ścieżkę pliku, dodając suffix _N jeśli plik już istnieje.

    Zapobiega nadpisaniu istniejącego archiwum.

    Args:
        dirpath:  Katalog docelowy.
        filename: Preferowana nazwa pliku.

    Returns:
        Unikalna ścieżka (dirpath / filename lub dirpath / filename_N).
    """
    candidate = dirpath / filename
    if not candidate.exists():
        return candidate

    stem = filename.removesuffix(".json.gz")
    for n in range(1, _MAX_FILENAME_SUFFIX + 1):
        candidate = dirpath / f"{stem}_{n}.json.gz"
        if not candidate.exists():
            return candidate

    # Ostateczny fallback — timestamp z mikrosekundami
    from datetime import datetime as _dt
    micro = _dt.now(timezone.utc).strftime("%f")
    return dirpath / f"{stem}_{micro}.json.gz"


def _default_json_serializer(obj: Any) -> Any:
    """
    Niestandardowy serializer JSON dla typów nieobsługiwanych przez orjson.

    Obsługuje: Decimal, date, datetime (bez strefy czasowej), bytes, set.

    Args:
        obj: Obiekt do serializacji.

    Returns:
        Wartość prymitywna możliwa do serializacji przez JSON.

    Raises:
        TypeError: Dla nieznanych typów.
    """
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.hex()
    if isinstance(obj, set):
        return sorted(obj)
    raise TypeError(f"Nie można serializować obiektu typu {type(obj)!r}")


def _serialize_record(record: dict) -> bytes:
    """
    Serializuje słownik rekordu do JSON.

    Używa orjson z custom handlerem dla Decimal/datetime.

    Args:
        record: Słownik danych rekordu.

    Returns:
        Bajty JSON (bez kompresji).

    Raises:
        ArchiveSerializationError: Gdy serializacja się nie powiodła.
    """
    try:
        return orjson.dumps(
            record,
            default=_default_json_serializer,
            option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS,
        )
    except Exception as exc:
        raise ArchiveSerializationError(
            f"Błąd serializacji rekordu do JSON: {exc}"
        ) from exc


def _orm_object_to_dict(obj: Any) -> dict:
    """
    Konwertuje obiekt SQLAlchemy ORM na słownik.

    Iteruje po kolumnach modelu przez SQLAlchemy Inspector.
    Bezpieczne — nie ładuje relacji (lazy load nie jest wywoływany).

    Args:
        obj: Instancja modelu SQLAlchemy.

    Returns:
        Słownik {nazwa_kolumny: wartość}.
    """
    # Metoda 1: przez __table__.columns (najszybsza, bez reflection)
    if hasattr(obj, "__table__"):
        return {
            col.name: getattr(obj, col.name, None)
            for col in obj.__table__.columns
        }

    # Metoda 2: przez __dict__ (fallback)
    return {
        k: v
        for k, v in obj.__dict__.items()
        if not k.startswith("_")
    }


def _extract_table_name(obj: Any) -> str:
    """
    Wyciąga nazwę tabeli z obiektu SQLAlchemy.

    Args:
        obj: Instancja modelu SQLAlchemy.

    Returns:
        Kwalifikowana nazwa tabeli (np. "dbo_ext.Users").
    """
    if hasattr(obj, "__table__"):
        table = obj.__table__
        schema = getattr(table, "schema", None) or "dbo_ext"
        return f"{schema}.{table.name}"
    return f"dbo_ext.{type(obj).__name__}"


def _extract_record_id(obj: Any, record_dict: dict) -> Any:
    """
    Wyciąga ID rekordu z obiektu lub słownika.

    Szuka w kolejności: id_*, ID_*, pk, id, first int value.

    Args:
        obj:         Obiekt (może być None dla wariantu dict).
        record_dict: Słownik danych rekordu.

    Returns:
        Wartość ID lub "unknown".
    """
    # Szukaj kolumny PK w obiekcie SQLAlchemy
    if obj is not None and hasattr(obj, "__table__"):
        pk_cols = list(obj.__table__.primary_key.columns)
        if pk_cols:
            return getattr(obj, pk_cols[0].name, None)

    # Szukaj w słowniku po typowych wzorcach kluczy PK
    for key in record_dict:
        if key.lower().startswith(("id_", "pk_")) or key.lower() == "id":
            return record_dict[key]

    return "unknown"


def _write_gzip_file(filepath: Path, content: bytes) -> int:
    """
    Zapisuje skompresowany plik archiwum.

    Args:
        filepath: Ścieżka docelowa.
        content:  Dane JSON (niekompresowane) do zapisania.

    Returns:
        Rozmiar skompresowanego pliku w bajtach.

    Raises:
        ArchiveWriteError: Gdy zapis się nie powiódł.
    """
    try:
        with gzip.open(filepath, "wb", compresslevel=_GZIP_LEVEL) as f:
            f.write(content)
        return filepath.stat().st_size
    except OSError as exc:
        raise ArchiveWriteError(
            f"Nie udało się zapisać pliku archiwum '{filepath}': {exc}"
        ) from exc


# ===========================================================================
# Publiczne API serwisu
# ===========================================================================

def archive(
    obj: Any,
    archive_type: str = "soft_delete",
    extra_metadata: Optional[dict] = None,
) -> Optional[Path]:
    """
    Archiwizuje obiekt SQLAlchemy ORM do pliku JSON.gz.

    Główna funkcja serwisu — wywoływana przez inne serwisy przed soft-delete.
    Synchroniczna (I/O do pliku) — nie używa async, bo zapis jest szybki.

    Przepływ:
        1. Konwersja obiektu ORM → słownik
        2. Budowanie envelope (metadane + dane)
        3. Serializacja do JSON (orjson)
        4. Kompresja gzip
        5. Zapis do pliku (anty-kolizja nazwy)
        6. Log INFO z rozmiarem pliku

    Args:
        obj:            Instancja modelu SQLAlchemy (np. User, Role, Comment).
        archive_type:   Typ archiwizacji ("soft_delete", "manual", etc.).
        extra_metadata: Dodatkowe metadane do dołączenia do envelope.

    Returns:
        Ścieżka do pliku archiwum lub None przy błędzie.
        Błąd NIE jest rzucany — operacja archiwizacji nie blokuje soft-delete.
    """
    if archive_type not in _VALID_ARCHIVE_TYPES:
        archive_type = "manual"

    try:
        now = datetime.now(timezone.utc)
        table_name  = _extract_table_name(obj)
        record_dict = _orm_object_to_dict(obj)
        record_id   = _extract_record_id(obj, record_dict)

        return _write_archive_file(
            table_name=table_name,
            record_id=record_id,
            record_dict=record_dict,
            archive_type=archive_type,
            now=now,
            extra_metadata=extra_metadata,
        )
    except Exception as exc:
        logger.error(
            "Błąd archiwizacji obiektu ORM",
            extra={
                "obj_type": type(obj).__name__,
                "archive_type": archive_type,
                "error": str(exc),
            }
        )
        return None


def archive_dict(
    table_name: str,
    record_id: Any,
    data: dict,
    archive_type: str = "soft_delete",
    extra_metadata: Optional[dict] = None,
) -> Optional[Path]:
    """
    Archiwizuje dane ze słownika (np. z pyodbc) do pliku JSON.gz.

    Wariant dla danych które nie przychodzą z SQLAlchemy ORM.

    Args:
        table_name:     Nazwa tabeli (np. "dbo.skw_kontrahenci").
        record_id:      ID rekordu (do nazwy pliku).
        data:           Słownik danych do archiwizacji.
        archive_type:   Typ archiwizacji.
        extra_metadata: Dodatkowe metadane.

    Returns:
        Ścieżka do pliku archiwum lub None przy błędzie.
    """
    if archive_type not in _VALID_ARCHIVE_TYPES:
        archive_type = "manual"

    try:
        now = datetime.now(timezone.utc)
        return _write_archive_file(
            table_name=table_name,
            record_id=record_id,
            record_dict=data,
            archive_type=archive_type,
            now=now,
            extra_metadata=extra_metadata,
        )
    except Exception as exc:
        logger.error(
            "Błąd archiwizacji słownika danych",
            extra={
                "table_name": table_name,
                "record_id": record_id,
                "archive_type": archive_type,
                "error": str(exc),
            }
        )
        return None


def archive_many(
    objects: list[Any],
    archive_type: str = "bulk_delete",
    extra_metadata: Optional[dict] = None,
) -> list[Path]:
    """
    Archiwizuje wiele obiektów jednocześnie (np. przy bulk delete).

    Kontynuuje archiwizację nawet jeśli jeden z obiektów się nie powiodło.

    Args:
        objects:        Lista obiektów SQLAlchemy ORM.
        archive_type:   Typ archiwizacji.
        extra_metadata: Wspólne metadane dla wszystkich archiwów.

    Returns:
        Lista ścieżek do plików archiwum (tylko te które się udały).
    """
    paths: list[Path] = []
    errors = 0

    for obj in objects:
        path = archive(obj, archive_type=archive_type, extra_metadata=extra_metadata)
        if path:
            paths.append(path)
        else:
            errors += 1

    if errors > 0:
        logger.warning(
            "Część archiwizacji w bulk nie powiodła się",
            extra={
                "total": len(objects),
                "succeeded": len(paths),
                "failed": errors,
            }
        )
    else:
        logger.info(
            "Bulk archiwizacja zakończona sukcesem",
            extra={"total": len(objects)}
        )

    return paths


def _write_archive_file(
    table_name: str,
    record_id: Any,
    record_dict: dict,
    archive_type: str,
    now: datetime,
    extra_metadata: Optional[dict] = None,
) -> Path:
    """
    Wewnętrzna funkcja zapisująca plik archiwum.

    Konstruuje envelope JSON i zapisuje do pliku .json.gz.

    Args:
        table_name:     Nazwa tabeli.
        record_id:      ID rekordu.
        record_dict:    Dane rekordu (słownik).
        archive_type:   Typ archiwizacji.
        now:            Timestamp operacji.
        extra_metadata: Dodatkowe metadane (opcjonalne).

    Returns:
        Ścieżka do zapisanego pliku.

    Raises:
        ArchiveSerializationError: Błąd serializacji.
        ArchiveWriteError:         Błąd zapisu.
    """
    # Budowa envelope
    envelope: dict = {
        "archived_at": now.isoformat(),
        "archive_type": archive_type,
        "table": table_name,
        "record_id": record_id,
        "record": record_dict,
    }
    if extra_metadata:
        envelope["metadata"] = extra_metadata

    # Serializacja
    json_bytes = _serialize_record(envelope)

    # Ścieżka pliku
    archive_dir = _get_archive_dir(now)
    filename = _build_archive_filename(table_name, record_id, now)
    filepath = _resolve_unique_filepath(archive_dir, filename)

    # Zapis
    compressed_size = _write_gzip_file(filepath, json_bytes)

    logger.info(
        "Archiwum zapisane",
        extra={
            "filepath": str(filepath),
            "table": table_name,
            "record_id": record_id,
            "archive_type": archive_type,
            "original_bytes": len(json_bytes),
            "compressed_bytes": compressed_size,
            "compression_ratio": round(len(json_bytes) / max(compressed_size, 1), 2),
        }
    )

    return filepath


def read(filepath: Union[str, Path]) -> dict:
    """
    Odczytuje i dekompresuje plik archiwum.

    Do użycia przy diagnostyce, restore lub audycie.

    Args:
        filepath: Ścieżka do pliku .json.gz.

    Returns:
        Słownik z danymi archiwum (envelope + record).

    Raises:
        ArchiveReadError: Gdy plik nie istnieje lub jest uszkodzony.
    """
    path = Path(filepath)
    if not path.exists():
        raise ArchiveReadError(f"Plik archiwum nie istnieje: {path}")
    if not path.suffix == ".gz":
        raise ArchiveReadError(
            f"Plik nie jest archiwum gzip: {path}. "
            f"Oczekiwano rozszerzenia .json.gz"
        )
    try:
        with gzip.open(path, "rb") as f:
            content = f.read()
        return orjson.loads(content)
    except gzip.BadGzipFile as exc:
        raise ArchiveReadError(f"Uszkodzony plik gzip: {path}: {exc}") from exc
    except Exception as exc:
        raise ArchiveReadError(f"Błąd odczytu archiwum '{path}': {exc}") from exc


def list_archives(
    date_filter: Optional[str] = None,
    table_filter: Optional[str] = None,
) -> list[dict]:
    """
    Listuje dostępne pliki archiwum.

    Skanuje katalog archives/ (lub jego podkatalog dla danej daty).
    NIE odczytuje zawartości plików — tylko metadane z nazwy pliku.

    Args:
        date_filter:  Filtr po dacie (format YYYY-MM-DD). None = wszystkie daty.
        table_filter: Filtr po nazwie tabeli (substring, case-insensitive).

    Returns:
        Lista słowników z metadanymi plików:
        [{"path": str, "date": str, "table": str, "size_bytes": int, ...}]
    """
    base = Path(_ARCHIVE_BASE_DIR)
    if not base.exists():
        return []

    results: list[dict] = []

    if date_filter:
        date_dirs = [base / date_filter] if (base / date_filter).exists() else []
    else:
        date_dirs = sorted(
            [d for d in base.iterdir() if d.is_dir()],
            reverse=True,
        )

    for date_dir in date_dirs:
        for filepath in sorted(date_dir.glob("archive_*.json.gz")):
            filename = filepath.name
            # Parsowanie nazwy: archive_{table}_{id}_{timestamp}.json.gz
            parts = filename.removesuffix(".json.gz").split("_", 2)

            # Wyciągnij tabelę z nazwy pliku
            name_without_prefix = filename.removeprefix("archive_")
            table_guess = name_without_prefix.split("_")[0] if "_" in name_without_prefix else "unknown"

            if table_filter and table_filter.lower() not in table_guess.lower():
                continue

            try:
                size = filepath.stat().st_size
            except OSError:
                size = 0

            results.append({
                "path": str(filepath),
                "date": date_dir.name,
                "filename": filename,
                "table_hint": table_guess,
                "size_bytes": size,
                "size_kb": round(size / 1024, 2),
            })

    return results


def get_archive_stats() -> dict:
    """
    Zwraca statystyki katalogu archiwum.

    Liczba plików, całkowity rozmiar, daty, itp.

    Returns:
        Słownik ze statystykami.
    """
    base = Path(_ARCHIVE_BASE_DIR)
    if not base.exists():
        return {
            "total_files": 0,
            "total_size_bytes": 0,
            "total_size_mb": 0.0,
            "date_dirs": [],
            "oldest_date": None,
            "newest_date": None,
        }

    total_files = 0
    total_size  = 0
    date_dirs: list[str] = []

    for date_dir in sorted(base.iterdir()):
        if not date_dir.is_dir():
            continue
        files = list(date_dir.glob("archive_*.json.gz"))
        dir_size = sum(f.stat().st_size for f in files if f.exists())
        total_files += len(files)
        total_size  += dir_size
        date_dirs.append(date_dir.name)

    return {
        "total_files": total_files,
        "total_size_bytes": total_size,
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "date_dirs": sorted(date_dirs, reverse=True),
        "oldest_date": date_dirs[0] if date_dirs else None,
        "newest_date": date_dirs[-1] if date_dirs else None,
    }