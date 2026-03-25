"""
Serwis Komentarzy — System Windykacja
=======================================

Odpowiedzialność:
    - Pełny CRUD komentarzy do dłużników (tabela dbo_ext.Comments)
    - Kontrola dostępu na poziomie serwisu: edit_own vs edit_any, delete_own vs delete_any
    - Dwuetapowe usuwanie (taki sam mechanizm jak user_service)
    - Archiwizacja przed soft-delete (przez archive_service)
    - Paginacja listy komentarzy per dłużnik
    - Sanityzacja treści komentarza (NFC + limit długości)
    - Plik logów comments_YYYY-MM-DD.jsonl

Architektura tabeli Comments (TABELE_REFERENCJA.md §8):
    - ID_COMMENT   INT IDENTITY PK
    - ID_KONTRAHENTA INT NOT NULL (ref WAPRO, BEZ FK constraint)
    - Tresc        NVARCHAR(MAX) NOT NULL — ⚠️ NIE "Content"
    - UzytkownikID INT NOT NULL FK → Users RESTRICT
    - IsActive     BIT DEFAULT 1
    - CreatedAt    DATETIME
    - UpdatedAt    DATETIME (trigger)

Decyzje projektowe:
    - Właściciel komentarza = UzytkownikID
    - Edycja: wywoływana z info czy user ma edit_own/edit_any (check po stronie serwisu)
    - UzytkownikID RESTRICT: nie można usunąć usera z komentarzami — dlatego soft-delete usera
    - Cache Redis: brak (komentarze nie są gorącą ścieżką, częste mutacje, krótkie TTL nie opłacalne)
    - Token DELETE: JWT (scope=confirm_delete, entity_type=Comment, TTL z delete_token.ttl_seconds)

"""

from __future__ import annotations

import logging
import secrets
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import orjson
from jose import JWTError, jwt
from redis.asyncio import Redis
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models.comment import Comment
from app.services import archive_service
from app.services import audit_service
from app.services import config_service

# ---------------------------------------------------------------------------
# Logger własny modułu
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stałe
# ---------------------------------------------------------------------------

_DEFAULT_PAGE_SIZE: int = 20
_MAX_PAGE_SIZE: int     = 100
_MAX_TRESC_LENGTH: int  = 10_000   # Limit treści komentarza (NVARCHAR(MAX) ale sanityzujemy)
_MIN_TRESC_LENGTH: int  = 1        # Komentarz nie może być pusty

_DEFAULT_DELETE_TOKEN_TTL: int = 60

# Klucz Redis dla tokenu DELETE (jednorazowość)
_REDIS_KEY_DELETE = "delete_confirm:comment:{jti}"

# Plik logów
_COMMENTS_LOG_FILE_PATTERN = "logs/comments_{date}.jsonl"


# ===========================================================================
# Dataclassy wejściowe
# ===========================================================================

@dataclass(frozen=True)
class CommentCreateData:
    """
    Dane do tworzenia komentarza.

    Attributes:
        debtor_id: ID kontrahenta WAPRO (ID_KONTRAHENTA).
        tresc:     Treść komentarza (NFC, sanityzowana).
    """
    debtor_id: int
    tresc: str

    def __post_init__(self) -> None:
        if self.debtor_id <= 0:
            raise CommentValidationError("debtor_id musi być dodatnią liczbą całkowitą.")
        tresc = unicodedata.normalize("NFC", self.tresc.strip())
        if len(tresc) < _MIN_TRESC_LENGTH:
            raise CommentValidationError("Treść komentarza nie może być pusta.")
        if len(tresc) > _MAX_TRESC_LENGTH:
            raise CommentValidationError(
                f"Treść komentarza przekracza maksymalną długość {_MAX_TRESC_LENGTH} znaków. "
                f"Obecna: {len(tresc)} znaków."
            )
        object.__setattr__(self, "tresc", tresc)


@dataclass
class CommentUpdateData:
    """
    Dane do aktualizacji komentarza.

    Attributes:
        tresc: Nowa treść komentarza (jedyne pole do aktualizacji).
    """
    tresc: str

    def __post_init__(self) -> None:
        tresc = unicodedata.normalize("NFC", self.tresc.strip())
        if len(tresc) < _MIN_TRESC_LENGTH:
            raise CommentValidationError("Treść komentarza nie może być pusta.")
        if len(tresc) > _MAX_TRESC_LENGTH:
            raise CommentValidationError(
                f"Treść komentarza przekracza maksymalną długość {_MAX_TRESC_LENGTH} znaków."
            )
        self.tresc = tresc


@dataclass(frozen=True)
class DeleteConfirmData:
    """
    Dane tokenu potwierdzającego DELETE komentarza.

    Attributes:
        token:       JWT token potwierdzający.
        expires_in:  TTL w sekundach.
        comment_id:  ID komentarza.
        debtor_id:   ID dłużnika.
        tresc_preview: Pierwsze 100 znaków treści (do podglądu w UI).
    """
    token: str
    expires_in: int
    comment_id: int
    debtor_id: int
    tresc_preview: str


# ===========================================================================
# Klasy wyjątków
# ===========================================================================

class CommentError(Exception):
    """Bazowy wyjątek serwisu komentarzy."""


class CommentValidationError(CommentError):
    """Błąd walidacji danych wejściowych."""


class CommentNotFoundError(CommentError):
    """Komentarz nie istnieje lub jest nieaktywny."""


class CommentPermissionError(CommentError):
    """
    Użytkownik nie ma uprawnień do edycji/usunięcia tego komentarza.

    Rzucany gdy user próbuje edytować/usuwać komentarz innego usera
    bez uprawnienia edit_any/delete_any.
    """


class CommentDeleteTokenError(CommentError):
    """Token potwierdzający DELETE jest nieprawidłowy, wygasły lub już użyty."""


# ===========================================================================
# Funkcje pomocnicze — prywatne
# ===========================================================================

def _get_comments_log_file() -> Path:
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return log_dir / f"comments_{today}.jsonl"


def _append_to_file(filepath: Path, record: dict) -> None:
    try:
        line = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        with filepath.open("ab") as f:
            f.write(line)
    except OSError as exc:
        logger.warning(
            "Nie można zapisać do pliku logu komentarzy",
            extra={"filepath": str(filepath), "error": str(exc)}
        )


def _build_log_record(action: str, **kwargs) -> dict:
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": "comment_service",
        "action": action,
        **kwargs,
    }


def _comment_to_dict(comment) -> dict:
    """
    Konwertuje obiekt Comment na słownik bezpieczny do zwrotu przez API.

    Relacja `uzytkownik` (lazy="selectin") jest już załadowana przez SQLAlchemy
    — brak dodatkowych zapytań do bazy.

    Zwracane pola autora:
        uzytkownik_id   — raw ID (zachowane: używane wewnętrznie do logiki edit_own)
        autor_full_name — imię i nazwisko (FullName z skw_Users), None jeśli brak
        autor_username  — login (Username), zawsze NOT NULL — fallback dla UI

    Przykładowa odpowiedź:
        {
            "id_comment": 42,
            "id_kontrahenta": 7,
            "tresc": "Obiecał zapłacić do piątku.",
            "uzytkownik_id": 1,
            "autor_full_name": "Jan Kowalski",
            "autor_username": "jkowalski",
            "is_active": true,
            "created_at": "2026-03-10T09:15:00+00:00",
            "updated_at": null
        }
    """
    # Bezpieczne pobranie danych autora — relacja może być None przy uszkodzonych danych
    autor = getattr(comment, "uzytkownik", None)
    autor_full_name: str | None = None

    if autor is not None:
        # full_name może być NULL w bazie (pole opcjonalne w skw_Users)
        autor_full_name = autor.full_name or None
    else:
        # Sytuacja awaryjna: relacja nie załadowana lub user usunięty
        # Logujemy ostrzeżenie — nie rzucamy wyjątku (nie blokujemy odpowiedzi)
        logger.warning(
            "Komentarz bez załadowanego autora (relacja uzytkownik=None)",
            extra={
                "comment_id":    comment.id_comment,
                "uzytkownik_id": comment.uzytkownik_id,
            }
        )

    return {
        "id_comment":      comment.id_comment,
        "id_kontrahenta":  comment.id_kontrahenta,
        "tresc":           comment.tresc,
        "uzytkownik_id":   comment.uzytkownik_id,   # zachowane — logika edit_own
        "autor_full_name": autor_full_name,          # NOWE: imię i nazwisko
        "is_active":       comment.is_active,
        "created_at":      comment.created_at.isoformat() if comment.created_at else None,
        "updated_at":      comment.updated_at.isoformat() if comment.updated_at else None,
    }



# ===========================================================================
# Publiczne API serwisu — READ
# ===========================================================================

async def get_list(
    db: AsyncSession,
    debtor_id: int,
    page: int = 1,
    page_size: int = _DEFAULT_PAGE_SIZE,
    requesting_user_id: Optional[int] = None,
) -> dict:
    """
    Pobiera paginowaną listę komentarzy dla dłużnika.

    Sortowanie: od najnowszego (CreatedAt DESC).

    Args:
        db:                  Sesja SQLAlchemy.
        debtor_id:           ID kontrahenta WAPRO.
        page:                Numer strony (1-based).
        page_size:           Rozmiar strony (max 100).
        requesting_user_id:  ID użytkownika (do logowania).

    Returns:
        Słownik z listą komentarzy i metadanymi paginacji.
    """
    if debtor_id <= 0:
        raise CommentValidationError("debtor_id musi być dodatnią liczbą całkowitą.")

    page      = max(page, 1)
    page_size = min(max(page_size, 1), _MAX_PAGE_SIZE)

    where = and_(
        Comment.id_kontrahenta == debtor_id,
        Comment.is_active == True,  # noqa: E712
    )

    count_result = await db.execute(
        select(func.count(Comment.id_comment)).where(where)
    )
    total = count_result.scalar_one() or 0

    if total == 0:
        return {
            "items": [], "total": 0,
            "page": page, "page_size": page_size, "total_pages": 0,
            "debtor_id": debtor_id,
        }

    from sqlalchemy import desc
    data_result = await db.execute(
        select(Comment)
        .where(where)
        .order_by(desc(Comment.created_at))
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    comments = data_result.scalars().all()
    total_pages = (total + page_size - 1) // page_size

    logger.debug(
        "Lista komentarzy pobrana",
        extra={
            "debtor_id": debtor_id,
            "total": total,
            "returned": len(comments),
            "user_id": requesting_user_id,
        }
    )

    return {
        "items":       [_comment_to_dict(c) for c in comments],
        "total":       total,
        "page":        page,
        "page_size":   page_size,
        "total_pages": total_pages,
        "debtor_id":   debtor_id,
    }


async def get_by_id(
    db: AsyncSession,
    comment_id: int,
) -> dict:
    """
    Pobiera szczegóły komentarza.

    Args:
        db:         Sesja SQLAlchemy.
        comment_id: ID komentarza.

    Returns:
        Słownik z danymi komentarza.

    Raises:
        CommentNotFoundError: Komentarz nie istnieje lub jest nieaktywny.
    """
    result = await db.execute(
        select(Comment).where(
            and_(Comment.id_comment == comment_id, Comment.is_active == True)  # noqa: E712
        )
    )
    comment = result.scalar_one_or_none()
    if comment is None:
        raise CommentNotFoundError(f"Komentarz ID={comment_id} nie istnieje.")
    return _comment_to_dict(comment)


# ===========================================================================
# Publiczne API serwisu — WRITE
# ===========================================================================

async def create(
    db: AsyncSession,
    data: CommentCreateData,
    author_user_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Tworzy nowy komentarz do dłużnika.

    Args:
        db:            Sesja SQLAlchemy.
        data:          Zwalidowane dane komentarza.
        author_user_id: ID użytkownika tworzącego komentarz.
        ip_address:    IP inicjatora.

    Returns:
        Słownik z danymi nowego komentarza.
    """
    new_comment = Comment(
        id_kontrahenta=data.debtor_id,
        tresc=data.tresc,
        uzytkownik_id=author_user_id,
        is_active=True,
    )
    db.add(new_comment)
    await db.flush()

    comment_id = new_comment.id_comment

    logger.info(
        "Komentarz utworzony",
        extra={
            "comment_id": comment_id,
            "debtor_id": data.debtor_id,
            "author_id": author_user_id,
            "ip_address": ip_address,
            "tresc_length": len(data.tresc),
        }
    )

    _append_to_file(
        _get_comments_log_file(),
        _build_log_record(
            action="comment_created",
            comment_id=comment_id,
            debtor_id=data.debtor_id,
            author_id=author_user_id,
            ip_address=ip_address,
            tresc_length=len(data.tresc),
        )
    )

    await db.commit()
    audit_service.log_crud(
        db=db,
        action="comment_created",
        entity_type="Comment",
        entity_id=comment_id,
        new_value={
            "id_kontrahenta": data.debtor_id,
            "tresc_length": len(data.tresc),
            "uzytkownik_id": author_user_id,
        },
        success=True,
    )
    return _comment_to_dict(new_comment)


async def update(
    db: AsyncSession,
    comment_id: int,
    data: CommentUpdateData,
    requesting_user_id: int,
    has_edit_any: bool = False,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Aktualizuje treść komentarza.

    Kontrola dostępu:
        - Właściciel komentarza (uzytkownik_id == requesting_user_id) → zawsze może edytować
        - Inny user → wymagane has_edit_any=True (uprawnienie comments.edit_any)

    Args:
        db:                  Sesja SQLAlchemy.
        comment_id:          ID komentarza do aktualizacji.
        data:                Nowe dane komentarza.
        requesting_user_id:  ID użytkownika wykonującego operację.
        has_edit_any:        Czy user ma uprawnienie comments.edit_any.
        ip_address:          IP inicjatora.

    Returns:
        Słownik z zaktualizowanymi danymi komentarza.

    Raises:
        CommentNotFoundError:    Komentarz nie istnieje.
        CommentPermissionError:  Brak uprawnień do edycji cudzego komentarza.
    """
    result = await db.execute(
        select(Comment).where(
            and_(Comment.id_comment == comment_id, Comment.is_active == True)  # noqa: E712
        )
    )
    comment = result.scalar_one_or_none()
    if comment is None:
        raise CommentNotFoundError(f"Komentarz ID={comment_id} nie istnieje.")

    # Kontrola własności
    is_owner = (comment.uzytkownik_id == requesting_user_id)
    if not is_owner and not has_edit_any:
        raise CommentPermissionError(
            f"Brak uprawnień do edycji komentarza ID={comment_id}. "
            f"Wymagane uprawnienie: comments.edit_any"
        )

    old_value = _comment_to_dict(comment)

    comment.tresc = data.tresc
    comment.updated_at = datetime.now(timezone.utc)
    await db.flush()
    await db.commit()
    new_value = _comment_to_dict(comment)

    logger.info(
        "Komentarz zaktualizowany",
        extra={
            "comment_id": comment_id,
            "debtor_id": comment.id_kontrahenta,
            "updated_by": requesting_user_id,
            "is_owner": is_owner,
            "has_edit_any": has_edit_any,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_comments_log_file(),
        _build_log_record(
            action="comment_updated",
            comment_id=comment_id,
            debtor_id=comment.id_kontrahenta,
            updated_by=requesting_user_id,
            is_owner=is_owner,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="comment_updated",
        entity_type="Comment",
        entity_id=comment_id,
        old_value={"tresc_length": len(old_value["tresc"] or "")},
        new_value={"tresc_length": len(data.tresc)},
        details={"is_owner": is_owner, "has_edit_any": has_edit_any},
        success=True,
    )

    return new_value


# ===========================================================================
# Dwuetapowe usuwanie
# ===========================================================================

async def initiate_delete(
    db: AsyncSession,
    redis: Redis,
    comment_id: int,
    requesting_user_id: int,
    has_delete_any: bool = False,
    ip_address: Optional[str] = None,
) -> DeleteConfirmData:
    """
    Inicjuje dwuetapowe usunięcie komentarza — Krok 1.

    Kontrola dostępu:
        - Właściciel → może usunąć własny komentarz
        - Inny user z has_delete_any=True → może usunąć dowolny

    Args:
        db:                  Sesja SQLAlchemy.
        redis:               Klient Redis.
        comment_id:          ID komentarza do usunięcia.
        requesting_user_id:  ID użytkownika inicjującego delete.
        has_delete_any:      Czy user ma uprawnienie comments.delete_any.
        ip_address:          IP inicjatora.

    Returns:
        DeleteConfirmData z tokenem i podglądem treści.

    Raises:
        CommentNotFoundError:    Komentarz nie istnieje.
        CommentPermissionError:  Brak uprawnień do usunięcia cudzego komentarza.
    """
    result = await db.execute(
        select(Comment).where(
            and_(Comment.id_comment == comment_id, Comment.is_active == True)  # noqa: E712
        )
    )
    comment = result.scalar_one_or_none()
    if comment is None:
        raise CommentNotFoundError(f"Komentarz ID={comment_id} nie istnieje.")

    # Kontrola własności
    is_owner = (comment.uzytkownik_id == requesting_user_id)
    if not is_owner and not has_delete_any:
        raise CommentPermissionError(
            f"Brak uprawnień do usunięcia komentarza ID={comment_id}. "
            f"Wymagane uprawnienie: comments.delete_any"
        )

    # TTL tokenu z konfiguracji
    ttl_seconds = await config_service.get_int(
        db, redis,
        key="delete_token.ttl_seconds",
        default=_DEFAULT_DELETE_TOKEN_TTL,
    )

    now = datetime.now(timezone.utc)
    jti = secrets.token_hex(16)
    expires_at = now + timedelta(seconds=ttl_seconds)

    token_payload = {
        "sub":           str(comment_id),
        "type":          "delete_confirm",
        "action":        "delete_comment",
        "entity_type":   "Comment",
        "initiated_by":  requesting_user_id,
        "debtor_id":     comment.id_kontrahenta,
        "jti":           jti,
        "iat":           int(now.timestamp()),
        "exp":           int(expires_at.timestamp()),
    }

    delete_token = jwt.encode(
        token_payload,
        settings.secret_key.get_secret_value(),
        algorithm=settings.algorithm,
    )

    # Zapis JTI do Redis (jednorazowość)
    await redis.set(
        _REDIS_KEY_DELETE.format(jti=jti),
        str(comment_id),
        ex=ttl_seconds,
    )

    tresc_preview = (comment.tresc or "")[:100]
    if len(comment.tresc or "") > 100:
        tresc_preview += "..."

    logger.info(
        "Zainicjowano usunięcie komentarza — krok 1",
        extra={
            "comment_id": comment_id,
            "debtor_id": comment.id_kontrahenta,
            "initiated_by": requesting_user_id,
            "is_owner": is_owner,
            "ttl_seconds": ttl_seconds,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_comments_log_file(),
        _build_log_record(
            action="comment_delete_initiated",
            comment_id=comment_id,
            debtor_id=comment.id_kontrahenta,
            initiated_by=requesting_user_id,
            is_owner=is_owner,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="comment_delete_initiated",
        entity_type="Comment",
        entity_id=comment_id,
        details={
            "debtor_id": comment.id_kontrahenta,
            "initiated_by": requesting_user_id,
            "is_owner": is_owner,
        },
        success=True,
    )

    return DeleteConfirmData(
        token=delete_token,
        expires_in=ttl_seconds,
        comment_id=comment_id,
        debtor_id=comment.id_kontrahenta,
        tresc_preview=tresc_preview,
    )


async def confirm_delete(
    db: AsyncSession,
    redis: Redis,
    comment_id: int,
    confirm_token: str,
    requesting_user_id: int,
    ip_address: Optional[str] = None,
) -> dict:
    """
    Potwierdza i wykonuje soft-delete komentarza — Krok 2.

    Archiwizuje komentarz przez archive_service przed usunięciem.

    Args:
        db:                  Sesja SQLAlchemy.
        redis:               Klient Redis.
        comment_id:          ID komentarza do usunięcia.
        confirm_token:       Token JWT z initiate_delete().
        requesting_user_id:  ID użytkownika potwierdzającego.
        ip_address:          IP inicjatora.

    Returns:
        Słownik z potwierdzeniem.

    Raises:
        CommentDeleteTokenError: Nieprawidłowy/wygasły/użyty token.
        CommentNotFoundError:    Komentarz nie istnieje.
    """
    # Weryfikacja tokenu
    try:
        payload = jwt.decode(
            confirm_token,
            settings.secret_key.get_secret_value(),
            algorithms=[settings.algorithm],
        )
    except JWTError as exc:
        raise CommentDeleteTokenError(
            f"Token potwierdzający jest nieprawidłowy lub wygasł: {exc}"
        )

    if payload.get("type") != "delete_confirm" or payload.get("action") != "delete_comment":
        raise CommentDeleteTokenError("Token nie jest tokenem potwierdzającym usunięcia komentarza.")

    token_comment_id = payload.get("sub")
    if token_comment_id is None or int(token_comment_id) != comment_id:
        raise CommentDeleteTokenError("Token dotyczy innego komentarza.")

    token_by = payload.get("initiated_by")
    if token_by is None or int(token_by) != requesting_user_id:
        raise CommentDeleteTokenError("Token był wygenerowany przez innego użytkownika.")

    # Sprawdź jednorazowość w Redis
    jti = payload.get("jti")
    redis_key = _REDIS_KEY_DELETE.format(jti=jti)
    stored = await redis.get(redis_key)
    if stored is None:
        raise CommentDeleteTokenError("Token wygasł lub został już użyty.")
    await redis.delete(redis_key)

    # Pobierz komentarz
    result = await db.execute(
        select(Comment).where(
            and_(Comment.id_comment == comment_id, Comment.is_active == True)  # noqa: E712
        )
    )
    comment = result.scalar_one_or_none()
    if comment is None:
        raise CommentNotFoundError(f"Komentarz ID={comment_id} nie istnieje.")

    # Archiwizacja
    archive_path = archive_service.archive(comment, archive_type="soft_delete")

    old_value = _comment_to_dict(comment)

    # Soft-delete
    comment.is_active = False
    comment.updated_at = datetime.now(timezone.utc)
    await db.flush()
    await db.commit()
    logger.warning(
        "Komentarz usunięty (soft-delete)",
        extra={
            "comment_id": comment_id,
            "debtor_id": comment.id_kontrahenta,
            "deleted_by": requesting_user_id,
            "archive_path": str(archive_path) if archive_path else None,
            "ip_address": ip_address,
        }
    )

    _append_to_file(
        _get_comments_log_file(),
        _build_log_record(
            action="comment_deleted",
            comment_id=comment_id,
            debtor_id=comment.id_kontrahenta,
            deleted_by=requesting_user_id,
            archive_path=str(archive_path) if archive_path else None,
            ip_address=ip_address,
        )
    )

    audit_service.log_crud(
        db=db,
        action="comment_deleted",
        entity_type="Comment",
        entity_id=comment_id,
        old_value={
            "id_kontrahenta": comment.id_kontrahenta,
            "uzytkownik_id": comment.uzytkownik_id,
            "tresc_length": len(old_value["tresc"] or ""),
        },
        new_value={"is_active": False},
        details={
            "deleted_by": requesting_user_id,
            "archive_path": str(archive_path) if archive_path else None,
        },
        success=True,
    )

    return {
        "message":       f"Komentarz ID={comment_id} został trwale dezaktywowany.",
        "comment_id":    comment_id,
        "debtor_id":     comment.id_kontrahenta,
        "deleted_at":    datetime.now(timezone.utc).isoformat(),
        "archive_path":  str(archive_path) if archive_path else None,
    }


# ===========================================================================
# Funkcje diagnostyczne
# ===========================================================================

async def count_comments_for_user(
    db: AsyncSession,
    user_id: int,
) -> int:
    """
    Liczy aktywne komentarze napisane przez użytkownika.

    Używane przed soft-delete usera — informacyjnie (nie blokuje, bo FK RESTRICT).

    Args:
        db:      Sesja SQLAlchemy.
        user_id: ID użytkownika.

    Returns:
        Liczba aktywnych komentarzy tego użytkownika.
    """
    result = await db.execute(
        select(func.count(Comment.id_comment)).where(
            and_(
                Comment.uzytkownik_id == user_id,
                Comment.is_active == True,  # noqa: E712
            )
        )
    )
    return result.scalar_one() or 0


async def count_comments_for_debtor(
    db: AsyncSession,
    debtor_id: int,
) -> int:
    """
    Liczy aktywne komentarze dla dłużnika.

    Args:
        db:       Sesja SQLAlchemy.
        debtor_id: ID kontrahenta WAPRO.

    Returns:
        Liczba komentarzy.
    """
    result = await db.execute(
        select(func.count(Comment.id_comment)).where(
            and_(
                Comment.id_kontrahenta == debtor_id,
                Comment.is_active == True,  # noqa: E712
            )
        )
    )
    return result.scalar_one() or 0