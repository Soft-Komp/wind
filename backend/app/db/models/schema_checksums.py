# =============================================================================
# Model SQLAlchemy dla tabeli dbo_ext.SchemaChecksums
#
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Literal

from sqlalchemy import DateTime, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.models.base import Base

logger = logging.getLogger(__name__)

# Literal type dla ObjectType — kompletna lista dozwolonych wartości
# Zmiana tutaj = zmiana w DDL (CHECK constraint) + schema_integrity.py
ObjectTypeEnum = Literal["VIEW", "PROCEDURE", "INDEX", "FUNCTION"]

# Literal type dla SchemaName — tylko te dwa schematy są monitorowane
SchemaNameEnum = Literal["dbo", "dbo_ext"]


class SchemaChecksums(Base):
    """
    Rejestr sum kontrolnych obiektów bazodanowych zarządzanych przez system.

    Tabela: dbo_ext.SchemaChecksums
    Schemat: dbo_ext

    Cel:
        Ochrona przed nieautoryzowaną modyfikacją widoków i procedur składowanych.
        Przy każdym starcie aplikacji core/schema_integrity.py porównuje:
          - Checksum przechowywany w tej tabeli (kolumna Checksum)
          - Aktualny CHECKSUM(definition) z sys.sql_modules

        Niezgodność → CRITICAL log + AuditLog + SystemExit(1) [tryb BLOCK]

    Monitorowane obiekty:
        - schemat dbo:     widoki WAPRO (skw_kontrahenci, skw_rozrachunki_faktur)
        - schemat dbo_ext: widoki i procedury własne systemu
        - obiekty INDEX:   indeksy wydajnościowe (weryfikowane przez sys.indexes)

    Bezpieczeństwo:
        - Tylko INSERT przez ORM (przy migracji Alembic)
        - UPDATE tylko dla LastVerifiedAt i Checksum (przy aktualizacji checksumu)
        - Brak DELETE — historię zmian zachowuje AuditLog
        - Uprawnienia DB: app_user ma SELECT + UPDATE LastVerifiedAt
    """

    __tablename__ = "skw_SchemaChecksums"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": (
            "Sumy kontrolne widoków i procedur — ochrona integralności schematu. "
            "Niezgodność przy starcie → SystemExit(1)."
        ),
    }

    # ── Klucz główny ──────────────────────────────────────────────────────────
    id_checksum: Mapped[int] = mapped_column(
        "ID_CHECKSUM",
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Klucz główny — IDENTITY(1,1)",
    )

    # ── Identyfikacja obiektu ─────────────────────────────────────────────────
    object_name: Mapped[str] = mapped_column(
        "ObjectName",
        String(200),
        nullable=False,
        comment=(
            "Nazwa obiektu DB: skw_kontrahenci, skw_rozrachunki_faktur, "
            "IX_Roz_Kontrahent_Dlugi itp. "
            "Unikalny razem z SchemaName + ObjectType."
        ),
    )

    object_type: Mapped[str] = mapped_column(
        "ObjectType",
        String(50),
        nullable=False,
        comment=(
            "Typ obiektu: VIEW | PROCEDURE | INDEX. "
            "[R5] Rozszerzono o INDEX vs poprzednia wersja (VIEW/PROCEDURE only). "
            "Sprawdź CHECK constraint w DDL."
        ),
    )

    # ── [R5] NOWA KOLUMNA — SchemaName ────────────────────────────────────────
    # AUDIT_ZGODNOSCI R5: bez tej kolumny nie można odróżnić:
    #   - dbo.skw_kontrahenci   (WAPRO, schemat dbo)
    #   - dbo_ext.VIEW_*         (własne widoki, schemat dbo_ext)
    # core/schema_integrity.py używa tej kolumny w GROUP BY i WHERE
    schema_name: Mapped[str] = mapped_column(
        "SchemaName",
        String(20),
        nullable=False,
        server_default=text("'dbo_ext'"),
        comment=(
            "[NOWA v1.1] Schemat obiektu: 'dbo' lub 'dbo_ext'. "
            "Widoki WAPRO (skw_kontrahenci) → 'dbo'. "
            "Obiekty własne → 'dbo_ext'. "
            "Default 'dbo_ext' dla wstecznej kompatybilności z istniejącymi wierszami."
        ),
    )

    # ── Suma kontrolna ────────────────────────────────────────────────────────
    checksum: Mapped[int] = mapped_column(
        "Checksum",
        Integer,
        nullable=False,
        comment=(
            "CHECKSUM(definition) z sys.sql_modules — obliczany przez MSSQL. "
            "Dla INDEX: CHECKSUM(index_columns_json) — obliczany przez schema_integrity.py. "
            "Wartość NULL oznacza błąd obliczania — traktowana jak niezgodność."
        ),
    )

    # ── Wersja migracji ───────────────────────────────────────────────────────
    alembic_revision: Mapped[str | None] = mapped_column(
        "AlembicRevision",
        String(50),
        nullable=True,
        comment=(
            "Wersja migracji Alembic która stworzyła/zaktualizowała ten obiekt. "
            "NULL = obiekt stworzony przed wdrożeniem systemu checksumów. "
            "Format: 'abc123def456' (12 znaków hex Alembic)."
        ),
    )

    # ── Weryfikacja ───────────────────────────────────────────────────────────
    last_verified_at: Mapped[datetime | None] = mapped_column(
        "LastVerifiedAt",
        DateTime,
        nullable=True,
        comment=(
            "Ostatnia pomyślna weryfikacja przez schema_integrity.verify(). "
            "Aktualizowane przy każdym starcie aplikacji (nawet jeśli checksum OK). "
            "NULL = obiekt nigdy nie był weryfikowany (np. nowo dodany)."
        ),
    )

    # ── Timestamps ────────────────────────────────────────────────────────────
    created_at: Mapped[datetime] = mapped_column(
        "CreatedAt",
        DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        server_default=text("GETDATE()"),
        comment="Data pierwszej rejestracji checksumu — przy migracji Alembic",
    )

    updated_at: Mapped[datetime | None] = mapped_column(
        "UpdatedAt",
        DateTime,
        nullable=True,
        onupdate=lambda: datetime.now(timezone.utc),
        comment=(
            "Data ostatniej aktualizacji checksumu. "
            "Aktualizowane przez Alembic po zmianie widoku/procedury. "
            "Trigger MSSQL (014_triggers_updated_at.sql) jako backup."
        ),
    )

    # =========================================================================
    # METODY POMOCNICZE
    # =========================================================================

    @property
    def full_object_name(self) -> str:
        """
        Pełna kwalifikowana nazwa obiektu: schemat.nazwa
        Przykłady:
            dbo.skw_kontrahenci
            dbo_ext.sp_ArchiveRecord
            dbo.IX_Roz_Kontrahent_Dlugi
        Używana w logach i komunikatach błędów schema_integrity.
        """
        return f"{self.schema_name}.{self.object_name}"

    @property
    def is_verified_recently(self, max_hours: int = 24) -> bool:
        """
        Sprawdza czy weryfikacja była przeprowadzona w ciągu ostatnich N godzin.
        Używana w health endpoint do sygnalizacji potrzeby restartu.
        """
        if self.last_verified_at is None:
            return False
        now = datetime.now(timezone.utc)
        verified = self.last_verified_at
        if verified.tzinfo is None:
            verified = verified.replace(tzinfo=timezone.utc)
        delta = now - verified
        return delta.total_seconds() < (max_hours * 3600)

    def __repr__(self) -> str:
        return (
            f"<SchemaChecksums("
            f"id={self.id_checksum!r}, "
            f"object={self.full_object_name!r}, "
            f"type={self.object_type!r}, "
            f"checksum={self.checksum!r}, "
            f"last_verified={self.last_verified_at!r}"
            f")>"
        )

    def __str__(self) -> str:
        return f"SchemaChecksums({self.full_object_name!r}, type={self.object_type!r})"