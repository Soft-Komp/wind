"""
Moduł bazowy modeli SQLAlchemy dla schematu dbo_ext.

Architektura:
  - Base           : deklaratywna baza ORM
  - TimestampMixin : CreatedAt + UpdatedAt (ORM + trigger MSSQL — redundantnie)
  - SoftDeleteMixin: IsActive (soft-delete — nigdy nie kasujemy fizycznie)
  - AuditMixin     : połączenie obu (używane w większości tabel)

WAŻNE — async driver:
  MSSQL async wymaga aioodbc, NIE pyodbc.
  pip install aioodbc sqlalchemy[asyncio]
  connection string: mssql+aioodbc:///?odbc_connect=DRIVER={ODBC Driver 18...}
"""

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, text
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped


class Base(DeclarativeBase):
    """Deklaratywna baza ORM. Wszystkie modele dbo_ext dziedziczą po tej klasie."""
    pass


class TimestampMixin:
    """
    Dwa niezależne mechanizmy aktualizacji UpdatedAt (redundancja celowa):
      1. SQLAlchemy onupdate=datetime.utcnow  — działa przy operacjach ORM
      2. Trigger MSSQL w database/triggers/  — działa przy RAW SQL poza ORM
    """

    created_at: Mapped[datetime] = mapped_column(
        "CreatedAt",
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=text("GETDATE()"),
        comment="Data i czas utworzenia rekordu (UTC)",
    )

    updated_at: Mapped[datetime | None] = mapped_column(
        "UpdatedAt",
        DateTime,
        nullable=True,
        onupdate=datetime.utcnow,
        comment="Data i czas ostatniej modyfikacji — ORM i trigger MSSQL",
    )


class SoftDeleteMixin:
    """
    Soft-delete: IsActive = 0 oznacza usunięcie logiczne.
    Przy usunięciu → archive_service.py tworzy zrzut JSON w /app/archives/
    """

    is_active: Mapped[bool] = mapped_column(
        "IsActive",
        Boolean,
        nullable=False,
        default=True,
        server_default=text("1"),
        comment="1 = aktywny, 0 = usunięty (soft-delete)",
    )


class AuditMixin(TimestampMixin, SoftDeleteMixin):
    """Mixin do użycia w tabelach biznesowych. Łączy timestamp + soft-delete."""
    pass