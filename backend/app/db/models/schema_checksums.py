"""
Model tabeli dbo_ext.SchemaChecksums.
Przechowuje checksummy widoków i procedur składowanych zarządzanych przez Alembic.
Weryfikacja przy każdym starcie aplikacji — BLOCK jeśli niezgodność.
Patrz: app/core/schema_integrity.py
"""

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

OBJECT_TYPES = frozenset({"VIEW", "PROCEDURE"})


class SchemaChecksum(Base):
    __tablename__ = "SchemaChecksums"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": (
            "Checksummy widoków i SP. "
            "Niezgodność przy starcie = BLOCK (SystemExit). "
            "Patrz: schema_integrity.py"
        ),
    }

    id_checksum: Mapped[int] = mapped_column(
        "ID_CHECKSUM", Integer, primary_key=True, autoincrement=True,
    )
    object_name: Mapped[str] = mapped_column(
        "ObjectName", String(200), nullable=False,
        comment="Nazwa widoku lub procedury składowanej",
    )
    object_type: Mapped[str] = mapped_column(
        "ObjectType", String(50), nullable=False,
        comment=f"Typ: {', '.join(sorted(OBJECT_TYPES))}",
    )
    checksum: Mapped[int] = mapped_column(
        "Checksum", Integer, nullable=False,
        comment="CHECKSUM(definition) z sys.sql_modules",
    )
    alembic_revision: Mapped[str | None] = mapped_column(
        "AlembicRevision", String(50), nullable=True,
        comment="Wersja migracji Alembic która stworzyła/zmieniła obiekt",
    )
    last_verified_at: Mapped[datetime | None] = mapped_column(
        "LastVerifiedAt", DateTime, nullable=True,
        comment="Kiedy ostatnio checksum był weryfikowany przy starcie",
    )
    created_at: Mapped[datetime] = mapped_column(
        "CreatedAt", DateTime, nullable=False,
        default=datetime.utcnow, server_default=text("GETDATE()"),
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        "UpdatedAt", DateTime, nullable=True, onupdate=datetime.utcnow,
    )

    def __repr__(self) -> str:
        return (
            f"<SchemaChecksum "
            f"name={self.object_name!r} "
            f"type={self.object_type!r} "
            f"checksum={self.checksum}>"
        )