# backend/app/db/models/koszt_dodatkowy.py
"""
Model ORM — dbo_ext.skw_KosztyDodatkowe.

Koszty dodatkowe doliczane do monitów (stała kwota per typ monitu).
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, Boolean, DateTime, Identity, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class KosztDodatkowy(Base):
    __tablename__ = "skw_KosztyDodatkowe"
    __table_args__ = {"schema": "dbo_ext"}

    id_kosztu: Mapped[int] = mapped_column(
        "ID_KOSZTU", BigInteger, Identity(start=1, increment=1), primary_key=True
    )
    nazwa: Mapped[str] = mapped_column(
        "Nazwa", String(200), nullable=False
    )
    kwota: Mapped[Decimal] = mapped_column(
        "Kwota", Numeric(15, 2), nullable=False
    )
    typ_monitu: Mapped[str] = mapped_column(
        "TypMonitu", String(20), nullable=False
    )
    opis: Mapped[str | None] = mapped_column(
        "Opis", String(500), nullable=True
    )
    is_active: Mapped[bool] = mapped_column(
        "IsActive", Boolean, nullable=False, default=True
    )
    created_at: Mapped[datetime] = mapped_column(
        "CreatedAt", DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        "UpdatedAt", DateTime, nullable=True
    )