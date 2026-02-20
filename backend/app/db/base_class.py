from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """
    Wspólna baza dla wszystkich modeli ORM.

    Alembic używa Base.metadata do autogenerowania migracji.
    """
    pass
