"""
Model tabeli dbo_ext.Roles.
Predefiniowane role: Admin, Manager, User, ReadOnly.
"""

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import AuditMixin, Base


class Role(AuditMixin, Base):
    __tablename__ = "Roles"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": "Role użytkowników systemu. Zarządzanie przez RBAC.",
    }

    id_role: Mapped[int] = mapped_column(
        "ID_ROLE", Integer, primary_key=True, autoincrement=True,
        comment="Klucz główny",
    )
    role_name: Mapped[str] = mapped_column(
        "RoleName", String(50), nullable=False, unique=True,
        comment="Unikalna nazwa roli (Admin/Manager/User/ReadOnly)",
    )
    description: Mapped[str | None] = mapped_column(
        "Description", String(200), nullable=True,
        comment="Opis roli i jej zastosowania",
    )

    # Relacje
    users: Mapped[list["User"]] = relationship(  # type: ignore[name-defined]
        "User", back_populates="role", lazy="selectin"
    )
    role_permissions: Mapped[list["RolePermission"]] = relationship(  # type: ignore[name-defined]
        "RolePermission", back_populates="role", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Role id={self.id_role} name={self.role_name!r}>"