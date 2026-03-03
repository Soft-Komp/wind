"""
Model tabeli dbo_ext.Permissions.
Granularna kontrola dostępu. Format nazwy: kategoria.akcja
Np. debtors.view_list, monits.send_email_bulk
"""

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import AuditMixin, Base

# Dozwolone kategorie uprawnień — walidacja na poziomie aplikacji
PERMISSION_CATEGORIES = frozenset({
    "auth", "users", "roles", "permissions", "debtors",
    "comments", "monits", "pdf", "reports", "audit",
    "snapshots", "system",
})


class Permission(AuditMixin, Base):
    __tablename__ = "skw_Permissions"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": "Granularne uprawnienia systemu. Format: kategoria.akcja",
    }

    id_permission: Mapped[int] = mapped_column(
        "ID_PERMISSION", Integer, primary_key=True, autoincrement=True,
        comment="Klucz główny",
    )
    permission_name: Mapped[str] = mapped_column(
        "PermissionName", String(100), nullable=False, unique=True,
        comment="Unikalna nazwa uprawnienia w formacie: kategoria.akcja",
    )
    description: Mapped[str | None] = mapped_column(
        "Description", String(200), nullable=True,
        comment="Opis uprawnienia — co pozwala wykonać",
    )
    category: Mapped[str] = mapped_column(
        "Category", String(50), nullable=False,
        comment=f"Kategoria. Dozwolone: {', '.join(sorted(PERMISSION_CATEGORIES))}",
    )

    # Relacje
    role_permissions: Mapped[list["RolePermission"]] = relationship(  # type: ignore[name-defined]
        "RolePermission", back_populates="permission", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Permission id={self.id_permission} name={self.permission_name!r}>"