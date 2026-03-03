"""
Model tabeli dbo_ext.RolePermissions.
Tabela pośrednia Many-to-Many: Role ↔ Permission.
Brak soft-delete — przypisania są albo aktywne albo nie istnieją.
Brak UpdatedAt — operacja jest zawsze delete + insert (CRUD).
"""

from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Integer, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, _utcnow 


class RolePermission(Base):
    __tablename__ = "skw_RolePermissions"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": "Przypisanie uprawnień do ról (Many-to-Many)",
    }

    id_role: Mapped[int] = mapped_column(
        "ID_ROLE",
        Integer,
        ForeignKey("dbo_ext.skw_Roles.ID_ROLE", ondelete="CASCADE"),
        primary_key=True,
        comment="FK → Roles",
    )
    id_permission: Mapped[int] = mapped_column(
        "ID_PERMISSION",
        Integer,
        ForeignKey("dbo_ext.skw_Permissions.ID_PERMISSION", ondelete="CASCADE"),
        primary_key=True,
        comment="FK → Permissions",
    )
    created_at: Mapped[datetime] = mapped_column(
        "CreatedAt",
        DateTime,
        nullable=False,
        default=_utcnow,           # ← było: datetime.utcnow
        server_default=text("GETDATE()"),
        comment="Kiedy przypisano uprawnienie do roli",
    )

    # Relacje
    role: Mapped["Role"] = relationship(  # type: ignore[name-defined]
        "Role", back_populates="role_permissions"
    )
    permission: Mapped["Permission"] = relationship(  # type: ignore[name-defined]
        "Permission", back_populates="role_permissions"
    )

    def __repr__(self) -> str:
        return f"<RolePermission role={self.id_role} permission={self.id_permission}>"