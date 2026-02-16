"""
Rejestr wszystkich modeli dbo_ext.
Import tego modułu zapewnia, że Alembic widzi wszystkie tabele.

Kolejność importów ma znaczenie — FK constraints muszą być spełnione:
  1. Role, Permission         (brak zależności)
  2. User                     (zależy od Role)
  3. RolePermission           (zależy od Role + Permission)
  4. RefreshToken, OtpCode    (zależy od User)
  5. Template                 (brak zależności)
  6. Comment, MonitHistory    (zależy od User + Template)
  7. AuditLog                 (zależy od User)
  8. SystemConfig, SchemaChecksum (brak FK do powyższych)
  9. MasterAccessLog          (zależy od User)
"""

from .base import AuditMixin, Base, SoftDeleteMixin, TimestampMixin
from .role import Role
from .permission import Permission
from .role_permission import RolePermission
from .user import User
from .refresh_token import RefreshToken
from .otp_code import OtpCode
from .template import Template
from .comment import Comment
from .monit_history import MonitHistory
from .audit_log import AuditLog
from .system_config import SystemConfig
from .schema_checksums import SchemaChecksum
from .master_access_log import MasterAccessLog

__all__ = [
    "Base",
    "AuditMixin",
    "TimestampMixin",
    "SoftDeleteMixin",
    # Tabele
    "Role",
    "Permission",
    "RolePermission",
    "User",
    "RefreshToken",
    "OtpCode",
    "Template",
    "Comment",
    "MonitHistory",
    "AuditLog",
    "SystemConfig",
    "SchemaChecksum",
    "MasterAccessLog",
]