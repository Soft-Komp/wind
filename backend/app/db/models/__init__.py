# =============================================================================
# Cel:
#   Centralny punkt importu wszystkich modeli SQLAlchemy.
#   Alembic autogenerate MUSI widzieć każdy model przed generowaniem migracji —
#   jeśli model nie jest tu zaimportowany, Alembic go pominie (cicha utrata tabeli).
#
# Zasada kolejności importów:
#   1. Base (metadane) — zawsze pierwszy
#   2. Modele bez FK (Role, Permission, SystemConfig, SchemaChecksums)
#   3. Modele z FK do Users (RefreshToken, OtpCode, AuditLog, MasterAccessLog)
#   4. Modele z FK do wielu tabel (Comment, MonitHistory, RolePermission)
#
# KRYTYCZNE DLA ALEMBIC:
#   - env.py musi importować ten moduł przed wywołaniem autogenerate
#   - Każdy nowy model MUSI być dodany do tej listy
#   - Kolejność importów ≠ kolejność tworzenia tabel (FK rozwiązuje Alembic)
#     ale poprawna kolejność eliminuje problemy z circular imports
#
# =============================================================================

from __future__ import annotations

# ── 1. Baza (metadane SQLAlchemy + AuditMixin) ────────────────────────────────
# MUSI być pierwszy — wszystkie modele dziedziczą po Base
from app.db.models.base import Base, AuditMixin  # noqa: F401

# ── 2. Modele bez zależności (lub tylko od siebie) ────────────────────────────
from app.db.models.role import Role  # noqa: F401
from app.db.models.permission import Permission  # noqa: F401
from app.db.models.system_config import SystemConfig  # noqa: F401
from app.db.models.schema_checksums import SchemaChecksums  # noqa: F401
from app.db.models.template import Template  # noqa: F401

# ── 3. User — centralny model (FK do Role) ────────────────────────────────────
from app.db.models.user import User  # noqa: F401

# ── 4. Modele zależne od User ─────────────────────────────────────────────────
from app.db.models.refresh_token import RefreshToken  # noqa: F401
from app.db.models.otp_code import OtpCode  # noqa: F401
from app.db.models.audit_log import AuditLog  # noqa: F401
from app.db.models.master_access_log import MasterAccessLog  # noqa: F401

# ── 5. Modele z FK do wielu tabel ─────────────────────────────────────────────
from app.db.models.role_permission import RolePermission  # noqa: F401
from app.db.models.comment import Comment  # noqa: F401
from app.db.models.monit_history import MonitHistory  # noqa: F401
from app.db.models.monit_history_invoices import MonitHistoryInvoice    # noqa: F401

from app.db.models.approval.document_source import DocumentSource  # noqa: F401
from app.db.models.approval.document_category import DocumentCategory  # noqa: F401
from app.db.models.approval.approval_group import ApprovalGroup  # noqa: F401
from app.db.models.approval.approval_group_member import ApprovalGroupMember  # noqa: F401
from app.db.models.approval.approval_path import ApprovalPath  # noqa: F401
from app.db.models.approval.approval_path_step import ApprovalPathStep  # noqa: F401
from app.db.models.approval.approval_path_change_log import ApprovalPathChangeLog  # noqa: F401
from app.db.models.approval.approval_filter import ApprovalFilter  # noqa: F401
from app.db.models.approval.approval_filter_condition import ApprovalFilterCondition  # noqa: F401
from app.db.models.approval.document_source_field_mapping import DocumentSourceFieldMapping  # noqa: F401
from app.db.models.approval.document_approval_instance import DocumentApprovalInstance  # noqa: F401
from app.db.models.approval.document_approval_snapshot_step import DocumentApprovalSnapshotStep  # noqa: F401
from app.db.models.approval.approval_delegation import ApprovalDelegation  # noqa: F401
from app.db.models.approval.approval_comment import ApprovalComment  # noqa: F401
from app.db.models.approval.approval_attachment import ApprovalAttachment  # noqa: F401
from app.db.models.approval.user_notification import UserNotification  # noqa: F401
# =============================================================================
# __all__ — eksplicytna lista publicznego API modułu
# Chroni przed przypadkowym importem przez `from app.db.models import *`
# =============================================================================
__all__: list[str] = [
    # Base
    "Base",
    "AuditMixin",
    # Bez FK
    "Role",
    "Permission",
    "SystemConfig",
    "SchemaChecksums",
    "Template",
    # Central
    "User",
    # FK → User
    "RefreshToken",
    "OtpCode",
    "AuditLog",
    "MasterAccessLog",
    # FK → wiele tabel
    "RolePermission",
    "Comment",
    "MonitHistory",
]
