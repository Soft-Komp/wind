# backend/app/db/models/approval/__init__.py
"""
Modele ORM modulu Obiegu Dokumentow i Akceptacji.

Kolejnosc importow wymuszona przez FK:
  1. Slowniki (bez FK miedzy soba)
  2. Grupy i sciezki
  3. Instancje (FK do slownikow i sciezek)
  4. Tabele zalezne od instancji
"""
from __future__ import annotations

# ── 1. Slowniki ───────────────────────────────────────────────────────────────
from app.db.models.approval.document_source import DocumentSource
from app.db.models.approval.document_category import DocumentCategory
# ── 1b. Etap 2 — hooki, akcje i logi zrodlowe (FK do DocumentSource) ─────────
from app.db.models.approval.source_hook import SourceHook
from app.db.models.approval.source_action import SourceAction
from app.db.models.approval.source_action_log import SourceActionLog

# ── 2. Grupy i sciezki ────────────────────────────────────────────────────────
from app.db.models.approval.approval_group import ApprovalGroup
from app.db.models.approval.approval_group_member import ApprovalGroupMember
from app.db.models.approval.approval_path import ApprovalPath
from app.db.models.approval.approval_path_step import ApprovalPathStep
from app.db.models.approval.approval_path_change_log import ApprovalPathChangeLog
from app.db.models.approval.approval_filter import ApprovalFilter
from app.db.models.approval.approval_filter_condition import ApprovalFilterCondition
from app.db.models.approval.document_source_field_mapping import DocumentSourceFieldMapping

# ── 3. Instancja obiegu ───────────────────────────────────────────────────────
from app.db.models.approval.document_approval_instance import DocumentApprovalInstance
from app.db.models.approval.document_approval_snapshot_step import (
    DocumentApprovalSnapshotStep,
)

# ── 4. Tabele zalezne od instancji ────────────────────────────────────────────
from app.db.models.approval.approval_delegation import ApprovalDelegation
from app.db.models.approval.approval_comment import ApprovalComment
from app.db.models.approval.approval_attachment import ApprovalAttachment
from app.db.models.approval.user_notification import UserNotification

__all__ = [
    "DocumentSource",
    "DocumentCategory",
    "ApprovalGroup",
    "ApprovalGroupMember",
    "ApprovalPath",
    "ApprovalPathStep",
    "ApprovalPathChangeLog",
    "ApprovalFilter",
    "ApprovalFilterCondition",
    "DocumentSourceFieldMapping",
    "DocumentApprovalInstance",
    "DocumentApprovalSnapshotStep",
    "ApprovalDelegation",
    "ApprovalComment",
    "ApprovalAttachment",
    "UserNotification",
    "SourceHook",
    "SourceAction",
    "SourceActionLog",
]