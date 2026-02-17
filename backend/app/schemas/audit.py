"""
Schematy Pydantic v2 — Audyt / logi.

Standard: CRUD + ListItem/Detail + ListQuery (page/limit, domyślny limit=12).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime
from decimal import Decimal

from app.schemas.base import BaseResponse, PaginatedResponse


# ---------------------------------------------------------------------------
# AuditLog
# ---------------------------------------------------------------------------

class AuditLogBase(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_user: int | None = Field(None)
    username: str | None = Field(None, max_length=50)
    action: str = Field(..., max_length=100)
    action_category: str | None = Field(None, max_length=50)
    entity_type: str | None = Field(None, max_length=50)
    entity_id: int | None = Field(None)
    old_value: str | None = Field(None)
    new_value: str | None = Field(None)
    details: str | None = Field(None)
    ip_address: str | None = Field(None, max_length=45)
    user_agent: str | None = Field(None, max_length=500)
    request_url: str | None = Field(None, max_length=500)
    request_method: str | None = Field(None, max_length=10)
    timestamp: datetime = Field(...)
    success: bool = Field(...)
    error_message: str | None = Field(None, max_length=500)


class {Entity}Create({Entity}Base):
    pass


class AuditLogUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_user: int | None = None
    username: str | None = Field(None, max_length=50)
    action: str | None = Field(None, max_length=100)
    action_category: str | None = Field(None, max_length=50)
    entity_type: str | None = Field(None, max_length=50)
    entity_id: int | None = None
    old_value: str | None = None
    new_value: str | None = None
    details: str | None = None
    ip_address: str | None = Field(None, max_length=45)
    user_agent: str | None = Field(None, max_length=500)
    request_url: str | None = Field(None, max_length=500)
    request_method: str | None = Field(None, max_length=10)
    timestamp: datetime | None = None
    success: bool | None = None
    error_message: str | None = Field(None, max_length=500)


class AuditLogRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_log: int | None = Field(None)
    id_user: int | None = Field(None)
    username: str | None = Field(None, max_length=50)
    action: str = Field(..., max_length=100)
    action_category: str | None = Field(None, max_length=50)
    entity_type: str | None = Field(None, max_length=50)
    entity_id: int | None = Field(None)
    old_value: str | None = Field(None)
    new_value: str | None = Field(None)
    details: str | None = Field(None)
    ip_address: str | None = Field(None, max_length=45)
    user_agent: str | None = Field(None, max_length=500)
    request_url: str | None = Field(None, max_length=500)
    request_method: str | None = Field(None, max_length=10)
    timestamp: datetime = Field(...)
    success: bool = Field(...)
    error_message: str | None = Field(None, max_length=500)


class AuditLogListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_log: int | None = Field(None)
    id_user: int | None = Field(None)
    username: str | None = Field(None, max_length=50)
    action: str = Field(..., max_length=100)
    action_category: str | None = Field(None, max_length=50)


class AuditLogDetail(AuditLogRead):
    pass


class AuditLogListQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    page: int = Field(1, ge=1)
    limit: int = Field(12, ge=1, le=500)
    sort: str | None = None


AuditLogResponse = BaseResponse[AuditLogRead]
AuditLogDetailResponse = BaseResponse[AuditLogDetail]
AuditLogListResponse = PaginatedResponse[AuditLogListItem]


# ---------------------------------------------------------------------------
# Comment
# ---------------------------------------------------------------------------

class CommentBase(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_kontrahenta: int = Field(...)
    tresc: str = Field(...)
    uzytkownik_id: int = Field(...)


class {Entity}Create({Entity}Base):
    pass


class CommentUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_kontrahenta: int | None = None
    tresc: str | None = None
    uzytkownik_id: int | None = None


class CommentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_comment: int | None = Field(None)
    id_kontrahenta: int = Field(...)
    tresc: str = Field(...)
    uzytkownik_id: int = Field(...)


class CommentListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_comment: int | None = Field(None)
    id_kontrahenta: int = Field(...)
    tresc: str = Field(...)
    uzytkownik_id: int = Field(...)


class CommentDetail(CommentRead):
    pass


class CommentListQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    page: int = Field(1, ge=1)
    limit: int = Field(12, ge=1, le=500)
    sort: str | None = None


CommentResponse = BaseResponse[CommentRead]
CommentDetailResponse = BaseResponse[CommentDetail]
CommentListResponse = PaginatedResponse[CommentListItem]


# ---------------------------------------------------------------------------
# MonitHistory
# ---------------------------------------------------------------------------

class MonitHistoryBase(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_kontrahenta: int = Field(...)
    id_user: int | None = Field(None)
    monit_type: str = Field(..., max_length=20)
    template_id: int | None = Field(None)
    status: str = Field(..., max_length=20)
    recipient: str | None = Field(None, max_length=100)
    subject: str | None = Field(None, max_length=200)
    message_body: str | None = Field(None)
    total_debt: Decimal | None = Field(None)
    invoice_numbers: str | None = Field(None, max_length=500)
    pdf_path: str | None = Field(None, max_length=500)
    external_id: str | None = Field(None, max_length=100)
    scheduled_at: datetime | None = Field(None)
    sent_at: datetime | None = Field(None)
    delivered_at: datetime | None = Field(None)
    opened_at: datetime | None = Field(None)
    clicked_at: datetime | None = Field(None)
    error_message: str | None = Field(None, max_length=500)
    retry_count: int = Field(...)
    cost: Decimal | None = Field(None)
    is_active: bool = Field(...)
    created_at: datetime = Field(...)
    updated_at: datetime | None = Field(None)


class {Entity}Create({Entity}Base):
    pass


class MonitHistoryUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_kontrahenta: int | None = None
    id_user: int | None = None
    monit_type: str | None = Field(None, max_length=20)
    template_id: int | None = None
    status: str | None = Field(None, max_length=20)
    recipient: str | None = Field(None, max_length=100)
    subject: str | None = Field(None, max_length=200)
    message_body: str | None = None
    total_debt: Decimal | None = None
    invoice_numbers: str | None = Field(None, max_length=500)
    pdf_path: str | None = Field(None, max_length=500)
    external_id: str | None = Field(None, max_length=100)
    scheduled_at: datetime | None = None
    sent_at: datetime | None = None
    delivered_at: datetime | None = None
    opened_at: datetime | None = None
    clicked_at: datetime | None = None
    error_message: str | None = Field(None, max_length=500)
    retry_count: int | None = None
    cost: Decimal | None = None
    is_active: bool | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class MonitHistoryRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_monit: int | None = Field(None)
    id_kontrahenta: int = Field(...)
    id_user: int | None = Field(None)
    monit_type: str = Field(..., max_length=20)
    template_id: int | None = Field(None)
    status: str = Field(..., max_length=20)
    recipient: str | None = Field(None, max_length=100)
    subject: str | None = Field(None, max_length=200)
    message_body: str | None = Field(None)
    total_debt: Decimal | None = Field(None)
    invoice_numbers: str | None = Field(None, max_length=500)
    pdf_path: str | None = Field(None, max_length=500)
    external_id: str | None = Field(None, max_length=100)
    scheduled_at: datetime | None = Field(None)
    sent_at: datetime | None = Field(None)
    delivered_at: datetime | None = Field(None)
    opened_at: datetime | None = Field(None)
    clicked_at: datetime | None = Field(None)
    error_message: str | None = Field(None, max_length=500)
    retry_count: int = Field(...)
    cost: Decimal | None = Field(None)
    is_active: bool = Field(...)
    created_at: datetime = Field(...)
    updated_at: datetime | None = Field(None)


class MonitHistoryListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_monit: int | None = Field(None)
    id_kontrahenta: int = Field(...)
    id_user: int | None = Field(None)
    monit_type: str = Field(..., max_length=20)
    template_id: int | None = Field(None)


class MonitHistoryDetail(MonitHistoryRead):
    pass


class MonitHistoryListQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    page: int = Field(1, ge=1)
    limit: int = Field(12, ge=1, le=500)
    sort: str | None = None


MonitHistoryResponse = BaseResponse[MonitHistoryRead]
MonitHistoryDetailResponse = BaseResponse[MonitHistoryDetail]
MonitHistoryListResponse = PaginatedResponse[MonitHistoryListItem]


