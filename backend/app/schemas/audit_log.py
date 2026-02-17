"""AuditLog — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class AuditLogCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_user: int | None | None = Field(default=None)
    username: str | None | None = Field(default=None)
    action: str = Field(...)
    action_category: str | None | None = Field(default=None)
    entity_type: str | None | None = Field(default=None)
    entity_id: int | None | None = Field(default=None)
    old_value: str | None | None = Field(default=None)
    new_value: str | None | None = Field(default=None)
    details: str | None | None = Field(default=None)
    ip_address: str | None | None = Field(default=None)
    user_agent: str | None | None = Field(default=None)
    request_url: str | None | None = Field(default=None)
    request_method: str | None | None = Field(default=None)
    timestamp: datetime = Field(...)
    success: bool = Field(...)
    error_message: str | None | None = Field(default=None)



class AuditLogUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_user: int | None | None = Field(default=None)
    username: str | None | None = Field(default=None)
    action: str | None = Field(default=None)
    action_category: str | None | None = Field(default=None)
    entity_type: str | None | None = Field(default=None)
    entity_id: int | None | None = Field(default=None)
    old_value: str | None | None = Field(default=None)
    new_value: str | None | None = Field(default=None)
    details: str | None | None = Field(default=None)
    ip_address: str | None | None = Field(default=None)
    user_agent: str | None | None = Field(default=None)
    request_url: str | None | None = Field(default=None)
    request_method: str | None | None = Field(default=None)
    timestamp: datetime | None = Field(default=None)
    success: bool | None = Field(default=None)
    error_message: str | None | None = Field(default=None)



class AuditLogRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_log: int = Field(...)
    id_user: int | None | None = Field(default=None)
    username: str | None | None = Field(default=None)
    action: str = Field(...)
    action_category: str | None | None = Field(default=None)
    entity_type: str | None | None = Field(default=None)
    entity_id: int | None | None = Field(default=None)
    old_value: str | None | None = Field(default=None)
    new_value: str | None | None = Field(default=None)
    details: str | None | None = Field(default=None)
    ip_address: str | None | None = Field(default=None)
    user_agent: str | None | None = Field(default=None)
    request_url: str | None | None = Field(default=None)
    request_method: str | None | None = Field(default=None)
    timestamp: datetime = Field(...)
    success: bool = Field(...)
    error_message: str | None | None = Field(default=None)



class AuditLogListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_log: int = Field(...)
    id_user: int | None | None = Field(default=None)
    username: str | None | None = Field(default=None)



class AuditLogDetail(AuditLogRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



AuditLogResponse = BaseResponse[AuditLogRead]

AuditLogDetailResponse = BaseResponse[AuditLogDetail]

AuditLogListResponse = PaginatedResponse[AuditLogListItem]



class AuditLogListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
