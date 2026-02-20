"""MasterAccessLog — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class MasterAccessLogCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    target_user_id: int | None | None = Field(default=None)
    target_username: str = Field(...)
    ip_address: str = Field(...)
    user_agent: str | None | None = Field(default=None)
    accessed_at: datetime = Field(...)
    session_ended_at: datetime | None | None = Field(default=None)
    notes: str | None | None = Field(default=None)



class MasterAccessLogUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    target_user_id: int | None | None = Field(default=None)
    target_username: str | None = Field(default=None)
    ip_address: str | None = Field(default=None)
    user_agent: str | None | None = Field(default=None)
    accessed_at: datetime | None = Field(default=None)
    session_ended_at: datetime | None | None = Field(default=None)
    notes: str | None | None = Field(default=None)



class MasterAccessLogRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_log: int = Field(...)
    target_user_id: int | None | None = Field(default=None)
    target_username: str = Field(...)
    ip_address: str = Field(...)
    user_agent: str | None | None = Field(default=None)
    accessed_at: datetime = Field(...)
    session_ended_at: datetime | None | None = Field(default=None)
    notes: str | None | None = Field(default=None)



class MasterAccessLogListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_log: int = Field(...)
    target_user_id: int | None | None = Field(default=None)
    target_username: str = Field(...)



class MasterAccessLogDetail(MasterAccessLogRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



MasterAccessLogResponse = BaseResponse[MasterAccessLogRead]

MasterAccessLogDetailResponse = BaseResponse[MasterAccessLogDetail]

MasterAccessLogListResponse = PaginatedResponse[MasterAccessLogListItem]



class MasterAccessLogListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
