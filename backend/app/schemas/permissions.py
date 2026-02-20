"""Permission — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class PermissionCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    permission_name: str = Field(...)
    description: str | None | None = Field(default=None)
    category: str = Field(...)



class PermissionUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    permission_name: str | None = Field(default=None)
    description: str | None | None = Field(default=None)
    category: str | None = Field(default=None)



class PermissionRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_permission: int = Field(...)
    permission_name: str = Field(...)
    description: str | None | None = Field(default=None)
    category: str = Field(...)
    created_at: datetime = Field(...)
    updated_at: datetime | None | None = Field(default=None)
    is_active: bool = Field(...)



class PermissionListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_permission: int = Field(...)
    permission_name: str = Field(...)
    description: str | None | None = Field(default=None)



class PermissionDetail(PermissionRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



PermissionResponse = BaseResponse[PermissionRead]

PermissionDetailResponse = BaseResponse[PermissionDetail]

PermissionListResponse = PaginatedResponse[PermissionListItem]



class PermissionListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
