"""RolePermission — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class RolePermissionCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    created_at: datetime = Field(...)



class RolePermissionUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    created_at: datetime | None = Field(default=None)



class RolePermissionRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_role: int = Field(...)
    id_permission: int = Field(...)
    created_at: datetime = Field(...)



class RolePermissionListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_role: int = Field(...)
    id_permission: int = Field(...)
    created_at: datetime = Field(...)



class RolePermissionDetail(RolePermissionRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



RolePermissionResponse = BaseResponse[RolePermissionRead]

RolePermissionDetailResponse = BaseResponse[RolePermissionDetail]

RolePermissionListResponse = PaginatedResponse[RolePermissionListItem]



class RolePermissionListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
