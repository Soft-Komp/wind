"""Role — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class RoleCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    role_name: str = Field(...)
    description: str | None | None = Field(default=None)



class RoleUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    role_name: str | None = Field(default=None)
    description: str | None | None = Field(default=None)



class RoleRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_role: int = Field(...)
    role_name: str = Field(...)
    description: str | None | None = Field(default=None)
    created_at: datetime = Field(...)
    updated_at: datetime | None | None = Field(default=None)
    is_active: bool = Field(...)



class RoleListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_role: int = Field(...)
    role_name: str = Field(...)
    description: str | None | None = Field(default=None)



class RoleDetail(RoleRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



RoleResponse = BaseResponse[RoleRead]

RoleDetailResponse = BaseResponse[RoleDetail]

RoleListResponse = PaginatedResponse[RoleListItem]



class RoleListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
