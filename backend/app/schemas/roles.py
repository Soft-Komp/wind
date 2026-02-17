"""
Schematy Pydantic v2 — Role & uprawnienia.

Standard: CRUD + ListItem/Detail + ListQuery (page/limit, domyślny limit=12).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime

from app.schemas.base import BaseResponse, PaginatedResponse


# ---------------------------------------------------------------------------
# Role
# ---------------------------------------------------------------------------

class RoleBase(BaseModel):
    model_config = ConfigDict(extra='forbid')
    role_name: str = Field(..., max_length=50)
    description: str | None = Field(None, max_length=200)


class {Entity}Create({Entity}Base):
    pass


class RoleUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    role_name: str | None = Field(None, max_length=50)
    description: str | None = Field(None, max_length=200)


class RoleRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_role: int | None = Field(None)
    role_name: str = Field(..., max_length=50)
    description: str | None = Field(None, max_length=200)


class RoleListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_role: int | None = Field(None)
    role_name: str = Field(..., max_length=50)
    description: str | None = Field(None, max_length=200)


class RoleDetail(RoleRead):
    pass


class RoleListQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    page: int = Field(1, ge=1)
    limit: int = Field(12, ge=1, le=500)
    sort: str | None = None


RoleResponse = BaseResponse[RoleRead]
RoleDetailResponse = BaseResponse[RoleDetail]
RoleListResponse = PaginatedResponse[RoleListItem]


# ---------------------------------------------------------------------------
# RolePermission
# ---------------------------------------------------------------------------

class RolePermissionBase(BaseModel):
    model_config = ConfigDict(extra='forbid')
    created_at: datetime = Field(...)


class {Entity}Create({Entity}Base):
    pass


class RolePermissionUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    created_at: datetime | None = None


class RolePermissionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_role: int | None = Field(None)
    id_permission: int | None = Field(None)
    created_at: datetime = Field(...)


class RolePermissionListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_role: int | None = Field(None)
    id_permission: int | None = Field(None)
    created_at: datetime = Field(...)


class RolePermissionDetail(RolePermissionRead):
    pass


class RolePermissionListQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    page: int = Field(1, ge=1)
    limit: int = Field(12, ge=1, le=500)
    sort: str | None = None


RolePermissionResponse = BaseResponse[RolePermissionRead]
RolePermissionDetailResponse = BaseResponse[RolePermissionDetail]
RolePermissionListResponse = PaginatedResponse[RolePermissionListItem]


