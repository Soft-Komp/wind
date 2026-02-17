"""
Schematy Pydantic v2 — Uprawnienia.

Standard: CRUD + ListItem/Detail + ListQuery (page/limit, domyślny limit=12).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse


# ---------------------------------------------------------------------------
# Permission
# ---------------------------------------------------------------------------

class PermissionBase(BaseModel):
    model_config = ConfigDict(extra='forbid')
    permission_name: str = Field(..., max_length=100)
    description: str | None = Field(None, max_length=200)
    category: str = Field(..., max_length=50)


class {Entity}Create({Entity}Base):
    pass


class PermissionUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    permission_name: str | None = Field(None, max_length=100)
    description: str | None = Field(None, max_length=200)
    category: str | None = Field(None, max_length=50)


class PermissionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_permission: int | None = Field(None)
    permission_name: str = Field(..., max_length=100)
    description: str | None = Field(None, max_length=200)
    category: str = Field(..., max_length=50)


class PermissionListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_permission: int | None = Field(None)
    permission_name: str = Field(..., max_length=100)
    description: str | None = Field(None, max_length=200)
    category: str = Field(..., max_length=50)


class PermissionDetail(PermissionRead):
    pass


class PermissionListQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    page: int = Field(1, ge=1)
    limit: int = Field(12, ge=1, le=500)
    sort: str | None = None


PermissionResponse = BaseResponse[PermissionRead]
PermissionDetailResponse = BaseResponse[PermissionDetail]
PermissionListResponse = PaginatedResponse[PermissionListItem]


