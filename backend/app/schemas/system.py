"""
Schematy Pydantic v2 — Konfiguracja systemu.

Standard: CRUD + ListItem/Detail + ListQuery (page/limit, domyślny limit=12).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime

from app.schemas.base import BaseResponse, PaginatedResponse


# ---------------------------------------------------------------------------
# SystemConfig
# ---------------------------------------------------------------------------

class SystemConfigBase(BaseModel):
    model_config = ConfigDict(extra='forbid')
    config_key: str = Field(..., max_length=100)
    config_value: str = Field(...)
    description: str | None = Field(None, max_length=500)


class {Entity}Create({Entity}Base):
    pass


class SystemConfigUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    config_key: str | None = Field(None, max_length=100)
    config_value: str | None = None
    description: str | None = Field(None, max_length=500)


class SystemConfigRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_config: int | None = Field(None)
    config_key: str = Field(..., max_length=100)
    config_value: str = Field(...)
    description: str | None = Field(None, max_length=500)


class SystemConfigListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_config: int | None = Field(None)
    config_key: str = Field(..., max_length=100)
    config_value: str = Field(...)
    description: str | None = Field(None, max_length=500)


class SystemConfigDetail(SystemConfigRead):
    pass


class SystemConfigListQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    page: int = Field(1, ge=1)
    limit: int = Field(12, ge=1, le=500)
    sort: str | None = None


SystemConfigResponse = BaseResponse[SystemConfigRead]
SystemConfigDetailResponse = BaseResponse[SystemConfigDetail]
SystemConfigListResponse = PaginatedResponse[SystemConfigListItem]


# ---------------------------------------------------------------------------
# Template
# ---------------------------------------------------------------------------

class TemplateBase(BaseModel):
    model_config = ConfigDict(extra='forbid')
    template_name: str = Field(..., max_length=100)
    template_type: str = Field(..., max_length=20)
    subject: str | None = Field(None, max_length=200)
    body: str = Field(...)


class {Entity}Create({Entity}Base):
    pass


class TemplateUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    template_name: str | None = Field(None, max_length=100)
    template_type: str | None = Field(None, max_length=20)
    subject: str | None = Field(None, max_length=200)
    body: str | None = None


class TemplateRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_template: int | None = Field(None)
    template_name: str = Field(..., max_length=100)
    template_type: str = Field(..., max_length=20)
    subject: str | None = Field(None, max_length=200)
    body: str = Field(...)


class TemplateListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_template: int | None = Field(None)
    template_name: str = Field(..., max_length=100)
    template_type: str = Field(..., max_length=20)
    subject: str | None = Field(None, max_length=200)
    body: str = Field(...)


class TemplateDetail(TemplateRead):
    pass


class TemplateListQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    page: int = Field(1, ge=1)
    limit: int = Field(12, ge=1, le=500)
    sort: str | None = None


TemplateResponse = BaseResponse[TemplateRead]
TemplateDetailResponse = BaseResponse[TemplateDetail]
TemplateListResponse = PaginatedResponse[TemplateListItem]


# ---------------------------------------------------------------------------
# SchemaChecksums
# ---------------------------------------------------------------------------

class SchemaChecksumsBase(BaseModel):
    model_config = ConfigDict(extra='forbid')
    object_name: str = Field(..., max_length=200)
    object_type: str = Field(..., max_length=50)
    checksum: int = Field(...)
    alembic_revision: str | None = Field(None, max_length=50)
    last_verified_at: datetime | None = Field(None)
    created_at: datetime = Field(...)
    updated_at: datetime | None = Field(None)


class {Entity}Create({Entity}Base):
    pass


class SchemaChecksumsUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    object_name: str | None = Field(None, max_length=200)
    object_type: str | None = Field(None, max_length=50)
    checksum: int | None = None
    alembic_revision: str | None = Field(None, max_length=50)
    last_verified_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class SchemaChecksumsRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_checksum: int | None = Field(None)
    object_name: str = Field(..., max_length=200)
    object_type: str = Field(..., max_length=50)
    checksum: int = Field(...)
    alembic_revision: str | None = Field(None, max_length=50)
    last_verified_at: datetime | None = Field(None)
    created_at: datetime = Field(...)
    updated_at: datetime | None = Field(None)


class SchemaChecksumsListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id_checksum: int | None = Field(None)
    object_name: str = Field(..., max_length=200)
    object_type: str = Field(..., max_length=50)
    checksum: int = Field(...)
    alembic_revision: str | None = Field(None, max_length=50)


class SchemaChecksumsDetail(SchemaChecksumsRead):
    pass


class SchemaChecksumsListQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    page: int = Field(1, ge=1)
    limit: int = Field(12, ge=1, le=500)
    sort: str | None = None


SchemaChecksumsResponse = BaseResponse[SchemaChecksumsRead]
SchemaChecksumsDetailResponse = BaseResponse[SchemaChecksumsDetail]
SchemaChecksumsListResponse = PaginatedResponse[SchemaChecksumsListItem]


