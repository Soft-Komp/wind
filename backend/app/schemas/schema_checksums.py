"""SchemaChecksum — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class SchemaChecksumCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    object_name: str = Field(...)
    object_type: str = Field(...)
    checksum: int = Field(...)
    alembic_revision: str | None | None = Field(default=None)
    last_verified_at: datetime | None | None = Field(default=None)
    created_at: datetime = Field(...)
    updated_at: datetime | None | None = Field(default=None)



class SchemaChecksumUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    object_name: str | None = Field(default=None)
    object_type: str | None = Field(default=None)
    checksum: int | None = Field(default=None)
    alembic_revision: str | None | None = Field(default=None)
    last_verified_at: datetime | None | None = Field(default=None)
    created_at: datetime | None = Field(default=None)
    updated_at: datetime | None | None = Field(default=None)



class SchemaChecksumRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_checksum: int = Field(...)
    object_name: str = Field(...)
    object_type: str = Field(...)
    checksum: int = Field(...)
    alembic_revision: str | None | None = Field(default=None)
    last_verified_at: datetime | None | None = Field(default=None)
    created_at: datetime = Field(...)
    updated_at: datetime | None | None = Field(default=None)



class SchemaChecksumListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_checksum: int = Field(...)
    object_name: str = Field(...)
    object_type: str = Field(...)



class SchemaChecksumDetail(SchemaChecksumRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



SchemaChecksumResponse = BaseResponse[SchemaChecksumRead]

SchemaChecksumDetailResponse = BaseResponse[SchemaChecksumDetail]

SchemaChecksumListResponse = PaginatedResponse[SchemaChecksumListItem]



class SchemaChecksumListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
