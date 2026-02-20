"""SystemConfig — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class SystemConfigCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    config_key: str = Field(...)
    config_value: str = Field(...)
    description: str | None | None = Field(default=None)



class SystemConfigUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    config_key: str | None = Field(default=None)
    config_value: str | None = Field(default=None)
    description: str | None | None = Field(default=None)



class SystemConfigRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_config: int = Field(...)
    config_key: str = Field(...)
    config_value: str = Field(...)
    description: str | None | None = Field(default=None)
    created_at: datetime = Field(...)
    updated_at: datetime | None | None = Field(default=None)
    is_active: bool = Field(...)



class SystemConfigListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_config: int = Field(...)
    config_key: str = Field(...)
    config_value: str = Field(...)



class SystemConfigDetail(SystemConfigRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



SystemConfigResponse = BaseResponse[SystemConfigRead]

SystemConfigDetailResponse = BaseResponse[SystemConfigDetail]

SystemConfigListResponse = PaginatedResponse[SystemConfigListItem]



class SystemConfigListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
