"""Template — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class TemplateCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    template_name: str = Field(...)
    template_type: str = Field(...)
    subject: str | None | None = Field(default=None)
    body: str = Field(...)



class TemplateUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    template_name: str | None = Field(default=None)
    template_type: str | None = Field(default=None)
    subject: str | None | None = Field(default=None)
    body: str | None = Field(default=None)



class TemplateRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_template: int = Field(...)
    template_name: str = Field(...)
    template_type: str = Field(...)
    subject: str | None | None = Field(default=None)
    body: str = Field(...)
    created_at: datetime = Field(...)
    updated_at: datetime | None | None = Field(default=None)
    is_active: bool = Field(...)



class TemplateListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_template: int = Field(...)
    template_name: str = Field(...)
    template_type: str = Field(...)



class TemplateDetail(TemplateRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



TemplateResponse = BaseResponse[TemplateRead]

TemplateDetailResponse = BaseResponse[TemplateDetail]

TemplateListResponse = PaginatedResponse[TemplateListItem]



class TemplateListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
