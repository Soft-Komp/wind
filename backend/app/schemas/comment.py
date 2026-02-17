"""Comment — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class CommentCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_kontrahenta: int = Field(...)
    tresc: str = Field(...)
    uzytkownik_id: int = Field(...)



class CommentUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_kontrahenta: int | None = Field(default=None)
    tresc: str | None = Field(default=None)
    uzytkownik_id: int | None = Field(default=None)



class CommentRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_comment: int = Field(...)
    id_kontrahenta: int = Field(...)
    tresc: str = Field(...)
    uzytkownik_id: int = Field(...)
    created_at: datetime = Field(...)
    updated_at: datetime | None | None = Field(default=None)
    is_active: bool = Field(...)



class CommentListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_comment: int = Field(...)
    id_kontrahenta: int = Field(...)
    tresc: str = Field(...)



class CommentDetail(CommentRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



CommentResponse = BaseResponse[CommentRead]

CommentDetailResponse = BaseResponse[CommentDetail]

CommentListResponse = PaginatedResponse[CommentListItem]



class CommentListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
