"""RefreshToken — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class RefreshTokenCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_user: int = Field(...)
    token: str = Field(...)
    expires_at: datetime = Field(...)
    created_at: datetime = Field(...)
    is_revoked: bool = Field(...)
    revoked_at: datetime | None | None = Field(default=None)
    ip_address: str | None | None = Field(default=None)
    user_agent: str | None | None = Field(default=None)



class RefreshTokenUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_user: int | None = Field(default=None)
    token: str | None = Field(default=None)
    expires_at: datetime | None = Field(default=None)
    created_at: datetime | None = Field(default=None)
    is_revoked: bool | None = Field(default=None)
    revoked_at: datetime | None | None = Field(default=None)
    ip_address: str | None | None = Field(default=None)
    user_agent: str | None | None = Field(default=None)



class RefreshTokenRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_token: int = Field(...)
    id_user: int = Field(...)
    token: str = Field(...)
    expires_at: datetime = Field(...)
    created_at: datetime = Field(...)
    is_revoked: bool = Field(...)
    revoked_at: datetime | None | None = Field(default=None)
    ip_address: str | None | None = Field(default=None)
    user_agent: str | None | None = Field(default=None)



class RefreshTokenListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_token: int = Field(...)
    id_user: int = Field(...)
    token: str = Field(...)



class RefreshTokenDetail(RefreshTokenRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



RefreshTokenResponse = BaseResponse[RefreshTokenRead]

RefreshTokenDetailResponse = BaseResponse[RefreshTokenDetail]

RefreshTokenListResponse = PaginatedResponse[RefreshTokenListItem]



class RefreshTokenListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
