"""OtpCode — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class OtpCodeCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_user: int = Field(...)
    code: str = Field(...)
    purpose: str = Field(...)
    expires_at: datetime = Field(...)
    is_used: bool = Field(...)
    created_at: datetime = Field(...)
    ip_address: str | None | None = Field(default=None)



class OtpCodeUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_user: int | None = Field(default=None)
    code: str | None = Field(default=None)
    purpose: str | None = Field(default=None)
    expires_at: datetime | None = Field(default=None)
    is_used: bool | None = Field(default=None)
    created_at: datetime | None = Field(default=None)
    ip_address: str | None | None = Field(default=None)



class OtpCodeRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_otp: int = Field(...)
    id_user: int = Field(...)
    code: str = Field(...)
    purpose: str = Field(...)
    expires_at: datetime = Field(...)
    is_used: bool = Field(...)
    created_at: datetime = Field(...)
    ip_address: str | None | None = Field(default=None)



class OtpCodeListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_otp: int = Field(...)
    id_user: int = Field(...)
    code: str = Field(...)



class OtpCodeDetail(OtpCodeRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



OtpCodeResponse = BaseResponse[OtpCodeRead]

OtpCodeDetailResponse = BaseResponse[OtpCodeDetail]

OtpCodeListResponse = PaginatedResponse[OtpCodeListItem]



class OtpCodeListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
