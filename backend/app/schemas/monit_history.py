"""MonitHistory — Pydantic schemas (CRUD + list/detail).

Zgodne z BaseResponse/PaginatedResponse.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import BaseResponse, PaginatedResponse, PaginationParams

class MonitHistoryCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_kontrahenta: int = Field(...)
    id_user: int | None = Field(default=None)
    template_id: int | None = Field(default=None)
    recipient: str | None = Field(default=None)
    subject: str | None = Field(default=None)
    message_body: str | None = Field(default=None)
    total_debt: Decimal | None = Field(default=None)
    invoice_numbers: str | None = Field(default=None)
    pdf_path: str | None = Field(default=None)
    external_id: str | None = Field(default=None)
    scheduled_at: datetime | None = Field(default=None)
    sent_at: datetime | None = Field(default=None)
    delivered_at: datetime | None = Field(default=None)
    opened_at: datetime | None = Field(default=None)
    clicked_at: datetime | None = Field(default=None)
    error_message: str | None = Field(default=None)
    cost: Decimal | None = Field(default=None)
    updated_at: datetime | None = Field(default=None)



class MonitHistoryUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id_kontrahenta: int | None = Field(default=None)
    id_user: int | None | None = Field(default=None)
    monit_type: str | None = Field(default=None)
    template_id: int | None | None = Field(default=None)
    status: str | None = Field(default=None)
    recipient: str | None | None = Field(default=None)
    subject: str | None | None = Field(default=None)
    message_body: str | None | None = Field(default=None)
    total_debt: Decimal | None | None = Field(default=None)
    invoice_numbers: str | None | None = Field(default=None)
    pdf_path: str | None | None = Field(default=None)
    external_id: str | None | None = Field(default=None)
    scheduled_at: datetime | None | None = Field(default=None)
    sent_at: datetime | None | None = Field(default=None)
    delivered_at: datetime | None | None = Field(default=None)
    opened_at: datetime | None | None = Field(default=None)
    clicked_at: datetime | None | None = Field(default=None)
    error_message: str | None | None = Field(default=None)
    retry_count: int | None = Field(default=None)
    cost: Decimal | None | None = Field(default=None)
    is_active: bool | None = Field(default=None)
    created_at: datetime | None = Field(default=None)
    updated_at: datetime | None | None = Field(default=None)



class MonitHistoryRead(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_monit: int = Field(...)
    id_kontrahenta: int = Field(...)
    id_user: int | None | None = Field(default=None)
    monit_type: str = Field(...)
    template_id: int | None | None = Field(default=None)
    status: str = Field(...)
    recipient: str | None | None = Field(default=None)
    subject: str | None | None = Field(default=None)
    message_body: str | None | None = Field(default=None)
    total_debt: Decimal | None | None = Field(default=None)
    invoice_numbers: str | None | None = Field(default=None)
    pdf_path: str | None | None = Field(default=None)
    external_id: str | None | None = Field(default=None)
    scheduled_at: datetime | None | None = Field(default=None)
    sent_at: datetime | None | None = Field(default=None)
    delivered_at: datetime | None | None = Field(default=None)
    opened_at: datetime | None | None = Field(default=None)
    clicked_at: datetime | None | None = Field(default=None)
    error_message: str | None | None = Field(default=None)
    retry_count: int = Field(...)
    cost: Decimal | None | None = Field(default=None)
    is_active: bool = Field(...)
    created_at: datetime = Field(...)
    updated_at: datetime | None | None = Field(default=None)



class MonitHistoryListItem(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)
    id_monit: int = Field(...)
    id_kontrahenta: int = Field(...)
    id_user: int | None | None = Field(default=None)



class MonitHistoryDetail(MonitHistoryRead):
    """Szczegóły (na start = pełny Read; można rozszerzyć o relacje)."""
    pass



MonitHistoryResponse = BaseResponse[MonitHistoryRead]

MonitHistoryDetailResponse = BaseResponse[MonitHistoryDetail]

MonitHistoryListResponse = PaginatedResponse[MonitHistoryListItem]



class MonitHistoryListQuery(PaginationParams):
    """Query params dla listy: paginacja + sortowanie."""
    sort: str | None = Field(default=None)
