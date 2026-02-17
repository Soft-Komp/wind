from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.system_config import SystemConfig
from app.schemas.system import (
    SystemConfigCreate, SystemConfigUpdate,
    SystemConfigRead, SystemConfigListItem, SystemConfigDetail,
    SystemConfigResponse, SystemConfigDetailResponse, SystemConfigListResponse,
    SystemConfigListQuery,
)
from app.schemas.base import BaseResponse, PaginatedData, PaginationMeta

router = APIRouter(prefix="/system/config", tags=["system"])


@router.get("", response_model=SystemConfigListResponse)
async def list_system(q: SystemConfigListQuery = Depends(), db: AsyncSession = Depends(get_db)):
    stmt = select(SystemConfig).offset(q.offset).limit(q.limit)
    result = await db.execute(stmt)
    items = result.scalars().all()

    total_stmt = select(func.count()).select_from(SystemConfig)
    total = (await db.execute(total_stmt)).scalar_one()

    data = PaginatedData[
        SystemConfigListItem
    ](
        items=[SystemConfigListItem.model_validate(x) for x in items],
        pagination=PaginationMeta(
            page=q.page,
            limit=q.limit,
            total=total,
            pages=(total + q.limit - 1) // q.limit if q.limit else 0,
        ),
    )
    return BaseResponse(code=200, data=data)


@router.get("/{item_id}", response_model=SystemConfigDetailResponse)
async def get_system(item_id: int, db: AsyncSession = Depends(get_db)):
    obj = (await db.execute(select(SystemConfig).where(SystemConfig.id_config == item_id))).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="SystemConfig not found")
    return BaseResponse(code=200, data=SystemConfigDetail.model_validate(obj))


@router.post("", response_model=SystemConfigResponse, status_code=status.HTTP_201_CREATED)
async def create_system(payload: SystemConfigCreate, db: AsyncSession = Depends(get_db)):
    obj = SystemConfig(**payload.model_dump(exclude_none=True))
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return BaseResponse(code=201, data=SystemConfigRead.model_validate(obj))


@router.put("/{item_id}", response_model=SystemConfigResponse)
async def update_system(item_id: int, payload: SystemConfigUpdate, db: AsyncSession = Depends(get_db)):
    obj = (await db.execute(select(SystemConfig).where(SystemConfig.id_config == item_id))).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="SystemConfig not found")

    for k, v in payload.model_dump(exclude_unset=True, exclude_none=True).items():
        setattr(obj, k, v)

    await db.commit()
    await db.refresh(obj)
    return BaseResponse(code=200, data=SystemConfigRead.model_validate(obj))


@router.delete("/{item_id}", response_model=SystemConfigResponse)
async def delete_system(item_id: int, db: AsyncSession = Depends(get_db)):
    obj = (await db.execute(select(SystemConfig).where(SystemConfig.id_config == item_id))).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="SystemConfig not found")
    # Soft-delete jeśli model ma is_active (AuditMixin)
    if hasattr(obj, "is_active"):
        setattr(obj, "is_active", False)
        await db.commit()
        await db.refresh(obj)
        return BaseResponse(code=200, data=SystemConfigRead.model_validate(obj))

    # Hard-delete dla tabel bez soft-delete
    await db.delete(obj)
    await db.commit()
    return BaseResponse(code=200, data=SystemConfigRead.model_validate(obj))
