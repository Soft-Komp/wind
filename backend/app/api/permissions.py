from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.permission import Permission
from app.schemas.permissions import (
    PermissionCreate, PermissionUpdate,
    PermissionRead, PermissionListItem, PermissionDetail,
    PermissionResponse, PermissionDetailResponse, PermissionListResponse,
    PermissionListQuery,
)
from app.schemas.base import BaseResponse, PaginatedData, PaginationMeta

router = APIRouter(prefix="/permissions", tags=["permissions"])


@router.get("", response_model=PermissionListResponse)
async def list_permissions(q: PermissionListQuery = Depends(), db: AsyncSession = Depends(get_db)):
    stmt = select(Permission).offset(q.offset).limit(q.limit)
    result = await db.execute(stmt)
    items = result.scalars().all()

    total_stmt = select(func.count()).select_from(Permission)
    total = (await db.execute(total_stmt)).scalar_one()

    data = PaginatedData[
        PermissionListItem
    ](
        items=[PermissionListItem.model_validate(x) for x in items],
        pagination=PaginationMeta(
            page=q.page,
            limit=q.limit,
            total=total,
            pages=(total + q.limit - 1) // q.limit if q.limit else 0,
        ),
    )
    return BaseResponse(code=200, data=data)


@router.get("/{item_id}", response_model=PermissionDetailResponse)
async def get_permissions(item_id: int, db: AsyncSession = Depends(get_db)):
    obj = (await db.execute(select(Permission).where(Permission.id_permission == item_id))).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Permission not found")
    return BaseResponse(code=200, data=PermissionDetail.model_validate(obj))


@router.post("", response_model=PermissionResponse, status_code=status.HTTP_201_CREATED)
async def create_permissions(payload: PermissionCreate, db: AsyncSession = Depends(get_db)):
    obj = Permission(**payload.model_dump(exclude_none=True))
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return BaseResponse(code=201, data=PermissionRead.model_validate(obj))


@router.put("/{item_id}", response_model=PermissionResponse)
async def update_permissions(item_id: int, payload: PermissionUpdate, db: AsyncSession = Depends(get_db)):
    obj = (await db.execute(select(Permission).where(Permission.id_permission == item_id))).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Permission not found")

    for k, v in payload.model_dump(exclude_unset=True, exclude_none=True).items():
        setattr(obj, k, v)

    await db.commit()
    await db.refresh(obj)
    return BaseResponse(code=200, data=PermissionRead.model_validate(obj))


@router.delete("/{item_id}", response_model=PermissionResponse)
async def delete_permissions(item_id: int, db: AsyncSession = Depends(get_db)):
    obj = (await db.execute(select(Permission).where(Permission.id_permission == item_id))).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Permission not found")
    # Soft-delete jeśli model ma is_active (AuditMixin)
    if hasattr(obj, "is_active"):
        setattr(obj, "is_active", False)
        await db.commit()
        await db.refresh(obj)
        return BaseResponse(code=200, data=PermissionRead.model_validate(obj))

    # Hard-delete dla tabel bez soft-delete
    await db.delete(obj)
    await db.commit()
    return BaseResponse(code=200, data=PermissionRead.model_validate(obj))
