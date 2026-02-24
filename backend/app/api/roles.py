from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.role import Role
from app.schemas.roles import (
    RoleCreate, RoleUpdate,
    RoleRead, RoleListItem, RoleDetail,
    RoleResponse, RoleDetailResponse, RoleListResponse,
    RoleListQuery,
)
from app.schemas.base import BaseResponse, PaginatedData, PaginationMeta

router = APIRouter()


@router.get("", response_model=RoleListResponse)
async def list_roles(q: RoleListQuery = Depends(), db: AsyncSession = Depends(get_db)):
    stmt = select(Role).order_by(Role.id_role).offset(q.offset).limit(q.limit)
    result = await db.execute(stmt)
    items = result.scalars().all()

    total_stmt = select(func.count()).select_from(Role)
    total = (await db.execute(total_stmt)).scalar_one()

    total_pages = (total + q.limit - 1) // q.limit if q.limit else 0
    data = PaginatedData[RoleListItem](
        items=[RoleListItem.model_validate(x) for x in items],
        pagination=PaginationMeta(
            page=q.page,
            limit=q.limit,
            total=total,
            pages=total_pages,
            has_next=q.page < total_pages,
            has_prev=q.page > 1,
        ),
    )
    return BaseResponse(code=200, data=data)


@router.get("/{item_id}", response_model=RoleDetailResponse)
async def get_roles(item_id: int, db: AsyncSession = Depends(get_db)):
    obj = (await db.execute(select(Role).where(Role.id_role == item_id))).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Role not found")
    return BaseResponse(code=200, data=RoleDetail.model_validate(obj))


@router.post("", response_model=RoleResponse, status_code=status.HTTP_201_CREATED)
async def create_roles(payload: RoleCreate, db: AsyncSession = Depends(get_db)):
    obj = Role(**payload.model_dump(exclude_none=True))
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return BaseResponse(code=201, data=RoleRead.model_validate(obj))


@router.put("/{item_id}", response_model=RoleResponse)
async def update_roles(item_id: int, payload: RoleUpdate, db: AsyncSession = Depends(get_db)):
    obj = (await db.execute(select(Role).where(Role.id_role == item_id))).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Role not found")

    for k, v in payload.model_dump(exclude_unset=True, exclude_none=True).items():
        setattr(obj, k, v)

    await db.commit()
    await db.refresh(obj)
    return BaseResponse(code=200, data=RoleRead.model_validate(obj))


@router.delete("/{item_id}", response_model=RoleResponse)
async def delete_roles(item_id: int, db: AsyncSession = Depends(get_db)):
    obj = (await db.execute(select(Role).where(Role.id_role == item_id))).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Role not found")
    # Soft-delete jeśli model ma is_active (AuditMixin)
    if hasattr(obj, "is_active"):
        setattr(obj, "is_active", False)
        await db.commit()
        await db.refresh(obj)
        return BaseResponse(code=200, data=RoleRead.model_validate(obj))

    # Hard-delete dla tabel bez soft-delete
    await db.delete(obj)
    await db.commit()
    return BaseResponse(code=200, data=RoleRead.model_validate(obj))
