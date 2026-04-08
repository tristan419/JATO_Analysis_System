from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.schemas import (
    CrudItemCreate,
    CrudItemPatch,
    CrudSortBy,
    CrudSortOrder,
)
from app.core.config import MAX_CRUD_PAGE_SIZE
from app.core.security import require_min_role
from app.services.query_service import (
    create_item,
    list_items_query,
    remove_item,
    update_item,
)

router = APIRouter(prefix="/crud/items", tags=["crud"])


@router.get("")
def get_items(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1),
    sort_by: CrudSortBy = Query(default="code"),
    sort_order: CrudSortOrder = Query(default="asc"),
    query: str = Query(default=""),
    _=Depends(require_min_role("viewer")),
) -> dict:
    normalized_page_size = min(int(page_size), MAX_CRUD_PAGE_SIZE)
    payload = list_items_query(
        page=page,
        page_size=normalized_page_size,
        sort_by=sort_by,
        sort_order=sort_order,
        query=query,
    )
    return payload


@router.post("")
def post_item(
    payload: CrudItemCreate,
    _=Depends(require_min_role("editor")),
) -> dict:
    row = create_item(payload.model_dump())
    return {"item": row}


@router.patch("/{item_id}")
def patch_item(
    item_id: str,
    payload: CrudItemPatch,
    _=Depends(require_min_role("editor")),
) -> dict:
    row = update_item(item_id, payload.model_dump())
    if row is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"item": row}


@router.delete("/{item_id}")
def delete_item(
    item_id: str,
    _=Depends(require_min_role("editor")),
) -> dict:
    ok = remove_item(item_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"deleted": True}
