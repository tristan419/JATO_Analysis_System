from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.msrp_materialization_models import (
    MsrpMaterializationApproval,
    MsrpMaterializationApprovalItem,
    MsrpMaterializationExecution,
)


def add(session: Session, item):
    session.add(item)
    return item


def get_approval(
    session: Session,
    approval_id: UUID,
    *,
    for_update: bool = False,
) -> MsrpMaterializationApproval | None:
    return session.get(
        MsrpMaterializationApproval,
        approval_id,
        with_for_update=for_update,
    )


def list_approval_items(
    session: Session,
    approval_id: UUID,
) -> list[MsrpMaterializationApprovalItem]:
    stmt = (
        select(MsrpMaterializationApprovalItem)
        .where(MsrpMaterializationApprovalItem.approval_id == approval_id)
        .order_by(MsrpMaterializationApprovalItem.ordinal.asc())
    )
    return list(session.execute(stmt).scalars().all())


def get_execution_by_idempotency_key(
    session: Session,
    idempotency_key: str,
) -> MsrpMaterializationExecution | None:
    stmt = select(MsrpMaterializationExecution).where(
        MsrpMaterializationExecution.idempotency_key == idempotency_key
    )
    return session.execute(stmt).scalar_one_or_none()


def get_execution(
    session: Session,
    execution_id: UUID,
    *,
    for_update: bool = False,
) -> MsrpMaterializationExecution | None:
    return session.get(
        MsrpMaterializationExecution,
        execution_id,
        with_for_update=for_update,
    )
