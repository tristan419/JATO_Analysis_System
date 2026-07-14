from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class MsrpMaterializationApproval(Base):
    """One explicit editor authorization for one immutable fact-write scope."""

    __tablename__ = "materialization_approvals"
    __table_args__ = (
        CheckConstraint(
            "operation = 'materialize'",
            name="ck_msrp_materialization_approvals_operation",
        ),
        CheckConstraint(
            "scope_kind IN ('observations', 'batch')",
            name="ck_msrp_materialization_approvals_scope_kind",
        ),
        CheckConstraint(
            "status IN ('approved', 'executing', 'consumed', 'revoked', 'expired')",
            name="ck_msrp_materialization_approvals_status",
        ),
        Index(
            "ix_msrp_materialization_approvals_status_expires",
            "status",
            "expires_at_utc",
        ),
        Index(
            "ix_msrp_materialization_approvals_batch",
            "scrape_batch_id",
        ),
        {"schema": "msrp"},
    )

    approval_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    operation: Mapped[str] = mapped_column(Text, nullable=False)
    scope_kind: Mapped[str] = mapped_column(Text, nullable=False)
    scrape_batch_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.scrape_batches.scrape_batch_id"),
        nullable=True,
    )
    country: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="approved",
        server_default=text("'approved'"),
    )
    editor_actor: Mapped[str] = mapped_column(Text, nullable=False)
    editor_role: Mapped[str] = mapped_column(Text, nullable=False)
    editor_identity_source: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    rollback_plan_ref: Mapped[str] = mapped_column(Text, nullable=False)
    compensation_plan_ref: Mapped[str] = mapped_column(Text, nullable=False)
    approved_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    expires_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    reserved_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    consumed_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class MsrpMaterializationApprovalItem(Base):
    """FK-backed observation/GateDecision pair inside an approval scope."""

    __tablename__ = "materialization_approval_items"
    __table_args__ = (
        UniqueConstraint(
            "approval_id",
            "observation_id",
            name="uq_msrp_materialization_approval_items_observation",
        ),
        UniqueConstraint(
            "approval_id",
            "gate_decision_id",
            name="uq_msrp_materialization_approval_items_gate",
        ),
        Index(
            "ix_msrp_materialization_approval_items_observation",
            "observation_id",
        ),
        Index(
            "ix_msrp_materialization_approval_items_gate",
            "gate_decision_id",
        ),
        {"schema": "msrp"},
    )

    approval_item_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    approval_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(
            "msrp.materialization_approvals.approval_id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    observation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.observations.observation_id"),
        nullable=False,
    )
    gate_decision_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.governance_gate_decisions.gate_decision_id"),
        nullable=False,
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class MsrpMaterializationExecution(Base):
    """Durable attempt and before/after provenance for one approval execution."""

    __tablename__ = "materialization_executions"
    __table_args__ = (
        CheckConstraint(
            "operation = 'materialize'",
            name="ck_msrp_materialization_executions_operation",
        ),
        CheckConstraint(
            "status IN ('running', 'succeeded', 'failed', 'blocked')",
            name="ck_msrp_materialization_executions_status",
        ),
        UniqueConstraint(
            "idempotency_key",
            name="uq_msrp_materialization_executions_idempotency_key",
        ),
        Index(
            "ix_msrp_materialization_executions_approval",
            "approval_id",
            "started_at_utc",
        ),
        Index(
            "ix_msrp_materialization_executions_run",
            "run_id",
        ),
        {"schema": "msrp"},
    )

    execution_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    approval_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.materialization_approvals.approval_id"),
        nullable=False,
    )
    scrape_batch_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("msrp.scrape_batches.scrape_batch_id"),
        nullable=True,
    )
    operation: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    editor_actor: Mapped[str] = mapped_column(Text, nullable=False)
    executed_by_actor: Mapped[str] = mapped_column(Text, nullable=False)
    executed_by_role: Mapped[str] = mapped_column(Text, nullable=False)
    executed_by_identity_source: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )
    execution_context: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    scope_json: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    gate_decision_ids_json: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
    )
    observation_ids_json: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
    )
    before_fact_refs_json: Mapped[list[dict[str, object]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
    )
    after_fact_refs_json: Mapped[list[dict[str, object]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
    )
    rollback_ref: Mapped[str] = mapped_column(Text, nullable=False)
    compensation_ref: Mapped[str] = mapped_column(Text, nullable=False)
    error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    finished_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
