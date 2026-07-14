"""Add editor approval and MSRP materialization execution provenance.

Revision ID: 20260715_0046
Revises: 20260715_0045
Create Date: 2026-07-15 17:20:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260715_0046"
down_revision = "20260715_0045"
branch_labels = None
depends_on = None


UUID = postgresql.UUID(as_uuid=True)
JSONB = postgresql.JSONB()


def upgrade() -> None:
    op.create_table(
        "materialization_approvals",
        sa.Column(
            "approval_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.Column("scope_kind", sa.Text(), nullable=False),
        sa.Column("scrape_batch_id", UUID, nullable=True),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'approved'"),
        ),
        sa.Column("editor_actor", sa.Text(), nullable=False),
        sa.Column("editor_role", sa.Text(), nullable=False),
        sa.Column("editor_identity_source", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("rollback_plan_ref", sa.Text(), nullable=False),
        sa.Column("compensation_plan_ref", sa.Text(), nullable=False),
        sa.Column("approved_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reserved_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consumed_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "operation = 'materialize'",
            name="ck_msrp_materialization_approvals_operation",
        ),
        sa.CheckConstraint(
            "scope_kind IN ('observations', 'batch')",
            name="ck_msrp_materialization_approvals_scope_kind",
        ),
        sa.CheckConstraint(
            "status IN ('approved', 'executing', 'consumed', 'revoked', 'expired')",
            name="ck_msrp_materialization_approvals_status",
        ),
        sa.ForeignKeyConstraint(
            ["scrape_batch_id"],
            ["msrp.scrape_batches.scrape_batch_id"],
            name="fk_msrp_materialization_approvals_batch",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_materialization_approvals_status_expires",
        "materialization_approvals",
        ["status", "expires_at_utc"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_materialization_approvals_batch",
        "materialization_approvals",
        ["scrape_batch_id"],
        schema="msrp",
    )

    op.create_table(
        "materialization_approval_items",
        sa.Column(
            "approval_item_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("approval_id", UUID, nullable=False),
        sa.Column("observation_id", UUID, nullable=False),
        sa.Column("gate_decision_id", UUID, nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(
            ["approval_id"],
            ["msrp.materialization_approvals.approval_id"],
            name="fk_msrp_materialization_approval_items_approval",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["observation_id"],
            ["msrp.observations.observation_id"],
            name="fk_msrp_materialization_approval_items_observation",
        ),
        sa.ForeignKeyConstraint(
            ["gate_decision_id"],
            ["msrp.governance_gate_decisions.gate_decision_id"],
            name="fk_msrp_materialization_approval_items_gate",
        ),
        sa.UniqueConstraint(
            "approval_id",
            "observation_id",
            name="uq_msrp_materialization_approval_items_observation",
        ),
        sa.UniqueConstraint(
            "approval_id",
            "gate_decision_id",
            name="uq_msrp_materialization_approval_items_gate",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_materialization_approval_items_observation",
        "materialization_approval_items",
        ["observation_id"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_materialization_approval_items_gate",
        "materialization_approval_items",
        ["gate_decision_id"],
        schema="msrp",
    )

    op.create_table(
        "materialization_executions",
        sa.Column(
            "execution_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("approval_id", UUID, nullable=False),
        sa.Column("scrape_batch_id", UUID, nullable=True),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("editor_actor", sa.Text(), nullable=False),
        sa.Column("executed_by_actor", sa.Text(), nullable=False),
        sa.Column("executed_by_role", sa.Text(), nullable=False),
        sa.Column("executed_by_identity_source", sa.Text(), nullable=False),
        sa.Column("execution_context", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("scope_json", JSONB, nullable=False),
        sa.Column("gate_decision_ids_json", JSONB, nullable=False),
        sa.Column("observation_ids_json", JSONB, nullable=False),
        sa.Column(
            "before_fact_refs_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "after_fact_refs_json",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("rollback_ref", sa.Text(), nullable=False),
        sa.Column("compensation_ref", sa.Text(), nullable=False),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("started_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "operation = 'materialize'",
            name="ck_msrp_materialization_executions_operation",
        ),
        sa.CheckConstraint(
            "status IN ('running', 'succeeded', 'failed', 'blocked')",
            name="ck_msrp_materialization_executions_status",
        ),
        sa.ForeignKeyConstraint(
            ["approval_id"],
            ["msrp.materialization_approvals.approval_id"],
            name="fk_msrp_materialization_executions_approval",
        ),
        sa.ForeignKeyConstraint(
            ["scrape_batch_id"],
            ["msrp.scrape_batches.scrape_batch_id"],
            name="fk_msrp_materialization_executions_batch",
        ),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_msrp_materialization_executions_idempotency_key",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_materialization_executions_approval",
        "materialization_executions",
        ["approval_id", "started_at_utc"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_materialization_executions_run",
        "materialization_executions",
        ["run_id"],
        schema="msrp",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_msrp_materialization_executions_run",
        table_name="materialization_executions",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_materialization_executions_approval",
        table_name="materialization_executions",
        schema="msrp",
    )
    op.drop_table("materialization_executions", schema="msrp")
    op.drop_index(
        "ix_msrp_materialization_approval_items_gate",
        table_name="materialization_approval_items",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_materialization_approval_items_observation",
        table_name="materialization_approval_items",
        schema="msrp",
    )
    op.drop_table("materialization_approval_items", schema="msrp")
    op.drop_index(
        "ix_msrp_materialization_approvals_batch",
        table_name="materialization_approvals",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_materialization_approvals_status_expires",
        table_name="materialization_approvals",
        schema="msrp",
    )
    op.drop_table("materialization_approvals", schema="msrp")
