"""Add role_upgrade_requests table.

Revision ID: 20260516_0016
Revises: 20260515_0015
Create Date: 2026-05-16
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID

revision = "20260516_0016"
down_revision = "20260515_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "role_upgrade_requests",
        sa.Column("request_id", PGUUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", PGUUID(as_uuid=True), sa.ForeignKey("auth.users.id"), nullable=False),
        sa.Column("username", sa.Text, nullable=False),
        sa.Column("current_role", sa.Text, nullable=False),
        sa.Column("requested_role", sa.Text, nullable=False),
        sa.Column("reason", sa.Text, nullable=True),
        sa.Column("status", sa.Text, nullable=False, server_default=sa.text("'pending'")),
        sa.Column("reviewed_by", sa.Text, nullable=True),
        sa.Column("reviewed_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        schema="auth",
    )
    op.create_index("ix_role_upgrade_requests_status", "role_upgrade_requests", ["status"], schema="auth")
    op.create_index("ix_role_upgrade_requests_user", "role_upgrade_requests", ["user_id"], schema="auth")


def downgrade() -> None:
    op.drop_table("role_upgrade_requests", schema="auth")
