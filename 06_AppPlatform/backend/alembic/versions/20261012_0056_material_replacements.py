"""Explicit base links and account-local material reminder acknowledgements."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20261012_0056"
down_revision = "20261009_0055"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table("material_replacement",
        sa.Column("brand", sa.Text(), primary_key=True),
        sa.Column("source_template", sa.Text(), primary_key=True),
        sa.Column("target_template", sa.Text(), nullable=True),
        sa.Column("previous_target", sa.Text(), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("changed_by", sa.Text(), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("row_version", sa.Integer(), nullable=False, server_default="1"),
        sa.UniqueConstraint("brand", "target_template", name="uq_material_replacement_target"), schema="ordering")
    op.create_table("material_notice_read",
        sa.Column("user_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("notice_key", sa.Text(), primary_key=True),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=False), schema="ordering")


def downgrade() -> None:
    op.drop_table("material_notice_read", schema="ordering")
    op.drop_table("material_replacement", schema="ordering")
