"""Add audit history for country material finance edits."""

revision = "20260619_0039"
down_revision = "20260617_0038"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def upgrade() -> None:
    op.create_table(
        "country_material_finance_history",
        sa.Column(
            "finance_history_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("country_material_finance_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("material_code", sa.Text(), nullable=False),
        sa.Column("old_values_json", postgresql.JSONB(), nullable=True),
        sa.Column("new_values_json", postgresql.JSONB(), nullable=False),
        sa.Column("changed_fields_json", postgresql.JSONB(), nullable=False),
        sa.Column("source_mode", sa.Text(), nullable=True),
        sa.Column("source_payload_json", postgresql.JSONB(), nullable=True),
        sa.Column("changed_by", sa.Text(), nullable=True),
        sa.Column(
            "changed_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        schema="ordering",
    )
    op.create_index(
        "ix_ordering_country_material_finance_history_code",
        "country_material_finance_history",
        ["country_code", "material_code", "changed_at_utc"],
        schema="ordering",
    )
    op.create_index(
        "ix_ordering_country_material_finance_history_finance",
        "country_material_finance_history",
        ["country_material_finance_id"],
        schema="ordering",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ordering_country_material_finance_history_finance",
        table_name="country_material_finance_history",
        schema="ordering",
    )
    op.drop_index(
        "ix_ordering_country_material_finance_history_code",
        table_name="country_material_finance_history",
        schema="ordering",
    )
    op.drop_table("country_material_finance_history", schema="ordering")
