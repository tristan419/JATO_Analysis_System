"""Add country material finance table for BOM Admin CBU notes."""

revision = "20260617_0037"
down_revision = "20260616_0036"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def upgrade() -> None:
    op.create_table(
        "country_material_finance",
        sa.Column(
            "country_material_finance_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("material_code", sa.Text(), nullable=False),
        sa.Column("fob_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("retail_price_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("wholesale_price_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("dealer_price_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("cost_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("margin_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("margin_rate", sa.Numeric(10, 6), nullable=True),
        sa.Column("memo", sa.Text(), nullable=True),
        sa.Column("source_mode", sa.Text(), nullable=False, server_default="manual"),
        sa.Column("source_payload_json", postgresql.JSONB(), nullable=True),
        sa.Column("updated_by", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        schema="ordering",
    )
    op.create_index(
        "uq_ordering_country_material_finance_active",
        "country_material_finance",
        ["country_code", "material_code"],
        unique=True,
        postgresql_where=sa.text("is_active = true"),
        schema="ordering",
    )
    op.create_index(
        "ix_ordering_country_material_finance_country",
        "country_material_finance",
        ["country_code"],
        schema="ordering",
    )
    op.create_index(
        "ix_ordering_country_material_finance_material",
        "country_material_finance",
        ["material_code"],
        schema="ordering",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ordering_country_material_finance_material",
        table_name="country_material_finance",
        schema="ordering",
    )
    op.drop_index(
        "ix_ordering_country_material_finance_country",
        table_name="country_material_finance",
        schema="ordering",
    )
    op.drop_index(
        "uq_ordering_country_material_finance_active",
        table_name="country_material_finance",
        schema="ordering",
    )
    op.drop_table("country_material_finance", schema="ordering")
