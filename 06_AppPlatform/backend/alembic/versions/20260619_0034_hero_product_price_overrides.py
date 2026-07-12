"""Add Hero Product price overrides."""

revision = "20260619_0034"
down_revision = "20260612_0033"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def _has_table(table_name: str, *, schema: str = "msrp") -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names(schema=schema)


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS msrp")

    if _has_table("hero_product_price_overrides"):
        return

    op.create_table(
        "hero_product_price_overrides",
        sa.Column(
            "override_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("price_period", sa.Text(), nullable=False, server_default=""),
        sa.Column("price_source", sa.Text(), nullable=False),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("trim", sa.Text(), nullable=False, server_default=""),
        sa.Column("powertrain", sa.Text(), nullable=False, server_default=""),
        sa.Column("price_value", sa.Numeric(14, 2), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False, server_default="EUR"),
        sa.Column("updated_by", sa.Text(), nullable=False, server_default="editor"),
        sa.Column("note", sa.Text(), nullable=True),
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
        sa.UniqueConstraint(
            "country",
            "price_period",
            "price_source",
            "brand",
            "model",
            "trim",
            "powertrain",
            name="uq_hero_product_price_overrides_key",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_hero_product_price_overrides_lookup",
        "hero_product_price_overrides",
        ["country", "price_period", "price_source", "brand", "model"],
        schema="msrp",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_hero_product_price_overrides_lookup",
        table_name="hero_product_price_overrides",
        schema="msrp",
    )
    op.drop_table("hero_product_price_overrides", schema="msrp")
