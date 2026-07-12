"""Add Hero Product spec overrides."""

revision = "20260619_0035"
down_revision = "20260619_0034"
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

    if _has_table("hero_product_spec_overrides"):
        return

    op.create_table(
        "hero_product_spec_overrides",
        sa.Column(
            "override_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("price_period", sa.Text(), nullable=False, server_default=""),
        sa.Column("brand", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("field_name", sa.Text(), nullable=False),
        sa.Column("field_value", sa.Text(), nullable=False),
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
            "brand",
            "model",
            "field_name",
            name="uq_hero_product_spec_overrides_key",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_hero_product_spec_overrides_lookup",
        "hero_product_spec_overrides",
        ["country", "price_period", "brand", "model"],
        schema="msrp",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_hero_product_spec_overrides_lookup",
        table_name="hero_product_spec_overrides",
        schema="msrp",
    )
    op.drop_table("hero_product_spec_overrides", schema="msrp")
