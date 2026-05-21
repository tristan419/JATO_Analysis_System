"""Add ordering schema for Order Genius — material master, FOB, quantity cells.

Revision ID: 20260521_0018
Revises: 20260516_0017
Create Date: 2026-05-21
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID, JSONB


revision = "20260521_0018"
down_revision = "20260516_0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS ordering")

    # ── material_baseline_version ─────────────────────────────────────
    op.create_table(
        "material_baseline_version",
        sa.Column(
            "baseline_version_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("source_upload_id", PGUUID(as_uuid=True), nullable=True),
        sa.Column("source_file_name", sa.Text, nullable=False),
        sa.Column("source_file_hash", sa.Text, nullable=True),
        sa.Column("baseline_name", sa.Text, nullable=False),
        sa.Column(
            "status",
            sa.Text,
            nullable=False,
            server_default=sa.text("'published'"),
        ),
        sa.Column("published_by", sa.Text, nullable=True),
        sa.Column("published_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        schema="ordering",
    )

    # ── material_sku_master ───────────────────────────────────────────
    op.create_table(
        "material_sku_master",
        sa.Column(
            "material_sku_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "baseline_version_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("ordering.material_baseline_version.baseline_version_id"),
            nullable=False,
        ),
        sa.Column("brand", sa.Text, nullable=False),
        sa.Column("model_code", sa.Text, nullable=True),
        sa.Column("model_name", sa.Text, nullable=False),
        sa.Column("powertrain", sa.Text, nullable=True),
        sa.Column("version", sa.Text, nullable=False),
        sa.Column("exterior_color_name", sa.Text, nullable=False),
        sa.Column("exterior_color_code", sa.Text, nullable=False),
        sa.Column("exterior_color_type", sa.Text, nullable=False),
        sa.Column("interior_color_name", sa.Text, nullable=True),
        sa.Column("bom_template", sa.Text, nullable=True),
        sa.Column("material_code", sa.Text, nullable=False),
        sa.Column(
            "lifecycle_status",
            sa.Text,
            nullable=False,
            server_default=sa.text("'active'"),
        ),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("is_published", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("effective_from_month", sa.Text, nullable=True),
        sa.Column("effective_to_month", sa.Text, nullable=True),
        sa.Column("remark", sa.Text, nullable=True),
        sa.Column("row_version", sa.Integer, nullable=False, server_default="1"),
        sa.Column("source_sheet_name", sa.Text, nullable=True),
        sa.Column("source_row_number", sa.Integer, nullable=True),
        sa.Column("raw_payload_json", JSONB, nullable=True),
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
        schema="ordering",
    )

    # ── country_payment_term_master ───────────────────────────────────
    op.create_table(
        "country_payment_term_master",
        sa.Column(
            "country_payment_term_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("country_code", sa.Text, nullable=False),
        sa.Column("country_name", sa.Text, nullable=False),
        sa.Column("payment_term_code", sa.Text, nullable=False),
        sa.Column("payment_method", sa.Text, nullable=False),
        sa.Column("lc_days", sa.Integer, nullable=False, server_default="0"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("remark", sa.Text, nullable=True),
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
        schema="ordering",
    )

    # ── payment_term_price_rule ───────────────────────────────────────
    op.create_table(
        "payment_term_price_rule",
        sa.Column(
            "payment_term_rule_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("payment_term_code", sa.Text, nullable=False),
        sa.Column("payment_method", sa.Text, nullable=False),
        sa.Column("lc_days", sa.Integer, nullable=False),
        sa.Column(
            "fob_adjustment_eur",
            sa.Numeric(12, 2),
            nullable=False,
            server_default="0",
        ),
        sa.Column("adjustment_rate", sa.Numeric(10, 6), nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
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
        schema="ordering",
    )

    # ── brand_colour_surcharge_rule ───────────────────────────────────
    op.create_table(
        "brand_colour_surcharge_rule",
        sa.Column(
            "colour_surcharge_rule_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("brand", sa.Text, nullable=False),
        sa.Column("colour_type", sa.Text, nullable=False),
        sa.Column(
            "surcharge_eur",
            sa.Numeric(12, 2),
            nullable=False,
            server_default="0",
        ),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
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
        schema="ordering",
    )

    # ── country_sku_fob_resolved ──────────────────────────────────────
    op.create_table(
        "country_sku_fob_resolved",
        sa.Column(
            "country_sku_fob_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "baseline_version_id",
            PGUUID(as_uuid=True),
            sa.ForeignKey("ordering.material_baseline_version.baseline_version_id"),
            nullable=False,
        ),
        sa.Column("country_code", sa.Text, nullable=False),
        sa.Column("material_code", sa.Text, nullable=False),
        sa.Column("payment_term_code", sa.Text, nullable=False),
        sa.Column("base_fob_eur", sa.Numeric(12, 2), nullable=False),
        sa.Column(
            "payment_term_adjustment_eur",
            sa.Numeric(12, 2),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "colour_surcharge_eur",
            sa.Numeric(12, 2),
            nullable=False,
            server_default="0",
        ),
        sa.Column("final_fob_eur", sa.Numeric(12, 2), nullable=False),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
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
        schema="ordering",
    )

    # ── order_quantity_cell ───────────────────────────────────────────
    op.create_table(
        "order_quantity_cell",
        sa.Column(
            "order_quantity_cell_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("country_code", sa.Text, nullable=False),
        sa.Column("order_year", sa.Integer, nullable=False),
        sa.Column("order_month", sa.Integer, nullable=False),
        sa.Column("material_code", sa.Text, nullable=False),
        sa.Column("quantity", sa.Integer, nullable=False, server_default="0"),
        sa.Column("fob_eur", sa.Numeric(12, 2), nullable=False),
        sa.Column("row_version", sa.Integer, nullable=False, server_default="1"),
        sa.Column("created_by", sa.Text, nullable=True),
        sa.Column("updated_by", sa.Text, nullable=True),
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
        schema="ordering",
    )

    # ── material_sku_remark_history ───────────────────────────────────
    op.create_table(
        "material_sku_remark_history",
        sa.Column(
            "remark_history_id",
            PGUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("material_code", sa.Text, nullable=False),
        sa.Column("old_remark", sa.Text, nullable=True),
        sa.Column("new_remark", sa.Text, nullable=True),
        sa.Column("updated_by", sa.Text, nullable=True),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        schema="ordering",
    )

    # ── Indexes ───────────────────────────────────────────────────────

    # material_sku_master
    op.create_index(
        "ix_ordering_sku_lookup",
        "material_sku_master",
        ["brand", "model_name", "version", "exterior_color_code"],
        schema="ordering",
    )
    op.create_index(
        "ix_ordering_sku_material_code",
        "material_sku_master",
        ["material_code"],
        schema="ordering",
    )
    op.create_index(
        "ix_ordering_sku_active",
        "material_sku_master",
        ["is_active", "lifecycle_status"],
        schema="ordering",
    )
    op.create_unique_constraint(
        "uq_ordering_sku_baseline_material",
        "material_sku_master",
        ["baseline_version_id", "material_code"],
        schema="ordering",
    )

    # country_payment_term_master — partial unique
    op.create_index(
        "uq_ordering_country_payment_active",
        "country_payment_term_master",
        ["country_code"],
        unique=True,
        schema="ordering",
        postgresql_where=sa.text("is_active = true"),
    )

    # payment_term_price_rule — partial unique
    op.create_index(
        "uq_ordering_payment_term_rule_active",
        "payment_term_price_rule",
        ["payment_term_code"],
        unique=True,
        schema="ordering",
        postgresql_where=sa.text("is_active = true"),
    )

    # brand_colour_surcharge_rule — partial unique
    op.create_index(
        "uq_ordering_colour_surcharge_active",
        "brand_colour_surcharge_rule",
        ["brand", "colour_type"],
        unique=True,
        schema="ordering",
        postgresql_where=sa.text("is_active = true"),
    )

    # country_sku_fob_resolved — partial unique
    op.create_index(
        "uq_ordering_country_sku_fob_active",
        "country_sku_fob_resolved",
        ["country_code", "material_code"],
        unique=True,
        schema="ordering",
        postgresql_where=sa.text("is_active = true"),
    )

    # order_quantity_cell
    op.create_unique_constraint(
        "uq_ordering_quantity_cell",
        "order_quantity_cell",
        ["country_code", "order_year", "order_month", "material_code"],
        schema="ordering",
    )
    op.create_check_constraint(
        "ck_ordering_quantity_month",
        "order_quantity_cell",
        "order_month BETWEEN 1 AND 12",
        schema="ordering",
    )
    op.create_check_constraint(
        "ck_ordering_quantity_non_negative",
        "order_quantity_cell",
        "quantity >= 0",
        schema="ordering",
    )

    # material_sku_remark_history
    op.create_index(
        "ix_ordering_remark_history_code_updated",
        "material_sku_remark_history",
        ["material_code", "updated_at_utc"],
        schema="ordering",
    )


def downgrade() -> None:
    op.drop_table("material_sku_remark_history", schema="ordering")
    op.drop_table("order_quantity_cell", schema="ordering")
    op.drop_table("country_sku_fob_resolved", schema="ordering")
    op.drop_table("brand_colour_surcharge_rule", schema="ordering")
    op.drop_table("payment_term_price_rule", schema="ordering")
    op.drop_table("country_payment_term_master", schema="ordering")
    op.drop_table("material_sku_master", schema="ordering")
    op.drop_table("material_baseline_version", schema="ordering")
    op.execute("DROP SCHEMA IF EXISTS ordering")
