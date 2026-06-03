"""Add Order Genius PI vehicle allocation tables.

Revision ID: 20260601_0029
Revises: 20260530_0028
Create Date: 2026-06-01
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID


revision = "20260601_0029"
down_revision = "20260530_0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pi_order_header",
        sa.Column("pi_id", PGUUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("pi_code", sa.Text(), nullable=False),
        sa.Column("official_pi_no", sa.Text(), nullable=True),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("country_name", sa.Text(), nullable=True),
        sa.Column("order_date", sa.Date(), nullable=True),
        sa.Column("order_month", sa.Text(), nullable=False),
        sa.Column("pi_sequence_no", sa.Integer(), nullable=False),
        sa.Column("shipping_schedule_url", sa.Text(), nullable=True),
        sa.Column("feishu_tracking_url", sa.Text(), nullable=True),
        sa.Column("ship_name", sa.Text(), nullable=True),
        sa.Column("etd", sa.Date(), nullable=True),
        sa.Column("eta", sa.Date(), nullable=True),
        sa.Column("actual_departure_date", sa.Date(), nullable=True),
        sa.Column("actual_arrival_date", sa.Date(), nullable=True),
        sa.Column("ready_for_pickup_date", sa.Date(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'draft'")),
        sa.Column("remark", sa.Text(), nullable=True),
        sa.Column("row_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("updated_by", sa.Text(), nullable=True),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("pi_code", name="uq_pi_order_header_pi_code"),
        sa.UniqueConstraint("country_code", "order_month", "pi_sequence_no", name="uq_pi_order_header_country_month_seq"),
        schema="ordering",
    )
    op.create_index("ix_pi_order_header_country_month", "pi_order_header", ["country_code", "order_month"], schema="ordering")
    op.create_index("ix_pi_order_header_status", "pi_order_header", ["status"], schema="ordering")

    op.create_table(
        "pi_order_line",
        sa.Column("pi_line_id", PGUUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("pi_id", PGUUID(as_uuid=True), sa.ForeignKey("ordering.pi_order_header.pi_id", ondelete="CASCADE"), nullable=False),
        sa.Column("pi_code", sa.Text(), nullable=False),
        sa.Column("pi_line_code", sa.Text(), nullable=False),
        sa.Column("line_sequence_no", sa.Integer(), nullable=False),
        sa.Column("material_code", sa.Text(), nullable=True),
        sa.Column("bom", sa.Text(), nullable=True),
        sa.Column("brand", sa.Text(), nullable=True),
        sa.Column("model_name", sa.Text(), nullable=True),
        sa.Column("version", sa.Text(), nullable=True),
        sa.Column("powertrain", sa.Text(), nullable=True),
        sa.Column("exterior_color_name", sa.Text(), nullable=True),
        sa.Column("exterior_color_code", sa.Text(), nullable=True),
        sa.Column("interior_color_name", sa.Text(), nullable=True),
        sa.Column("interior_colour_code", sa.Text(), nullable=True),
        sa.Column("quantity", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("fob_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("amount_eur", sa.Numeric(14, 2), nullable=True),
        sa.Column("remark", sa.Text(), nullable=True),
        sa.Column("row_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("updated_by", sa.Text(), nullable=True),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("pi_line_code", name="uq_pi_order_line_code"),
        sa.UniqueConstraint("pi_id", "line_sequence_no", name="uq_pi_order_line_pi_line_seq"),
        sa.CheckConstraint("quantity >= 0", name="ck_pi_order_line_quantity_non_negative"),
        schema="ordering",
    )
    op.create_index("ix_pi_order_line_pi_id", "pi_order_line", ["pi_id"], schema="ordering")
    op.create_index("ix_pi_order_line_material_code", "pi_order_line", ["material_code"], schema="ordering")

    op.create_table(
        "pi_vehicle_unit",
        sa.Column("vehicle_unit_id", PGUUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("pi_id", PGUUID(as_uuid=True), sa.ForeignKey("ordering.pi_order_header.pi_id", ondelete="CASCADE"), nullable=False),
        sa.Column("pi_line_id", PGUUID(as_uuid=True), sa.ForeignKey("ordering.pi_order_line.pi_line_id", ondelete="CASCADE"), nullable=False),
        sa.Column("pi_code", sa.Text(), nullable=False),
        sa.Column("pi_line_code", sa.Text(), nullable=False),
        sa.Column("car_code", sa.Text(), nullable=False),
        sa.Column("vin", sa.Text(), nullable=True),
        sa.Column("material_code", sa.Text(), nullable=True),
        sa.Column("bom", sa.Text(), nullable=True),
        sa.Column("brand", sa.Text(), nullable=True),
        sa.Column("model_name", sa.Text(), nullable=True),
        sa.Column("version", sa.Text(), nullable=True),
        sa.Column("powertrain", sa.Text(), nullable=True),
        sa.Column("exterior_color_name", sa.Text(), nullable=True),
        sa.Column("exterior_color_code", sa.Text(), nullable=True),
        sa.Column("interior_color_name", sa.Text(), nullable=True),
        sa.Column("interior_colour_code", sa.Text(), nullable=True),
        sa.Column("production_date", sa.Date(), nullable=True),
        sa.Column("etd", sa.Date(), nullable=True),
        sa.Column("eta", sa.Date(), nullable=True),
        sa.Column("actual_departure_date", sa.Date(), nullable=True),
        sa.Column("actual_arrival_date", sa.Date(), nullable=True),
        sa.Column("ready_for_pickup_date", sa.Date(), nullable=True),
        sa.Column("ship_name", sa.Text(), nullable=True),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("dealer_code", sa.Text(), nullable=True),
        sa.Column("dealer_name", sa.Text(), nullable=True),
        sa.Column("customer_ref", sa.Text(), nullable=True),
        sa.Column("allocation_status", sa.Text(), nullable=False, server_default=sa.text("'unallocated'")),
        sa.Column("logistics_status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("remark", sa.Text(), nullable=True),
        sa.Column("row_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("updated_by", sa.Text(), nullable=True),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("car_code", name="uq_pi_vehicle_unit_car_code"),
        sa.UniqueConstraint("pi_code", "car_code", name="uq_pi_vehicle_unit_pi_car"),
        schema="ordering",
    )
    op.create_index("uq_pi_vehicle_unit_vin_not_null", "pi_vehicle_unit", ["vin"], unique=True, schema="ordering", postgresql_where=sa.text("vin IS NOT NULL AND vin <> ''"))
    op.create_index("ix_pi_vehicle_unit_pi_code", "pi_vehicle_unit", ["pi_code"], schema="ordering")
    op.create_index("ix_pi_vehicle_unit_line_code", "pi_vehicle_unit", ["pi_line_code"], schema="ordering")
    op.create_index("ix_pi_vehicle_unit_country_status", "pi_vehicle_unit", ["country_code", "allocation_status", "logistics_status"], schema="ordering")
    op.create_index("ix_pi_vehicle_unit_eta", "pi_vehicle_unit", ["eta"], schema="ordering")
    op.create_index("ix_pi_vehicle_unit_ready", "pi_vehicle_unit", ["ready_for_pickup_date"], schema="ordering")


def downgrade() -> None:
    op.drop_table("pi_vehicle_unit", schema="ordering")
    op.drop_table("pi_order_line", schema="ordering")
    op.drop_table("pi_order_header", schema="ordering")
