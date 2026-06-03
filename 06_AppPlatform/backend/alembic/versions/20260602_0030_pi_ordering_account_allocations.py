"""Add PI ordering account and market allocation details.

Revision ID: 20260602_0030
Revises: 20260601_0029
Create Date: 2026-06-02
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID


revision = "20260602_0030"
down_revision = "20260601_0029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "pi_order_header",
        sa.Column("ordering_account_code", sa.Text(), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "pi_order_header",
        sa.Column("ordering_account_name", sa.Text(), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "pi_order_header",
        sa.Column(
            "market_country_codes",
            JSONB(),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        schema="ordering",
    )
    op.add_column(
        "pi_order_header",
        sa.Column("shipment_batch_code", sa.Text(), nullable=True),
        schema="ordering",
    )
    op.add_column(
        "pi_order_header",
        sa.Column("port_of_discharge", sa.Text(), nullable=True),
        schema="ordering",
    )
    op.execute(
        """
        UPDATE ordering.pi_order_header
        SET ordering_account_code = country_code,
            ordering_account_name = country_name,
            market_country_codes = jsonb_build_array(country_code)
        WHERE ordering_account_code IS NULL
        """
    )
    op.alter_column(
        "pi_order_header",
        "ordering_account_code",
        existing_type=sa.Text(),
        nullable=False,
        schema="ordering",
    )
    op.drop_constraint(
        "uq_pi_order_header_country_month_seq",
        "pi_order_header",
        schema="ordering",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_pi_order_header_account_month_seq",
        "pi_order_header",
        ["ordering_account_code", "order_month", "pi_sequence_no"],
        schema="ordering",
    )
    op.create_index(
        "ix_pi_order_header_ordering_account",
        "pi_order_header",
        ["ordering_account_code", "order_month"],
        schema="ordering",
    )

    op.create_table(
        "pi_order_line_allocation",
        sa.Column("pi_line_allocation_id", PGUUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("pi_id", PGUUID(as_uuid=True), sa.ForeignKey("ordering.pi_order_header.pi_id", ondelete="CASCADE"), nullable=False),
        sa.Column("pi_line_id", PGUUID(as_uuid=True), sa.ForeignKey("ordering.pi_order_line.pi_line_id", ondelete="CASCADE"), nullable=False),
        sa.Column("pi_code", sa.Text(), nullable=False),
        sa.Column("pi_line_code", sa.Text(), nullable=False),
        sa.Column("market_country_code", sa.Text(), nullable=False),
        sa.Column("order_year", sa.Integer(), nullable=False),
        sa.Column("order_month", sa.Integer(), nullable=False),
        sa.Column("material_code", sa.Text(), nullable=True),
        sa.Column("quantity", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("fob_eur", sa.Numeric(12, 2), nullable=True),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("updated_by", sa.Text(), nullable=True),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("pi_line_id", "market_country_code", name="uq_pi_order_line_alloc_line_country"),
        sa.CheckConstraint("quantity >= 0", name="ck_pi_order_line_alloc_quantity_non_negative"),
        schema="ordering",
    )
    op.create_index("ix_pi_order_line_alloc_line", "pi_order_line_allocation", ["pi_line_id"], schema="ordering")
    op.create_index(
        "ix_pi_order_line_alloc_market_month",
        "pi_order_line_allocation",
        ["market_country_code", "order_year", "order_month"],
        schema="ordering",
    )
    op.create_index("ix_pi_order_line_alloc_material", "pi_order_line_allocation", ["material_code"], schema="ordering")

    op.execute(
        """
        INSERT INTO ordering.pi_order_line_allocation (
            pi_id,
            pi_line_id,
            pi_code,
            pi_line_code,
            market_country_code,
            order_year,
            order_month,
            material_code,
            quantity,
            fob_eur,
            created_by,
            updated_by
        )
        SELECT
            h.pi_id,
            l.pi_line_id,
            l.pi_code,
            l.pi_line_code,
            h.country_code,
            substring(h.order_month, 1, 4)::integer,
            substring(h.order_month, 6, 2)::integer,
            l.material_code,
            l.quantity,
            l.fob_eur,
            l.created_by,
            l.updated_by
        FROM ordering.pi_order_line l
        JOIN ordering.pi_order_header h ON h.pi_id = l.pi_id
        """
    )


def downgrade() -> None:
    op.drop_table("pi_order_line_allocation", schema="ordering")
    op.drop_index("ix_pi_order_header_ordering_account", table_name="pi_order_header", schema="ordering")
    op.drop_constraint(
        "uq_pi_order_header_account_month_seq",
        "pi_order_header",
        schema="ordering",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_pi_order_header_country_month_seq",
        "pi_order_header",
        ["country_code", "order_month", "pi_sequence_no"],
        schema="ordering",
    )
    op.alter_column(
        "pi_order_header",
        "ordering_account_code",
        existing_type=sa.Text(),
        nullable=True,
        schema="ordering",
    )
    op.drop_column("pi_order_header", "port_of_discharge", schema="ordering")
    op.drop_column("pi_order_header", "shipment_batch_code", schema="ordering")
    op.drop_column("pi_order_header", "market_country_codes", schema="ordering")
    op.drop_column("pi_order_header", "ordering_account_name", schema="ordering")
    op.drop_column("pi_order_header", "ordering_account_code", schema="ordering")
