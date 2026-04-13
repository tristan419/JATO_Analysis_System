"""Track last confirmed timestamps on open price history periods.

Revision ID: 20260412_0007
Revises: 20260411_0006
Create Date: 2026-04-12 15:10:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260412_0007"
down_revision = "20260411_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "price_history",
        sa.Column(
            "last_confirmed_at_utc",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        schema="msrp",
    )
    op.add_column(
        "price_history",
        sa.Column(
            "last_confirmed_by_observation_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        schema="msrp",
    )
    op.create_foreign_key(
        "fk_msrp_price_history_last_confirmed_observation",
        "price_history",
        "observations",
        ["last_confirmed_by_observation_id"],
        ["observation_id"],
        source_schema="msrp",
        referent_schema="msrp",
    )
    op.execute(
        """
        UPDATE msrp.price_history
        SET last_confirmed_at_utc = valid_from_utc,
            last_confirmed_by_observation_id = started_by_observation_id
        WHERE last_confirmed_at_utc IS NULL
           OR last_confirmed_by_observation_id IS NULL
        """
    )
    op.alter_column(
        "price_history",
        "last_confirmed_at_utc",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
        schema="msrp",
    )
    op.alter_column(
        "price_history",
        "last_confirmed_by_observation_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=False,
        schema="msrp",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_msrp_price_history_last_confirmed_observation",
        "price_history",
        schema="msrp",
        type_="foreignkey",
    )
    op.drop_column(
        "price_history",
        "last_confirmed_by_observation_id",
        schema="msrp",
    )
    op.drop_column(
        "price_history",
        "last_confirmed_at_utc",
        schema="msrp",
    )
