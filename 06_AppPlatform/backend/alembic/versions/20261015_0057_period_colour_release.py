"""Country/period colour release; existing periods remain unrestricted."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20261015_0057"
down_revision = "20261012_0056"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("country_template_fob_period",
                  sa.Column("allowed_colour_codes", postgresql.JSONB(none_as_null=True), nullable=True),
                  schema="ordering")


def downgrade() -> None:
    op.drop_column("country_template_fob_period", "allowed_colour_codes", schema="ordering")
