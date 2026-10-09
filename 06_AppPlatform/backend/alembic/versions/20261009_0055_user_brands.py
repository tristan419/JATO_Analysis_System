"""Account brands and reuse of permission requests for brand assignments."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20261009_0055"
down_revision = "20261008_0054"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("brands", postgresql.JSONB(), nullable=False,
                                   server_default=sa.text("'[]'::jsonb")), schema="auth")
    op.execute("UPDATE auth.users SET brands = '[\"OMODA\", \"JAECOO\"]'::jsonb WHERE role = 'order_filler'")
    op.add_column("role_upgrade_requests", sa.Column("requested_brands", postgresql.JSONB(), nullable=True), schema="auth")


def downgrade() -> None:
    op.drop_column("role_upgrade_requests", "requested_brands", schema="auth")
    op.drop_column("users", "brands", schema="auth")
