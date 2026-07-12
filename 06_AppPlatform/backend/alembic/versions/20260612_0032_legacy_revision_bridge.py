"""Bridge a legacy production Alembic revision with no schema operations."""

revision = "20260612_0032"
down_revision = "20260604_0031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Preserve compatibility with databases stamped by the legacy revision."""


def downgrade() -> None:
    """The legacy bridge has no schema operations to undo."""

