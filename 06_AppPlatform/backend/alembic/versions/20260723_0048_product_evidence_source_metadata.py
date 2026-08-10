"""Add governed source metadata for Product Evidence MCP.

Revision ID: 20260723_0048
Revises: 20260722_0047
Create Date: 2026-07-23 00:48:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260723_0048"
down_revision = "20260722_0047"
branch_labels = None
depends_on = None

SCHEMA = "engineering_config"
TABLE = "engineering_config_source_context_links"


def _has_column(column_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return any(
        column["name"] == column_name
        for column in inspector.get_columns(TABLE, schema=SCHEMA)
    )


def upgrade() -> None:
    columns = (
        ("source_role", sa.Column("source_role", sa.Text(), nullable=True)),
        ("document_type", sa.Column("document_type", sa.Text(), nullable=True)),
        ("source_url", sa.Column("source_url", sa.Text(), nullable=True)),
        ("effective_from", sa.Column("effective_from", sa.Date(), nullable=True)),
        ("effective_to", sa.Column("effective_to", sa.Date(), nullable=True)),
    )
    for name, column in columns:
        if not _has_column(name):
            op.add_column(TABLE, column, schema=SCHEMA)


def downgrade() -> None:
    for name in (
        "effective_to",
        "effective_from",
        "source_url",
        "document_type",
        "source_role",
    ):
        if _has_column(name):
            op.drop_column(TABLE, name, schema=SCHEMA)
