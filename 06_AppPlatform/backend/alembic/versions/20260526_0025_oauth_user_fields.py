"""Add OAuth provider fields to auth users (email, oauth_provider, oauth_subject, avatar_url).

Revision ID: 20260526_0025
Revises: 20260524_0024
Create Date: 2026-05-26
"""

from alembic import op
import sqlalchemy as sa


revision = "20260526_0025"
down_revision = "20260524_0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("email", sa.Text(), nullable=True),
        schema="auth",
    )
    op.add_column(
        "users",
        sa.Column("oauth_provider", sa.Text(), nullable=True),
        schema="auth",
    )
    op.add_column(
        "users",
        sa.Column("oauth_subject", sa.Text(), nullable=True),
        schema="auth",
    )
    op.add_column(
        "users",
        sa.Column("avatar_url", sa.Text(), nullable=True),
        schema="auth",
    )
    op.create_index(
        "ix_users_oauth_subject",
        "users",
        ["oauth_provider", "oauth_subject"],
        unique=True,
        schema="auth",
        postgresql_where=sa.text("oauth_provider IS NOT NULL AND oauth_subject IS NOT NULL"),
    )
    op.create_index(
        "ix_users_email",
        "users",
        ["email"],
        unique=True,
        schema="auth",
        postgresql_where=sa.text("email IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_users_email", table_name="users", schema="auth")
    op.drop_index("ix_users_oauth_subject", table_name="users", schema="auth")
    op.drop_column("users", "avatar_url", schema="auth")
    op.drop_column("users", "oauth_subject", schema="auth")
    op.drop_column("users", "oauth_provider", schema="auth")
    op.drop_column("users", "email", schema="auth")
