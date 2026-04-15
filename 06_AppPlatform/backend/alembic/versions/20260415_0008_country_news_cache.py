"""Persist country news digests and article snapshots.

Revision ID: 20260415_0008
Revises: 20260412_0007
Create Date: 2026-04-15 22:30:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260415_0008"
down_revision = "20260412_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "country_news_digests",
        sa.Column(
            "country_news_digest_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("country_label", sa.Text(), nullable=False),
        sa.Column(
            "article_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "published_at_utc",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "synced_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("headline", sa.Text(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("highlights_json", postgresql.JSONB(), nullable=True),
        sa.Column("summary_provider", sa.Text(), nullable=True),
        sa.Column("summary_model", sa.Text(), nullable=True),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.UniqueConstraint(
            "country_code",
            name="uq_country_news_digests_country_code",
        ),
        schema="ops",
    )
    op.create_index(
        "ix_ops_country_news_digests_synced",
        "country_news_digests",
        ["synced_at_utc"],
        schema="ops",
    )
    op.create_index(
        "ix_ops_country_news_digests_published",
        "country_news_digests",
        ["published_at_utc"],
        schema="ops",
    )

    op.create_table(
        "country_news_articles",
        sa.Column(
            "country_news_article_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("country_label", sa.Text(), nullable=False),
        sa.Column("source_code", sa.Text(), nullable=False),
        sa.Column("publisher", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("raw_summary", sa.Text(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column(
            "published_at_utc",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("tags_json", postgresql.JSONB(), nullable=True),
        sa.Column("raw_payload_json", postgresql.JSONB(), nullable=True),
        sa.Column("intelligence_provider", sa.Text(), nullable=True),
        sa.Column("intelligence_model", sa.Text(), nullable=True),
        sa.Column(
            "synced_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "created_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.UniqueConstraint(
            "country_code",
            "source_url",
            name="uq_country_news_articles_country_url",
        ),
        schema="ops",
    )
    op.create_index(
        "ix_ops_country_news_articles_country_published",
        "country_news_articles",
        ["country_code", "published_at_utc"],
        schema="ops",
    )
    op.create_index(
        "ix_ops_country_news_articles_country_synced",
        "country_news_articles",
        ["country_code", "synced_at_utc"],
        schema="ops",
    )
    op.create_index(
        "ix_ops_country_news_articles_source_code",
        "country_news_articles",
        ["source_code"],
        schema="ops",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ops_country_news_articles_source_code",
        table_name="country_news_articles",
        schema="ops",
    )
    op.drop_index(
        "ix_ops_country_news_articles_country_synced",
        table_name="country_news_articles",
        schema="ops",
    )
    op.drop_index(
        "ix_ops_country_news_articles_country_published",
        table_name="country_news_articles",
        schema="ops",
    )
    op.drop_table("country_news_articles", schema="ops")

    op.drop_index(
        "ix_ops_country_news_digests_published",
        table_name="country_news_digests",
        schema="ops",
    )
    op.drop_index(
        "ix_ops_country_news_digests_synced",
        table_name="country_news_digests",
        schema="ops",
    )
    op.drop_table("country_news_digests", schema="ops")
