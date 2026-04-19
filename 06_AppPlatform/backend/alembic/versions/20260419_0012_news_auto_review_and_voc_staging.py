"""Persist news auto review and add VOC staging tables.

Revision ID: 20260419_0012
Revises: 20260417_0011
Create Date: 2026-04-19 10:08:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260419_0012"
down_revision = "20260417_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "country_news_digests",
        sa.Column("auto_review_json", postgresql.JSONB(), nullable=True),
        schema="ops",
    )
    op.add_column(
        "country_news_digests",
        sa.Column("publish_tier", sa.Text(), nullable=True),
        schema="ops",
    )
    op.add_column(
        "country_news_digests",
        sa.Column("publish_decision", sa.Text(), nullable=True),
        schema="ops",
    )
    op.add_column(
        "country_news_articles",
        sa.Column("auto_review_json", postgresql.JSONB(), nullable=True),
        schema="ops",
    )
    op.add_column(
        "country_news_articles",
        sa.Column("publish_tier", sa.Text(), nullable=True),
        schema="ops",
    )
    op.add_column(
        "country_news_articles",
        sa.Column("publish_decision", sa.Text(), nullable=True),
        schema="ops",
    )

    op.create_table(
        "voc_source_runs",
        sa.Column(
            "voc_source_run_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("country_label", sa.Text(), nullable=False),
        sa.Column("source_code", sa.Text(), nullable=False),
        sa.Column("site_name", sa.Text(), nullable=False),
        sa.Column("site_type", sa.Text(), nullable=False),
        sa.Column("language", sa.Text(), nullable=True),
        sa.Column("taxonomy_profile", sa.Text(), nullable=True),
        sa.Column("collected_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_file_path", sa.Text(), nullable=True),
        sa.Column("source_meta_json", postgresql.JSONB(), nullable=True),
        sa.Column("landing_page_json", postgresql.JSONB(), nullable=True),
        sa.Column("collection_strategy_json", postgresql.JSONB(), nullable=True),
        sa.Column("taxonomy_json", postgresql.JSONB(), nullable=True),
        sa.Column("auto_review_json", postgresql.JSONB(), nullable=True),
        sa.Column("publish_tier", sa.Text(), nullable=True),
        sa.Column("publish_decision", sa.Text(), nullable=True),
        sa.Column("candidate_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("document_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("publish_ready_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("errors_json", postgresql.JSONB(), nullable=True),
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
            "source_code",
            "collected_at_utc",
            name="uq_voc_source_runs_source_collected",
        ),
        schema="ops",
    )
    op.create_index(
        "ix_ops_voc_source_runs_country_collected",
        "voc_source_runs",
        ["country_code", "collected_at_utc"],
        unique=False,
        schema="ops",
    )
    op.create_index(
        "ix_ops_voc_source_runs_publish_tier",
        "voc_source_runs",
        ["publish_tier"],
        unique=False,
        schema="ops",
    )

    op.create_table(
        "voc_raw_documents",
        sa.Column(
            "voc_raw_document_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "voc_source_run_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("country_code", sa.Text(), nullable=False),
        sa.Column("country_label", sa.Text(), nullable=False),
        sa.Column("source_code", sa.Text(), nullable=False),
        sa.Column("site_name", sa.Text(), nullable=False),
        sa.Column("site_type", sa.Text(), nullable=False),
        sa.Column("language", sa.Text(), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("page_kind", sa.Text(), nullable=True),
        sa.Column("link_text", sa.Text(), nullable=True),
        sa.Column("published_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("excerpt", sa.Text(), nullable=True),
        sa.Column("raw_text", sa.Text(), nullable=False),
        sa.Column("collected_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("auto_review_json", postgresql.JSONB(), nullable=True),
        sa.Column("publish_tier", sa.Text(), nullable=True),
        sa.Column("publish_decision", sa.Text(), nullable=True),
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
        sa.ForeignKeyConstraint(
            ["voc_source_run_id"],
            ["ops.voc_source_runs.voc_source_run_id"],
        ),
        sa.UniqueConstraint(
            "voc_source_run_id",
            "source_url",
            name="uq_voc_raw_documents_run_url",
        ),
        schema="ops",
    )
    op.create_index(
        "ix_ops_voc_raw_documents_country_collected",
        "voc_raw_documents",
        ["country_code", "collected_at_utc"],
        unique=False,
        schema="ops",
    )
    op.create_index(
        "ix_ops_voc_raw_documents_publish_tier",
        "voc_raw_documents",
        ["publish_tier"],
        unique=False,
        schema="ops",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ops_voc_raw_documents_publish_tier",
        table_name="voc_raw_documents",
        schema="ops",
    )
    op.drop_index(
        "ix_ops_voc_raw_documents_country_collected",
        table_name="voc_raw_documents",
        schema="ops",
    )
    op.drop_table("voc_raw_documents", schema="ops")

    op.drop_index(
        "ix_ops_voc_source_runs_publish_tier",
        table_name="voc_source_runs",
        schema="ops",
    )
    op.drop_index(
        "ix_ops_voc_source_runs_country_collected",
        table_name="voc_source_runs",
        schema="ops",
    )
    op.drop_table("voc_source_runs", schema="ops")

    op.drop_column("country_news_articles", "publish_decision", schema="ops")
    op.drop_column("country_news_articles", "publish_tier", schema="ops")
    op.drop_column("country_news_articles", "auto_review_json", schema="ops")
    op.drop_column("country_news_digests", "publish_decision", schema="ops")
    op.drop_column("country_news_digests", "publish_tier", schema="ops")
    op.drop_column("country_news_digests", "auto_review_json", schema="ops")
