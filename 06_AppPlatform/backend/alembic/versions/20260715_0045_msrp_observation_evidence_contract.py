"""Add replayable evidence linkage to governed MSRP facts.

Revision ID: 20260715_0045
Revises: 20260714_0044
Create Date: 2026-07-15 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260715_0045"
down_revision = "20260714_0044"
branch_labels = None
depends_on = None


UUID = postgresql.UUID(as_uuid=True)
JSONB = postgresql.JSONB()


def upgrade() -> None:
    op.add_column(
        "observations",
        sa.Column("source_version_id", UUID, nullable=True),
        schema="msrp",
    )
    op.create_foreign_key(
        "fk_msrp_observations_source_version",
        "observations",
        "source_versions",
        ["source_version_id"],
        ["source_version_id"],
        source_schema="msrp",
        referent_schema="msrp",
    )
    op.create_index(
        "ix_msrp_observations_source_version",
        "observations",
        ["source_version_id"],
        schema="msrp",
    )

    op.create_table(
        "observation_evidence_links",
        sa.Column(
            "observation_evidence_link_id",
            UUID,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "observation_id",
            UUID,
            sa.ForeignKey("msrp.observations.observation_id"),
            nullable=False,
        ),
        sa.Column(
            "evidence_asset_id",
            UUID,
            sa.ForeignKey("msrp.source_evidence_assets.evidence_asset_id"),
            nullable=False,
        ),
        sa.Column(
            "source_version_id",
            UUID,
            sa.ForeignKey("msrp.source_versions.source_version_id"),
            nullable=False,
        ),
        sa.Column("evidence_role", sa.Text(), nullable=False),
        sa.Column("evidence_sha256", sa.Text(), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column(
            "linked_at_utc",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "evidence_role IN ('raw_payload', 'price_page', 'supporting')",
            name="ck_msrp_observation_evidence_links_role",
        ),
        sa.CheckConstraint(
            "evidence_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_msrp_observation_evidence_links_sha256",
        ),
        sa.UniqueConstraint(
            "observation_id",
            "evidence_asset_id",
            "evidence_role",
            name="uq_msrp_observation_evidence_links_asset_role",
        ),
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_observation_evidence_links_observation_role",
        "observation_evidence_links",
        ["observation_id", "evidence_role"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_observation_evidence_links_evidence_asset",
        "observation_evidence_links",
        ["evidence_asset_id"],
        schema="msrp",
    )
    op.create_index(
        "ix_msrp_observation_evidence_links_source_version",
        "observation_evidence_links",
        ["source_version_id"],
        schema="msrp",
    )

    for table_name in ("current_prices", "price_history"):
        op.add_column(
            table_name,
            sa.Column("source_version_id", UUID, nullable=True),
            schema="msrp",
        )
        op.add_column(
            table_name,
            sa.Column(
                "evidence_refs_json",
                JSONB,
                nullable=False,
                server_default=sa.text("'[]'::jsonb"),
            ),
            schema="msrp",
        )
        op.create_foreign_key(
            f"fk_msrp_{table_name}_source_version",
            table_name,
            "source_versions",
            ["source_version_id"],
            ["source_version_id"],
            source_schema="msrp",
            referent_schema="msrp",
        )
        op.create_index(
            f"ix_msrp_{table_name}_source_version",
            table_name,
            ["source_version_id"],
            schema="msrp",
        )


def downgrade() -> None:
    for table_name in ("price_history", "current_prices"):
        op.drop_index(
            f"ix_msrp_{table_name}_source_version",
            table_name=table_name,
            schema="msrp",
        )
        op.drop_constraint(
            f"fk_msrp_{table_name}_source_version",
            table_name,
            schema="msrp",
            type_="foreignkey",
        )
        op.drop_column(table_name, "evidence_refs_json", schema="msrp")
        op.drop_column(table_name, "source_version_id", schema="msrp")

    op.drop_index(
        "ix_msrp_observation_evidence_links_source_version",
        table_name="observation_evidence_links",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_observation_evidence_links_evidence_asset",
        table_name="observation_evidence_links",
        schema="msrp",
    )
    op.drop_index(
        "ix_msrp_observation_evidence_links_observation_role",
        table_name="observation_evidence_links",
        schema="msrp",
    )
    op.drop_table("observation_evidence_links", schema="msrp")
    op.drop_index(
        "ix_msrp_observations_source_version",
        table_name="observations",
        schema="msrp",
    )
    op.drop_constraint(
        "fk_msrp_observations_source_version",
        "observations",
        schema="msrp",
        type_="foreignkey",
    )
    op.drop_column("observations", "source_version_id", schema="msrp")
