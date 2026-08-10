"""Release engineering configuration source context and version snapshots.

Revision ID: 20260722_0047
Revises: 20260715_0046
Create Date: 2026-07-22 00:47:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260722_0047"
down_revision = "20260715_0046"
branch_labels = None
depends_on = None

SCHEMA = "engineering_config"
SOURCE_TRIM_INDEX = "uq_vehicle_trims_source_full_name"
TRIM_VERSION_INDEX = "uq_config_versions_trim_version_no"
PUBLISHED_IDENTITY_INDEX = "uq_config_versions_single_published_identity"


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _has_table(table_name: str) -> bool:
    return table_name in _inspector().get_table_names(schema=SCHEMA)


def _has_column(table_name: str, column_name: str) -> bool:
    return any(
        column["name"] == column_name
        for column in _inspector().get_columns(table_name, schema=SCHEMA)
    )


def _has_index(table_name: str, index_name: str) -> bool:
    return any(
        index["name"] == index_name
        for index in _inspector().get_indexes(table_name, schema=SCHEMA)
    )


def _ensure_source_context_table() -> None:
    table_name = "engineering_config_source_context_links"
    if not _has_table(table_name):
        op.create_table(
            table_name,
            sa.Column(
                "id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column(
                "source_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("ops.import_batches.import_batch_id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "batch_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("ops.import_batches.import_batch_id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("brand", sa.Text(), nullable=True),
            sa.Column("model_name", sa.Text(), nullable=True),
            sa.Column("model_year", sa.Text(), nullable=True),
            sa.Column("market", sa.Text(), nullable=True),
            sa.Column("country", sa.Text(), nullable=True),
            sa.Column("powertrain", sa.Text(), nullable=True),
            sa.Column("segment", sa.Text(), nullable=True),
            sa.Column(
                "trim_ids",
                postgresql.JSONB(),
                nullable=False,
                server_default=sa.text("'[]'::jsonb"),
            ),
            sa.Column(
                "sales_version_ids",
                postgresql.JSONB(),
                nullable=False,
                server_default=sa.text("'[]'::jsonb"),
            ),
            sa.Column(
                "context_type",
                sa.Text(),
                nullable=False,
                server_default=sa.text("'compare'"),
            ),
            sa.Column("scenario", sa.Text(), nullable=True),
            sa.Column("identity_anchor", sa.Text(), nullable=True),
            sa.Column(
                "status",
                sa.Text(),
                nullable=False,
                server_default=sa.text("'active'"),
            ),
            sa.Column("created_by", sa.Text(), nullable=True),
            sa.Column(
                "created_at_utc",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            schema=SCHEMA,
        )
    else:
        optional_columns = (
            ("powertrain", sa.Column("powertrain", sa.Text(), nullable=True)),
            ("segment", sa.Column("segment", sa.Text(), nullable=True)),
            ("scenario", sa.Column("scenario", sa.Text(), nullable=True)),
            ("identity_anchor", sa.Column("identity_anchor", sa.Text(), nullable=True)),
            (
                "status",
                sa.Column(
                    "status",
                    sa.Text(),
                    nullable=False,
                    server_default=sa.text("'active'"),
                ),
            ),
        )
        for column_name, column in optional_columns:
            if not _has_column(table_name, column_name):
                op.add_column(table_name, column, schema=SCHEMA)

    indexes = (
        ("ix_eng_config_source_context_source", ["source_id"]),
        ("ix_eng_config_source_context_batch", ["batch_id"]),
        ("ix_eng_config_source_context_brand_model", ["brand", "model_name"]),
        ("ix_eng_config_source_context_market_year", ["market", "model_year"]),
        ("ix_eng_config_source_context_market_segment", ["market", "segment"]),
        ("ix_eng_config_source_context_status", ["status"]),
    )
    for index_name, columns in indexes:
        if not _has_index(table_name, index_name):
            op.create_index(index_name, table_name, columns, schema=SCHEMA)


def _ensure_source_scoped_trim_identity() -> None:
    if _has_index("vehicle_trims", SOURCE_TRIM_INDEX):
        return
    duplicate = op.get_bind().execute(sa.text("""
        SELECT source_upload_id, full_trim_name, COUNT(*) AS duplicate_count
        FROM engineering_config.vehicle_trims
        WHERE source_upload_id IS NOT NULL
        GROUP BY source_upload_id, full_trim_name
        HAVING COUNT(*) > 1
        LIMIT 1
    """)).mappings().first()
    if duplicate is not None:
        raise RuntimeError(
            "Cannot enforce source-scoped trim identity while duplicate "
            f"vehicle_trims exist for source={duplicate['source_upload_id']} "
            f"and full_trim_name={duplicate['full_trim_name']!r}. "
            "Reconcile duplicate draft columns before retrying the migration."
        )
    op.create_index(
        SOURCE_TRIM_INDEX,
        "vehicle_trims",
        ["source_upload_id", "full_trim_name"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("source_upload_id IS NOT NULL"),
    )


def _ensure_version_snapshots() -> None:
    if not _has_column("config_versions", "snapshot_values"):
        op.add_column(
            "config_versions",
            sa.Column(
                "snapshot_values",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
            ),
            schema=SCHEMA,
        )
    if not _has_column("config_versions", "snapshot_feature_count"):
        op.add_column(
            "config_versions",
            sa.Column(
                "snapshot_feature_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
            ),
            schema=SCHEMA,
        )


def _ensure_version_invariants() -> None:
    connection = op.get_bind()
    if not _has_index("config_versions", TRIM_VERSION_INDEX):
        duplicate_version = connection.execute(sa.text("""
            SELECT trim_id, version_no, COUNT(*) AS duplicate_count
            FROM engineering_config.config_versions
            GROUP BY trim_id, version_no
            HAVING COUNT(*) > 1
            LIMIT 1
        """)).mappings().first()
        if duplicate_version is not None:
            raise RuntimeError(
                "Cannot enforce unique configuration version numbers while duplicate "
                f"rows exist for trim={duplicate_version['trim_id']} and "
                f"version_no={duplicate_version['version_no']}."
            )
        op.create_index(
            TRIM_VERSION_INDEX,
            "config_versions",
            ["trim_id", "version_no"],
            unique=True,
            schema=SCHEMA,
        )

    if not _has_index("config_versions", PUBLISHED_IDENTITY_INDEX):
        duplicate_published = connection.execute(sa.text("""
            SELECT identity_key, COUNT(*) AS duplicate_count
            FROM engineering_config.config_versions
            WHERE status = 'published'
            GROUP BY identity_key
            HAVING COUNT(*) > 1
            LIMIT 1
        """)).mappings().first()
        if duplicate_published is not None:
            raise RuntimeError(
                "Cannot enforce one published configuration version while duplicate "
                "published rows exist for "
                f"identity={duplicate_published['identity_key']!r}."
            )
        op.create_index(
            PUBLISHED_IDENTITY_INDEX,
            "config_versions",
            ["identity_key"],
            unique=True,
            schema=SCHEMA,
            postgresql_where=sa.text("status = 'published'"),
        )


def upgrade() -> None:
    _ensure_source_context_table()
    _ensure_source_scoped_trim_identity()
    _ensure_version_snapshots()
    _ensure_version_invariants()


def downgrade() -> None:
    if _has_index("config_versions", PUBLISHED_IDENTITY_INDEX):
        op.drop_index(PUBLISHED_IDENTITY_INDEX, table_name="config_versions", schema=SCHEMA)
    if _has_index("config_versions", TRIM_VERSION_INDEX):
        op.drop_index(TRIM_VERSION_INDEX, table_name="config_versions", schema=SCHEMA)
    if _has_column("config_versions", "snapshot_feature_count"):
        op.drop_column("config_versions", "snapshot_feature_count", schema=SCHEMA)
    if _has_column("config_versions", "snapshot_values"):
        op.drop_column("config_versions", "snapshot_values", schema=SCHEMA)
    if _has_index("vehicle_trims", SOURCE_TRIM_INDEX):
        op.drop_index(SOURCE_TRIM_INDEX, table_name="vehicle_trims", schema=SCHEMA)
    if _has_table("engineering_config_source_context_links"):
        op.drop_table("engineering_config_source_context_links", schema=SCHEMA)
