"""Shared material files/bindings and immutable configuration values."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "20261031_0062"
down_revision = "20261028_0061"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("config_versions", sa.Column("values_snapshot_json", pg.JSONB(), nullable=True), schema="engineering_config")
    def timestamps():
        return [sa.Column("created_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
                sa.Column("updated_at_utc", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False)]
    op.create_table("material_asset_group",
        sa.Column("asset_group_id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("brand", sa.Text(), nullable=False), sa.Column("model_name", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False), sa.Column("colour_key", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False), sa.Column("row_version", sa.Integer(), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False), *timestamps(),
        sa.CheckConstraint("kind IN ('exterior', 'interior')", name="ck_material_asset_group_kind"), schema="ordering")
    op.create_index("ix_material_asset_group_model", "material_asset_group", ["brand", "model_name"], schema="ordering")
    op.create_table("material_asset_file",
        sa.Column("asset_file_id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("asset_group_id", pg.UUID(as_uuid=True), sa.ForeignKey("ordering.material_asset_group.asset_group_id")),
        sa.Column("config_version_id", pg.UUID(as_uuid=True), sa.ForeignKey("engineering_config.config_versions.version_id")),
        sa.Column("filename", sa.Text(), nullable=False), sa.Column("content_type", sa.Text(), nullable=False),
        sa.Column("sha256", sa.Text(), nullable=False), sa.Column("content", sa.LargeBinary(), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False), *timestamps(),
        sa.CheckConstraint("(asset_group_id IS NULL) <> (config_version_id IS NULL)", name="ck_material_asset_file_owner"), schema="ordering")
    op.create_table("material_asset_binding",
        sa.Column("binding_id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("brand", sa.Text(), nullable=False), sa.Column("model_name", sa.Text(), nullable=False),
        sa.Column("bom_template", sa.Text(), nullable=False), sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("colour_key", sa.Text(), nullable=False),
        sa.Column("asset_group_id", pg.UUID(as_uuid=True), sa.ForeignKey("ordering.material_asset_group.asset_group_id")),
        sa.Column("config_version_id", pg.UUID(as_uuid=True), sa.ForeignKey("engineering_config.config_versions.version_id")),
        sa.Column("row_version", sa.Integer(), nullable=False), sa.Column("updated_by", sa.Text(), nullable=False), *timestamps(),
        sa.UniqueConstraint("brand", "model_name", "bom_template", "kind", "colour_key", name="uq_material_asset_binding"),
        sa.CheckConstraint("(kind = 'config' AND config_version_id IS NOT NULL AND asset_group_id IS NULL AND colour_key = '') OR (kind IN ('exterior', 'interior') AND asset_group_id IS NOT NULL AND config_version_id IS NULL)", name="ck_material_asset_binding_kind"), schema="ordering")


def downgrade() -> None:
    for name in ("material_asset_binding", "material_asset_file", "material_asset_group"):
        op.drop_table(name, schema="ordering")
    op.drop_column("config_versions", "values_snapshot_json", schema="engineering_config")
