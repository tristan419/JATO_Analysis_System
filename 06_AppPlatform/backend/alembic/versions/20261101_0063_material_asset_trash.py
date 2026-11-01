"""Recoverable material files and bounded image renditions."""
from alembic import op
import sqlalchemy as sa

revision = "20261101_0063"
down_revision = "20261031_0062"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table, schema in (("config_versions", "engineering_config"), ("material_asset_group", "ordering"), ("material_asset_file", "ordering")):
        op.add_column(table, sa.Column("deleted_at_utc", sa.DateTime(timezone=True)), schema=schema)
    for name in ("thumbnail", "display_image"):
        op.add_column("material_asset_file", sa.Column(name, sa.LargeBinary()), schema="ordering")
    for name in ("deleted_by", "upload_id"):
        op.add_column("material_asset_file", sa.Column(name, sa.Text()), schema="ordering")
    op.create_index("ix_ordering_material_asset_file_deleted_at_utc", "material_asset_file", ["deleted_at_utc"], schema="ordering")
    op.create_unique_constraint("uq_material_asset_file_upload_id", "material_asset_file", ["upload_id"], schema="ordering")


def downgrade() -> None:
    op.drop_constraint("uq_material_asset_file_upload_id", "material_asset_file", schema="ordering")
    op.drop_index("ix_ordering_material_asset_file_deleted_at_utc", table_name="material_asset_file", schema="ordering")
    for name in ("upload_id", "deleted_by", "display_image", "thumbnail", "deleted_at_utc"):
        op.drop_column("material_asset_file", name, schema="ordering")
    op.drop_column("material_asset_group", "deleted_at_utc", schema="ordering")
    op.drop_column("config_versions", "deleted_at_utc", schema="engineering_config")
