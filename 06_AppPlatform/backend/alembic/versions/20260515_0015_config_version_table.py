"""Add ConfigVersion table and VehicleTrim identity fields.

Revision ID: 20260515_0015
Revises: 20260515_0014
Create Date: 2026-05-15
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PGUUID

revision = "20260515_0015"
down_revision = "20260515_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("vehicle_trims", sa.Column("identity_key", sa.Text, nullable=True), schema="engineering_config")
    op.add_column("vehicle_trims", sa.Column("material_no", sa.Text, nullable=True), schema="engineering_config")
    op.add_column("vehicle_trims", sa.Column("vehicle_code", sa.Text, nullable=True), schema="engineering_config")
    op.add_column("vehicle_trims", sa.Column("market", sa.Text, nullable=True), schema="engineering_config")
    op.create_index("ix_vehicle_trims_identity_key", "vehicle_trims", ["identity_key"], schema="engineering_config")

    op.create_table(
        "config_versions",
        sa.Column("version_id", PGUUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("trim_id", PGUUID(as_uuid=True), sa.ForeignKey("engineering_config.vehicle_trims.trim_id"), nullable=False),
        sa.Column("identity_key", sa.Text, nullable=False),
        sa.Column("material_no", sa.Text, nullable=True),
        sa.Column("vehicle_code", sa.Text, nullable=True),
        sa.Column("market", sa.Text, nullable=True),
        sa.Column("model_year", sa.Text, nullable=True),
        sa.Column("brand", sa.Text, nullable=False),
        sa.Column("model_name", sa.Text, nullable=False),
        sa.Column("trim_name", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default=sa.text("'draft'")),
        sa.Column("version_no", sa.Integer, nullable=False, server_default="1"),
        sa.Column("source_upload_id", PGUUID(as_uuid=True), sa.ForeignKey("ops.import_batches.import_batch_id"), nullable=True),
        sa.Column("parent_version_id", PGUUID(as_uuid=True), sa.ForeignKey("engineering_config.config_versions.version_id"), nullable=True),
        sa.Column("created_by", sa.Text, nullable=True),
        sa.Column("created_at_utc", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("published_by", sa.Text, nullable=True),
        sa.Column("published_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        schema="engineering_config",
    )
    op.create_index("ix_config_versions_identity_key", "config_versions", ["identity_key"], schema="engineering_config")
    op.create_index("ix_config_versions_status", "config_versions", ["status"], schema="engineering_config")
    op.create_index("ix_config_versions_trim_created", "config_versions", ["trim_id", "created_at_utc"], schema="engineering_config")


def downgrade() -> None:
    op.drop_table("config_versions", schema="engineering_config")
    op.drop_index("ix_vehicle_trims_identity_key", table_name="vehicle_trims", schema="engineering_config")
    op.drop_column("vehicle_trims", "market", schema="engineering_config")
    op.drop_column("vehicle_trims", "vehicle_code", schema="engineering_config")
    op.drop_column("vehicle_trims", "material_no", schema="engineering_config")
    op.drop_column("vehicle_trims", "identity_key", schema="engineering_config")
