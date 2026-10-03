"""Per-vehicle EUR costs, independent of PI FOB snapshots."""
from alembic import op
import sqlalchemy as sa

revision = "20261004_0053"
down_revision = "20261001_0052"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name in ("freight_eur", "insurance_eur"):
        op.add_column("pi_vehicle_unit", sa.Column(name, sa.Numeric(14, 2), nullable=True), schema="ordering")
        op.create_check_constraint(f"ck_vehicle_{name}", "pi_vehicle_unit", f"{name} >= 0 AND {name} < 1000000000000", schema="ordering")


def downgrade() -> None:
    for name in ("insurance_eur", "freight_eur"):
        op.drop_constraint(f"ck_vehicle_{name}", "pi_vehicle_unit", schema="ordering", type_="check")
        op.drop_column("pi_vehicle_unit", name, schema="ordering")
