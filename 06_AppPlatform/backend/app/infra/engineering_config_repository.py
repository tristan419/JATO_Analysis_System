"""Database operations for engineering_config schema."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import Select, desc, select, update
from sqlalchemy.orm import Session

from app.db.models import (
    ConfigAuditLog,
    FeatureCatalog,
    ImportBatch,
    TrimFeatureValue,
    VehicleTrim,
)


def add_import_batch(session: Session, batch: ImportBatch) -> None:
    session.add(batch)


def get_import_batch(session: Session, import_batch_id: UUID) -> ImportBatch | None:
    return session.get(ImportBatch, import_batch_id)


def add_feature_catalog_batch(session: Session, features: list[FeatureCatalog]) -> None:
    session.add_all(features)


def list_feature_catalog(
    session: Session,
    category: str | None = None,
    is_active: bool | None = None,
    limit: int = 500,
) -> list[FeatureCatalog]:
    stmt: Select = select(FeatureCatalog).order_by(FeatureCatalog.display_order)
    if category is not None:
        stmt = stmt.where(FeatureCatalog.category == category)
    if is_active is not None:
        stmt = stmt.where(FeatureCatalog.is_active == is_active)
    stmt = stmt.limit(min(limit, 1000))
    return list(session.execute(stmt).scalars().all())


def get_feature_catalog_by_code(
    session: Session,
    feature_code: str,
) -> FeatureCatalog | None:
    stmt = select(FeatureCatalog).where(FeatureCatalog.feature_code == feature_code)
    return session.execute(stmt).scalars().first()


def add_vehicle_trim(session: Session, trim: VehicleTrim) -> None:
    session.add(trim)


def add_vehicle_trims_batch(session: Session, trims: list[VehicleTrim]) -> None:
    session.add_all(trims)


def list_vehicle_trims(
    session: Session,
    brand: str | None = None,
    model_name: str | None = None,
    status: str | None = None,
    limit: int = 200,
) -> list[VehicleTrim]:
    stmt = select(VehicleTrim).order_by(desc(VehicleTrim.created_at_utc))
    if brand is not None:
        stmt = stmt.where(VehicleTrim.brand == brand)
    if model_name is not None:
        stmt = stmt.where(VehicleTrim.model_name == model_name)
    if status is not None:
        stmt = stmt.where(VehicleTrim.status == status)
    stmt = stmt.limit(min(limit, 500))
    return list(session.execute(stmt).scalars().all())


def get_vehicle_trim(session: Session, trim_id: UUID) -> VehicleTrim | None:
    return session.get(VehicleTrim, trim_id)


def get_vehicle_trim_by_full_name(
    session: Session,
    full_trim_name: str,
) -> VehicleTrim | None:
    stmt = select(VehicleTrim).where(VehicleTrim.full_trim_name == full_trim_name)
    return session.execute(stmt).scalars().first()


def add_trim_feature_values_batch(
    session: Session,
    values: list[TrimFeatureValue],
) -> None:
    session.add_all(values)


def list_trim_feature_values(
    session: Session,
    trim_id: UUID,
    category: str | None = None,
    limit: int = 1000,
) -> list[TrimFeatureValue]:
    stmt = (
        select(TrimFeatureValue)
        .join(FeatureCatalog, TrimFeatureValue.feature_id == FeatureCatalog.feature_id)
        .where(TrimFeatureValue.trim_id == trim_id)
        .order_by(FeatureCatalog.display_order)
    )
    if category is not None:
        stmt = stmt.where(FeatureCatalog.category == category)
    stmt = stmt.limit(min(limit, 2000))
    return list(session.execute(stmt).scalars().all())


def get_trim_feature_value(
    session: Session,
    value_id: UUID,
) -> TrimFeatureValue | None:
    return session.get(TrimFeatureValue, value_id)


def update_trim_feature_value(
    session: Session,
    value_id: UUID,
    raw_value: str,
    normalized_value: str | None,
    availability: str,
    updated_by: str,
    expected_version: int,
) -> TrimFeatureValue | None:
    """Optimistic-lock update. Returns None if version mismatch."""
    stmt = (
        update(TrimFeatureValue)
        .where(
            TrimFeatureValue.value_id == value_id,
            TrimFeatureValue.version == expected_version,
        )
        .values(
            raw_value=raw_value,
            normalized_value=normalized_value,
            availability=availability,
            updated_by=updated_by,
            version=expected_version + 1,
        )
        .returning(TrimFeatureValue.value_id)
    )
    result = session.execute(stmt)
    row = result.fetchone()
    return row is not None


def add_audit_log(session: Session, entry: ConfigAuditLog) -> None:
    session.add(entry)


def list_audit_logs(
    session: Session,
    entity_type: str | None = None,
    entity_id: UUID | None = None,
    limit: int = 200,
) -> list[ConfigAuditLog]:
    stmt = select(ConfigAuditLog).order_by(desc(ConfigAuditLog.changed_at_utc))
    if entity_type is not None:
        stmt = stmt.where(ConfigAuditLog.entity_type == entity_type)
    if entity_id is not None:
        stmt = stmt.where(ConfigAuditLog.entity_id == entity_id)
    stmt = stmt.limit(min(limit, 1000))
    return list(session.execute(stmt).scalars().all())


def delete_trim_feature_value(session: Session, value_id: UUID) -> bool:
    val = session.get(TrimFeatureValue, value_id)
    if val is None:
        return False
    session.delete(val)
    return True


def update_vehicle_trim(
    session: Session, trim_id: UUID, **kwargs: object
) -> VehicleTrim | None:
    trim = session.get(VehicleTrim, trim_id)
    if trim is None:
        return None
    for key, value in kwargs.items():
        if value is not None and hasattr(trim, key):
            setattr(trim, key, value)
    return trim
