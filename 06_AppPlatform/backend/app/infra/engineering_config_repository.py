"""Database operations for engineering_config schema."""

from __future__ import annotations

from collections.abc import Iterable
from hashlib import blake2b
from uuid import UUID

from sqlalchemy import Select, delete, desc, distinct, func, or_, select, update
from sqlalchemy.orm import Session

from app.db.models import (
    ConfigAuditLog,
    ConfigVersion,
    EngineeringConfigSourceContextLink,
    FeatureCatalog,
    ImportBatch,
    TrimFeatureValue,
    VehicleTrim,
)


def add_import_batch(session: Session, batch: ImportBatch) -> None:
    session.add(batch)


def get_import_batch(session: Session, import_batch_id: UUID) -> ImportBatch | None:
    return session.get(ImportBatch, import_batch_id)


def list_import_batches_by_ids(
    session: Session,
    import_batch_ids: Iterable[UUID],
) -> dict[UUID, ImportBatch]:
    """Return source batches for a trim page in one query."""
    source_ids = list(dict.fromkeys(import_batch_ids))
    if not source_ids:
        return {}
    batches = session.execute(
        select(ImportBatch).where(ImportBatch.import_batch_id.in_(source_ids))
    ).scalars().all()
    return {batch.import_batch_id: batch for batch in batches}


def get_import_batch_by_hash(
    session: Session,
    domain: str,
    source_file_hash: str,
) -> ImportBatch | None:
    stmt = (
        select(ImportBatch)
        .where(
            ImportBatch.domain == domain,
            ImportBatch.source_file_hash == source_file_hash,
        )
        .order_by(desc(ImportBatch.created_at_utc))
        .limit(1)
    )
    return session.execute(stmt).scalars().first()


def list_import_batches(
    session: Session,
    domain: str,
    limit: int = 20,
) -> list[ImportBatch]:
    stmt = (
        select(ImportBatch)
        .where(ImportBatch.domain == domain)
        .order_by(desc(ImportBatch.created_at_utc))
        .limit(min(limit, 100))
    )
    return list(session.execute(stmt).scalars().all())


def list_source_snapshot_batches(
    session: Session,
    domain: str,
    *,
    brand: str | None = None,
    country: str | None = None,
    model_year: str | None = None,
    powertrain: str | None = None,
    segment: str | None = None,
    query: str | None = None,
    include_trash: bool = False,
    trash_only: bool = False,
    limit: int = 20,
) -> list[ImportBatch]:
    stmt: Select = select(ImportBatch).where(ImportBatch.domain == domain)
    trimmed_query = query.strip() if query is not None else ""
    needs_context_join = (
        (brand is not None and brand.strip())
        or (country is not None and country.strip())
        or (model_year is not None and model_year.strip())
        or (powertrain is not None and powertrain.strip())
        or (segment is not None and segment.strip())
        or bool(trimmed_query)
        or trash_only
    )
    if needs_context_join:
        join_method = stmt.outerjoin if trimmed_query else stmt.join
        stmt = join_method(
            EngineeringConfigSourceContextLink,
            EngineeringConfigSourceContextLink.source_id == ImportBatch.import_batch_id,
        )
    context_status = func.coalesce(EngineeringConfigSourceContextLink.status, "active")
    if country is not None and country.strip():
        country_pattern = f"%{country.strip()}%"
        stmt = stmt.where(
            or_(
                EngineeringConfigSourceContextLink.country.ilike(country_pattern),
                EngineeringConfigSourceContextLink.market.ilike(country_pattern),
            )
        )
    if brand is not None and brand.strip():
        stmt = stmt.where(or_(
            EngineeringConfigSourceContextLink.brand.ilike(f"%{brand.strip()}%"),
            EngineeringConfigSourceContextLink.brand.is_(None),
            EngineeringConfigSourceContextLink.brand == "",
        ))
    if segment is not None and segment.strip():
        stmt = stmt.where(EngineeringConfigSourceContextLink.segment.ilike(f"%{segment.strip()}%"))
    if model_year is not None and model_year.strip():
        stmt = stmt.where(or_(
            EngineeringConfigSourceContextLink.model_year.ilike(f"%{model_year.strip()}%"),
            EngineeringConfigSourceContextLink.model_year.is_(None),
            EngineeringConfigSourceContextLink.model_year == "",
        ))
    if powertrain is not None and powertrain.strip():
        stmt = stmt.where(or_(
            EngineeringConfigSourceContextLink.powertrain.ilike(f"%{powertrain.strip()}%"),
            EngineeringConfigSourceContextLink.powertrain.is_(None),
            EngineeringConfigSourceContextLink.powertrain == "",
        ))
    if trimmed_query:
        query_pattern = f"%{trimmed_query}%"
        stmt = stmt.where(
            or_(
                ImportBatch.source_file_name.ilike(query_pattern),
                ImportBatch.source_file_hash.ilike(query_pattern),
                ImportBatch.source_file_path.ilike(query_pattern),
                ImportBatch.import_status.ilike(query_pattern),
                EngineeringConfigSourceContextLink.brand.ilike(query_pattern),
                EngineeringConfigSourceContextLink.model_name.ilike(query_pattern),
                EngineeringConfigSourceContextLink.market.ilike(query_pattern),
                EngineeringConfigSourceContextLink.country.ilike(query_pattern),
                EngineeringConfigSourceContextLink.segment.ilike(query_pattern),
            )
        )
    if needs_context_join:
        stmt = stmt.distinct()
    if trash_only:
        stmt = stmt.where(ImportBatch.import_status != "purged")
        stmt = stmt.where(context_status == "trashed")
    elif include_trash:
        stmt = stmt.where(ImportBatch.import_status != "purged")
        if country is not None and country.strip():
            stmt = stmt.where(context_status != "purged")
    else:
        stmt = stmt.where(ImportBatch.import_status.notin_(["trashed", "purged"]))
        if country is not None and country.strip():
            stmt = stmt.where(context_status == "active")
    stmt = stmt.order_by(desc(ImportBatch.created_at_utc)).limit(min(limit, 1000))
    return list(session.execute(stmt).scalars().all())


def set_import_batch_status(
    session: Session,
    import_batch_id: UUID,
    status: str,
) -> int:
    result = session.execute(
        update(ImportBatch)
        .where(ImportBatch.import_batch_id == import_batch_id)
        .values(import_status=status)
    )
    return int(result.rowcount or 0)


def clear_source_snapshot_trash(
    session: Session,
    domain: str,
    *,
    country: str | None = None,
) -> int:
    stmt = (
        update(EngineeringConfigSourceContextLink)
        .where(EngineeringConfigSourceContextLink.source_id.in_(
            select(ImportBatch.import_batch_id).where(ImportBatch.domain == domain)
        ))
        .where(func.coalesce(EngineeringConfigSourceContextLink.status, "active") == "trashed")
    )
    if country is not None and country.strip():
        country_pattern = f"%{country.strip()}%"
        stmt = stmt.where(
            or_(
                EngineeringConfigSourceContextLink.country.ilike(country_pattern),
                EngineeringConfigSourceContextLink.market.ilike(country_pattern),
            )
        )
    result = session.execute(stmt.values(status="purged"))
    return int(result.rowcount or 0)


def set_source_context_status(
    session: Session,
    source_id: UUID,
    status: str,
    *,
    country: str | None = None,
) -> int:
    stmt = (
        update(EngineeringConfigSourceContextLink)
        .where(EngineeringConfigSourceContextLink.source_id == source_id)
        .where(func.coalesce(EngineeringConfigSourceContextLink.status, "active") != "purged")
    )
    if country is not None and country.strip():
        country_pattern = f"%{country.strip()}%"
        stmt = stmt.where(
            or_(
                EngineeringConfigSourceContextLink.country.ilike(country_pattern),
                EngineeringConfigSourceContextLink.market.ilike(country_pattern),
            )
        )
    result = session.execute(stmt.values(status=status))
    return int(result.rowcount or 0)


def add_source_context_link(
    session: Session,
    link: EngineeringConfigSourceContextLink,
) -> None:
    session.add(link)


def list_source_context_links(
    session: Session,
    source_id: UUID,
) -> list[EngineeringConfigSourceContextLink]:
    stmt = (
        select(EngineeringConfigSourceContextLink)
        .where(EngineeringConfigSourceContextLink.source_id == source_id)
        .order_by(desc(EngineeringConfigSourceContextLink.created_at_utc))
    )
    return list(session.execute(stmt).scalars().all())


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


def get_feature_catalog_by_category_field(
    session: Session,
    category: str,
    standard_field_name: str,
) -> FeatureCatalog | None:
    stmt = select(FeatureCatalog).where(
        FeatureCatalog.category == category,
        FeatureCatalog.standard_field_name == standard_field_name,
    )
    return session.execute(stmt).scalars().first()


def add_vehicle_trim(session: Session, trim: VehicleTrim) -> None:
    session.add(trim)


def add_vehicle_trims_batch(session: Session, trims: list[VehicleTrim]) -> None:
    session.add_all(trims)


def _vehicle_trim_filtered_stmt(
    brand: str | None = None,
    model_name: str | None = None,
    trim_name: str | None = None,
    market: str | None = None,
    model_year: str | None = None,
    energy_type: str | None = None,
    source_query: str | None = None,
    has_material_no: bool | None = None,
    status: str | None = None,
    query: str | None = None,
) -> Select:
    stmt = select(VehicleTrim)
    needs_source_join = (
        (source_query is not None and source_query.strip())
        or (query is not None and query.strip())
    )
    if needs_source_join:
        stmt = stmt.join(
            ImportBatch,
            VehicleTrim.source_upload_id == ImportBatch.import_batch_id,
            isouter=True,
        )
    if source_query is not None and source_query.strip():
        source_pattern = f"%{source_query.strip()}%"
        stmt = stmt.where(
            or_(
                ImportBatch.source_file_name.ilike(source_pattern),
                ImportBatch.source_file_path.ilike(source_pattern),
                ImportBatch.triggered_by.ilike(source_pattern),
            )
        )
    if brand is not None:
        stmt = stmt.where(VehicleTrim.brand.ilike(f"%{brand.strip()}%"))
    if model_name is not None:
        stmt = stmt.where(VehicleTrim.model_name.ilike(f"%{model_name.strip()}%"))
    if trim_name is not None:
        stmt = stmt.where(VehicleTrim.trim_name.ilike(f"%{trim_name.strip()}%"))
    if market is not None:
        stmt = stmt.where(VehicleTrim.market.ilike(f"%{market.strip()}%"))
    if model_year is not None:
        stmt = stmt.where(VehicleTrim.model_year.ilike(f"%{model_year.strip()}%"))
    if energy_type is not None:
        stmt = stmt.where(VehicleTrim.energy_type.ilike(f"%{energy_type.strip()}%"))
    if has_material_no is True:
        stmt = stmt.where(VehicleTrim.material_no.is_not(None), VehicleTrim.material_no != "")
    if has_material_no is False:
        stmt = stmt.where(or_(VehicleTrim.material_no.is_(None), VehicleTrim.material_no == ""))
    if status is not None:
        stmt = stmt.where(VehicleTrim.status == status)
    else:
        stmt = stmt.where(VehicleTrim.status.notin_(["trashed", "purged"]))
    if query is not None and query.strip():
        pattern = f"%{query.strip()}%"
        stmt = stmt.where(
            or_(
                VehicleTrim.brand.ilike(pattern),
                VehicleTrim.model_name.ilike(pattern),
                VehicleTrim.trim_name.ilike(pattern),
                VehicleTrim.full_trim_name.ilike(pattern),
                VehicleTrim.market.ilike(pattern),
                VehicleTrim.vehicle_code.ilike(pattern),
                VehicleTrim.material_no.ilike(pattern),
                VehicleTrim.identity_key.ilike(pattern),
                ImportBatch.source_file_name.ilike(pattern),
                ImportBatch.source_file_path.ilike(pattern),
                ImportBatch.triggered_by.ilike(pattern),
            )
        )
    return stmt


def count_vehicle_trims(
    session: Session,
    brand: str | None = None,
    model_name: str | None = None,
    trim_name: str | None = None,
    market: str | None = None,
    model_year: str | None = None,
    energy_type: str | None = None,
    source_query: str | None = None,
    has_material_no: bool | None = None,
    status: str | None = None,
    query: str | None = None,
) -> int:
    stmt = _vehicle_trim_filtered_stmt(
        brand=brand,
        model_name=model_name,
        trim_name=trim_name,
        market=market,
        model_year=model_year,
        energy_type=energy_type,
        source_query=source_query,
        has_material_no=has_material_no,
        status=status,
        query=query,
    )
    count_stmt = select(func.count()).select_from(stmt.subquery())
    return int(session.execute(count_stmt).scalar_one() or 0)


def list_vehicle_trims(
    session: Session,
    brand: str | None = None,
    model_name: str | None = None,
    trim_name: str | None = None,
    market: str | None = None,
    model_year: str | None = None,
    energy_type: str | None = None,
    source_query: str | None = None,
    has_material_no: bool | None = None,
    status: str | None = None,
    query: str | None = None,
    limit: int = 200,
) -> list[VehicleTrim]:
    stmt = _vehicle_trim_filtered_stmt(
        brand=brand,
        model_name=model_name,
        trim_name=trim_name,
        market=market,
        model_year=model_year,
        energy_type=energy_type,
        source_query=source_query,
        has_material_no=has_material_no,
        status=status,
        query=query,
    ).order_by(desc(VehicleTrim.created_at_utc))
    stmt = stmt.limit(min(limit, 500))
    return list(session.execute(stmt).scalars().all())


def get_vehicle_trim(session: Session, trim_id: UUID) -> VehicleTrim | None:
    return session.get(VehicleTrim, trim_id)


def list_vehicle_trims_by_ids(
    session: Session,
    trim_ids: Iterable[UUID],
) -> list[VehicleTrim]:
    ids = list(dict.fromkeys(trim_ids))
    if not ids:
        return []
    return list(session.execute(
        select(VehicleTrim).where(VehicleTrim.trim_id.in_(ids))
    ).scalars().all())


def get_vehicle_trim_by_full_name(
    session: Session,
    full_trim_name: str,
) -> VehicleTrim | None:
    stmt = select(VehicleTrim).where(VehicleTrim.full_trim_name == full_trim_name)
    return session.execute(stmt).scalars().first()


def get_vehicle_trim_by_source_full_name(
    session: Session,
    source_upload_id: UUID,
    full_trim_name: str,
) -> VehicleTrim | None:
    """Find a digest-created trim within one immutable source snapshot.

    A trim name and material number are not globally unique configuration
    identities: the same vehicle can legitimately appear in separate source
    snapshots. The source snapshot is therefore part of the editable-column
    identity, while a retry of the same snapshot remains idempotent.
    """
    stmt = select(VehicleTrim).where(
        VehicleTrim.source_upload_id == source_upload_id,
        VehicleTrim.full_trim_name == full_trim_name,
    )
    return session.execute(stmt).scalars().first()


def acquire_source_digest_lock(
    session: Session,
    source_upload_id: UUID,
    group_id: str,
) -> None:
    """Serialize all digest upserts for one immutable source snapshot."""
    _ = group_id
    _acquire_transaction_lock(session, f"engineering-config-source:{source_upload_id}")


def acquire_config_identity_lock(session: Session, identity_key: str) -> None:
    """Serialize publication for one configuration identity."""
    _acquire_transaction_lock(session, f"engineering-config-publish:{identity_key}")


def acquire_config_trim_lock(session: Session, trim_id: UUID) -> None:
    """Serialize snapshot and live-projection writes for one editable trim."""
    _acquire_transaction_lock(session, f"engineering-config-trim:{trim_id}")


def _acquire_transaction_lock(session: Session, lock_material: str) -> None:
    get_bind = getattr(session, "get_bind", None)
    if not callable(get_bind):
        return
    bind = get_bind()
    if getattr(getattr(bind, "dialect", None), "name", None) != "postgresql":
        return
    lock_key = int.from_bytes(
        blake2b(lock_material.encode("utf-8"), digest_size=8).digest(),
        "big",
        signed=True,
    )
    session.execute(select(func.pg_advisory_xact_lock(lock_key)))


def get_vehicle_trim_by_config_version_source_full_name(
    session: Session,
    source_upload_id: UUID,
    full_trim_name: str,
) -> VehicleTrim | None:
    """Find a pre-source-scoped digest trim through its version provenance.

    Early Source Digest drafts stored a derived import-batch id on the trim,
    while their ConfigVersion already retained the original source snapshot id.
    This lookup lets a same-source retry repair that representation without
    treating a different snapshot as the same editable column.
    """
    unambiguous_trim_ids = (
        select(ConfigVersion.trim_id)
        .where(ConfigVersion.source_upload_id.is_not(None))
        .group_by(ConfigVersion.trim_id)
        .having(func.count(distinct(ConfigVersion.source_upload_id)) == 1)
    )
    stmt = (
        select(VehicleTrim)
        .join(ConfigVersion, ConfigVersion.trim_id == VehicleTrim.trim_id)
        .where(
            ConfigVersion.source_upload_id == source_upload_id,
            VehicleTrim.full_trim_name == full_trim_name,
            VehicleTrim.trim_id.in_(unambiguous_trim_ids),
        )
        .order_by(desc(ConfigVersion.created_at_utc))
    )
    return session.execute(stmt).scalars().first()


def get_latest_config_version_for_trim(
    session: Session,
    trim_id: UUID,
    *,
    for_update: bool = False,
) -> ConfigVersion | None:
    stmt = (
        select(ConfigVersion)
        .where(ConfigVersion.trim_id == trim_id)
        .order_by(desc(ConfigVersion.version_no), desc(ConfigVersion.created_at_utc))
        .limit(1)
    )
    if for_update:
        stmt = stmt.with_for_update()
    return session.execute(stmt).scalars().first()


def list_config_versions_for_trims(
    session: Session,
    trim_ids: Iterable[UUID],
) -> dict[UUID, list[ConfigVersion]]:
    """Load every candidate version for a small compare selection in one query."""
    ids = list(dict.fromkeys(trim_ids))
    if not ids:
        return {}
    stmt = (
        select(ConfigVersion)
        .where(ConfigVersion.trim_id.in_(ids))
        .order_by(
            ConfigVersion.trim_id,
            desc(ConfigVersion.version_no),
            desc(ConfigVersion.created_at_utc),
        )
    )
    versions_by_trim: dict[UUID, list[ConfigVersion]] = {trim_id: [] for trim_id in ids}
    for version in session.execute(stmt).scalars().all():
        versions_by_trim.setdefault(version.trim_id, []).append(version)
    return versions_by_trim


def list_published_config_versions_by_identity(
    session: Session,
    identity_key: str,
) -> list[ConfigVersion]:
    stmt = select(ConfigVersion).where(
        ConfigVersion.identity_key == identity_key,
        ConfigVersion.status == "published",
    )
    return list(session.execute(stmt).scalars().all())


def clear_vehicle_trim_trash(
    session: Session,
    *,
    market: str,
) -> int:
    trashed_trims = list_vehicle_trims(
        session,
        market=market,
        status="trashed",
        limit=500,
    )
    if not trashed_trims:
        return 0
    trim_ids = [trim.trim_id for trim in trashed_trims]
    result = session.execute(
        update(VehicleTrim)
        .where(VehicleTrim.trim_id.in_(trim_ids))
        .values(status="purged")
    )
    return int(result.rowcount or 0)


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


def list_trim_feature_values_with_features(
    session: Session,
    trim_ids: Iterable[UUID],
) -> list[tuple[TrimFeatureValue, FeatureCatalog]]:
    ids = list(dict.fromkeys(trim_ids))
    if not ids:
        return []
    stmt = (
        select(TrimFeatureValue, FeatureCatalog)
        .join(FeatureCatalog, TrimFeatureValue.feature_id == FeatureCatalog.feature_id)
        .where(TrimFeatureValue.trim_id.in_(ids))
        .order_by(FeatureCatalog.category, FeatureCatalog.display_order)
    )
    return list(session.execute(stmt).all())


def get_trim_feature_value(
    session: Session,
    value_id: UUID,
) -> TrimFeatureValue | None:
    return session.get(TrimFeatureValue, value_id)


def get_trim_feature_value_by_trim_feature(
    session: Session,
    trim_id: UUID,
    feature_id: UUID,
) -> TrimFeatureValue | None:
    stmt = select(TrimFeatureValue).where(
        TrimFeatureValue.trim_id == trim_id,
        TrimFeatureValue.feature_id == feature_id,
    )
    return session.execute(stmt).scalars().first()


def update_trim_feature_value(
    session: Session,
    value_id: UUID,
    raw_value: str,
    normalized_value: str | None,
    availability: str,
    unit: str | None,
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
            unit=unit,
            source_upload_id=None,
            source_row=0,
            source_column="manual",
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


def delete_trim_feature_values_not_in(
    session: Session,
    trim_id: UUID,
    feature_ids: Iterable[UUID],
) -> int:
    """Keep the mutable live projection aligned with a newly digested snapshot."""
    retained_ids = list(dict.fromkeys(feature_ids))
    stmt = delete(TrimFeatureValue).where(TrimFeatureValue.trim_id == trim_id)
    if retained_ids:
        stmt = stmt.where(TrimFeatureValue.feature_id.not_in(retained_ids))
    result = session.execute(stmt.execution_options(synchronize_session=False))
    return int(result.rowcount or 0)


def update_vehicle_trim(
    session: Session, trim_id: UUID, **kwargs: object
) -> VehicleTrim | None:
    trim = session.get(VehicleTrim, trim_id)
    if trim is None:
        return None
    for key, value in kwargs.items():
        if hasattr(trim, key):
            setattr(trim, key, value)
    return trim
