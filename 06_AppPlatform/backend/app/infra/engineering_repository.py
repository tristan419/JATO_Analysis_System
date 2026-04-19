from datetime import datetime, timezone

from sqlalchemy import Select, delete, select, update
from sqlalchemy.orm import Session

from app.db.models import (
    ConfigBaseVariant,
    ConfigImportBatch,
    ConfigMarketFeatureOverride,
    ConfigMarketVariant,
    ConfigProject,
    ConfigVariant,
    ImportBatch,
)


def list_projects(
    session: Session,
    status: str | None,
    brand: str | None,
    market_country: str | None,
    limit: int,
) -> list[ConfigProject]:
    stmt: Select[tuple[ConfigProject]] = select(ConfigProject)
    if status:
        stmt = stmt.where(ConfigProject.status == status)
    if brand:
        stmt = stmt.where(ConfigProject.brand == brand)
    if market_country:
        stmt = stmt.where(ConfigProject.market_country == market_country)
    stmt = stmt.order_by(
        ConfigProject.updated_at_utc.desc(),
        ConfigProject.project_code.asc(),
    ).limit(max(1, min(int(limit), 200)))
    return session.execute(stmt).scalars().all()


def get_project(session: Session, project_id: object) -> ConfigProject | None:
    return session.get(ConfigProject, project_id)


def add_project(session: Session, project: ConfigProject) -> ConfigProject:
    session.add(project)
    return project


def add_import_batch(
    session: Session,
    import_batch: ImportBatch,
) -> ImportBatch:
    session.add(import_batch)
    return import_batch


def add_config_import_batch(
    session: Session,
    batch: ConfigImportBatch,
) -> ConfigImportBatch:
    session.add(batch)
    return batch


def get_import_batch(
    session: Session,
    import_batch_id: object,
) -> ImportBatch | None:
    return session.get(ImportBatch, import_batch_id)


def get_config_import_batch(
    session: Session,
    config_import_batch_id: object,
) -> ConfigImportBatch | None:
    return session.get(ConfigImportBatch, config_import_batch_id)


def add_variants(
    session: Session,
    variants: list[ConfigVariant],
) -> list[ConfigVariant]:
    session.add_all(variants)
    return variants


def add_base_variants(
    session: Session,
    items: list[ConfigBaseVariant],
) -> list[ConfigBaseVariant]:
    session.add_all(items)
    return items


def add_market_variants(
    session: Session,
    items: list[ConfigMarketVariant],
) -> list[ConfigMarketVariant]:
    session.add_all(items)
    return items


def add_market_feature_overrides(
    session: Session,
    items: list[ConfigMarketFeatureOverride],
) -> list[ConfigMarketFeatureOverride]:
    session.add_all(items)
    return items


def deactivate_project_variants(
    session: Session,
    project_id: object,
) -> int:
    result = session.execute(
        update(ConfigVariant)
        .where(
            ConfigVariant.project_id == project_id,
            ConfigVariant.is_active.is_(True),
        )
        .values(
            is_active=False,
            updated_at_utc=datetime.now(timezone.utc),
        )
    )
    return int(result.rowcount or 0)


def list_active_variants_for_project(
    session: Session,
    project_id: object,
) -> list[ConfigVariant]:
    stmt: Select[tuple[ConfigVariant]] = (
        select(ConfigVariant)
        .where(
            ConfigVariant.project_id == project_id,
            ConfigVariant.is_active.is_(True),
        )
        .order_by(
            ConfigVariant.model.asc(),
            ConfigVariant.trim_name.asc(),
            ConfigVariant.market_country.asc(),
        )
    )
    return session.execute(stmt).scalars().all()


def replace_project_normalized_variants(
    session: Session,
    project_id: object,
) -> None:
    session.execute(
        delete(ConfigMarketFeatureOverride).where(
            ConfigMarketFeatureOverride.project_id == project_id
        )
    )
    session.execute(
        delete(ConfigMarketVariant).where(
            ConfigMarketVariant.project_id == project_id
        )
    )
    session.execute(
        delete(ConfigBaseVariant).where(
            ConfigBaseVariant.project_id == project_id
        )
    )


def list_config_import_batches(
    session: Session,
    project_id: object | None,
    import_status: str | None,
    limit: int,
) -> list[ConfigImportBatch]:
    stmt: Select[tuple[ConfigImportBatch]] = select(ConfigImportBatch)
    if project_id is not None:
        stmt = stmt.where(ConfigImportBatch.project_id == project_id)
    if import_status:
        stmt = stmt.where(ConfigImportBatch.import_status == import_status)
    stmt = stmt.order_by(
        ConfigImportBatch.created_at_utc.desc()
    ).limit(max(1, min(int(limit), 200)))
    return session.execute(stmt).scalars().all()


def list_config_variants(
    session: Session,
    project_id: object | None,
    config_import_batch_id: object | None,
    model: str | None,
    market_country: str | None,
    is_active: bool | None,
    limit: int,
) -> list[ConfigVariant]:
    stmt: Select[tuple[ConfigVariant]] = select(ConfigVariant)
    if project_id is not None:
        stmt = stmt.where(ConfigVariant.project_id == project_id)
    if config_import_batch_id is not None:
        stmt = stmt.where(
            ConfigVariant.config_import_batch_id == config_import_batch_id
        )
    if model:
        stmt = stmt.where(ConfigVariant.model == model)
    if market_country:
        stmt = stmt.where(ConfigVariant.market_country == market_country)
    if is_active is not None:
        stmt = stmt.where(ConfigVariant.is_active == is_active)
    stmt = stmt.order_by(
        ConfigVariant.updated_at_utc.desc(),
        ConfigVariant.model.asc(),
        ConfigVariant.trim_name.asc(),
    ).limit(max(1, min(int(limit), 500)))
    return session.execute(stmt).scalars().all()


def list_base_variants(
    session: Session,
    project_id: object,
    model: str | None,
    limit: int,
) -> list[ConfigBaseVariant]:
    stmt: Select[tuple[ConfigBaseVariant]] = select(ConfigBaseVariant).where(
        ConfigBaseVariant.project_id == project_id
    )
    if model:
        stmt = stmt.where(ConfigBaseVariant.model == model)
    stmt = stmt.order_by(
        ConfigBaseVariant.model.asc(),
        ConfigBaseVariant.trim_name.asc(),
        ConfigBaseVariant.powertrain.asc(),
    ).limit(max(1, min(int(limit), 500)))
    return session.execute(stmt).scalars().all()


def list_market_variants(
    session: Session,
    project_id: object,
    base_variant_id: object | None,
    market_country: str | None,
    limit: int,
) -> list[ConfigMarketVariant]:
    stmt: Select[tuple[ConfigMarketVariant]] = select(ConfigMarketVariant).where(
        ConfigMarketVariant.project_id == project_id
    )
    if base_variant_id is not None:
        stmt = stmt.where(ConfigMarketVariant.base_variant_id == base_variant_id)
    if market_country:
        stmt = stmt.where(ConfigMarketVariant.market_country == market_country)
    stmt = stmt.order_by(
        ConfigMarketVariant.market_country.asc(),
        ConfigMarketVariant.external_row_key.asc(),
    ).limit(max(1, min(int(limit), 1000)))
    return session.execute(stmt).scalars().all()


def list_market_feature_overrides(
    session: Session,
    project_id: object,
    base_variant_id: object | None,
    market_variant_id: object | None,
    market_country: str | None,
    feature_code: str | None,
    limit: int,
) -> list[ConfigMarketFeatureOverride]:
    stmt: Select[tuple[ConfigMarketFeatureOverride]] = (
        select(ConfigMarketFeatureOverride)
        .join(
            ConfigMarketVariant,
            ConfigMarketVariant.market_variant_id
            == ConfigMarketFeatureOverride.market_variant_id,
        )
        .where(ConfigMarketFeatureOverride.project_id == project_id)
    )
    if base_variant_id is not None:
        stmt = stmt.where(ConfigMarketVariant.base_variant_id == base_variant_id)
    if market_variant_id is not None:
        stmt = stmt.where(
            ConfigMarketFeatureOverride.market_variant_id == market_variant_id
        )
    if market_country:
        stmt = stmt.where(ConfigMarketVariant.market_country == market_country)
    if feature_code:
        stmt = stmt.where(ConfigMarketFeatureOverride.feature_code == feature_code)
    stmt = stmt.order_by(
        ConfigMarketVariant.market_country.asc(),
        ConfigMarketFeatureOverride.feature_code.asc(),
    ).limit(max(1, min(int(limit), 2000)))
    return session.execute(stmt).scalars().all()
