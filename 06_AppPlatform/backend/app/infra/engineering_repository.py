from datetime import datetime, timezone

from sqlalchemy import Select, select, update
from sqlalchemy.orm import Session

from app.db.models import (
    ConfigImportBatch,
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
