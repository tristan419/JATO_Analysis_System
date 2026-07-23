from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from app.infra import engineering_config_repository as repo


@pytest.fixture()
def config_repo_session() -> Iterator[Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    connection = engine.connect()
    try:
        _create_minimal_config_tables(connection)
        session = Session(bind=connection, expire_on_commit=False)
        try:
            yield session
        finally:
            session.close()
    finally:
        connection.close()
        engine.dispose()


def _create_minimal_config_tables(connection: Connection) -> None:
    connection.execute(text("ATTACH DATABASE ':memory:' AS ops"))
    connection.execute(text("ATTACH DATABASE ':memory:' AS engineering_config"))
    connection.execute(
        text(
            """
            CREATE TABLE ops.import_batches (
                import_batch_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                source_file_name TEXT NOT NULL,
                source_file_path TEXT NOT NULL,
                source_file_hash TEXT,
                import_status TEXT NOT NULL,
                row_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0,
                triggered_by TEXT,
                started_at_utc DATETIME,
                finished_at_utc DATETIME,
                created_at_utc DATETIME NOT NULL
            )
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE engineering_config.engineering_config_source_context_links (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                brand TEXT,
                model_name TEXT,
                model_year TEXT,
                market TEXT,
                country TEXT,
                powertrain TEXT,
                segment TEXT,
                trim_ids TEXT NOT NULL DEFAULT '[]',
                sales_version_ids TEXT NOT NULL DEFAULT '[]',
                context_type TEXT NOT NULL DEFAULT 'compare',
                scenario TEXT,
                identity_anchor TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_by TEXT,
                created_at_utc DATETIME NOT NULL
            )
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE engineering_config.vehicle_trims (
                trim_id TEXT PRIMARY KEY,
                source_upload_id TEXT,
                identity_key TEXT,
                material_no TEXT,
                vehicle_code TEXT,
                market TEXT,
                brand TEXT NOT NULL,
                model_name TEXT NOT NULL,
                trim_name TEXT NOT NULL,
                full_trim_name TEXT NOT NULL,
                energy_type TEXT,
                drivetrain TEXT,
                engine TEXT,
                model_year TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at_utc DATETIME NOT NULL,
                updated_at_utc DATETIME NOT NULL
            )
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE engineering_config.config_versions (
                version_id TEXT PRIMARY KEY,
                trim_id TEXT NOT NULL,
                source_upload_id TEXT,
                created_at_utc DATETIME NOT NULL
            )
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE engineering_config.trim_feature_values (
                value_id TEXT PRIMARY KEY,
                trim_id TEXT NOT NULL,
                feature_id TEXT NOT NULL,
                raw_value TEXT NOT NULL,
                normalized_value TEXT,
                availability TEXT NOT NULL,
                unit TEXT,
                source_row INTEGER NOT NULL,
                source_column TEXT NOT NULL,
                source_upload_id TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                updated_by TEXT,
                created_at_utc DATETIME NOT NULL,
                updated_at_utc DATETIME NOT NULL
            )
            """
        )
    )
    connection.commit()


def _uuid_value(value: UUID) -> str:
    return value.hex


def _insert_import_batch(session: Session, source_id: UUID, *, domain: str) -> None:
    now = datetime.now(timezone.utc)
    session.execute(
        text(
            """
            INSERT INTO ops.import_batches (
                import_batch_id,
                domain,
                source_file_name,
                source_file_path,
                source_file_hash,
                import_status,
                row_count,
                error_count,
                triggered_by,
                started_at_utc,
                finished_at_utc,
                created_at_utc
            )
            VALUES (
                :source_id,
                :domain,
                :source_file_name,
                :source_file_path,
                :source_file_hash,
                'stored',
                0,
                0,
                'tester',
                :now,
                :now,
                :now
            )
            """
        ),
        {
            "source_id": _uuid_value(source_id),
            "domain": domain,
            "source_file_name": "source.xlsx",
            "source_file_path": "/tmp/source.xlsx",
            "source_file_hash": f"hash-{source_id}",
            "now": now,
        },
    )


def _insert_source_context(
    session: Session,
    *,
    context_id: UUID,
    source_id: UUID,
    country: str,
    market: str,
    status: str,
) -> None:
    now = datetime.now(timezone.utc)
    session.execute(
        text(
            """
            INSERT INTO engineering_config.engineering_config_source_context_links (
                id,
                source_id,
                batch_id,
                brand,
                model_name,
                model_year,
                market,
                country,
                powertrain,
                segment,
                trim_ids,
                sales_version_ids,
                context_type,
                status,
                created_by,
                created_at_utc
            )
            VALUES (
                :context_id,
                :source_id,
                :source_id,
                'Volvo',
                'XC60',
                '2026',
                :market,
                :country,
                'BEV',
                'SUV C',
                '[]',
                '[]',
                'compare',
                :status,
                'tester',
                :now
            )
            """
        ),
        {
            "context_id": _uuid_value(context_id),
            "source_id": _uuid_value(source_id),
            "country": country,
            "market": market,
            "status": status,
            "now": now,
        },
    )


def _insert_vehicle_trim(
    session: Session,
    *,
    trim_id: UUID,
    market: str,
    status: str,
    source_upload_id: UUID | None = None,
    full_trim_name: str | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    session.execute(
        text(
            """
            INSERT INTO engineering_config.vehicle_trims (
                trim_id,
                source_upload_id,
                identity_key,
                material_no,
                vehicle_code,
                market,
                brand,
                model_name,
                trim_name,
                full_trim_name,
                energy_type,
                drivetrain,
                engine,
                model_year,
                status,
                created_at_utc,
                updated_at_utc
            )
            VALUES (
                :trim_id,
                :source_upload_id,
                :identity_key,
                NULL,
                NULL,
                :market,
                'Omoda',
                'T19C',
                :trim_name,
                :full_trim_name,
                'ICE',
                'FWD',
                NULL,
                '2026',
                :status,
                :now,
                :now
            )
            """
        ),
        {
            "trim_id": _uuid_value(trim_id),
            "source_upload_id": _uuid_value(source_upload_id) if source_upload_id else None,
            "identity_key": f"trim-{trim_id}",
            "market": market,
            "trim_name": f"{market} {status}",
            "full_trim_name": full_trim_name or f"{market} {status}",
            "status": status,
            "now": now,
        },
    )


def _source_context_status(session: Session, context_id: UUID) -> str:
    return str(
        session.execute(
            text(
                """
                SELECT status
                FROM engineering_config.engineering_config_source_context_links
                WHERE id = :context_id
                """
            ),
            {"context_id": _uuid_value(context_id)},
        ).scalar_one()
    )


def _vehicle_trim_status(session: Session, trim_id: UUID) -> str:
    return str(
        session.execute(
            text(
                """
                SELECT status
                FROM engineering_config.vehicle_trims
                WHERE trim_id = :trim_id
                """
            ),
            {"trim_id": _uuid_value(trim_id)},
        ).scalar_one()
    )


def _insert_legacy_config_version(
    session: Session,
    *,
    trim_id: UUID,
    source_upload_id: UUID,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO engineering_config.config_versions (
                version_id,
                trim_id,
                source_upload_id,
                created_at_utc
            ) VALUES (:version_id, :trim_id, :source_upload_id, :created_at_utc)
            """
        ),
        {
            "version_id": _uuid_value(uuid4()),
            "trim_id": _uuid_value(trim_id),
            "source_upload_id": _uuid_value(source_upload_id),
            "created_at_utc": datetime.now(timezone.utc),
        },
    )


def _insert_trim_feature_value(
    session: Session,
    *,
    value_id: UUID,
    trim_id: UUID,
    feature_id: UUID,
) -> None:
    now = datetime.now(timezone.utc)
    session.execute(
        text(
            """
            INSERT INTO engineering_config.trim_feature_values (
                value_id,
                trim_id,
                feature_id,
                raw_value,
                availability,
                source_row,
                source_column,
                version,
                created_at_utc,
                updated_at_utc
            ) VALUES (
                :value_id,
                :trim_id,
                :feature_id,
                '●',
                'STANDARD',
                1,
                'D',
                1,
                :now,
                :now
            )
            """
        ),
        {
            "value_id": _uuid_value(value_id),
            "trim_id": _uuid_value(trim_id),
            "feature_id": _uuid_value(feature_id),
            "now": now,
        },
    )


def test_delete_trim_feature_values_not_in_removes_only_stale_projection_rows(
    config_repo_session: Session,
) -> None:
    trim_id = uuid4()
    other_trim_id = uuid4()
    retained_feature_id = uuid4()
    stale_feature_id = uuid4()
    other_feature_id = uuid4()
    for current_trim_id, feature_id in (
        (trim_id, retained_feature_id),
        (trim_id, stale_feature_id),
        (other_trim_id, other_feature_id),
    ):
        _insert_trim_feature_value(
            config_repo_session,
            value_id=uuid4(),
            trim_id=current_trim_id,
            feature_id=feature_id,
        )
    config_repo_session.commit()

    deleted_count = repo.delete_trim_feature_values_not_in(
        config_repo_session,
        trim_id,
        [retained_feature_id],
    )
    config_repo_session.commit()

    remaining = {
        (str(row.trim_id), str(row.feature_id))
        for row in config_repo_session.execute(
            text(
                """
                SELECT trim_id, feature_id
                FROM engineering_config.trim_feature_values
                """
            )
        )
    }
    assert deleted_count == 1
    assert remaining == {
        (_uuid_value(trim_id), _uuid_value(retained_feature_id)),
        (_uuid_value(other_trim_id), _uuid_value(other_feature_id)),
    }


def test_source_context_status_and_clear_trash_are_country_scoped(
    config_repo_session: Session,
) -> None:
    source_id = uuid4()
    other_domain_source_id = uuid4()
    germany_context_id = uuid4()
    france_context_id = uuid4()
    purged_context_id = uuid4()
    other_domain_context_id = uuid4()
    _insert_import_batch(
        config_repo_session,
        source_id,
        domain="engineering_config_source",
    )
    _insert_import_batch(
        config_repo_session,
        other_domain_source_id,
        domain="other_domain",
    )
    _insert_source_context(
        config_repo_session,
        context_id=germany_context_id,
        source_id=source_id,
        country="Germany",
        market="Germany",
        status="active",
    )
    _insert_source_context(
        config_repo_session,
        context_id=france_context_id,
        source_id=source_id,
        country="France",
        market="France",
        status="trashed",
    )
    _insert_source_context(
        config_repo_session,
        context_id=purged_context_id,
        source_id=source_id,
        country="Germany",
        market="Germany",
        status="purged",
    )
    _insert_source_context(
        config_repo_session,
        context_id=other_domain_context_id,
        source_id=other_domain_source_id,
        country="Germany",
        market="Germany",
        status="trashed",
    )

    updated = repo.set_source_context_status(
        config_repo_session,
        source_id,
        "trashed",
        country="Germany",
    )

    assert updated == 1
    assert _source_context_status(config_repo_session, germany_context_id) == "trashed"
    assert _source_context_status(config_repo_session, france_context_id) == "trashed"
    assert _source_context_status(config_repo_session, purged_context_id) == "purged"

    cleared = repo.clear_source_snapshot_trash(
        config_repo_session,
        "engineering_config_source",
        country="Germany",
    )

    assert cleared == 1
    assert _source_context_status(config_repo_session, germany_context_id) == "purged"
    assert _source_context_status(config_repo_session, france_context_id) == "trashed"
    assert _source_context_status(config_repo_session, purged_context_id) == "purged"
    assert _source_context_status(config_repo_session, other_domain_context_id) == "trashed"


def test_clear_vehicle_trim_trash_only_purges_target_market_trashed_trims(
    config_repo_session: Session,
) -> None:
    germany_trashed_trim_id = uuid4()
    germany_active_trim_id = uuid4()
    france_trashed_trim_id = uuid4()
    _insert_vehicle_trim(
        config_repo_session,
        trim_id=germany_trashed_trim_id,
        market="Germany",
        status="trashed",
    )
    _insert_vehicle_trim(
        config_repo_session,
        trim_id=germany_active_trim_id,
        market="Germany",
        status="active",
    )
    _insert_vehicle_trim(
        config_repo_session,
        trim_id=france_trashed_trim_id,
        market="France",
        status="trashed",
    )

    cleared = repo.clear_vehicle_trim_trash(config_repo_session, market="Germany")

    assert cleared == 1
    assert _vehicle_trim_status(config_repo_session, germany_trashed_trim_id) == "purged"
    assert _vehicle_trim_status(config_repo_session, germany_active_trim_id) == "active"
    assert _vehicle_trim_status(config_repo_session, france_trashed_trim_id) == "trashed"


def test_update_vehicle_trim_can_clear_nullable_identity_field(
    config_repo_session: Session,
) -> None:
    trim_id = uuid4()
    _insert_vehicle_trim(
        config_repo_session,
        trim_id=trim_id,
        market="Germany",
        status="draft",
    )
    config_repo_session.commit()

    updated = repo.update_vehicle_trim(config_repo_session, trim_id, market=None)
    config_repo_session.flush()
    config_repo_session.expire_all()
    reloaded = repo.get_vehicle_trim(config_repo_session, trim_id)

    assert updated is not None
    assert reloaded is not None
    assert reloaded.market is None


def test_get_vehicle_trim_by_source_full_name_keeps_same_trim_name_in_separate_snapshots(
    config_repo_session: Session,
) -> None:
    source_a_id = uuid4()
    source_b_id = uuid4()
    source_a_trim_id = uuid4()
    source_b_trim_id = uuid4()
    shared_full_name = "T19C Premium"
    _insert_vehicle_trim(
        config_repo_session,
        trim_id=source_a_trim_id,
        market="Germany",
        status="active",
        source_upload_id=source_a_id,
        full_trim_name=shared_full_name,
    )
    _insert_vehicle_trim(
        config_repo_session,
        trim_id=source_b_trim_id,
        market="Germany",
        status="active",
        source_upload_id=source_b_id,
        full_trim_name=shared_full_name,
    )
    config_repo_session.commit()

    source_a_trim = repo.get_vehicle_trim_by_source_full_name(
        config_repo_session,
        source_a_id,
        shared_full_name,
    )
    source_b_trim = repo.get_vehicle_trim_by_source_full_name(
        config_repo_session,
        source_b_id,
        shared_full_name,
    )

    assert source_a_trim is not None
    assert source_b_trim is not None
    assert source_a_trim.trim_id == source_a_trim_id
    assert source_b_trim.trim_id == source_b_trim_id


def test_legacy_digest_trim_can_be_found_from_config_version_source_provenance(
    config_repo_session: Session,
) -> None:
    original_source_id = uuid4()
    legacy_derived_batch_id = uuid4()
    trim_id = uuid4()
    full_trim_name = "T19C Basic"
    _insert_vehicle_trim(
        config_repo_session,
        trim_id=trim_id,
        market="Germany",
        status="active",
        source_upload_id=legacy_derived_batch_id,
        full_trim_name=full_trim_name,
    )
    _insert_legacy_config_version(
        config_repo_session,
        trim_id=trim_id,
        source_upload_id=original_source_id,
    )
    config_repo_session.commit()

    recovered = repo.get_vehicle_trim_by_config_version_source_full_name(
        config_repo_session,
        original_source_id,
        full_trim_name,
    )

    assert recovered is not None
    assert recovered.trim_id == trim_id


def test_legacy_digest_trim_with_multiple_source_versions_is_not_reassigned(
    config_repo_session: Session,
) -> None:
    source_a_id = uuid4()
    source_b_id = uuid4()
    trim_id = uuid4()
    full_trim_name = "T19C Premium"
    _insert_vehicle_trim(
        config_repo_session,
        trim_id=trim_id,
        market="Germany",
        status="active",
        source_upload_id=uuid4(),
        full_trim_name=full_trim_name,
    )
    _insert_legacy_config_version(
        config_repo_session,
        trim_id=trim_id,
        source_upload_id=source_a_id,
    )
    _insert_legacy_config_version(
        config_repo_session,
        trim_id=trim_id,
        source_upload_id=source_b_id,
    )
    config_repo_session.commit()

    recovered_a = repo.get_vehicle_trim_by_config_version_source_full_name(
        config_repo_session,
        source_a_id,
        full_trim_name,
    )
    recovered_b = repo.get_vehicle_trim_by_config_version_source_full_name(
        config_repo_session,
        source_b_id,
        full_trim_name,
    )

    assert recovered_a is None
    assert recovered_b is None
