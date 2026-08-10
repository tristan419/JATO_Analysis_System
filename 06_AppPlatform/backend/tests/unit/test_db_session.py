from sqlalchemy import create_engine

from app.db.session import _sync_database_url


def test_sync_database_url_uses_installed_psycopg_driver_for_plain_postgres_url() -> None:
    assert (
        _sync_database_url("postgres://postgres:postgres@localhost:5432/jato_app")
        == "postgresql+psycopg://postgres:postgres@localhost:5432/jato_app"
    )
    assert (
        _sync_database_url("postgresql://postgres:postgres@localhost:5432/jato_app")
        == "postgresql+psycopg://postgres:postgres@localhost:5432/jato_app"
    )


def test_sync_database_url_swaps_async_postgres_drivers_to_sync_psycopg() -> None:
    assert (
        _sync_database_url("postgresql+asyncpg://user:pass@db:5432/app")
        == "postgresql+psycopg://user:pass@db:5432/app"
    )
    assert (
        _sync_database_url("postgresql+psycopg2://user:pass@db:5432/app")
        == "postgresql+psycopg://user:pass@db:5432/app"
    )
    assert (
        _sync_database_url("postgresql+aiopg://user:pass@db:5432/app")
        == "postgresql+psycopg://user:pass@db:5432/app"
    )


def test_sync_database_url_preserves_installed_sync_driver_url() -> None:
    database_url = "postgresql+psycopg://user:pass@db:5432/app"

    assert _sync_database_url(database_url) == database_url


def test_sync_database_url_only_rewrites_the_driver_prefix() -> None:
    database_url = "postgresql+asyncpg://user:pass+asyncpg@db:5432/app"

    assert _sync_database_url(database_url) == (
        "postgresql+psycopg://user:pass+asyncpg@db:5432/app"
    )


def test_sync_database_url_loads_psycopg_v3_dialect() -> None:
    engine = create_engine(
        _sync_database_url("postgresql+asyncpg://user:pass@localhost:5432/app")
    )

    assert engine.dialect.dbapi.__name__ == "psycopg"
