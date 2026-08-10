from collections.abc import Generator
from functools import lru_cache

from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import DATABASE_ECHO, DATABASE_ENABLED, DATABASE_URL


def _sync_database_url(database_url: str) -> str:
    for prefix in (
        "postgres://",
        "postgresql://",
        "postgresql+asyncpg://",
        "postgresql+aiopg://",
        "postgresql+psycopg2://",
    ):
        if database_url.startswith(prefix):
            return f"postgresql+psycopg://{database_url.removeprefix(prefix)}"
    return database_url


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    if not DATABASE_ENABLED or not DATABASE_URL:
        raise RuntimeError("Database is not configured")
    # Normalize PostgreSQL URLs to the synchronous driver installed at runtime.
    sync_url = _sync_database_url(DATABASE_URL)
    return create_engine(
        sync_url,
        echo=DATABASE_ECHO,
        future=True,
        pool_pre_ping=True,
    )


@lru_cache(maxsize=1)
def get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(
        bind=get_engine(),
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        class_=Session,
    )


def get_db_session() -> Generator[Session, None, None]:
    if not DATABASE_ENABLED or not DATABASE_URL:
        raise HTTPException(
            status_code=503,
            detail="Database is not configured",
        )
    session_factory = get_session_factory()
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def get_database_health() -> dict[str, object]:
    if not DATABASE_ENABLED:
        return {
            "enabled": False,
            "connected": False,
            "detail": "APP_DATABASE_ENABLED=false",
        }
    if not DATABASE_URL:
        return {
            "enabled": True,
            "connected": False,
            "detail": "APP_DATABASE_URL is empty",
        }

    try:
        with get_engine().connect() as connection:
            scalar = connection.execute(text("SELECT 1")).scalar_one()
    except Exception as exc:
        return {
            "enabled": True,
            "connected": False,
            "detail": str(exc),
        }

    return {
        "enabled": True,
        "connected": bool(scalar == 1),
        "detail": "ok",
    }
