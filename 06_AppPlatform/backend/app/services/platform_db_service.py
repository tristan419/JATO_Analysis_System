from app.db.session import get_database_health


def read_database_health() -> dict[str, object]:
    return get_database_health()
