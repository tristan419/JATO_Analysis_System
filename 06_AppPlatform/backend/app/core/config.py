from pathlib import Path
import os


def _parse_bool_env(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _default_project_root() -> Path:
    # .../06_AppPlatform/backend/app/core/config.py -> project root
    return Path(__file__).resolve().parents[4]


PROJECT_ROOT = Path(
    os.getenv("APP_PROJECT_ROOT", str(_default_project_root()))
).resolve()

PARQUET_PATH = Path(
    os.getenv(
        "JATO_PARQUET_PATH",
        str(PROJECT_ROOT / "04_Processed_data" / "jato_full_archive.parquet"),
    )
)
PARTITIONED_PATH = Path(
    os.getenv(
        "JATO_PARTITIONED_PATH",
        str(PROJECT_ROOT / "04_Processed_data" / "partitioned_dataset_v1"),
    )
)
PRECOMPUTED_DIR = Path(
    os.getenv(
        "JATO_PRECOMPUTED_DIR",
        str(PROJECT_ROOT / "04_Processed_data" / "summaries"),
    )
)
CRUD_DATA_PATH = Path(
    os.getenv(
        "APP_CRUD_DATA_PATH",
        str(PROJECT_ROOT / "04_Processed_data" / "app_entities.json"),
    )
)

API_PREFIX = "/v1"
APP_NAME = "JATO Fullstack API"
APP_VERSION = "0.1.0"

MAX_RAW_ROWS = int(os.getenv("APP_MAX_RAW_ROWS", "5000"))
MAX_GROUP_METRICS = int(os.getenv("APP_MAX_GROUP_METRICS", "6"))
DEFAULT_GROUP_BY = os.getenv("APP_DEFAULT_GROUP_BY", "国家")
MAX_DETAIL_PAGE_SIZE = int(os.getenv("APP_MAX_DETAIL_PAGE_SIZE", "1000"))
MAX_EXPORT_ROWS = int(os.getenv("APP_MAX_EXPORT_ROWS", "10000"))
MAX_CRUD_PAGE_SIZE = int(os.getenv("APP_MAX_CRUD_PAGE_SIZE", "200"))
FILTER_OPTIONS_CACHE_TTL_SECONDS = int(
    os.getenv("APP_FILTER_OPTIONS_CACHE_TTL_SECONDS", "300")
)
FILTER_OPTIONS_CACHE_MAX_ENTRIES = int(
    os.getenv("APP_FILTER_OPTIONS_CACHE_MAX_ENTRIES", "512")
)
FILTER_OPTIONS_SNAPSHOT_TTL_SECONDS = int(
    os.getenv("APP_FILTER_OPTIONS_SNAPSHOT_TTL_SECONDS", "300")
)

AUTH_ENABLED = _parse_bool_env("APP_AUTH_ENABLED", True)
AUTH_TOKEN = os.getenv("APP_AUTH_TOKEN", "change-me")
