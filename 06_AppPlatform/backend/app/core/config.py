from pathlib import Path
import os


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        env_key = key.strip()
        if not env_key or env_key in os.environ:
            continue
        env_value = raw_value.strip().strip("'\"")
        os.environ[env_key] = env_value


def _load_local_env_files() -> None:
    current_file = Path(__file__).resolve()
    for env_path in (
        current_file.parents[2] / ".env",
        current_file.parents[3] / ".env",
        current_file.parents[4] / ".env",
    ):
        _load_env_file(env_path)


_load_local_env_files()


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
ENGINEERING_IMPORT_ROOT = Path(
    os.getenv(
        "APP_ENGINEERING_IMPORT_ROOT",
        str(PROJECT_ROOT / "01_RAW_DATA"),
    )
).resolve()
JATO_MONTHLY_UPDATE_JOB_ROOT = Path(
    os.getenv(
        "APP_JATO_MONTHLY_UPDATE_JOB_ROOT",
        str(PROJECT_ROOT / "04_Processed_data" / "ops" / "jato_monthly_update_jobs"),
    )
).resolve()
JATO_MONTHLY_UPDATE_UPLOAD_CHUNK_SIZE_BYTES = max(
    int(os.getenv("APP_JATO_MONTHLY_UPDATE_UPLOAD_CHUNK_SIZE_BYTES", str(8 * 1024 * 1024))),
    1024 * 1024,
)
DATABASE_URL = os.getenv("APP_DATABASE_URL", "").strip()
DATABASE_ENABLED = _parse_bool_env(
    "APP_DATABASE_ENABLED",
    bool(DATABASE_URL),
)
DATABASE_ECHO = _parse_bool_env("APP_DATABASE_ECHO", False)

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

# Token → role mapping: "token1:admin,token2:editor,token3:viewer"
# Falls back to AUTH_TOKEN with editor role when not set.
_raw_token_role_map = os.getenv("APP_TOKEN_ROLE_MAP", "").strip()
TOKEN_ROLE_MAP: dict[str, str] = {}
if _raw_token_role_map:
    for pair in _raw_token_role_map.split(","):
        pair = pair.strip()
        if ":" in pair:
            tok, role = pair.rsplit(":", 1)
            TOKEN_ROLE_MAP[tok.strip()] = role.strip().lower()
if not TOKEN_ROLE_MAP and AUTH_TOKEN:
    TOKEN_ROLE_MAP[AUTH_TOKEN] = "editor"

CORS_ORIGINS: list[str] = [
    origin.strip()
    for origin in os.getenv(
        "APP_CORS_ORIGINS",
        "http://localhost:5173,http://localhost:3000",
    ).split(",")
    if origin.strip()
]
