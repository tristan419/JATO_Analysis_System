import json
import os
from pathlib import Path


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


def _parse_float_env(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return float(raw_value.strip())
    except ValueError:
        return default


def _parse_csv_env(name: str, default: str) -> list[str]:
    raw_value = os.getenv(name, default)
    return [item.strip().rstrip("/") for item in raw_value.split(",") if item.strip()]


def _parse_filter_sets_env(name: str, default: str) -> list[dict[str, list[str]]]:
    raw_value = os.getenv(name, default).strip()
    if not raw_value:
        return []
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        return []

    filter_sets: list[dict[str, list[str]]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        filters: dict[str, list[str]] = {}
        for raw_column, raw_values in item.items():
            column = str(raw_column).strip()
            if not column:
                continue
            if isinstance(raw_values, str):
                values = [value.strip() for value in raw_values.split(",")]
            elif isinstance(raw_values, list):
                values = [
                    str(value).strip()
                    for value in raw_values
                    if value is not None
                ]
            else:
                continue
            normalized_values = _dedupe_values([value for value in values if value])
            if normalized_values:
                filters[column] = normalized_values
        filter_sets.append(filters)
    return filter_sets


def _dedupe_values(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
    return result


def _default_project_root() -> Path:
    # .../06_AppPlatform/backend/app/core/config.py -> project root
    return Path(__file__).resolve().parents[4]


PROJECT_ROOT = Path(
    os.getenv("APP_PROJECT_ROOT", str(_default_project_root()))
).resolve()


def resolve_msrp_governance_evidence_root(
    evidence_root: str | Path | None = None,
    *,
    project_root: Path | None = None,
) -> Path:
    base_root = (project_root or PROJECT_ROOT).resolve()
    configured_root = (
        str(evidence_root)
        if evidence_root is not None
        else os.getenv("MSRP_GOVERNANCE_EVIDENCE_ROOT", "").strip()
    )
    candidate = (
        Path(configured_root).expanduser()
        if configured_root
        else base_root
        / "04_Processed_data"
        / "ops"
        / "msrp_source_evidence"
    )
    if not candidate.is_absolute():
        candidate = base_root / candidate
    return candidate.resolve()


MSRP_GOVERNANCE_EVIDENCE_ROOT = resolve_msrp_governance_evidence_root()

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
JATO_MONTHLY_UPDATE_UPLOAD_MAX_BYTES = max(
    int(
        os.getenv(
            "APP_JATO_MONTHLY_UPDATE_UPLOAD_MAX_BYTES",
            str(1024 * 1024 * 1024),
        )
    ),
    1024 * 1024,
)
COC_MATCH_JOB_ROOT = Path(
    os.getenv(
        "APP_COC_MATCH_JOB_ROOT",
        str(PROJECT_ROOT / "04_Processed_data" / "ops" / "coc_match"),
    )
).resolve()
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
METADATA_PERSISTENT_CACHE_TTL_SECONDS = int(
    os.getenv("APP_METADATA_PERSISTENT_CACHE_TTL_SECONDS", "1800")
)
METADATA_PERSISTENT_CACHE_MAX_ENTRIES = int(
    os.getenv("APP_METADATA_PERSISTENT_CACHE_MAX_ENTRIES", "32")
)
METADATA_PERSISTENT_CACHE_ENABLED = _parse_bool_env(
    "APP_METADATA_PERSISTENT_CACHE_ENABLED",
    True,
)
METADATA_PERSISTENT_CACHE_DIR = Path(
    os.getenv(
        "APP_METADATA_PERSISTENT_CACHE_DIR",
        str(PROJECT_ROOT / "04_Processed_data" / "ops" / "metadata_cache"),
    )
).resolve()
METADATA_PREWARM_ENABLED = _parse_bool_env(
    "APP_METADATA_PREWARM_ENABLED",
    True,
)
HTTP_STRONG_CACHE_SECONDS = int(
    os.getenv("APP_HTTP_STRONG_CACHE_SECONDS", "3600")
)
_DEFAULT_GROUPED_TIME_SERIES_PREWARM_FILTERS_JSON = json.dumps(
    [
        {
            "国家": [
                "丹麦",
                "克罗地亚",
                "匈牙利",
                "奥地利",
                "希腊",
                "德国",
                "意大利",
                "挪威",
                "捷克",
                "斯洛伐克",
                "斯洛文尼亚",
                "比利时",
                "法国",
                "波兰",
                "瑞典",
                "瑞士",
                "罗马尼亚",
                "芬兰",
                "荷兰",
                "葡萄牙",
                "西班牙",
            ],
            "动总规整": ["ICE", "HEV", "BEV", "MHEV", "PHEV"],
        }
    ],
    ensure_ascii=False,
)
GROUPED_TIME_SERIES_CACHE_TTL_SECONDS = int(
    os.getenv("APP_GROUPED_TIME_SERIES_CACHE_TTL_SECONDS", "1800")
)
GROUPED_TIME_SERIES_REDIS_WAIT_SECONDS = _parse_float_env(
    "APP_GROUPED_TIME_SERIES_REDIS_WAIT_SECONDS",
    8.0,
)
GROUPED_TIME_SERIES_CACHE_MAX_ENTRIES = int(
    os.getenv("APP_GROUPED_TIME_SERIES_CACHE_MAX_ENTRIES", "64")
)
GROUPED_TIME_SERIES_PERSISTENT_CACHE_ENABLED = _parse_bool_env(
    "APP_GROUPED_TIME_SERIES_PERSISTENT_CACHE_ENABLED",
    True,
)
GROUPED_TIME_SERIES_PERSISTENT_CACHE_DIR = Path(
    os.getenv(
        "APP_GROUPED_TIME_SERIES_PERSISTENT_CACHE_DIR",
        str(PROJECT_ROOT / "04_Processed_data" / "ops" / "grouped_time_series_cache"),
    )
).resolve()
GROUPED_TIME_SERIES_PREWARM_ENABLED = _parse_bool_env(
    "APP_GROUPED_TIME_SERIES_PREWARM_ENABLED",
    True,
)
GROUPED_TIME_SERIES_PREWARM_GROUP_BY = _parse_csv_env(
    "APP_GROUPED_TIME_SERIES_PREWARM_GROUP_BY",
    "动总规整,国家,四驱占比,Business/Private 占比",
)
GROUPED_TIME_SERIES_PREWARM_SHARE_SPLIT_BY = _parse_csv_env(
    "APP_GROUPED_TIME_SERIES_PREWARM_SHARE_SPLIT_BY",
    "segment,powertrain",
)
GROUPED_TIME_SERIES_PREWARM_GRAINS = _parse_csv_env(
    "APP_GROUPED_TIME_SERIES_PREWARM_GRAINS",
    "month,year",
)
GROUPED_TIME_SERIES_PREWARM_SCOPES = _parse_csv_env(
    "APP_GROUPED_TIME_SERIES_PREWARM_SCOPES",
    "viewer,order_filler,editor,admin",
)
GROUPED_TIME_SERIES_PREWARM_TOP_N = max(
    1,
    int(os.getenv("APP_GROUPED_TIME_SERIES_PREWARM_TOP_N", "10")),
)
GROUPED_TIME_SERIES_PREWARM_INCLUDE_OTHERS = _parse_bool_env(
    "APP_GROUPED_TIME_SERIES_PREWARM_INCLUDE_OTHERS",
    False,
)
GROUPED_TIME_SERIES_PREWARM_FILTERS = _parse_filter_sets_env(
    "APP_GROUPED_TIME_SERIES_PREWARM_FILTERS_JSON",
    _DEFAULT_GROUPED_TIME_SERIES_PREWARM_FILTERS_JSON,
)
DASHBOARD_OVERVIEW_CACHE_TTL_SECONDS = int(
    os.getenv("APP_DASHBOARD_OVERVIEW_CACHE_TTL_SECONDS", "1800")
)
DASHBOARD_OVERVIEW_REDIS_WAIT_SECONDS = _parse_float_env(
    "APP_DASHBOARD_OVERVIEW_REDIS_WAIT_SECONDS",
    4.0,
)
DASHBOARD_OVERVIEW_CACHE_MAX_ENTRIES = int(
    os.getenv("APP_DASHBOARD_OVERVIEW_CACHE_MAX_ENTRIES", "64")
)
DASHBOARD_OVERVIEW_PERSISTENT_CACHE_ENABLED = _parse_bool_env(
    "APP_DASHBOARD_OVERVIEW_PERSISTENT_CACHE_ENABLED",
    True,
)
DASHBOARD_OVERVIEW_PERSISTENT_CACHE_DIR = Path(
    os.getenv(
        "APP_DASHBOARD_OVERVIEW_PERSISTENT_CACHE_DIR",
        str(PROJECT_ROOT / "04_Processed_data" / "ops" / "dashboard_overview_cache"),
    )
).resolve()
DASHBOARD_OVERVIEW_PREWARM_ENABLED = _parse_bool_env(
    "APP_DASHBOARD_OVERVIEW_PREWARM_ENABLED",
    True,
)
DASHBOARD_OVERVIEW_PREWARM_SCOPES = _parse_csv_env(
    "APP_DASHBOARD_OVERVIEW_PREWARM_SCOPES",
    "viewer,order_filler,editor,admin",
)
DASHBOARD_OVERVIEW_PREWARM_FILTERS = _parse_filter_sets_env(
    "APP_DASHBOARD_OVERVIEW_PREWARM_FILTERS_JSON",
    _DEFAULT_GROUPED_TIME_SERIES_PREWARM_FILTERS_JSON,
)
DASHBOARD_REDIS_WAIT_INTERVAL_SECONDS = _parse_float_env(
    "APP_DASHBOARD_REDIS_WAIT_INTERVAL_SECONDS",
    0.25,
)

REDIS_URL = os.getenv("APP_REDIS_URL", "redis://localhost:6379/0").strip()
REDIS_ENABLED = _parse_bool_env("APP_REDIS_ENABLED", True)
MARKET_SCAN_CACHE_TTL_SECONDS = int(
    os.getenv("APP_MARKET_SCAN_CACHE_TTL_SECONDS", "1800")
)
MARKET_SCAN_CACHE_SCHEMA_VERSION = int(
    os.getenv("APP_MARKET_SCAN_CACHE_SCHEMA_VERSION", "4")
)
VERSION_COMPARISON_PERSISTENT_CACHE_ENABLED = _parse_bool_env(
    "APP_VERSION_COMPARISON_PERSISTENT_CACHE_ENABLED",
    True,
)
VERSION_COMPARISON_PERSISTENT_CACHE_TTL_SECONDS = int(
    os.getenv(
        "APP_VERSION_COMPARISON_PERSISTENT_CACHE_TTL_SECONDS",
        str(MARKET_SCAN_CACHE_TTL_SECONDS),
    )
)
VERSION_COMPARISON_PERSISTENT_CACHE_MAX_ENTRIES = int(
    os.getenv("APP_VERSION_COMPARISON_PERSISTENT_CACHE_MAX_ENTRIES", "96")
)
VERSION_COMPARISON_PERSISTENT_CACHE_DIR = Path(
    os.getenv(
        "APP_VERSION_COMPARISON_PERSISTENT_CACHE_DIR",
        str(PROJECT_ROOT / "04_Processed_data" / "ops" / "version_comparison_cache"),
    )
).resolve()

AUTH_ENABLED = _parse_bool_env("APP_AUTH_ENABLED", False)
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

# ── Feishu OAuth ──
FEISHU_APP_ID = os.getenv("APP_FEISHU_APP_ID", "").strip()
FEISHU_APP_SECRET = os.getenv("APP_FEISHU_APP_SECRET", "").strip()
FEISHU_ENABLED = bool(FEISHU_APP_ID and FEISHU_APP_SECRET)
FEISHU_REDIRECT_URI = os.getenv(
    "APP_FEISHU_REDIRECT_URI",
    "http://127.0.0.1:8000/v1/auth/feishu/callback",
).strip()

# ── Google OAuth ──
GOOGLE_CLIENT_ID = os.getenv("APP_GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("APP_GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_ENABLED = bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET)
GOOGLE_REDIRECT_URI = os.getenv(
    "APP_GOOGLE_REDIRECT_URI",
    "http://127.0.0.1:8000/v1/auth/google/callback",
).strip()
GOOGLE_OAUTH_PROXY_URL = os.getenv("APP_GOOGLE_OAUTH_PROXY_URL", "").strip()
GOOGLE_OAUTH_RELAY_URL = os.getenv("APP_GOOGLE_OAUTH_RELAY_URL", "").strip()
GOOGLE_OAUTH_RELAY_TOKEN = os.getenv("APP_GOOGLE_OAUTH_RELAY_TOKEN", "").strip()
GOOGLE_OAUTH_TIMEOUT_SECONDS = _parse_float_env(
    "APP_GOOGLE_OAUTH_TIMEOUT_SECONDS",
    15.0,
)

APP_FRONTEND_ORIGIN = os.getenv(
    "APP_FRONTEND_ORIGIN",
    "http://127.0.0.1:5173",
).strip().rstrip("/")
FRONTEND_ORIGINS = _dedupe_values(
    [
        APP_FRONTEND_ORIGIN,
        *_parse_csv_env(
            "APP_FRONTEND_ORIGINS",
            "http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000,https://www.ojeur.cloud,https://intl.ojeur.cloud",
        ),
    ]
)

CORS_ORIGINS = _dedupe_values(
    [
        *_parse_csv_env(
            "APP_CORS_ORIGINS",
            "http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000,https://www.ojeur.cloud,https://intl.ojeur.cloud",
        ),
        *FRONTEND_ORIGINS,
    ]
)
