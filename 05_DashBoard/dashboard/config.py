from pathlib import Path
import os
import re

import plotly.express as px


APP_TITLE = "JATO 数据全量可视化分析"
PARQUET_RELATIVE_PATH = Path("04_Processed_data/jato_full_archive.parquet")
PARTITIONED_DATASET_RELATIVE_PATH = Path(
    "04_Processed_data/partitioned_dataset_v1"
)

# Allow overriding dataset locations in containers/cloud runtimes.
PARQUET_RELATIVE_PATH = Path(
    os.getenv(
        "JATO_PARQUET_PATH",
        str(PARQUET_RELATIVE_PATH),
    )
)
PARTITIONED_DATASET_RELATIVE_PATH = Path(
    os.getenv(
        "JATO_PARTITIONED_PATH",
        str(PARTITIONED_DATASET_RELATIVE_PATH),
    )
)

YEAR_COL_PATTERN = re.compile(r"^\d{4}$")
MONTH_COL_PATTERN = re.compile(
    r"^\d{4}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$",
    re.IGNORECASE,
)

MSRP_CANDIDATES = (
    "MSRP规整",
    "MSRP including delivery charge",
    "MSRP",
    "MSRP区间",
)

LENGTH_CANDIDATES = (
    "length (mm)",
    "车长(mm)",
    "车长",
    "length",
)

BATTERY_RANGE_CANDIDATES = (
    "Battery range",
    "Battery Range",
    "battery range",
    "WLTP range",
    "EV range",
    "续航里程",
    "电池续航",
)

BATTERY_CAPACITY_CANDIDATES = (
    "Battery kwh",
    "Battery kWh",
    "Useable battery kilowatt hour (kWh)",
    "Battery capacity",
    "Battery Capacity",
    "电池容量",
)

CACHE_TTL_SCHEMA_SECONDS = 6 * 60 * 60
CACHE_TTL_SIDEBAR_SECONDS = 30 * 60
CACHE_TTL_ANALYSIS_SECONDS = 10 * 60
CACHE_TTL_DETAIL_SECONDS = 3 * 60

CACHE_MAX_ENTRIES_SCHEMA = 64
CACHE_MAX_ENTRIES_SIDEBAR = 192
CACHE_MAX_ENTRIES_ANALYSIS = 256
CACHE_MAX_ENTRIES_DETAIL = 96

CSV_DOWNLOAD_MAX_ROWS = 10_000
CSV_DOWNLOAD_MAX_BYTES = 12 * 1024 * 1024

PLOT_CONFIG = {
    "displaylogo": False,
    "responsive": True,
}
COLOR_SEQ = px.colors.qualitative.Safe
