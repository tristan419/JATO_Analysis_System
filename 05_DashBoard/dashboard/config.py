from pathlib import Path
import re

import plotly.express as px


APP_TITLE = "JATO 数据全量可视化分析"
PARQUET_RELATIVE_PATH = Path("04_Processed_data/jato_full_archive.parquet")

YEAR_COL_PATTERN = re.compile(r"^\d{4}$")
MONTH_COL_PATTERN = re.compile(
    r"^\d{4}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$",
    re.IGNORECASE,
)

PLOT_CONFIG = {
    "displaylogo": False,
    "responsive": True,
}
COLOR_SEQ = px.colors.qualitative.Safe
