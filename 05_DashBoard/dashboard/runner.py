import streamlit as st

from .config import APP_TITLE, PARQUET_RELATIVE_PATH
from .data import get_project_root, load_full_data, resolve_columns
from .filters import render_sidebar_filters
from .styles import apply_ui_style
from .views import render_dashboard


def configure_page() -> None:
    st.set_page_config(
        page_title=APP_TITLE,
        layout="wide",
        page_icon="🚗",
    )


def main() -> None:
    configure_page()
    apply_ui_style()

    parquet_path = get_project_root() / PARQUET_RELATIVE_PATH
    try:
        df = load_full_data(str(parquet_path))
    except FileNotFoundError as error:
        st.error(str(error))
        st.stop()

    columns = resolve_columns(df)
    filtered_df, selections = render_sidebar_filters(df, columns)
    render_dashboard(filtered_df, columns, selections)
