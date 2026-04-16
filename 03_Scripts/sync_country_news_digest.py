#!/usr/bin/env python3
from pathlib import Path
import runpy

runpy.run_path(
    str(Path(__file__).resolve().parent / "news" / "sync_country_news_digest.py"),
    run_name="__main__",
)
