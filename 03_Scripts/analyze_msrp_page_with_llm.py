#!/usr/bin/env python3
"""Thin wrapper around the toolkit MSRP page analyzer CLI."""

from __future__ import annotations

import sys
from pathlib import Path


_toolkit_dir = str(
    Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"
)
if _toolkit_dir not in sys.path:
    sys.path.insert(0, _toolkit_dir)


def _run() -> int:
    from jato_scraper.llm.msrp_page_analyzer import main

    return main()


if __name__ == "__main__":
    raise SystemExit(_run())
