"""Compatibility bridge for the standalone 07_ScrapingToolkit package."""

from __future__ import annotations

import sys
from pathlib import Path


def enable_external_scraper_package() -> None:
	repo_root = Path(__file__).resolve().parents[4]
	toolkit_root = repo_root / "07_ScrapingToolkit"
	toolkit_path = str(toolkit_root)
	if toolkit_root.exists() and toolkit_path not in sys.path:
		sys.path.insert(0, toolkit_path)


enable_external_scraper_package()
