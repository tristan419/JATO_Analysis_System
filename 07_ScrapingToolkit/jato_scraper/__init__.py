"""Standalone scraping toolkit package for JATO MSRP acquisition."""

from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation

__all__ = [
    "BaseExtractor",
    "ExtractorConfig",
    "RawObservation",
    "load_all_sources",
    "main",
    "run_scrape",
]


def __getattr__(name: str):
    if name in {"main", "run_scrape"}:
        from jato_scraper.runner import main, run_scrape

        return {"main": main, "run_scrape": run_scrape}[name]
    if name == "load_all_sources":
        from jato_scraper.config_loader import load_all_sources

        return load_all_sources
    raise AttributeError(f"module 'jato_scraper' has no attribute {name!r}")
