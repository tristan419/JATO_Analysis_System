"""Extractor registry — maps source_code to extractor config and class."""

from __future__ import annotations

from typing import Type

from jato_scraper.base import BaseExtractor, ExtractorConfig

_REGISTRY: dict[str, tuple[ExtractorConfig, Type[BaseExtractor]]] = {}


def register(config: ExtractorConfig, cls: Type[BaseExtractor]) -> None:
    if config.source_code in _REGISTRY:
        raise ValueError(f"Duplicate source_code: {config.source_code}")
    _REGISTRY[config.source_code] = (config, cls)


def get(source_code: str) -> BaseExtractor:
    entry = _REGISTRY.get(source_code)
    if entry is None:
        raise KeyError(
            f"No extractor registered for source_code={source_code!r}. "
            f"Available: {sorted(_REGISTRY)}"
        )
    config, cls = entry
    return cls(config)


def list_registered() -> list[str]:
    return sorted(_REGISTRY)


def get_config(source_code: str) -> ExtractorConfig:
    entry = _REGISTRY.get(source_code)
    if entry is None:
        raise KeyError(f"No extractor registered for source_code={source_code!r}")
    return entry[0]
