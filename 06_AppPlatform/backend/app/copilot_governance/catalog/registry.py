"""Metadata Catalog registry — load and query dataset catalog."""

from __future__ import annotations

import json
from pathlib import Path

from app.copilot_governance.catalog.models import DatasetCatalogItem

_CATALOG: dict[str, DatasetCatalogItem] = {}
_LOADED = False

_CATALOG_DIR = Path(__file__).resolve().parent / "datasets"


def load_catalog() -> dict[str, DatasetCatalogItem]:
    global _CATALOG, _LOADED
    if _LOADED:
        return _CATALOG
    _CATALOG = {}
    if _CATALOG_DIR.is_dir():
        for yaml_file in sorted(_CATALOG_DIR.glob("*.yaml")):
            try:
                import yaml
                with open(yaml_file) as fh:
                    data = yaml.safe_load(fh)
                if isinstance(data, dict) and "dataset_id" in data:
                    item = DatasetCatalogItem(**data)
                    _CATALOG[item.dataset_id] = item
            except Exception:
                pass
    if not _CATALOG:
        _CATALOG = _build_default_catalog()
    _LOADED = True
    return _CATALOG


def get_dataset(dataset_id: str) -> DatasetCatalogItem | None:
    return load_catalog().get(dataset_id)


def find_datasets_by_intent(intent: str) -> list[DatasetCatalogItem]:
    return [
        item
        for item in load_catalog().values()
        if intent in item.allowed_intents
    ]


def find_datasets_by_lane(source_lane: str) -> list[DatasetCatalogItem]:
    return [
        item
        for item in load_catalog().values()
        if item.source_lane == source_lane
    ]


def _build_default_catalog() -> dict[str, DatasetCatalogItem]:
    return {
        "jato_sales_parquet": DatasetCatalogItem(
            dataset_id="jato_sales_parquet",
            display_name="JATO Sales Parquet",
            source_lane="structured_bi",
            storage_type="parquet",
            grain="country_month_model_version",
            freshness_field="year_month",
            allowed_intents=[
                "metric_query", "comparison", "trend", "distribution",
                "correlation", "pricing_strategy", "product_strategy", "country_report",
                "brand-ranking", "segment-analysis", "origin-analysis",
                "powertrain-mix", "nev-analysis", "general-summary", "trend-summary",
            ],
            required_filters=["country"],
            governance={"max_rows": 5000, "allow_full_scan": False, "readonly": True},
        ),
        "current_price_postgres": DatasetCatalogItem(
            dataset_id="current_price_postgres",
            display_name="CurrentPrice (PostgreSQL)",
            source_lane="canonical_entity",
            storage_type="postgresql",
            grain="country_model_trim_powertrain",
            allowed_intents=[
                "pricing_strategy", "product_strategy", "comparison",
                "precise-lookup", "positioning-focus", "positioning-analysis",
            ],
            governance={"readonly": True},
        ),
        "voc_forum_artifacts": DatasetCatalogItem(
            dataset_id="voc_forum_artifacts",
            display_name="VOC Forum Artifacts",
            source_lane="voc",
            storage_type="artifact_json",
            grain="country_source_document_content_unit",
            freshness_field="fetched_at",
            allowed_intents=["voc_insight", "product_strategy", "pricing_strategy", "country_report"],
            governance={"qualitative_only": True, "cannot_claim_market_share": True},
        ),
        "news_digest": DatasetCatalogItem(
            dataset_id="news_digest",
            display_name="News Digest",
            source_lane="news",
            storage_type="postgresql",
            grain="country_article",
            freshness_field="published_at",
            allowed_intents=["news_intelligence", "market-context", "country_report", "policy_tax"],
            governance={"readonly": True},
        ),
        "country_profiles": DatasetCatalogItem(
            dataset_id="country_profiles",
            display_name="Country Profiles",
            source_lane="policy_tax",
            storage_type="python_dict",
            grain="country",
            allowed_intents=["policy_tax", "country_report", "market-context"],
            governance={"readonly": True},
        ),
    }
