"""Country-aware VOC batch planner."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
from typing import Any

from jato_scraper.voc_base import VocBatchConfig
from jato_scraper.voc_config_loader import load_voc_batch_config
from jato_scraper.voc_taxonomy import get_source_collection_strategy
from jato_scraper.voc_taxonomy import get_voc_taxonomy_profile

REPO_ROOT = Path(__file__).resolve().parents[2]


def _normalize_country_filter(values: list[str] | None) -> set[str] | None:
    if not values:
        return None
    normalized = {value.strip().upper() for value in values if value.strip()}
    return normalized or None


def _resolve_output_root(output_root: str | Path) -> Path:
    candidate = Path(output_root).expanduser()
    if candidate.is_absolute():
        return candidate
    return REPO_ROOT / candidate


def build_voc_collection_plan(
    batch: VocBatchConfig,
    country_filter: set[str] | None = None,
    output_root: str | Path = "04_Processed_data/voc",
) -> dict[str, Any]:
    root = _resolve_output_root(output_root)
    countries_payload: list[dict[str, Any]] = []
    source_count = 0

    for country in batch.countries:
        if country_filter and country.country_code.upper() not in country_filter:
            continue
        country_root = root / country.country_code.lower()
        taxonomy = get_voc_taxonomy_profile(country.taxonomy_profile)
        payload = {
            "country_code": country.country_code,
            "country_label": country.country_label,
            "languages": list(country.languages),
            "taxonomy_profile": country.taxonomy_profile,
            "taxonomy": taxonomy,
            "source_count": len(country.sources),
            "raw_output_path": str(country_root / "raw"),
            "enriched_output_path": str(country_root / "enriched"),
            "deck_output_path": str(country_root / "deck" / "customer_insight_deck.json"),
            "sources": [
                {
                    **asdict(source),
                    "collection_strategy": get_source_collection_strategy(
                        source.site_type,
                    ),
                }
                for source in country.sources
            ],
        }
        source_count += len(country.sources)
        countries_payload.append(payload)

    return {
        "batch_code": batch.batch_code,
        "description": batch.description,
        "country_count": len(countries_payload),
        "source_count": source_count,
        "countries": countries_payload,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build a country-aware VOC collection plan from batch YAML files.",
    )
    parser.add_argument(
        "--batch-files",
        nargs="+",
        required=True,
        help="Batch YAML files under voc_sources/ or absolute paths.",
    )
    parser.add_argument(
        "--countries",
        nargs="*",
        help="Optional list of country codes to keep (e.g. SE FI NO DK).",
    )
    parser.add_argument(
        "--output-root",
        default="04_Processed_data/voc",
        help="Root path prefix to use in planned raw/enriched/deck outputs.",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Optional JSON output path.",
    )
    args = parser.parse_args(argv)

    country_filter = _normalize_country_filter(args.countries)
    payload = [
        build_voc_collection_plan(
            load_voc_batch_config(batch_file),
            country_filter=country_filter,
            output_root=args.output_root,
        )
        for batch_file in args.batch_files
    ]

    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
