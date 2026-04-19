"""Generate draft MSRP source YAMLs from candidate scope reports."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REPORT_PATH = (
    PROJECT_ROOT
    / "04_Processed_data"
    / "msrp_candidate_scope"
    / "all_market"
    / "candidate_scope_report.json"
)
DEFAULT_OUTPUT_DIR = (
    PROJECT_ROOT
    / "07_ScrapingToolkit"
    / "source_drafts"
    / "all_market_country_model_top30"
)
COUNTRY_CODE_MAP = {
    "丹麦": "dk",
    "克罗地亚": "hr",
    "匈牙利": "hu",
    "奥地利": "at",
    "希腊": "gr",
    "德国": "de",
    "意大利": "it",
    "挪威": "no",
    "捷克": "cz",
    "斯洛伐克": "sk",
    "斯洛文尼亚": "si",
    "比利时": "be",
    "法国": "fr",
    "波兰": "pl",
    "瑞典": "se",
    "瑞士": "ch",
    "罗马尼亚": "ro",
    "芬兰": "fi",
    "荷兰": "nl",
    "葡萄牙": "pt",
    "西班牙": "es",
}


@dataclass(frozen=True)
class DraftOpportunity:
    country: str
    country_code: str
    country_priority_rank: int
    country_model_rank: int
    brand: str
    brand_slug: str
    model: str
    model_slug: str
    sales_12m: float
    candidate_model_count: int
    top_models: tuple[str, ...]
    source_code: str
    file_name: str
    relative_path: str
    jato_powertrains: tuple[str, ...]


def _coerce_path(path_value: str | Path | None) -> Path | None:
    if path_value is None:
        return None
    return Path(path_value).expanduser().resolve()


def _slugify_brand(brand: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(brand).casefold())
    normalized = normalized.strip("_")
    if not normalized:
        raise ValueError(f"Unable to build brand slug from {brand!r}")
    return normalized


def _slugify_token(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value).casefold())
    normalized = normalized.strip("_")
    return normalized or "value"


def _normalize_lookup_value(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split()).casefold()


def _resolve_existing_column(
    column_names: list[str],
    candidates: tuple[str, ...],
) -> str | None:
    lookup = {_normalize_lookup_value(name): name for name in column_names}
    for candidate in candidates:
        match = lookup.get(_normalize_lookup_value(candidate))
        if match:
            return match
    return None


def _open_parquet_dataset(path: Path) -> ds.Dataset:
    if path.is_file():
        return ds.dataset(path, format="parquet")
    return ds.dataset(
        path,
        format="parquet",
        partitioning="hive",
        exclude_invalid_files=True,
    )


def _resolve_report_dataset_path(report: dict[str, Any]) -> Path | None:
    dataset_path = report.get("dataset_path")
    if not dataset_path:
        return None
    candidate = Path(str(dataset_path)).expanduser()
    resolved = (
        candidate.resolve()
        if candidate.is_absolute()
        else (PROJECT_ROOT / candidate).resolve()
    )
    if resolved.exists():
        return resolved
    return None


def _canonicalize_jato_powertrain(value: object) -> str | None:
    normalized = _normalize_lookup_value(value).replace("-", " ")
    normalized = " ".join(normalized.split())
    if not normalized or normalized == "?":
        return None

    aliases = {
        "bev": "BEV",
        "battery electric vehicle": "BEV",
        "ev": "BEV",
        "phev": "PHEV",
        "plug in hybrid": "PHEV",
        "plugin hybrid": "PHEV",
        "plug in hybrid electric": "PHEV",
        "mhev": "MHEV",
        "mild hybrid": "MHEV",
        "hev": "HEV",
        "full hybrid": "HEV",
        "hybrid electric": "HEV",
        "ice": "ICE",
        "combustion": "ICE",
        "petrol": "ICE",
        "gasoline": "ICE",
        "diesel": "ICE",
        "lpg": "LPG",
        "cng": "CNG",
        "fcev": "FCEV",
        "fuel cell": "FCEV",
        "hydrogen": "FCEV",
    }
    mapped = aliases.get(normalized)
    if mapped:
        return mapped
    return re.sub(r"\s+", " ", str(value).strip()).upper() or None


def _sort_powertrains(values: set[str]) -> tuple[str, ...]:
    order = {
        "BEV": 1,
        "PHEV": 2,
        "HEV": 3,
        "MHEV": 4,
        "ICE": 5,
        "LPG": 6,
        "CNG": 7,
        "FCEV": 8,
    }
    return tuple(sorted(values, key=lambda item: (order.get(item, 99), item)))


def _unique_preserve_order(values: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        token = str(value).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return tuple(ordered)


def load_jato_powertrain_lookup(
    report: dict[str, Any],
) -> dict[tuple[str, str, str], tuple[str, ...]]:
    dataset_path = _resolve_report_dataset_path(report)
    if dataset_path is None:
        return {}

    dataset = _open_parquet_dataset(dataset_path)
    column_names = [str(name).strip() for name in dataset.schema.names]
    country_col = _resolve_existing_column(
        column_names,
        ("国家", "Countries", "Country", "country"),
    )
    brand_col = _resolve_existing_column(
        column_names,
        ("Make", "品牌 (英)", "Brand", "brand"),
    )
    model_col = _resolve_existing_column(
        column_names,
        ("Model", "车型规整", "model"),
    )
    powertrain_col = _resolve_existing_column(
        column_names,
        ("powertrain", "Powertrain", "动总规整", "Powertrain type"),
    )
    if not country_col or not brand_col or not model_col or not powertrain_col:
        return {}

    frame = dataset.to_table(
        columns=[country_col, brand_col, model_col, powertrain_col]
    ).to_pandas()

    lookup: dict[tuple[str, str, str], set[str]] = {}
    for country, brand, model, powertrain in frame.itertuples(
        index=False,
        name=None,
    ):
        key = (
            _normalize_lookup_value(country),
            _normalize_lookup_value(brand),
            _normalize_lookup_value(model),
        )
        if not all(key):
            continue
        canonical_powertrain = _canonicalize_jato_powertrain(powertrain)
        if canonical_powertrain is None:
            continue
        lookup.setdefault(key, set()).add(canonical_powertrain)

    return {
        key: _sort_powertrains(values)
        for key, values in lookup.items()
        if values
    }


def _build_powertrain_rule_scaffold(
    opportunity: DraftOpportunity,
) -> list[dict[str, Any]]:
    if not opportunity.jato_powertrains:
        return [
            {
                "key": "powertrain_primary",
                "powertrain": "TODO_JATO_POWERTRAIN",
                "keywords": ["TODO_KEYWORD"],
            },
        ]

    return [
        {
            "key": f"powertrain_{_slugify_token(powertrain)}",
            "powertrain": powertrain,
            "keywords": [
                f"TODO_{_slugify_token(powertrain).upper()}_KEYWORD"
            ],
        }
        for powertrain in opportunity.jato_powertrains
    ]


def _build_powertrain_bonus_scaffold(
    opportunity: DraftOpportunity,
) -> list[dict[str, Any]]:
    return [
        {
            "key": f"powertrain_{_slugify_token(powertrain)}",
            "label": f"Powertrain matched: {powertrain}",
            "powertrain": powertrain,
            "delta": 0.03,
        }
        for powertrain in opportunity.jato_powertrains
    ]


def _build_model_rule_scaffold(
    opportunity: DraftOpportunity,
    max_rules: int = 5,
) -> list[dict[str, Any]]:
    return [
        {
            "key": f"model_{_slugify_token(model)}",
            "keyword": model,
            "jato_model": model,
        }
        for model in opportunity.top_models[: max(1, max_rules)]
    ]


def load_candidate_scope_report(
    report_path: str | Path | None = None,
) -> dict[str, Any]:
    path = _coerce_path(report_path) or DEFAULT_REPORT_PATH
    if not path.exists():
        raise FileNotFoundError(
            f"Candidate scope report does not exist: {path}"
        )
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Candidate scope report must be a JSON object.")
    return payload


def rank_source_draft_opportunities(
    report: dict[str, Any],
    batch_size: int = 0,
    rollout_batch_size: int = 10,
    jato_powertrains_by_key: (
        dict[tuple[str, str, str], tuple[str, ...]] | None
    ) = None,
) -> list[DraftOpportunity]:
    del rollout_batch_size

    grouped_rows: dict[tuple[str, str], dict[str, Any]] = {}
    country_order: dict[str, int] = {}
    for country_index, country_summary in enumerate(
        report.get("country_summaries", []),
        start=1,
    ):
        country = str(country_summary.get("country") or "").strip()
        if not country:
            continue
        country_order.setdefault(country, country_index)
        country_code = COUNTRY_CODE_MAP.get(country)
        if not country_code:
            raise KeyError(f"Missing country code mapping for {country!r}")

        for candidate in country_summary.get("candidates", []):
            if candidate.get("coverage_status") != "missing_source":
                continue
            brand = str(candidate.get("brand") or "").strip()
            model = str(candidate.get("model") or "").strip()
            if not brand or not model:
                continue
            sales_12m = float(candidate.get("sales_12m") or 0.0)
            row = grouped_rows.setdefault(
                (country, brand),
                {
                    "country": country,
                    "country_code": country_code,
                    "country_order": country_order[country],
                    "country_model_rank": (
                        int(candidate.get("rank") or 0) or 9999
                    ),
                    "brand": brand,
                    "brand_slug": _slugify_brand(brand),
                    "models": [],
                    "sales_12m": 0.0,
                },
            )
            row["country_model_rank"] = min(
                int(row["country_model_rank"]),
                int(candidate.get("rank") or 0) or 9999,
            )
            row["sales_12m"] = float(row["sales_12m"]) + sales_12m
            row["models"].append(
                {
                    "model": model,
                    "model_slug": _slugify_token(model),
                    "sales_12m": sales_12m,
                    "rank": int(candidate.get("rank") or 0) or 9999,
                }
            )

    ranked_rows = list(grouped_rows.values())
    ranked_rows.sort(
        key=lambda row: (
            row["country_order"],
            row["country_model_rank"],
            row["brand"],
        )
    )

    country_rank_counters: dict[str, int] = {}
    opportunities: list[DraftOpportunity] = []
    for row in ranked_rows:
        country_rank_counters[row["country"]] = (
            country_rank_counters.get(row["country"], 0) + 1
        )
        models = sorted(
            row["models"],
            key=lambda item: (
                int(item["rank"]),
                -float(item["sales_12m"]),
                str(item["model"]),
            ),
        )
        top_models = _unique_preserve_order(
            [str(item["model"]) for item in models]
        )
        representative_model = top_models[0] if top_models else "MODEL"
        model_slug = _slugify_token(representative_model)
        country_code = row["country_code"]
        powertrain_values: set[str] = set()
        for model in top_models:
            lookup_key = (
                _normalize_lookup_value(row["country"]),
                _normalize_lookup_value(row["brand"]),
                _normalize_lookup_value(model),
            )
            powertrain_values.update(
                (jato_powertrains_by_key or {}).get(lookup_key, ())
            )
        file_name = (
            f"{country_rank_counters[row['country']]:02d}_"
            f"{row['brand_slug']}_{country_code}.yaml"
        )
        source_code = f"{row['brand_slug']}_{country_code}_draft_scrapling"
        opportunities.append(
            DraftOpportunity(
                country=row["country"],
                country_code=country_code,
                country_priority_rank=country_rank_counters[row["country"]],
                country_model_rank=row["country_model_rank"],
                brand=row["brand"],
                brand_slug=row["brand_slug"],
                model=representative_model,
                model_slug=model_slug,
                sales_12m=row["sales_12m"],
                candidate_model_count=len(top_models),
                top_models=top_models,
                source_code=source_code,
                file_name=file_name,
                relative_path=str(Path(row["country_code"]) / file_name),
                jato_powertrains=_sort_powertrains(powertrain_values),
            )
        )

    if batch_size > 0:
        opportunities = opportunities[:batch_size]

    return opportunities


def render_source_yaml_draft(opportunity: DraftOpportunity) -> str:
    placeholder_url = (
        "https://todo.invalid/"
        f"{opportunity.country_code}/{opportunity.brand_slug}/"
        f"{opportunity.brand_slug}"
    )
    model_rules = _build_model_rule_scaffold(opportunity)
    powertrain_rules = _build_powertrain_rule_scaffold(opportunity)
    powertrain_bonuses = _build_powertrain_bonus_scaffold(opportunity)
    payload = {
        "source_code": opportunity.source_code,
        "country": opportunity.country,
        "brand": opportunity.brand,
        "source_url": placeholder_url,
        "source_type": "manufacturer_official",
        "price_semantics": "base_msrp",
        "extractor_type": "scrapling",
        "profile": {
            "url": placeholder_url,
            "tier": "stealth",
            "headless": True,
            "network_idle": True,
            "impersonate": "chrome",
            "css": {
                "vehicle_container": "TODO_SELECTOR",
                "model": "TODO_SELECTOR",
                "trim": "TODO_SELECTOR",
                "price": "TODO_SELECTOR",
            },
            "default_currency": "TODO",
            "default_tax_included": True,
            "default_price_label": "TODO verify local MSRP label",
            "fixed_model": None,
            "fixed_jato_model": None,
            "fixed_jato_powertrain": None,
            "model_rules": model_rules,
            "skip_if_model_unmapped": False,
            "copy_trim_to_jato_trim": True,
            "confidence_rules": {
                "base": 0.20,
                "fixed_model_bonus": 0.18,
                "fixed_jato_model_bonus": 0.12,
                "model_rule_bonus": 0.12,
                "trim_present_bonus": 0.10,
                "copy_trim_to_jato_trim_bonus": 0.09,
                "parsed_price_text_bonus": 0.03,
                "currency_bonus": 0.01,
                "price_label_bonus": 0.02,
                "trim_keyword_bonuses": [],
                "price_band_bonuses": [
                    {
                        "key": "price_band_entry",
                        "label": "Entry price band matched",
                        "max": 0,
                        "delta": 0.00,
                    },
                    {
                        "key": "price_band_mid",
                        "label": "Mid price band matched",
                        "min": 0,
                        "max": 0,
                        "delta": 0.01,
                    },
                    {
                        "key": "price_band_high",
                        "label": "High price band matched",
                        "min": 0,
                        "delta": 0.03,
                    },
                ],
                "powertrain_keyword_bonuses": [],
                "powertrain_bonuses": powertrain_bonuses,
                "clamp_min": 0.0,
                "clamp_max": 1.0,
            },
            "structured_fields": {
                "edition_rules": [
                    {
                        "key": "edition_special",
                        "label": "Special Edition",
                        "keyword": "special edition",
                        "special": True,
                    },
                ],
                "powertrain_rules": powertrain_rules,
            },
            "auto_accept_gates": {
                "review_threshold": 0.95,
                "semi_auto_threshold": 0.98,
                "require_powertrain_match": True,
                "force_review_if_powertrain_missing": True,
                "force_review_if_powertrain_ambiguous": True,
                "force_review_for_special_edition": True,
            },
        },
        "schedule": {
            "frequency": "manual_only",
            "run_window_start": "00:00",
            "run_window_end": "05:00",
            "preferred_weekday": "monday",
        },
        "notes": (
            "Draft scaffold generated from country×brand top30 backlog. "
            "Replace source_url, profile.url, selectors, currency, "
            "price label, and the brand-cluster placeholders before "
            "promoting to "
            "07_ScrapingToolkit/sources."
        ),
        "bootstrap_meta": {
            "country_priority_rank": opportunity.country_priority_rank,
            "country_model_rank": opportunity.country_model_rank,
            "suggested_scope_kind": "brand_cluster",
            "candidate_model_count": opportunity.candidate_model_count,
            "sales_12m": round(opportunity.sales_12m, 2),
            "top_models": list(opportunity.top_models),
        },
    }
    header_lines = [
        "# Draft MSRP source scaffold generated from candidate scope report.",
        f"# Country: {opportunity.country} ({opportunity.country_code})",
        f"# Country queue rank: {opportunity.country_priority_rank}",
        f"# Country model rank: {opportunity.country_model_rank}",
        f"# Brand: {opportunity.brand}",
        (
            "# Candidate models: "
            f"{', '.join(opportunity.top_models) or opportunity.model}"
        ),
        "# Suggested scope kind: brand_cluster",
        f"# Sales 12M: {opportunity.sales_12m:,.0f}",
        f"# Draft path: {opportunity.relative_path}",
        (
            "# TODO: Replace source_url/profile.url with the official "
            "brand landing page, model hub, or configurator entry page."
        ),
        (
            "# TODO: Prefer model_rules over fixed_model/fixed_jato_model for "
            "brand-cluster drafts, and only set fixed_jato_powertrain when "
            "the page is powertrain-specific; otherwise rely on "
            "structured_fields.powertrain_rules."
        ),
        (
            "# TODO: Use JATO powertrain enums such as "
            "BEV/PHEV/HEV/MHEV/ICE/LPG for fixed_jato_powertrain and "
            "structured_fields.powertrain_rules."
        ),
        (
            "# TODO: Promote only if the page exposes true MSRP semantics, "
            "not "
            "leasing or finance monthly payments."
        ),
        (
            "# TODO: Replace price_band_bonuses min/max placeholders "
            "with local pricing bands."
        ),
        "",
    ]
    yaml_text = yaml.safe_dump(
        payload,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )
    return "\n".join(header_lines) + yaml_text


def render_draft_batch_markdown(
    opportunities: list[DraftOpportunity],
    report: dict[str, Any],
) -> str:
    filters = report.get("filters") or {}
    lines = [
        "# MSRP Source Draft Batch",
        "",
        f"Source report: {report.get('dataset_path')}",
        f"Candidate report top_n: {report.get('top_n')}",
        (
            f"Report vehicle category: {filters.get('vehicle_category')}"
            if filters.get("vehicle_category")
            else "Report vehicle category: ALL"
        ),
        f"Draft count: {len(opportunities)}",
        "Selection unit: country×brand backlog groups from the report.",
        "",
    ]

    grouped: dict[str, list[DraftOpportunity]] = {}
    for opportunity in opportunities:
        grouped.setdefault(opportunity.country, []).append(opportunity)

    for country in sorted(grouped):
        lines.extend(
            [
                f"## {country}",
                "",
                "| Rank | Brand | Model | Sales 12M | Draft Path |",
                "| ---: | --- | --- | ---: | --- |",
            ]
        )
        for opportunity in sorted(
            grouped[country],
            key=lambda item: (item.country_model_rank, item.brand, item.model),
        ):
            lines.append(
                (
                    f"| {opportunity.country_model_rank} | "
                    f"{opportunity.brand} | {opportunity.model} | "
                    f"{opportunity.sales_12m:,.0f} | "
                    f"{opportunity.relative_path} |"
                )
            )
        lines.append("")

    lines.extend([
        "## Notes",
        "",
        (
            "- These are draft scaffolds only and are not loaded by the "
            "current runner."
        ),
        (
            "- Promote a draft into 07_ScrapingToolkit/sources only after "
            "URL and selector verification."
        ),
        (
            "- Draft files are grouped by country and mirror the per-country "
            "top_n candidate ranking from the report."
        ),
        (
            "- Every draft is emitted as brand_cluster scope with "
            "model_rules, "
            "optional fixed_jato_powertrain placeholders, JATO powertrain "
            "rules, edition rules, and price_band_bonuses."
        ),
        (
            "- The current production candidate coverage report remains "
            "unchanged until promotion."
        ),
        "",
    ])
    return "\n".join(lines)


def write_source_draft_batch(
    opportunities: list[DraftOpportunity],
    report: dict[str, Any],
    output_dir: str | Path | None = None,
) -> dict[str, Path]:
    destination = _coerce_path(output_dir) or DEFAULT_OUTPUT_DIR
    destination.mkdir(parents=True, exist_ok=True)

    for opportunity in opportunities:
        draft_path = destination / opportunity.relative_path
        draft_path.parent.mkdir(parents=True, exist_ok=True)
        draft_path.write_text(
            render_source_yaml_draft(opportunity),
            encoding="utf-8",
        )

    summary_json_path = destination / "draft_batch_summary.json"
    summary_md_path = destination / "draft_batch_summary.md"
    summary_json_path.write_text(
        json.dumps(
            {
                "report_top_n": report.get("top_n"),
                "report_filters": report.get("filters") or {},
                "draft_count": len(opportunities),
                "opportunities": [asdict(item) for item in opportunities],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    summary_md_path.write_text(
        render_draft_batch_markdown(opportunities, report),
        encoding="utf-8",
    )
    return {
        "directory": destination,
        "summary_json": summary_json_path,
        "summary_markdown": summary_md_path,
    }


def generate_source_draft_batch(
    report_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    batch_size: int = 0,
    rollout_batch_size: int = 10,
) -> dict[str, Any]:
    report = load_candidate_scope_report(report_path)
    jato_powertrains_by_key = load_jato_powertrain_lookup(report)
    opportunities = rank_source_draft_opportunities(
        report=report,
        batch_size=batch_size,
        rollout_batch_size=rollout_batch_size,
        jato_powertrains_by_key=jato_powertrains_by_key,
    )
    output_paths = write_source_draft_batch(
        opportunities=opportunities,
        report=report,
        output_dir=output_dir,
    )
    return {
        "report": report,
        "opportunities": opportunities,
        "output_paths": output_paths,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate draft MSRP source YAMLs from candidate scope report."
        ),
    )
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--batch-size", type=int, default=0)
    parser.add_argument("--rollout-batch-size", type=int, default=10)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    result = generate_source_draft_batch(
        report_path=args.report_path,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        rollout_batch_size=args.rollout_batch_size,
    )
    opportunities = result["opportunities"]
    output_paths = result["output_paths"]
    print(f"Drafts={len(opportunities)}")
    print(f"DIR={output_paths['directory']}")
    print(f"SUMMARY_JSON={output_paths['summary_json']}")
    print(f"SUMMARY_MARKDOWN={output_paths['summary_markdown']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
