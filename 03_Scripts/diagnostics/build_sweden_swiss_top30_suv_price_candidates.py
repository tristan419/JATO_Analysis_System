#!/usr/bin/env python3
"""Build review-only top30 SUV MSRP movement candidates for Sweden/Switzerland.

The artifact is a work queue for official historical backfill. It compares
existing JATO processed snapshots and current MSRP coverage, but it does not
claim official price movement without manufacturer evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCOPE_REPORT = (
    PROJECT_ROOT
    / "04_Processed_data"
    / "msrp_candidate_scope"
    / "suv_only"
    / "candidate_scope_report.json"
)
DEFAULT_SOURCE_DRAFT_ROOT = (
    PROJECT_ROOT
    / "07_ScrapingToolkit"
    / "source_drafts"
    / "suv_only_country_model_top30"
)
DEFAULT_OUTPUT_DIR = (
    PROJECT_ROOT
    / "03_Scripts"
    / "diagnostics"
    / "artifacts"
    / "msrp_backfill"
    / "sweden_swiss_top30_suv"
)
DEFAULT_DATABASE_URL = "postgresql+psycopg://litristan@127.0.0.1:5432/jato"

COUNTRIES = {
    "se": {
        "label": "Sweden",
        "scope_country": "瑞典",
        "parquet_dir": "国家=%E7%91%9E%E5%85%B8",
        "currency": "SEK",
    },
    "ch": {
        "label": "Switzerland",
        "scope_country": "瑞士",
        "parquet_dir": "国家=%E7%91%9E%E5%A3%AB",
        "currency": "CHF",
    },
}

SNAPSHOT_ROOTS = {
    "2026-02-corrected": "04_Processed_data/staging/2026-02-corrected/partitioned_dataset_v1",
    "2026-02-mixed": "04_Processed_data/staging/2026-02-mixed/partitioned_dataset_v1",
    "2026-03-partial12": "04_Processed_data/staging/2026-03-partial12/partitioned_dataset_v1",
    "current": "04_Processed_data/partitioned_dataset_v1",
}

LOCAL_PRICE_COLUMNS = (
    "MSRP including delivery charge",
    "Retail price",
    "Base price",
)
EUR_PRICE_COLUMNS = ("MSRP规整",)
VARIANT_KEY_COLUMNS = (
    "Version name",
    "Powertrain type",
    "Trim level",
    "Body type",
    "Fuel type",
    "Transmission type",
    "Driven wheels",
)


@dataclass(frozen=True)
class TopModel:
    country_code: str
    country_label: str
    rank: int
    brand: str
    model: str
    sales_12m: float
    latest_month: str | None
    window_start_month: str | None
    window_end_month: str | None
    months_in_window: int | None


def read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(f"Could not read JSON artifact {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"Expected JSON object in {path}")
    return data


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )


def _json_default(value: object) -> object:
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def _clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _norm(value: object) -> str:
    return _clean_text(value).casefold()


def _coerce_price_series(frame: pd.DataFrame, columns: tuple[str, ...]) -> tuple[pd.Series, str | None]:
    for column in columns:
        if column not in frame.columns:
            continue
        series = pd.to_numeric(frame[column], errors="coerce")
        series = series.where(series > 0)
        if series.dropna().empty:
            continue
        return series, column
    return pd.Series(dtype="float64"), None


def _series_stats(series: pd.Series) -> dict[str, float | None]:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return {"min": None, "median": None, "max": None}
    return {
        "min": round(float(clean.min()), 2),
        "median": round(float(clean.median()), 2),
        "max": round(float(clean.max()), 2),
    }


def load_top_models(scope_report_path: Path) -> list[TopModel]:
    report = read_json(scope_report_path)
    country_summaries = report.get("country_summaries") or []
    top_models: list[TopModel] = []
    for country_code, country_meta in COUNTRIES.items():
        scope_country = country_meta["scope_country"]
        summary = next(
            (
                item
                for item in country_summaries
                if isinstance(item, dict) and item.get("country") == scope_country
            ),
            None,
        )
        if not summary:
            continue
        for candidate in (summary.get("candidates") or [])[:30]:
            if not isinstance(candidate, dict):
                continue
            top_models.append(
                TopModel(
                    country_code=country_code,
                    country_label=str(country_meta["label"]),
                    rank=int(candidate.get("rank") or 0),
                    brand=str(candidate.get("brand") or "").strip().upper(),
                    model=str(candidate.get("model") or "").strip().upper(),
                    sales_12m=float(candidate.get("sales_12m") or 0.0),
                    latest_month=str(candidate.get("latest_month") or "") or None,
                    window_start_month=str(candidate.get("window_start_month") or "") or None,
                    window_end_month=str(candidate.get("window_end_month") or "") or None,
                    months_in_window=int(candidate["months_in_window"])
                    if candidate.get("months_in_window") is not None
                    else None,
                )
            )
    return top_models


def source_draft_payload(source_draft_root: Path, top_model: TopModel) -> dict[str, Any]:
    country_dir = source_draft_root / top_model.country_code
    if not country_dir.exists():
        return {"status": "missing_source_draft", "path": None}
    rank_prefix = f"{top_model.rank:02d}_"
    candidates = sorted(country_dir.glob(f"{rank_prefix}*.yaml"))
    if not candidates:
        candidates = [
            path
            for path in sorted(country_dir.glob("*.yaml"))
            if _norm(top_model.brand) in _norm(path.name)
            and _norm(top_model.model).replace(" ", "_") in _norm(path.name)
        ]
    if not candidates:
        return {"status": "missing_source_draft", "path": None}
    path = candidates[0]
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        return {
            "status": "unreadable_source_draft",
            "path": str(path.relative_to(PROJECT_ROOT)),
            "error": str(exc),
        }
    return {
        "status": "source_draft_found",
        "path": str(path.relative_to(PROJECT_ROOT)),
        "sourceCode": data.get("source_code"),
        "sourceUrl": data.get("source_url"),
        "sourceType": data.get("source_type"),
        "priceSemantics": data.get("price_semantics"),
        "extractorType": data.get("extractor_type"),
        "profilePreset": data.get("profile_preset"),
    }


def load_snapshot_frames(project_root: Path) -> dict[tuple[str, str], pd.DataFrame]:
    frames: dict[tuple[str, str], pd.DataFrame] = {}
    for snapshot_name, snapshot_root in SNAPSHOT_ROOTS.items():
        for country_code, country_meta in COUNTRIES.items():
            folder = project_root / snapshot_root / str(country_meta["parquet_dir"])
            files = sorted(folder.glob("*.parquet"))
            if not files:
                continue
            frames[(snapshot_name, country_code)] = pd.read_parquet(files[0])
    return frames


def model_frame(frame: pd.DataFrame, top_model: TopModel) -> pd.DataFrame:
    return frame[
        (frame.get("Make", "").astype(str).str.upper() == top_model.brand)
        & (frame.get("Model", "").astype(str).str.upper() == top_model.model)
    ].copy()


def snapshot_price_payload(frame: pd.DataFrame, top_model: TopModel) -> dict[str, Any]:
    matched = model_frame(frame, top_model)
    if matched.empty:
        return {
            "rowCount": 0,
            "localPriceField": None,
            "localPrice": {"min": None, "median": None, "max": None},
            "eurNormalizedField": None,
            "eurNormalized": {"min": None, "median": None, "max": None},
            "versionCount": 0,
        }
    local_series, local_field = _coerce_price_series(matched, LOCAL_PRICE_COLUMNS)
    eur_series, eur_field = _coerce_price_series(matched, EUR_PRICE_COLUMNS)
    return {
        "rowCount": int(len(matched)),
        "localPriceField": local_field,
        "localPrice": _series_stats(local_series),
        "eurNormalizedField": eur_field,
        "eurNormalized": _series_stats(eur_series),
        "versionCount": int(matched[["Version name", "Powertrain type"]].drop_duplicates().shape[0]),
    }


def _variant_key(row: pd.Series) -> str:
    return " | ".join(_clean_text(row.get(column)) or "-" for column in VARIANT_KEY_COLUMNS)


def variant_price_changes(
    frames: dict[tuple[str, str], pd.DataFrame],
    top_model: TopModel,
    *,
    threshold_pct: float,
) -> list[dict[str, Any]]:
    per_snapshot: dict[str, dict[str, dict[str, Any]]] = {}
    for snapshot_name in SNAPSHOT_ROOTS:
        frame = frames.get((snapshot_name, top_model.country_code))
        if frame is None:
            continue
        matched = model_frame(frame, top_model)
        if matched.empty:
            continue
        local_series, local_field = _coerce_price_series(matched, LOCAL_PRICE_COLUMNS)
        if local_field is None:
            continue
        matched = matched.assign(_local_price=local_series)
        matched = matched[matched["_local_price"].notna()]
        per_snapshot[snapshot_name] = {}
        for _, row in matched.iterrows():
            key = _variant_key(row)
            current = per_snapshot[snapshot_name].get(key)
            price = float(row["_local_price"])
            if current is None or price < current["localPrice"]:
                per_snapshot[snapshot_name][key] = {
                    "variantKey": key,
                    "localPrice": round(price, 2),
                    "localPriceField": local_field,
                    "powertrain": _clean_text(row.get("Powertrain type")),
                    "fuelType": _clean_text(row.get("Fuel type")),
                    "lengthMm": _safe_float(row.get("length (mm)")),
                }
    changes: list[dict[str, Any]] = []
    snapshot_names = [name for name in SNAPSHOT_ROOTS if name in per_snapshot]
    if len(snapshot_names) < 2:
        return changes
    common_keys = set(per_snapshot[snapshot_names[0]])
    for snapshot_name in snapshot_names[1:]:
        common_keys &= set(per_snapshot[snapshot_name])
    for key in sorted(common_keys):
        points = [
            {
                "snapshot": snapshot_name,
                **per_snapshot[snapshot_name][key],
            }
            for snapshot_name in snapshot_names
        ]
        prices = [point["localPrice"] for point in points]
        if max(prices) == min(prices):
            continue
        first_price = prices[0]
        last_price = prices[-1]
        change_pct = ((last_price - first_price) / first_price * 100.0) if first_price else None
        if change_pct is None or abs(change_pct) < threshold_pct:
            continue
        changes.append(
            {
                "variantKey": key,
                "firstSnapshot": snapshot_names[0],
                "lastSnapshot": snapshot_names[-1],
                "firstLocalPrice": first_price,
                "lastLocalPrice": last_price,
                "deltaLocal": round(last_price - first_price, 2),
                "deltaPct": round(change_pct, 2) if change_pct is not None else None,
                "points": points,
                "status": "candidate_from_jato_snapshot_not_official",
            }
        )
    changes.sort(key=lambda item: abs(float(item.get("deltaPct") or 0.0)), reverse=True)
    return changes[:10]


def _safe_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return round(number, 2)


def snapshot_movement_summary(
    snapshot_payloads: dict[str, dict[str, Any]],
    *,
    threshold_pct: float,
) -> dict[str, Any]:
    points = [
        (snapshot, payload)
        for snapshot, payload in snapshot_payloads.items()
        if payload["localPrice"]["median"] is not None
    ]
    if len(points) < 2:
        return {
            "status": "insufficient_price_snapshots",
            "firstLocalMedian": None,
            "lastLocalMedian": None,
            "deltaLocalMedian": None,
            "deltaPctMedian": None,
        }
    first_snapshot, first_payload = points[0]
    last_snapshot, last_payload = points[-1]
    first = float(first_payload["localPrice"]["median"])
    last = float(last_payload["localPrice"]["median"])
    delta = last - first
    delta_pct = (delta / first * 100.0) if first else None
    status = "no_jato_snapshot_median_shift"
    if delta_pct is not None and abs(delta_pct) >= threshold_pct:
        status = "candidate_from_jato_snapshot_median_shift"
    return {
        "status": status,
        "firstSnapshot": first_snapshot,
        "lastSnapshot": last_snapshot,
        "firstLocalMedian": round(first, 2),
        "lastLocalMedian": round(last, 2),
        "deltaLocalMedian": round(delta, 2),
        "deltaPctMedian": round(delta_pct, 2) if delta_pct is not None else None,
        "evidenceStatus": "review_only_needs_official_history",
    }


def current_price_coverage(database_url: str) -> dict[tuple[str, str, str], dict[str, Any]]:
    engine = create_engine(database_url)
    query = text(
        """
        select
            country,
            upper(brand) as brand,
            upper(jato_model) as jato_model,
            count(*) as row_count,
            min(source_msrp_value) as local_min,
            max(source_msrp_value) as local_max,
            min(current_msrp_value) as eur_min,
            max(current_msrp_value) as eur_max,
            min(source_currency) as source_currency,
            array_agg(distinct source_url) as source_urls,
            array_agg(distinct match_status) as match_statuses
        from msrp.current_prices
        group by country, upper(brand), upper(jato_model)
        """
    )
    coverage: dict[tuple[str, str, str], dict[str, Any]] = {}
    try:
        with engine.connect() as connection:
            rows = connection.execute(query).fetchall()
    except Exception as exc:  # pragma: no cover - local DB dependent
        print(f"[warn] Could not query current_price coverage: {exc}", file=sys.stderr)
        return coverage
    for row in rows:
        item = dict(row._mapping)
        country_label = _clean_text(item["country"])
        normalized_country = country_label.casefold()
        if normalized_country == "sweden":
            code = "se"
        elif normalized_country in {"switzerland", "swiss"}:
            code = "ch"
        else:
            code = ""
        if not code:
            continue
        key = (code, item["brand"], item["jato_model"])
        coverage[key] = {
            "status": "current_price_found",
            "rowCount": int(item["row_count"] or 0),
            "localMin": _safe_float(item["local_min"]),
            "localMax": _safe_float(item["local_max"]),
            "eurMin": _safe_float(item["eur_min"]),
            "eurMax": _safe_float(item["eur_max"]),
            "sourceCurrency": item["source_currency"],
            "matchStatuses": list(item["match_statuses"] or []),
            "sourceUrls": list(item["source_urls"] or [])[:5],
        }
    return coverage


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    scope_report = read_json(args.scope_report)
    top_models = load_top_models(args.scope_report)
    frames = load_snapshot_frames(PROJECT_ROOT)
    db_coverage = current_price_coverage(args.database_url)
    countries: list[dict[str, Any]] = []
    for country_code, country_meta in COUNTRIES.items():
        country_models = [model for model in top_models if model.country_code == country_code]
        model_payloads: list[dict[str, Any]] = []
        for top_model in country_models:
            snapshots = {
                snapshot_name: snapshot_price_payload(frame, top_model)
                for (snapshot_name, frame_country), frame in frames.items()
                if frame_country == country_code
            }
            snapshots = {name: snapshots[name] for name in SNAPSHOT_ROOTS if name in snapshots}
            variant_changes = variant_price_changes(
                frames,
                top_model,
                threshold_pct=args.threshold_pct,
            )
            current_key = (country_code, top_model.brand, top_model.model)
            current = db_coverage.get(current_key, {"status": "missing_current_price"})
            model_payloads.append(
                {
                    "rank": top_model.rank,
                    "brand": top_model.brand,
                    "model": top_model.model,
                    "sales12m": top_model.sales_12m,
                    "salesWindow": {
                        "latestMonth": top_model.latest_month,
                        "windowStartMonth": top_model.window_start_month,
                        "windowEndMonth": top_model.window_end_month,
                        "monthsInWindow": top_model.months_in_window,
                    },
                    "countryCode": country_code,
                    "countryLabel": top_model.country_label,
                    "sourceDraft": source_draft_payload(args.source_draft_root, top_model),
                    "currentPriceCoverage": current,
                    "snapshotPrices": snapshots,
                    "snapshotMovement": snapshot_movement_summary(
                        snapshots,
                        threshold_pct=args.threshold_pct,
                    ),
                    "variantMovementCandidates": variant_changes,
                    "officialHistoricalEvidenceStatus": "not_collected",
                    "nextAction": (
                        "collect_official_2026_price_list_or_archived_configurator_snapshot"
                        if current["status"] != "missing_current_price"
                        or (
                            source_draft_payload(args.source_draft_root, top_model).get("status")
                            == "source_draft_found"
                        )
                        else "promote_current_official_source_before_backfill"
                    ),
                }
            )
        countries.append(
            {
                "countryCode": country_code,
                "countryLabel": country_meta["label"],
                "scopeCountry": country_meta["scope_country"],
                "rankingWindow": _country_ranking_window(country_models),
                "candidateCount": len(model_payloads),
                "currentPriceCoveredCount": sum(
                    1 for item in model_payloads if item["currentPriceCoverage"]["status"] == "current_price_found"
                ),
                "sourceDraftCoveredCount": sum(
                    1 for item in model_payloads if item["sourceDraft"]["status"] == "source_draft_found"
                ),
                "snapshotMovementCandidateCount": sum(
                    1
                    for item in model_payloads
                    if item["snapshotMovement"]["status"] == "candidate_from_jato_snapshot_median_shift"
                    or item["variantMovementCandidates"]
                ),
                "models": model_payloads,
            }
        )
    return {
        "schemaVersion": "sweden_swiss_top30_suv_price_candidates_v1",
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "countries": ["Sweden", "Switzerland"],
            "topN": 30,
            "segmentFilter": "SUV",
            "candidateSource": str(args.scope_report.relative_to(PROJECT_ROOT)),
            "window": {
                "sourceLatestMonth": _scope_latest_month(scope_report),
                "sourceWindow": "rolling 12m from candidate_scope_report",
                "rankingMethod": "rolling_12m_sales_rank",
            },
            "candidateThresholdPct": args.threshold_pct,
        },
        "evidencePolicy": {
            "status": "review_only",
            "officialConclusionAllowed": False,
            "reason": (
                "JATO processed snapshot price shifts identify candidates only; official "
                "manufacturer price lists, archived configurator snapshots, PDFs, or cached "
                "pages are still required before writing price_history backfill."
            ),
        },
        "countries": countries,
    }


def _scope_latest_month(scope_report: dict[str, Any]) -> str | None:
    months = [
        item.get("latest_month")
        for item in scope_report.get("country_summaries") or []
        if isinstance(item, dict)
    ]
    return str(months[0]) if months else None


def _country_ranking_window(country_models: list[TopModel]) -> dict[str, Any]:
    first = country_models[0] if country_models else None
    return {
        "rankingMethod": "rolling_12m_sales_rank",
        "latestMonth": first.latest_month if first else None,
        "windowStartMonth": first.window_start_month if first else None,
        "windowEndMonth": first.window_end_month if first else None,
        "monthsInWindow": first.months_in_window if first else None,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    window = payload["scope"]["window"]
    lines = [
        "# Sweden + Switzerland Rolling 12M Top30 SUV MSRP Movement Candidates",
        "",
        f"Generated: {payload['generatedAtUtc']}",
        (
            "Ranking scope: SUV top30 by rolling 12-month sales"
            f" (latest source month: {window.get('sourceLatestMonth')})."
        ),
        "",
        "This artifact is review-only. Snapshot price shifts are leads, not official MSRP conclusions.",
        "",
        "## Coverage",
        "",
        "| Country | Top30 candidates | Current MSRP covered | Source drafts | Snapshot movement leads |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for country in payload["countries"]:
        lines.append(
            "| {countryLabel} | {candidateCount} | {currentPriceCoveredCount} | "
            "{sourceDraftCoveredCount} | {snapshotMovementCandidateCount} |".format(**country)
        )
    for country in payload["countries"]:
        ranking_window = country.get("rankingWindow") or {}
        lines.extend(["", f"## {country['countryLabel']} Top30", ""])
        lines.append(
            "Rolling 12M sales window: {start} -> {end} ({months} months).".format(
                start=ranking_window.get("windowStartMonth") or "-",
                end=ranking_window.get("windowEndMonth") or "-",
                months=ranking_window.get("monthsInWindow") or "-",
            )
        )
        lines.append("")
        lines.append(
            "| Rank | Brand | Model | Sales 12m | Current MSRP | Source draft | Snapshot lead | Next action |"
        )
        lines.append("| ---: | --- | --- | ---: | --- | --- | --- | --- |")
        for model in country["models"]:
            current = model["currentPriceCoverage"]
            if current["status"] == "current_price_found":
                current_label = (
                    f"{current['rowCount']} rows; "
                    f"{current.get('localMin')}-{current.get('localMax')} {current.get('sourceCurrency')}"
                )
            else:
                current_label = "missing"
            source_draft = model["sourceDraft"]
            source_label = "found" if source_draft["status"] == "source_draft_found" else "missing"
            movement = model["snapshotMovement"]
            variant_count = len(model["variantMovementCandidates"])
            if movement["status"] == "candidate_from_jato_snapshot_median_shift":
                movement_label = f"median {movement['deltaPctMedian']}%"
            elif variant_count:
                movement_label = f"{variant_count} variant leads"
            else:
                movement_label = "-"
            lines.append(
                "| {rank} | {brand} | {model_name} | {sales12m:.0f} | {current} | "
                "{source} | {movement} | {next_action} |".format(
                    rank=model["rank"],
                    brand=model["brand"],
                    model_name=model["model"],
                    sales12m=model["sales12m"],
                    current=current_label,
                    source=source_label,
                    movement=movement_label,
                    next_action=model["nextAction"],
                )
            )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            (
                "- `Current MSRP covered` means the local PostgreSQL `msrp.current_prices` table has "
                "accepted current rows for that country/model."
            ),
            "- `Source drafts` are scraper source definitions, not successful current MSRP observations.",
            (
                "- `Snapshot movement leads` compare processed JATO parquet snapshots only. They require "
                "official PDF/configurator/cache evidence before backfilled `price_history` writes."
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope-report", type=Path, default=DEFAULT_SCOPE_REPORT)
    parser.add_argument("--source-draft-root", type=Path, default=DEFAULT_SOURCE_DRAFT_ROOT)
    parser.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--threshold-pct", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload(args)
    json_path = args.output_dir / "top30_suv_price_movement_candidates.json"
    markdown_path = args.output_dir / "top30_suv_price_movement_candidates.md"
    write_json(json_path, payload)
    markdown_path.write_text(render_markdown(payload), encoding="utf-8")
    print(f"Wrote {json_path.relative_to(PROJECT_ROOT)}")
    print(f"Wrote {markdown_path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
