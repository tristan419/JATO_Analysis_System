#!/usr/bin/env python3
"""Batch ingest — run non-dry-run scrape and optionally auto-resolve review.

Usage:
    python batch_ingest.py batch_a
    python batch_ingest.py se
    python batch_ingest.py fi,dk
    python batch_ingest.py batch_a --auto-review --materialize
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path

import requests
import yaml

_toolkit_dir = str(
    Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"
)
if _toolkit_dir not in sys.path:
    sys.path.insert(0, _toolkit_dir)
_hermes_script_dir = str(Path(__file__).resolve().parent / "hermes")
if _hermes_script_dir not in sys.path:
    sys.path.insert(0, _hermes_script_dir)

from pipeline_status_writer import write_pipeline_status

from jato_scraper.runner import run_scrape

_TOOLKIT_ROOT = Path(__file__).resolve().parent.parent / "07_ScrapingToolkit"
_SOURCES_DIR = _TOOLKIT_ROOT / "sources"
_DRAFTS_DIR = _TOOLKIT_ROOT / "source_drafts" / "suv_only_country_model_top30"
_BATCHES_DIR = _TOOLKIT_ROOT / "msrp_batches"

LEGACY_BATCH_COUNTRIES = {
    "1": ["se", "hr"],
    "2": ["hu", "no", "at", "cz", "ch"],
    "all": ["se", "fi", "no", "dk", "hu", "hr", "at", "cz"],
}

API_BASE = os.getenv("JATO_API_BASE", "http://localhost:8000/v1").rstrip("/")
STRICT_EXIT = os.getenv("JATO_STRICT_EXIT", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
log = logging.getLogger(__name__)


def _is_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _float_env(name: str) -> float | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        return float(raw)
    except ValueError:
        print(f"[WARN] Invalid {name}={raw!r}; ignoring")
        return None


def _auth_headers(
    auth_token: str | None = None,
    user_name: str | None = None,
) -> dict[str, str]:
    token = (
        auth_token
        or os.getenv("JATO_AUTH_TOKEN")
        or os.getenv("APP_AUTH_TOKEN")
        or os.getenv("VITE_AUTH_TOKEN")
        or "change-me"
    ).strip()
    user = (
        user_name
        or os.getenv("JATO_USER_NAME")
        or os.getenv("APP_USER_NAME")
        or "copilot"
    ).strip() or "copilot"
    return {
        "X-Auth-Token": token,
        "X-User-Name": user,
    }


def _load_yaml_mapping(path: Path) -> dict[str, object]:
    with open(path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path} did not parse into a YAML mapping")
    return data


def _iter_yaml_files(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(
        path
        for path in root.rglob("*.y*ml")
        if path.is_file() and not path.name.startswith("_")
    )


def _load_named_batch_countries(name: str) -> list[str] | None:
    normalized = name.strip().lower()
    if normalized in LEGACY_BATCH_COUNTRIES:
        return LEGACY_BATCH_COUNTRIES[normalized]
    if normalized in {"batch_a", "a"}:
        path = _BATCHES_DIR / "batch_a.yaml"
        data = _load_yaml_mapping(path)
        countries = [
            str(item.get("country_code") or "").strip().lower()
            for item in list(data.get("countries") or [])
            if str(item.get("country_code") or "").strip()
        ]
        return countries
    if normalized == "all":
        countries: list[str] = []
        for batch_path in sorted(_BATCHES_DIR.glob("*.yaml")):
            data = _load_yaml_mapping(batch_path)
            countries.extend(
                str(item.get("country_code") or "").strip().lower()
                for item in list(data.get("countries") or [])
                if str(item.get("country_code") or "").strip()
            )
        deduped = []
        for country in countries:
            if country not in deduped:
                deduped.append(country)
        return deduped
    return None


def _resolve_countries(target: str) -> list[str]:
    named = _load_named_batch_countries(target)
    if named is not None:
        return named
    return [
        country
        for country in (
            part.strip().lower() for part in target.split(",")
        )
        if country
    ]


def _promoted_code_for_draft(code: str) -> str:
    if not code.endswith("_draft_scrapling"):
        return code
    return code.replace("_draft_scrapling", "_scrapling")


def _file_matches_country(path: Path, country_code: str) -> bool:
    pattern = re.compile(rf"(?:^|_){re.escape(country_code.lower())}(?:_|$)")
    return bool(pattern.search(path.stem.lower()))


def _resolve_target_sources(
    countries: list[str],
) -> tuple[list[tuple[str, str, str]], list[tuple[str, str]]]:
    production_refs: list[tuple[str, str, str]] = []
    production_codes: set[str] = set()
    for path in _iter_yaml_files(_SOURCES_DIR):
        matched_country = next(
            (cc for cc in countries if _file_matches_country(path, cc)),
            None,
        )
        if matched_country is None:
            continue
        source_code = str(_load_yaml_mapping(path).get("source_code") or "").strip()
        if not source_code:
            continue
        production_refs.append((matched_country, source_code, str(path)))
        production_codes.add(source_code)

    draft_refs: list[tuple[str, str, str]] = []
    skipped_promoted: list[tuple[str, str]] = []
    for country in countries:
        draft_dir = _DRAFTS_DIR / country
        for path in sorted(draft_dir.glob("*.y*ml")):
            source_code = str(_load_yaml_mapping(path).get("source_code") or "").strip()
            if not source_code:
                continue
            promoted_code = _promoted_code_for_draft(source_code)
            if promoted_code in production_codes:
                skipped_promoted.append((source_code, promoted_code))
                continue
            draft_refs.append((country, source_code, str(path)))

    return sorted(production_refs + draft_refs), skipped_promoted


def _post_backend_json(
    path: str,
    payload: dict[str, object],
    *,
    auth_token: str | None,
    user_name: str | None,
) -> dict[str, object]:
    last_error = None
    for attempt in range(3):
        try:
            response = requests.post(
                f"{API_BASE}{path}",
                json=payload,
                headers=_auth_headers(auth_token, user_name),
                timeout=120,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            last_error = e
            if attempt < 2 and e.response is not None and e.response.status_code in (502, 503, 504):
                wait = (attempt + 1) * 5
                print(
                    f"  [retry] {path} returned {e.response.status_code}, "
                    f"retrying in {wait}s (attempt {attempt+1}/3)"
                )
                time.sleep(wait)
                continue
            raise
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = e
            if attempt < 2:
                wait = (attempt + 1) * 10
                print(f"  [retry] {path} connection failed: {e!s:.60s}, retrying in {wait}s (attempt {attempt+1}/3)")
                time.sleep(wait)
                continue
            raise
    raise last_error  # type: ignore


def _auto_resolve_reviews(
    countries: list[str],
    *,
    decided_by: str,
    limit: int,
    min_score: float | None,
    note: str | None,
    auth_token: str | None,
    user_name: str | None,
) -> dict[str, int]:
    totals = {
        "candidateCases": 0,
        "autoApprovedCount": 0,
        "directAutoReviewApprovedCount": 0,
        "linkAppliedCount": 0,
        "overrideAppliedCount": 0,
        "unresolvedCount": 0,
        "missingObservationCount": 0,
        "scoreRejectedCount": 0,
    }
    for country in countries:
        payload = {
            "country": country.upper(),
            "decided_by": decided_by,
            "limit": limit,
            "note": note,
        }
        if min_score is not None:
            payload["min_score"] = min_score
        result = _post_backend_json(
            "/review/cases/auto-resolve",
            payload,
            auth_token=auth_token,
            user_name=user_name,
        ).get("item", {})
        print(
            "  [auto-review]"
            f" country={country}"
            f" approved={result.get('autoApprovedCount', 0)}"
            f" direct={result.get('directAutoReviewApprovedCount', 0)}"
            f" unresolved={result.get('unresolvedCount', 0)}"
            f" score_rejected={result.get('scoreRejectedCount', 0)}"
            f" links={result.get('linkAppliedCount', 0)}"
            f" overrides={result.get('overrideAppliedCount', 0)}"
        )
        for key in totals:
            totals[key] += int(result.get(key, 0) or 0)
    return totals


def _materialize_current_prices(
    countries: list[str],
    *,
    limit: int,
    auth_token: str | None,
    user_name: str | None,
) -> dict[str, int]:
    totals = {
        "candidateObservations": 0,
        "materializedKeys": 0,
    }
    for country in countries:
        payload = {
            "country": country.upper(),
            "limit": limit,
        }
        result = _post_backend_json(
            "/msrp/current-prices/materialize",
            payload,
            auth_token=auth_token,
            user_name=user_name,
        ).get("item", {})
        print(
            "  [materialize]"
            f" country={country}"
            f" candidates={result.get('candidateObservations', 0)}"
            f" materialized={result.get('materializedKeys', 0)}"
        )
        totals["candidateObservations"] += int(
            result.get("candidateObservations", 0) or 0
        )
        totals["materializedKeys"] += int(
            result.get("materializedKeys", 0) or 0
        )
    return totals


def _write_ingest_status(
    countries: list[str],
    ok_count: int,
    empty_count: int,
    fail_count: int,
    total: int = 0,
    auto_review_totals: dict[str, int] | None = None,
    materialize_totals: dict[str, int] | None = None,
) -> None:
    """Write msrp_ingest status to scheduled_fetch_status.json."""
    import json as _json
    from datetime import datetime as _datetime, timezone as _timezone
    status_path = Path(__file__).resolve().parent / "logs" / "scheduled_fetch_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if status_path.exists():
        try:
            existing = _json.loads(status_path.read_text())
        except (_json.JSONDecodeError, OSError):
            existing = {}
    success_count = ok_count
    failure_count_total = fail_count
    total_count = max(total, success_count + failure_count_total)
    ok_pct = round(success_count / total_count * 100, 1) if total_count > 0 else 0.0
    if ok_pct >= 90:
        status = "success"
    elif ok_pct >= 50:
        status = "degraded"
    else:
        status = "failure"
    existing["msrp_ingest"] = {
        "lastRunAt": _datetime.now(_timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": status,
        "countryCount": len(countries),
        "totalSources": total_count,
        "successCount": success_count,
        "emptyCount": empty_count,
        "failureCount": failure_count_total,
        "okPct": ok_pct,
    }
    status_path.write_text(_json.dumps(existing, indent=2) + "\n")
    write_pipeline_status(
        pipeline_id="msrp_ingest",
        status=status,
        records_processed=total_count,
        failed_count=failure_count_total,
        warning_count=empty_count,
        artifact_refs=["03_Scripts/logs/scheduled_fetch_status.json"],
        source="03_Scripts/batch_ingest.py",
        message=f"okPct={ok_pct}%",
        extra={
            "countryCount": len(countries),
            "successCount": success_count,
            "emptyCount": empty_count,
            "okPct": ok_pct,
            "requiresReview": True,
            "dryRunBeforeIngest": True,
            "autoReview": auto_review_totals or {},
            "materialize": materialize_totals or {},
        },
    )
    print(f"[status] msrp_ingest={status} okPct={ok_pct}% written to {status_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run MSRP ingest for a country list or named batch and optionally "
            "auto-resolve review cases plus current-price materialization."
        )
    )
    parser.add_argument("target", nargs="?", default="batch_a")
    parser.add_argument(
        "--auto-review",
        action="store_true",
        default=_is_truthy(os.getenv("JATO_AUTO_REVIEW")),
    )
    parser.add_argument(
        "--materialize",
        action="store_true",
        default=_is_truthy(os.getenv("JATO_AUTO_MATERIALIZE")),
    )
    parser.add_argument(
        "--decided-by",
        default=(
            os.getenv("JATO_AUTO_REVIEW_DECIDED_BY")
            or os.getenv("APP_USER_NAME")
            or "msrp-auto-review"
        ),
    )
    parser.add_argument(
        "--note",
        default=os.getenv("JATO_AUTO_REVIEW_NOTE"),
    )
    parser.add_argument(
        "--auto-review-limit",
        type=int,
        default=int(os.getenv("JATO_AUTO_REVIEW_LIMIT", "500")),
    )
    parser.add_argument(
        "--auto-review-min-score",
        type=float,
        default=_float_env("JATO_MSRP_AUTO_REVIEW_MIN_SCORE"),
    )
    parser.add_argument(
        "--materialize-limit",
        type=int,
        default=int(os.getenv("JATO_MATERIALIZE_LIMIT", "500")),
    )
    parser.add_argument(
        "--auth-token",
        default=os.getenv("JATO_AUTH_TOKEN") or os.getenv("APP_AUTH_TOKEN"),
    )
    parser.add_argument(
        "--user-name",
        default=os.getenv("JATO_USER_NAME") or os.getenv("APP_USER_NAME"),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )
    countries = _resolve_countries(args.target)
    target_sources, skipped_promoted = _resolve_target_sources(countries)

    print(
        f"Batch ingest {args.target}: {len(target_sources)} sources across {countries}"
    )
    print(f"API: {API_BASE}")
    if skipped_promoted:
        print(
            "Skipped "
            f"{len(skipped_promoted)} promoted draft(s) because matching "
            "production sources already exist."
        )
    print(f"{'='*70}\n")

    ok_count = 0
    empty_count = 0
    fail_count = 0

    for i, (country, source_code, source_ref) in enumerate(target_sources, 1):
        t0 = time.time()
        try:
            summary = run_scrape(
                source_codes=[source_ref],
                api_base=API_BASE,
                trigger_type="manual",
                dry_run=False,
                auth_token=args.auth_token,
                user_name=args.user_name,
            )
            src = summary["sources"].get(source_code, {})
            status = src.get("status", "error")
            valid = src.get("valid", 0)
            elapsed = time.time() - t0

            if status == "ok":
                icon = "✅"
                ok_count += 1
            elif status == "empty":
                icon = "⬚"
                empty_count += 1
            else:
                icon = "❌"
                fail_count += 1
                if "error" in src:
                    print(f"    error: {src['error'][:160]}")

            print(
                f"  [{i:3d}/{len(target_sources)}] {icon} {source_code:50s} "
                f"status={status} valid={valid} ({elapsed:.1f}s)"
            )
        except Exception as exc:
            elapsed = time.time() - t0
            print(
                f"  [{i:3d}/{len(target_sources)}] ❌ {source_code:50s} "
                f"EXCEPTION: {exc!s:.120s} ({elapsed:.1f}s)"
            )
            fail_count += 1

    total = len(target_sources)
    print(f"\n{'='*70}")
    print(
        f"Ingest: {ok_count}/{total} OK, {empty_count} empty, "
        f"{fail_count} failed"
    )

    auto_review_totals: dict[str, int] | None = None
    materialize_totals: dict[str, int] | None = None

    if args.auto_review:
        auto_review_totals = _auto_resolve_reviews(
            countries,
            decided_by=args.decided_by,
            limit=args.auto_review_limit,
            min_score=args.auto_review_min_score,
            note=args.note,
            auth_token=args.auth_token,
            user_name=args.user_name,
        )
        print(
            "Auto-review:"
            f" approved={auto_review_totals['autoApprovedCount']}"
            f" direct={auto_review_totals['directAutoReviewApprovedCount']}"
            f" unresolved={auto_review_totals['unresolvedCount']}"
            f" score_rejected={auto_review_totals['scoreRejectedCount']}"
            f" links={auto_review_totals['linkAppliedCount']}"
            f" overrides={auto_review_totals['overrideAppliedCount']}"
        )

    if args.materialize:
        materialize_totals = _materialize_current_prices(
            countries,
            limit=args.materialize_limit,
            auth_token=args.auth_token,
            user_name=args.user_name,
        )
        print(
            "Materialize:"
            f" candidates={materialize_totals['candidateObservations']}"
            f" materialized={materialize_totals['materializedKeys']}"
        )

    print(f"{'='*70}")
    _write_ingest_status(
        countries,
        ok_count,
        empty_count,
        fail_count,
        total=total,
        auto_review_totals=auto_review_totals,
        materialize_totals=materialize_totals,
    )
    if STRICT_EXIT and fail_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
