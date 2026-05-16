"""Extractor audit logging — JSONL structured logs per the MSRP observability spec.

Writes one JSON object per source per extraction run so the dashboard can show:
- Which strategy won (attr_json / json_script_selector / css / pdf_fallback)
- Coverage level (L3 full trim / L2 entry-range / L1 reachable / L0 failed)
- Attempt timeline and error context
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any

DEFAULT_AUDIT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "artifacts",
    "extractor_audit",
)

_audit_lock = threading.Lock()


def _ensure_audit_dir(audit_dir: str | None = None) -> str:
    target = audit_dir or os.environ.get("JATO_AUDIT_DIR", DEFAULT_AUDIT_DIR)
    os.makedirs(target, exist_ok=True)
    return target


def write_audit_event(event: dict[str, Any], audit_dir: str | None = None) -> None:
    """Append one audit event to the daily JSONL file (thread-safe)."""
    target = _ensure_audit_dir(audit_dir)
    run_id = event.get("run_id", "unknown")
    # One file per run so it's easy to find and clean up
    fname = f"{run_id}.jsonl"
    fpath = os.path.join(target, fname)
    line = json.dumps(event, ensure_ascii=False, default=str)
    with _audit_lock:
        with open(fpath, "a") as fh:
            fh.write(line + "\n")


def classify_coverage(observations: list[Any]) -> str:
    """Classify coverage level from extraction results.

    L3: Full trim-level prices (multiple distinct trims observed)
    L2: Entry price or price range (single trim or AggregateOffer)
    L1: Page reachable (page loaded but no prices extracted)
    L0: Failed (page didn't load)
    """
    if not observations:
        return "L1_PAGE_REACHABLE"

    unique_trims = set()
    for obs in observations:
        trim = getattr(obs, "official_trim", "") or ""
        if trim:
            unique_trims.add(trim.lower())

    if len(unique_trims) >= 2:
        return "L3_FULL_TRIM_PRICE"

    # Check if this looks like an AggregateOffer (range)
    for obs in observations:
        payload = getattr(obs, "raw_payload", {}) or {}
        if "lowPrice" in payload or "highPrice" in payload:
            return "L2_ENTRY_OR_RANGE_PRICE"

    return "L2_ENTRY_OR_RANGE_PRICE"


def build_audit_event(
    *,
    run_id: str,
    source_code: str,
    brand: str,
    country: str,
    url: str,
    attempted_strategies: list[dict[str, Any]],
    winning_strategy: str | None,
    observations: list[Any],
    tier: str = "http",
    error: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a single audit event matching the spec schema."""
    coverage = "L0_FAILED"
    if observations:
        coverage = classify_coverage(observations)

    event: dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "source_id": source_code,
        "brand": brand,
        "country": country,
        "url": url,
        "tier": tier,
        "attempted_strategies": attempted_strategies,
        "winning_strategy": winning_strategy,
        "coverage_level": coverage,
        "observations_count": len(observations),
        "status": "success" if observations else ("error" if error else "failed"),
    }
    if error:
        event["error"] = error
    if observations:
        event["currency"] = getattr(observations[0], "currency", None)
        event["price_kind"] = "MSRP"
    if extra:
        event.update(extra)
    return event
