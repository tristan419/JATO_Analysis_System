from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from app.db.models import CurrentPrice, MsrpObservation, MsrpSource, PriceHistory
from app.infra import msrp_repository as msrp_repo
from app.services.country_service import to_display_country


POWERTRAIN_COLORS = {
    "BEV": "#16a34a",
    "HEV": "#f2b705",
    "PHEV": "#2563eb",
    "ICE": "#64748b",
    "MHEV": "#f97316",
}
POWERTRAIN_FALLBACK_COLOR = "#94a3b8"
OFFICIAL_SOURCE_TYPES = {
    "manufacturer_official",
    "official_api",
    "official_configurator",
    "official_price_list",
    "official_price_list_pdf",
    "official_website",
    "manufacturer_site",
}
SOURCE_RISK_MATCH_STATUSES = {"review_required", "rejected", "failed"}
DEFAULT_MONITORING_LIMIT = 500
MONITORING_MODE_LIVE = "live"
MONITORING_MODE_SWEDEN_DEMO = "sweden_demo"
AUDIT_PRIORITY_ORDER = {
    "auto_pass": 0,
    "sample": 1,
    "priority_audit": 2,
    "block": 3,
}
AUDIT_ACTION_LABELS = {
    "auto_pass": "Auto pass after next scrape confirms unchanged MSRP.",
    "sample": "Sample one source snapshot in the routine spot-check batch.",
    "priority_audit": "Prioritize manual audit before accepting the movement.",
    "block": "Block automatic acceptance until lifecycle/source evidence is verified.",
}
SWEDEN_DEMO_SCENARIOS: dict[tuple[str, str, str], dict[str, object]] = {
    ("VOLVO", "EX90", "BEV"): {
        "changePct": -6.8,
        "daysAgo": 6,
        "lengthMm": 5037,
        "scenario": "2026 spring MSRP repositioning",
    },
    ("VOLVO", "XC90", "PHEV"): {
        "changePct": 2.9,
        "daysAgo": 12,
        "lengthMm": 4953,
        "scenario": "2026 plug-in hybrid package price update",
    },
    ("SKODA", "ENYAQ", "BEV"): {
        "changePct": -4.6,
        "daysAgo": 18,
        "lengthMm": 4653,
        "scenario": "2026 BEV campaign price cut",
    },
    ("VOLKSWAGEN", "TAYRON", "UNKNOWN"): {
        "changePct": -3.2,
        "daysAgo": 24,
        "lengthMm": 4770,
        "scenario": "Configurator availability signal",
        "sourceStatus": "review_required",
        "riskReasons": ["demo_unavailable_signal", "demo_backfilled_price"],
        "lifecycleStatus": "removed_from_configurator",
    },
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_powertrain(value: object | None) -> str:
    text = str(value or "").strip().upper()
    if text in POWERTRAIN_COLORS:
        return text
    if "PHEV" in text or "PLUGIN" in text or "PLUG-IN" in text:
        return "PHEV"
    if "MHEV" in text or "MILD" in text:
        return "MHEV"
    if "BEV" in text or "ELECTRIC" in text or text == "EV":
        return "BEV"
    if "HEV" in text or "HYBRID" in text:
        return "HEV"
    if text in {"PETROL", "GASOLINE", "DIESEL", "ICE"}:
        return "ICE"
    return text or "UNKNOWN"


def _float_or_none(value: object | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def _iso(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _event_key(item: CurrentPrice) -> tuple[str, str, str]:
    return (
        str(item.brand or "").strip(),
        str(item.jato_model or "").strip(),
        _normalize_powertrain(item.jato_powertrain),
    )


def _change_pct(current_value: float | None, previous_value: float | None) -> float | None:
    if current_value is None or previous_value in {None, 0}:
        return None
    return round((current_value - previous_value) / previous_value * 100.0, 2)


def _changed_in_window(period: PriceHistory, since: datetime | None) -> bool:
    if since is None:
        return True
    changed_at = period.valid_from_utc
    if changed_at.tzinfo is None:
        changed_at = changed_at.replace(tzinfo=timezone.utc)
    return changed_at >= since


def _extract_numeric_length(value: object | None) -> int | None:
    parsed = _float_or_none(value)
    if parsed is None:
        return None
    if 2500 <= parsed <= 7000:
        return int(round(parsed))
    return None


def _extract_length_from_context(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, dict):
        direct_keys = (
            "lengthMm",
            "length_mm",
            "vehicleLengthMm",
            "vehicle_length_mm",
            "length",
            "Length",
            "length (mm)",
        )
        for key in direct_keys:
            if key in value:
                length = _extract_numeric_length(value.get(key))
                if length is not None:
                    return length
        for key, nested in value.items():
            if "length" in str(key).lower():
                length = _extract_numeric_length(nested)
                if length is not None:
                    return length
            length = _extract_length_from_context(nested)
            if length is not None:
                return length
    if isinstance(value, list):
        for item in value:
            length = _extract_length_from_context(item)
            if length is not None:
                return length
    return None


def _extract_dryrun_run_id(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        for key in ("dryrunRunId", "dryrun_run_id", "runId", "run_id"):
            text = str(value.get(key) or "").strip()
            if text.startswith("msrp-dryrun-"):
                return text
        for nested in value.values():
            run_id = _extract_dryrun_run_id(nested)
            if run_id:
                return run_id
    if isinstance(value, list):
        for item in value:
            run_id = _extract_dryrun_run_id(item)
            if run_id:
                return run_id
    text = str(value).strip()
    return text if text.startswith("msrp-dryrun-") else None


def _median_or_none(values: list[float]) -> float | None:
    return round(float(median(values)), 2) if values else None


def _range_payload(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "max": None}
    return {"min": round(min(values), 2), "max": round(max(values), 2)}


def _summary_payload(events: list[dict[str, object]]) -> dict[str, object]:
    audit_priority_counts = {
        priority: sum(1 for item in events if item.get("auditPriority") == priority)
        for priority in AUDIT_PRIORITY_ORDER
    }
    return {
        "eventCount": len(events),
        "timelineEventCount": sum(int(item.get("timelineEventCount") or 0) for item in events),
        "affectedCountryCount": len(
            {
                country_event.get("country")
                for event in events
                for country_event in list(event.get("countries") or [])
            }
        ),
        "sourceRiskCount": sum(int(item.get("sourceRiskCount") or 0) for item in events),
        "reviewRequiredCount": sum(int(item.get("reviewRequiredCount") or 0) for item in events),
        "outlierCount": sum(int(item.get("outlierCount") or 0) for item in events),
        "lengthMissingCount": sum(1 for item in events if item.get("lengthMissing")),
        "auditPriorityCounts": audit_priority_counts,
        "autoPassCount": audit_priority_counts["auto_pass"],
        "sampleCount": audit_priority_counts["sample"],
        "priorityAuditCount": audit_priority_counts["priority_audit"],
        "blockCount": audit_priority_counts["block"],
    }


def _source_status(
    current_price: CurrentPrice,
    source: MsrpSource | None,
    source_currency_changed: bool,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    match_status = str(current_price.match_status or "").strip().lower()
    match_confidence = _float_or_none(current_price.match_confidence) or 0.0
    source_type = str(source.source_type if source is not None else "").strip().lower()
    if match_status in SOURCE_RISK_MATCH_STATUSES:
        reasons.append(f"match_status:{match_status}")
    if match_confidence < 0.8:
        reasons.append("low_match_confidence")
    if source is None:
        reasons.append("missing_source_registry")
    elif source_type not in OFFICIAL_SOURCE_TYPES:
        reasons.append(f"non_official_source:{source_type or 'unknown'}")
    if source_currency_changed:
        reasons.append("source_currency_changed")
    if reasons:
        return "review_required" if match_status == "review_required" else "source_risk", reasons
    return "confirmed", []


def _timeline_payload(
    *,
    item: CurrentPrice,
    current_period: PriceHistory,
    previous_period: PriceHistory,
    source: MsrpSource | None,
    observation: MsrpObservation | None,
    threshold_pct: float,
) -> dict[str, object] | None:
    current_eur = _float_or_none(current_period.msrp_value)
    previous_eur = _float_or_none(previous_period.msrp_value)
    delta_eur = (
        round(current_eur - previous_eur, 2)
        if current_eur is not None and previous_eur is not None
        else None
    )
    change_pct = _change_pct(current_eur, previous_eur)
    if change_pct is None or abs(change_pct) < threshold_pct:
        return None

    source_currency_changed = current_period.source_currency != previous_period.source_currency
    current_source = _float_or_none(current_period.source_msrp_value)
    previous_source = _float_or_none(previous_period.source_msrp_value)
    delta_source = (
        round(current_source - previous_source, 2)
        if (
            current_source is not None
            and previous_source is not None
            and not source_currency_changed
        )
        else None
    )
    status, risk_reasons = _source_status(item, source, source_currency_changed)
    review_flag = status != "confirmed"
    return {
        "country": item.country,
        "countryLabel": to_display_country(item.country),
        "brand": item.brand,
        "jatoModel": item.jato_model,
        "jatoTrim": item.jato_trim,
        "jatoPowertrain": _normalize_powertrain(item.jato_powertrain),
        "changedAtUtc": _iso(current_period.valid_from_utc),
        "oldMsrpEur": previous_eur,
        "currentMsrpEur": current_eur,
        "changeAmountEur": delta_eur,
        "changePct": change_pct,
        "oldSourceMsrp": previous_source,
        "currentSourceMsrp": current_source,
        "changeAmountSource": delta_source,
        "sourceCurrency": current_period.source_currency,
        "previousSourceCurrency": previous_period.source_currency,
        "sourceCurrencyChanged": source_currency_changed,
        "sourceStatus": status,
        "reviewFlag": review_flag,
        "riskReasons": risk_reasons,
        "currentPriceId": str(item.current_price_id),
        "priceHistoryId": str(current_period.price_history_id),
        "currentObservationId": str(current_period.started_by_observation_id),
        "previousObservationId": str(previous_period.started_by_observation_id),
        "lastConfirmedObservationId": str(
            current_period.last_confirmed_by_observation_id
        ),
        "effectiveObservationId": str(item.effective_observation_id),
        "source": {
            "sourceCode": source.source_code if source is not None else None,
            "sourceType": source.source_type if source is not None else None,
            "extractorName": source.extractor_name if source is not None else None,
            "extractorVersion": source.extractor_version if source is not None else None,
            "sourceRegistryUrl": source.source_url if source is not None else None,
        },
        "evidence": {
            "sourceUrl": item.source_url,
            "sourceSnapshotPath": item.source_snapshot_path,
            "matchConfidence": _float_or_none(item.match_confidence),
            "matchStatus": item.match_status,
            "observationSourceUrl": observation.source_url if observation is not None else None,
            "sourcePayloadHash": observation.source_payload_hash if observation is not None else None,
            "observedAtUtc": _iso(observation.observed_at_utc if observation is not None else None),
        },
    }


def _hydrate_timeline_evidence(
    session: Session,
    timeline: list[dict[str, object]],
) -> None:
    observation_ids = {
        item.get("currentObservationId")
        for item in timeline
        if item.get("currentObservationId")
    }
    observation_ids.update(
        item.get("effectiveObservationId")
        for item in timeline
        if item.get("effectiveObservationId")
    )
    observations = msrp_repo.list_observations_by_ids(
        session,
        [
            UUID(str(item))
            for item in observation_ids
            if item is not None
        ],
    )
    observation_by_id = {str(item.observation_id): item for item in observations}
    batch_by_id: dict[str, object] = {}

    for event in timeline:
        observation = (
            observation_by_id.get(str(event.get("currentObservationId") or ""))
            or observation_by_id.get(str(event.get("effectiveObservationId") or ""))
        )
        if observation is None:
            continue
        batch_id = str(observation.scrape_batch_id)
        if batch_id not in batch_by_id:
            batch_by_id[batch_id] = msrp_repo.get_scrape_batch(
                session,
                observation.scrape_batch_id,
            )
        batch = batch_by_id.get(batch_id)
        evidence = dict(event.get("evidence") or {})
        evidence["scrapeBatchId"] = batch_id
        evidence["scrapeBatchCode"] = getattr(batch, "batch_code", None)
        evidence["dryrunRunId"] = (
            _extract_dryrun_run_id(observation.source_context_json)
            or _extract_dryrun_run_id(observation.match_reason_json)
            or (
                getattr(batch, "batch_code", None)
                if str(getattr(batch, "batch_code", "")).startswith("msrp-dryrun-")
                else None
            )
        )
        event["evidence"] = evidence


def _mark_outliers(events: list[dict[str, object]]) -> None:
    values = [
        float(item["changePct"])
        for item in events
        if item.get("changePct") is not None
    ]
    if len(values) < 3:
        for item in events:
            item["outlier"] = False
            item["suspectedFalsePositive"] = bool(item.get("reviewFlag"))
        return
    center = float(median(values))
    deviations = [abs(value - center) for value in values]
    deviation_center = float(median(deviations))
    threshold = max(5.0, deviation_center * 3.0)
    for item in events:
        change_pct = _float_or_none(item.get("changePct"))
        outlier = change_pct is not None and abs(change_pct - center) >= threshold
        item["outlier"] = outlier
        item["suspectedFalsePositive"] = bool(item.get("reviewFlag")) or outlier


def _audit_decision_payload(
    priority: str,
    reasons: list[str],
    bucket: str,
) -> dict[str, object]:
    safe_priority = priority if priority in AUDIT_PRIORITY_ORDER else "priority_audit"
    return {
        "auditPriority": safe_priority,
        "suggestedAction": safe_priority,
        "auditActionLabel": AUDIT_ACTION_LABELS[safe_priority],
        "auditReasons": reasons,
        "samplingBucket": bucket,
    }


def _raise_audit_priority(current: str, candidate: str) -> str:
    if AUDIT_PRIORITY_ORDER[candidate] > AUDIT_PRIORITY_ORDER[current]:
        return candidate
    return current


def _audit_decision_for_country_event(item: dict[str, object]) -> dict[str, object]:
    reasons: list[str] = []
    priority = "auto_pass"
    bucket = "clean_confirmed"
    lifecycle_status = str(item.get("lifecycleStatus") or "active")
    source_status = str(item.get("sourceStatus") or "confirmed")
    change_pct = _float_or_none(item.get("changePct"))
    absolute_change_pct = abs(change_pct) if change_pct is not None else 0.0
    risk_reasons = [str(reason) for reason in list(item.get("riskReasons") or [])]
    demo_backfilled = bool(dict(item.get("evidence") or {}).get("demoBackfilled"))

    if lifecycle_status not in {"", "active"}:
        priority = "block"
        bucket = "lifecycle_block"
        reasons.append(f"lifecycle_signal:{lifecycle_status}")
    if any(reason in {"match_status:rejected", "match_status:failed"} for reason in risk_reasons):
        priority = "block"
        bucket = "source_rejected"
        reasons.extend(reason for reason in risk_reasons if reason.startswith("match_status:"))
    if source_status != "confirmed":
        priority = _raise_audit_priority(priority, "priority_audit")
        if bucket == "clean_confirmed":
            bucket = "source_risk"
        reasons.append(f"source_status:{source_status}")
    if bool(item.get("outlier")):
        priority = _raise_audit_priority(priority, "priority_audit")
        if bucket == "clean_confirmed":
            bucket = "outlier"
        reasons.append("outlier_vs_model_country_cluster")
    if bool(item.get("sourceCurrencyChanged")):
        priority = _raise_audit_priority(priority, "priority_audit")
        if bucket == "clean_confirmed":
            bucket = "currency_change"
        reasons.append("source_currency_changed")
    if absolute_change_pct >= 5.0:
        priority = _raise_audit_priority(priority, "priority_audit")
        if bucket == "clean_confirmed":
            bucket = "large_price_move"
        reasons.append("large_price_move:>=5pct")
    elif absolute_change_pct >= 1.0:
        priority = _raise_audit_priority(priority, "sample")
        if bucket == "clean_confirmed":
            bucket = "routine_price_move"
        reasons.append("price_move:>=1pct")
    if demo_backfilled:
        priority = _raise_audit_priority(priority, "sample")
        if bucket == "clean_confirmed":
            bucket = "demo_backfill"
        reasons.append("demo_backfilled_scenario")

    if not reasons:
        reasons.append("confirmed_low_variance")
    return _audit_decision_payload(priority, sorted(set(reasons)), bucket)


def _audit_decision_for_model_event(
    country_events: list[dict[str, object]],
    *,
    multi_country_sync: bool,
    demo_event: bool,
) -> dict[str, object]:
    priority = "auto_pass"
    reasons: set[str] = set()
    bucket = "clean_confirmed"
    for item in country_events:
        item_priority = str(item.get("auditPriority") or "auto_pass")
        priority = _raise_audit_priority(priority, item_priority)
        reasons.update(str(reason) for reason in list(item.get("auditReasons") or []))
        if bucket == "clean_confirmed" and item_priority != "auto_pass":
            bucket = str(item.get("samplingBucket") or item_priority)

    change_values = [
        abs(float(item["changePct"]))
        for item in country_events
        if item.get("changePct") is not None
    ]
    max_change_pct = max(change_values) if change_values else 0.0
    trim_count = len({item.get("jatoTrim") for item in country_events})
    country_change_count = len(country_events)

    if priority != "block" and country_change_count == 1 and trim_count == 1 and max_change_pct >= 3.0:
        priority = _raise_audit_priority(priority, "priority_audit")
        reasons.add("single_trim_market_move:>=3pct")
        if bucket == "clean_confirmed":
            bucket = "single_trim_market_move"
    if priority == "auto_pass" and max_change_pct >= 1.0:
        priority = "sample"
        bucket = "routine_price_move"
        reasons.add("price_move:>=1pct")
    if multi_country_sync and priority in {"auto_pass", "sample"}:
        reasons.add("multi_country_sync_support")
    if demo_event:
        reasons.add("demo_backfilled_scenario")
        if priority == "auto_pass":
            priority = "sample"
            bucket = "demo_backfill"

    if not reasons:
        reasons.add("confirmed_low_variance")
    return _audit_decision_payload(priority, sorted(reasons), bucket)


def _country_latest_events(events: list[dict[str, object]]) -> list[dict[str, object]]:
    latest_by_country_trim: dict[tuple[str, str], dict[str, object]] = {}
    for event in sorted(
        events,
        key=lambda item: str(item.get("changedAtUtc") or ""),
        reverse=True,
    ):
        country = str(event.get("country") or "")
        trim = str(event.get("jatoTrim") or "")
        latest_by_country_trim.setdefault((country, trim), event)
    return sorted(
        latest_by_country_trim.values(),
        key=lambda item: (
            -abs(float(item.get("changePct") or 0.0)),
            str(item.get("countryLabel") or ""),
            str(item.get("jatoTrim") or ""),
        ),
    )


def _load_market_scan_length_lookup(
    current_prices: list[CurrentPrice],
) -> tuple[dict[tuple[str, str, str, str], int], dict[tuple[str, str, str], int], str | None]:
    """Best-effort model length lookup from the market scan parquet dataset."""
    if not current_prices:
        return {}, {}, None
    try:
        import pandas as pd
        from app.infra import parquet_repository as parquet_repo
        from app.services import market_scan_service
    except Exception as exc:  # pragma: no cover - optional runtime dependency
        return {}, {}, f"market_scan_length_lookup_unavailable:{type(exc).__name__}"

    try:
        columns = market_scan_service._get_columns()
        if not columns.length:
            return {}, {}, "market_scan_length_column_missing"
        selected_columns = [
            columns.country_value,
            columns.make,
            columns.model,
            columns.powertrain,
            columns.length,
        ]
        table = parquet_repo._open_dataset().to_table(columns=selected_columns)
        frame = table.to_pandas()
        if frame.empty:
            return {}, {}, "market_scan_length_dataset_empty"
        wanted_models = {
            (
                str(item.brand or "").strip().lower(),
                str(item.jato_model or "").strip().lower(),
                _normalize_powertrain(item.jato_powertrain),
            )
            for item in current_prices
        }
        frame["__country"] = frame[columns.country_value].astype(str).str.strip()
        frame["__brand"] = frame[columns.make].astype(str).str.strip()
        frame["__model"] = frame[columns.model].astype(str).str.strip()
        frame["__brand_key"] = frame["__brand"].str.lower()
        frame["__model_key"] = frame["__model"].str.lower()
        frame["__powertrain"] = frame[columns.powertrain].map(_normalize_powertrain)
        frame["__length"] = pd.to_numeric(frame[columns.length], errors="coerce")
        frame = frame[
            frame.apply(
                lambda row: (
                    str(row["__brand_key"]),
                    str(row["__model_key"]),
                    str(row["__powertrain"]),
                )
                in wanted_models,
                axis=1,
            )
            & frame["__length"].between(2500, 7000)
        ].copy()
        if frame.empty:
            return {}, {}, "market_scan_length_no_matches"

        country_lookup: dict[tuple[str, str, str, str], int] = {}
        model_lookup: dict[tuple[str, str, str], int] = {}
        for key, group in frame.groupby(["__country", "__brand_key", "__model_key", "__powertrain"]):
            values = [float(item) for item in group["__length"].dropna().tolist()]
            if values:
                country_lookup[
                    (
                        str(key[0]),
                        str(key[1]),
                        str(key[2]),
                        str(key[3]),
                    )
                ] = int(round(median(values)))
        for key, group in frame.groupby(["__brand_key", "__model_key", "__powertrain"]):
            values = [float(item) for item in group["__length"].dropna().tolist()]
            if values:
                model_lookup[
                    (
                        str(key[0]),
                        str(key[1]),
                        str(key[2]),
                    )
                ] = int(round(median(values)))
        return country_lookup, model_lookup, None
    except Exception as exc:  # pragma: no cover - defensive best effort
        return {}, {}, f"market_scan_length_lookup_failed:{type(exc).__name__}"


def _market_scan_length_for_item(
    item: CurrentPrice,
    country_lookup: dict[tuple[str, str, str, str], int],
    model_lookup: dict[tuple[str, str, str], int],
) -> int | None:
    powertrain = _normalize_powertrain(item.jato_powertrain)
    country_keys = [
        (
            str(item.country or "").strip(),
            str(item.brand or "").strip().lower(),
            str(item.jato_model or "").strip().lower(),
            powertrain,
        ),
        (
            to_display_country(item.country),
            str(item.brand or "").strip().lower(),
            str(item.jato_model or "").strip().lower(),
            powertrain,
        ),
    ]
    for key in country_keys:
        if key in country_lookup:
            return country_lookup[key]
    return model_lookup.get(
        (
            str(item.brand or "").strip().lower(),
            str(item.jato_model or "").strip().lower(),
            powertrain,
        )
    )


def _build_model_event(
    key: tuple[str, str, str],
    timeline: list[dict[str, object]],
    length_by_country: dict[str, tuple[int | None, str | None]],
) -> dict[str, object]:
    _mark_outliers(timeline)
    country_events = _country_latest_events(timeline)
    for item in country_events:
        item.update(_audit_decision_for_country_event(item))
    change_values = [
        float(item["changePct"])
        for item in country_events
        if item.get("changePct") is not None
    ]
    current_values = [
        float(item["currentMsrpEur"])
        for item in country_events
        if item.get("currentMsrpEur") is not None
    ]
    old_values = [
        float(item["oldMsrpEur"])
        for item in country_events
        if item.get("oldMsrpEur") is not None
    ]
    source_risk_count = sum(
        1 for item in country_events if item.get("sourceStatus") != "confirmed"
    )
    review_required_count = sum(1 for item in country_events if item.get("reviewFlag"))
    outlier_count = sum(1 for item in country_events if item.get("outlier"))
    suspected_false_positive_count = sum(
        1 for item in country_events if item.get("suspectedFalsePositive")
    )
    lengths = [value[0] for value in length_by_country.values() if value[0] is not None]
    length_mm = int(round(median(lengths))) if lengths else None
    length_sources = sorted(
        {
            str(source)
            for _, source in length_by_country.values()
            if source
        }
    )
    risk_reasons: dict[str, int] = {}
    for item in country_events:
        for reason in list(item.get("riskReasons") or []):
            key_text = str(reason)
            risk_reasons[key_text] = risk_reasons.get(key_text, 0) + 1
    lifecycle_statuses = sorted(
        {
            str(item.get("lifecycleStatus") or "active")
            for item in country_events
            if item.get("lifecycleStatus") or item.get("sourceStatus") == "confirmed"
        }
    )
    demo_event = any(
        bool(dict(item.get("evidence") or {}).get("demoBackfilled"))
        for item in timeline
    )
    multi_country_sync = len({item.get("country") for item in country_events}) >= 2
    audit_decision = _audit_decision_for_model_event(
        country_events,
        multi_country_sync=multi_country_sync,
        demo_event=demo_event,
    )

    brand, model, powertrain = key
    return {
        "eventId": "|".join(key),
        "brand": brand,
        "jatoModel": model,
        "jatoPowertrain": powertrain,
        "powertrainColor": POWERTRAIN_COLORS.get(powertrain, POWERTRAIN_FALLBACK_COLOR),
        "lengthMm": length_mm,
        "lengthMissing": length_mm is None,
        "lengthSource": "mixed" if len(length_sources) > 1 else (length_sources[0] if length_sources else None),
        "affectedCountryCount": len({item.get("country") for item in country_events}),
        "countryChangeCount": len(country_events),
        "timelineEventCount": len(timeline),
        "trimChangeCount": len({item.get("jatoTrim") for item in country_events}),
        "medianChangePct": _median_or_none(change_values),
        "minChangePct": round(min(change_values), 2) if change_values else None,
        "maxChangePct": round(max(change_values), 2) if change_values else None,
        "medianOldMsrpEur": _median_or_none(old_values),
        "medianCurrentMsrpEur": _median_or_none(current_values),
        "oldMsrpEurRange": _range_payload(old_values),
        "currentMsrpEurRange": _range_payload(current_values),
        "sourceRiskCount": source_risk_count,
        "reviewRequiredCount": review_required_count,
        "outlierCount": outlier_count,
        "suspectedFalsePositiveCount": suspected_false_positive_count,
        "multiCountrySync": multi_country_sync,
        "lifecycleStatus": (
            "mixed"
            if len(lifecycle_statuses) > 1
            else lifecycle_statuses[0]
            if lifecycle_statuses
            else "active"
        ),
        "demo": demo_event,
        **audit_decision,
        "confidence": (
            "low"
            if suspected_false_positive_count
            else "medium"
            if source_risk_count
            else "high"
        ),
        "riskReasons": risk_reasons,
        "countries": country_events,
        "timeline": sorted(
            timeline,
            key=lambda item: str(item.get("changedAtUtc") or ""),
        ),
    }


def _sweden_demo_scenario(item: CurrentPrice) -> dict[str, object] | None:
    key = (
        str(item.brand or "").strip().upper(),
        str(item.jato_model or "").strip().upper(),
        _normalize_powertrain(item.jato_powertrain),
    )
    return SWEDEN_DEMO_SCENARIOS.get(key)


def _demo_previous_value(current_value: float, change_pct: float) -> float:
    divisor = 1.0 + change_pct / 100.0
    if divisor <= 0:
        return current_value
    return round(current_value / divisor, 2)


def _demo_timeline_payload(
    *,
    item: CurrentPrice,
    observation: MsrpObservation | None,
    source: MsrpSource | None,
    generated_at: datetime,
    scenario: dict[str, object],
    threshold_pct: float,
) -> dict[str, object] | None:
    current_eur = _float_or_none(item.current_msrp_value)
    if current_eur is None:
        return None
    change_pct = _float_or_none(scenario.get("changePct"))
    if change_pct is None or abs(change_pct) < threshold_pct:
        return None
    previous_eur = _demo_previous_value(current_eur, change_pct)
    current_source = _float_or_none(item.source_msrp_value) or current_eur
    previous_source = _demo_previous_value(current_source, change_pct)
    days_ago = int(_float_or_none(scenario.get("daysAgo")) or 7)
    changed_at = generated_at - timedelta(days=max(1, days_ago))
    source_status = str(scenario.get("sourceStatus") or "confirmed")
    risk_reasons = [str(item) for item in list(scenario.get("riskReasons") or [])]
    lifecycle_status = str(scenario.get("lifecycleStatus") or "active")
    observation_id = str(getattr(observation, "observation_id", item.effective_observation_id))
    return {
        "country": item.country,
        "countryLabel": to_display_country(item.country),
        "brand": item.brand,
        "jatoModel": item.jato_model,
        "jatoTrim": item.jato_trim,
        "jatoPowertrain": _normalize_powertrain(item.jato_powertrain),
        "changedAtUtc": changed_at.isoformat(),
        "oldMsrpEur": previous_eur,
        "currentMsrpEur": current_eur,
        "changeAmountEur": round(current_eur - previous_eur, 2),
        "changePct": round(change_pct, 2),
        "oldSourceMsrp": previous_source,
        "currentSourceMsrp": current_source,
        "changeAmountSource": round(current_source - previous_source, 2),
        "sourceCurrency": item.source_currency,
        "previousSourceCurrency": item.source_currency,
        "sourceCurrencyChanged": False,
        "sourceStatus": source_status,
        "reviewFlag": source_status != "confirmed",
        "riskReasons": risk_reasons,
        "lifecycleStatus": lifecycle_status,
        "currentPriceId": str(item.current_price_id),
        "priceHistoryId": f"demo-sweden-history:{item.current_price_id}:{changed_at.date().isoformat()}",
        "currentObservationId": observation_id,
        "previousObservationId": f"demo-sweden-backfill:{item.current_price_id}",
        "lastConfirmedObservationId": observation_id,
        "effectiveObservationId": str(item.effective_observation_id),
        "source": {
            "sourceCode": source.source_code if source is not None else None,
            "sourceType": source.source_type if source is not None else None,
            "extractorName": source.extractor_name if source is not None else None,
            "extractorVersion": source.extractor_version if source is not None else None,
            "sourceRegistryUrl": source.source_url if source is not None else None,
        },
        "evidence": {
            "sourceUrl": item.source_url,
            "sourceSnapshotPath": item.source_snapshot_path,
            "matchConfidence": _float_or_none(item.match_confidence),
            "matchStatus": item.match_status,
            "observationSourceUrl": observation.source_url if observation is not None else None,
            "sourcePayloadHash": observation.source_payload_hash if observation is not None else None,
            "observedAtUtc": _iso(observation.observed_at_utc if observation is not None else None),
            "demoBackfilled": True,
            "demoScenario": str(scenario.get("scenario") or "Sweden MSRP monitoring demo"),
            "dryrunRunId": "msrp-demo-sweden-2026",
            "scrapeBatchCode": "msrp-demo-sweden-backfill",
        },
    }


def _build_sweden_demo_events(
    session: Session,
    *,
    brand: str | None,
    jato_model: str | None,
    generated_at: datetime,
    safe_window_days: int,
    safe_threshold_pct: float,
    safe_limit: int,
) -> dict[str, object]:
    current_prices = msrp_repo.list_current_prices(
        session,
        "瑞典",
        brand,
        jato_model,
        safe_limit,
        0,
    )
    current_prices = [
        item
        for item in current_prices
        if _sweden_demo_scenario(item) is not None
    ]
    current_observations = msrp_repo.list_observations_by_ids(
        session,
        [item.effective_observation_id for item in current_prices],
    )
    current_observation_by_id = {
        str(item.observation_id): item for item in current_observations
    }
    sources = msrp_repo.list_sources_by_ids(
        session,
        [item.source_id for item in current_observations],
    )
    source_by_id = {str(item.source_id): item for item in sources}
    since = generated_at - timedelta(days=safe_window_days)
    grouped_timeline: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    grouped_lengths: dict[tuple[str, str, str], dict[str, tuple[int | None, str | None]]] = {}

    for item in current_prices:
        scenario = _sweden_demo_scenario(item)
        if scenario is None:
            continue
        changed_at = generated_at - timedelta(days=int(_float_or_none(scenario.get("daysAgo")) or 7))
        if changed_at < since:
            continue
        observation = current_observation_by_id.get(str(item.effective_observation_id))
        source = (
            source_by_id.get(str(observation.source_id))
            if observation is not None
            else None
        )
        timeline_item = _demo_timeline_payload(
            item=item,
            observation=observation,
            source=source,
            generated_at=generated_at,
            scenario=scenario,
            threshold_pct=safe_threshold_pct,
        )
        if timeline_item is None:
            continue
        key = _event_key(item)
        grouped_timeline.setdefault(key, []).append(timeline_item)
        grouped_lengths.setdefault(key, {})[item.country] = (
            int(scenario.get("lengthMm") or 0) or None,
            "sweden_demo_backfill",
        )

    events = [
        _build_model_event(
            key,
            timeline,
            grouped_lengths.get(key, {}),
        )
        for key, timeline in grouped_timeline.items()
        if timeline
    ]
    events.sort(
        key=lambda item: (
            -abs(float(item.get("medianChangePct") or 0.0)),
            str(item.get("brand") or ""),
            str(item.get("jatoModel") or ""),
        )
    )
    return {
        "schemaVersion": "msrp_monitoring_events_v1",
        "mode": MONITORING_MODE_SWEDEN_DEMO,
        "generatedAtUtc": generated_at.isoformat(),
        "filters": {
            "country": "瑞典",
            "brand": brand,
            "jatoModel": jato_model,
            "windowDays": safe_window_days,
            "thresholdPct": safe_threshold_pct,
            "limit": safe_limit,
        },
        "summary": _summary_payload(events),
        "powertrainColors": POWERTRAIN_COLORS,
        "events": events,
        "warnings": ["sweden_demo_backfilled_not_written_to_price_history"],
        "demo": {
            "enabled": True,
            "country": "Sweden",
            "backfilled": True,
            "description": "Synthetic Sweden MSRP movements built from live current prices for product review.",
        },
    }


def build_msrp_monitoring_events(
    session: Session,
    *,
    country: str | None = None,
    brand: str | None = None,
    jato_model: str | None = None,
    window_days: int = 30,
    threshold_pct: float = 0.0,
    limit: int = DEFAULT_MONITORING_LIMIT,
    mode: str = MONITORING_MODE_LIVE,
) -> dict[str, object]:
    generated_at = _utc_now()
    safe_window_days = max(1, min(int(window_days), 365))
    safe_threshold_pct = max(0.0, float(threshold_pct))
    safe_limit = max(1, min(int(limit), DEFAULT_MONITORING_LIMIT))
    since = generated_at - timedelta(days=safe_window_days)
    safe_mode = (
        MONITORING_MODE_SWEDEN_DEMO
        if mode == MONITORING_MODE_SWEDEN_DEMO
        else MONITORING_MODE_LIVE
    )

    if safe_mode == MONITORING_MODE_SWEDEN_DEMO:
        return _build_sweden_demo_events(
            session,
            brand=brand,
            jato_model=jato_model,
            generated_at=generated_at,
            safe_window_days=safe_window_days,
            safe_threshold_pct=safe_threshold_pct,
            safe_limit=safe_limit,
        )

    if not msrp_repo.has_price_history_table(session):
        return {
            "schemaVersion": "msrp_monitoring_events_v1",
            "mode": MONITORING_MODE_LIVE,
            "generatedAtUtc": generated_at.isoformat(),
            "filters": {
                "country": country,
                "brand": brand,
                "jatoModel": jato_model,
                "windowDays": safe_window_days,
                "thresholdPct": safe_threshold_pct,
                "limit": safe_limit,
            },
            "summary": _summary_payload([]),
            "powertrainColors": POWERTRAIN_COLORS,
            "events": [],
            "warnings": ["price_history_unavailable"],
            "demo": None,
        }

    current_prices = msrp_repo.list_current_price_alerts(
        session,
        country,
        brand,
        jato_model,
        safe_limit,
        0,
    )
    (
        market_scan_country_lengths,
        market_scan_model_lengths,
        length_lookup_warning,
    ) = _load_market_scan_length_lookup(current_prices)
    current_observations = msrp_repo.list_observations_by_ids(
        session,
        [item.effective_observation_id for item in current_prices],
    )
    current_observation_by_id = {
        str(item.observation_id): item for item in current_observations
    }
    sources = msrp_repo.list_sources_by_ids(
        session,
        [item.source_id for item in current_observations],
    )
    source_by_id = {str(item.source_id): item for item in sources}

    grouped_timeline: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    grouped_lengths: dict[tuple[str, str, str], dict[str, tuple[int | None, str | None]]] = {}

    for item in current_prices:
        observation = current_observation_by_id.get(str(item.effective_observation_id))
        source = (
            source_by_id.get(str(observation.source_id))
            if observation is not None
            else None
        )
        group_key = _event_key(item)
        if observation is not None:
            length = (
                _extract_length_from_context(observation.source_context_json)
                or _extract_length_from_context(observation.match_reason_json)
            )
            length_source = "observation_context" if length is not None else None
        else:
            length = None
            length_source = None
        if length is None:
            length = _market_scan_length_for_item(
                item,
                market_scan_country_lengths,
                market_scan_model_lengths,
            )
            length_source = "market_scan" if length is not None else None
        grouped_lengths.setdefault(group_key, {})[item.country] = (
            length,
            length_source,
        )
        history = msrp_repo.list_price_history(
            session,
            item.country,
            item.brand,
            item.jato_model,
            item.jato_trim,
            item.jato_powertrain,
            20,
        )
        for index, current_period in enumerate(history[:-1]):
            previous_period = history[index + 1]
            if not _changed_in_window(current_period, since):
                continue
            timeline_item = _timeline_payload(
                item=item,
                current_period=current_period,
                previous_period=previous_period,
                source=source,
                observation=observation,
                threshold_pct=safe_threshold_pct,
            )
            if timeline_item is None:
                continue
            grouped_timeline.setdefault(group_key, []).append(timeline_item)

    all_timeline = [
        item
        for group in grouped_timeline.values()
        for item in group
    ]
    _hydrate_timeline_evidence(session, all_timeline)

    events = [
        _build_model_event(
            key,
            timeline,
            grouped_lengths.get(key, {}),
        )
        for key, timeline in grouped_timeline.items()
        if timeline
    ]
    events.sort(
        key=lambda item: (
            -int(item.get("affectedCountryCount") or 0),
            -abs(float(item.get("medianChangePct") or 0.0)),
            str(item.get("brand") or ""),
            str(item.get("jatoModel") or ""),
        )
    )

    return {
        "schemaVersion": "msrp_monitoring_events_v1",
        "mode": MONITORING_MODE_LIVE,
        "generatedAtUtc": generated_at.isoformat(),
        "filters": {
            "country": country,
            "brand": brand,
            "jatoModel": jato_model,
            "windowDays": safe_window_days,
            "thresholdPct": safe_threshold_pct,
            "limit": safe_limit,
        },
        "summary": _summary_payload(events),
        "powertrainColors": POWERTRAIN_COLORS,
        "events": events,
        "warnings": [item for item in [length_lookup_warning] if item],
        "demo": None,
    }
