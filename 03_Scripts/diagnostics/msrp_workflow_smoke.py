#!/usr/bin/env python3
"""Run an end-to-end MSRP workflow smoke test through the public API.

This diagnostic intentionally writes isolated ``codex_msrp_smoke_*`` rows to
the target database. It is opt-in via ``--write`` so it cannot accidentally
mutate production while someone is only checking command wiring.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


DEFAULT_API_BASE = "http://127.0.0.1:8000/v1"
DEFAULT_COUNTRY = "dk"
DEFAULT_BRAND = "CODEX"
DEFAULT_MODEL_PREFIX = "SMOKE MODEL"
DEFAULT_TRIM = "Smoke Trim"
DEFAULT_POWERTRAIN = "BEV"


class SmokeFailure(RuntimeError):
    """Raised when the smoke flow cannot prove an expected contract."""


@dataclass(frozen=True)
class SmokeContext:
    api_base: str
    country: str
    brand: str
    jato_model: str
    jato_trim: str
    jato_powertrain: str
    run_id: str


class ApiClient:
    def __init__(
        self,
        *,
        api_base: str,
        timeout_seconds: int,
        auth_token: str | None,
        user_name: str,
    ) -> None:
        self.api_base = api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.auth_token = auth_token
        self.user_name = user_name

    def request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        query_text = ""
        if query:
            query_text = "?" + urllib.parse.urlencode({
                key: value
                for key, value in query.items()
                if value is not None
            })
        body = None
        headers = {
            "Accept": "application/json",
            "X-User-Name": self.user_name,
        }
        if self.auth_token:
            headers["X-Auth-Token"] = self.auth_token
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.api_base + path + query_text,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self.timeout_seconds,
            ) as response:
                text = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise SmokeFailure(
                f"{method} {path} failed with HTTP {exc.code}: {detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise SmokeFailure(
                f"{method} {path} failed: {exc.reason}"
            ) from exc
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise SmokeFailure(
                f"{method} {path} returned non-JSON response: {text[:200]}"
            ) from exc
        if not isinstance(parsed, dict):
            raise SmokeFailure(f"{method} {path} returned a non-object JSON")
        return parsed


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def _source_payload(
    *,
    source_code: str,
    country: str,
    brand: str,
    source_type: str,
    price_semantics: str,
) -> dict[str, Any]:
    return {
        "source_code": source_code,
        "country": country,
        "brand": brand,
        "source_url": f"https://example.test/msrp-smoke/{source_code}",
        "source_type": source_type,
        "tier": 1,
        "extractor_name": "codex_smoke",
        "extractor_version": "v1",
        "price_semantics": price_semantics,
        "requires_location": False,
        "enabled": True,
        "notes": "Created by msrp_workflow_smoke.py",
    }


def _ensure_source(
    client: ApiClient,
    *,
    source_code: str,
    country: str,
    brand: str,
    source_type: str = "official_configurator",
    price_semantics: str = "base_msrp",
) -> dict[str, Any]:
    existing = client.request_json(
        "GET",
        "/msrp/sources",
        query={"source_code": source_code, "limit": 1},
    )
    items = existing.get("items")
    if isinstance(items, list) and items:
        item = items[0]
        if isinstance(item, dict):
            return item
    created = client.request_json(
        "POST",
        "/msrp/sources",
        _source_payload(
            source_code=source_code,
            country=country,
            brand=brand,
            source_type=source_type,
            price_semantics=price_semantics,
        ),
    )
    item = created.get("item")
    if not isinstance(item, dict):
        raise SmokeFailure(f"Source create returned unexpected payload: {created}")
    return item


def _observation_payload(
    *,
    source_id: str,
    ctx: SmokeContext,
    msrp_value: float,
    observed_at_utc: str,
    price_label: str = "MSRP incl VAT",
    price_semantics: str = "base_msrp",
    match_status: str = "auto_accepted",
    match_confidence: float = 0.99,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source_id": source_id,
        "country": ctx.country,
        "brand": ctx.brand,
        "jato_model": ctx.jato_model,
        "jato_trim": ctx.jato_trim,
        "jato_powertrain": ctx.jato_powertrain,
        "official_model": ctx.jato_model,
        "official_trim": ctx.jato_trim,
        "official_edition": None,
        "official_powertrain": ctx.jato_powertrain,
        "msrp_value": msrp_value,
        "currency": "EUR",
        "tax_included": True,
        "price_label": price_label,
        "availability_text": "smoke available",
        "observed_at_utc": observed_at_utc,
        "source_url": f"https://example.test/msrp-smoke/{ctx.run_id}",
        "source_snapshot_path": f"diagnostics/smoke/{ctx.run_id}.json",
        "source_payload_hash": f"{ctx.run_id}-{source_id}-{observed_at_utc}",
        "extraction_version": "smoke-v1",
        "match_confidence": match_confidence,
        "match_status": match_status,
        "match_reason_json": {"source": "msrp_workflow_smoke"},
        "source_context_json": {"runId": ctx.run_id},
        "candidate_matches_json": [
            {
                "officialModel": ctx.jato_model,
                "officialTrim": ctx.jato_trim,
                "score": match_confidence,
            }
        ],
        "price_semantics": price_semantics,
    }
    if extra:
        payload.update(extra)
    return payload


def _post_batch(
    client: ApiClient,
    *,
    ctx: SmokeContext,
    batch_suffix: str,
    observations: list[dict[str, Any]],
    observed_at_utc: str,
) -> dict[str, Any]:
    return client.request_json(
        "POST",
        "/msrp/batches",
        {
            "batch_code": f"{ctx.run_id}_{batch_suffix}",
            "trigger_type": "smoke",
            "scope_country": ctx.country,
            "scope_brands": [ctx.brand],
            "failed_count": 0,
            "notes": "MSRP workflow smoke test",
            "started_at_utc": observed_at_utc,
            "finished_at_utc": observed_at_utc,
            "observations": observations,
        },
    ).get("item", {})


def _expect(condition: bool, message: str, payload: Any = None) -> None:
    if condition:
        return
    suffix = ""
    if payload is not None:
        suffix = "\n" + json.dumps(payload, ensure_ascii=False, indent=2)[:2000]
    raise SmokeFailure(message + suffix)


def _query(
    client: ApiClient,
    path: str,
    ctx: SmokeContext,
    *,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    query = {
        "country": ctx.country,
        "brand": ctx.brand,
        "jato_model": ctx.jato_model,
        "limit": 50,
    }
    if extra:
        query.update(extra)
    return client.request_json("GET", path, query=query)


def run_smoke(client: ApiClient, ctx: SmokeContext) -> dict[str, Any]:
    primary_source = _ensure_source(
        client,
        source_code=f"codex_msrp_smoke_primary_{ctx.run_id}",
        country=ctx.country,
        brand=ctx.brand,
    )
    secondary_source = _ensure_source(
        client,
        source_code=f"codex_msrp_smoke_secondary_{ctx.run_id}",
        country=ctx.country,
        brand=ctx.brand,
    )
    finance_source = _ensure_source(
        client,
        source_code=f"codex_msrp_smoke_finance_{ctx.run_id}",
        country=ctx.country,
        brand=ctx.brand,
        source_type="official_finance_offer",
        price_semantics="lease_monthly",
    )
    primary_source_id = str(primary_source["sourceId"])
    secondary_source_id = str(secondary_source["sourceId"])
    finance_source_id = str(finance_source["sourceId"])

    first_batch = _post_batch(
        client,
        ctx=ctx,
        batch_suffix="primary_week1",
        observed_at_utc="2026-05-01T08:00:00+00:00",
        observations=[
            _observation_payload(
                source_id=primary_source_id,
                ctx=ctx,
                msrp_value=40000.0,
                observed_at_utc="2026-05-01T08:00:00+00:00",
            )
        ],
    )
    _expect(
        first_batch.get("currentPricesTouched", 0) >= 1,
        "First MSRP batch did not materialize current price",
        first_batch,
    )

    second_batch = _post_batch(
        client,
        ctx=ctx,
        batch_suffix="primary_week2_drop",
        observed_at_utc="2026-05-15T08:00:00+00:00",
        observations=[
            _observation_payload(
                source_id=primary_source_id,
                ctx=ctx,
                msrp_value=37000.0,
                observed_at_utc="2026-05-15T08:00:00+00:00",
            )
        ],
    )
    _expect(
        second_batch.get("currentPricesTouched", 0) >= 1,
        "Second MSRP batch did not update current price",
        second_batch,
    )

    current_prices_after_drop = _query(client, "/msrp/current-prices", ctx)
    _expect(
        current_prices_after_drop.get("total", 0) >= 1,
        "Current price query did not return the smoke row",
        current_prices_after_drop,
    )
    current_item = dict((current_prices_after_drop.get("items") or [{}])[0])
    _expect(
        float(current_item.get("currentMsrpValue") or 0) == 37000.0,
        "Current price did not reflect latest primary MSRP",
        current_item,
    )

    history_after_drop = _query(
        client,
        "/msrp/price-history",
        ctx,
        extra={
            "jato_trim": ctx.jato_trim,
            "jato_powertrain": ctx.jato_powertrain,
        },
    )
    _expect(
        history_after_drop.get("rows", 0) >= 2,
        "Price history did not retain both weekly snapshots",
        history_after_drop,
    )

    alerts_after_drop = _query(
        client,
        "/msrp/current-prices/alerts",
        ctx,
        extra={"threshold_pct": 3.0},
    )
    alert_items = [
        item
        for item in (alerts_after_drop.get("items") or [])
        if isinstance(item, dict)
    ]
    _expect(alert_items, "Price alert query returned no events", alerts_after_drop)
    _expect(
        any(
            item.get("direction") == "decrease"
            and item.get("severity") == "critical"
            for item in alert_items
        ),
        "Price alert did not flag the 7.5% drop as critical",
        alerts_after_drop,
    )

    finance_batch = _post_batch(
        client,
        ctx=ctx,
        batch_suffix="finance_lease",
        observed_at_utc="2026-05-16T08:00:00+00:00",
        observations=[
            _observation_payload(
                source_id=finance_source_id,
                ctx=ctx,
                msrp_value=499.0,
                observed_at_utc="2026-05-16T08:00:00+00:00",
                price_label="Private lease monthly payment incl VAT",
                price_semantics="lease_monthly",
                extra={
                    "monthly_payment": 499.0,
                    "down_payment": 3000.0,
                    "down_payment_pct": 8.0,
                    "term_months": 36,
                    "apr": 3.9,
                    "effective_apr": 4.2,
                    "finance_type": "private_lease",
                    "total_credit_cost": 2800.0,
                    "total_amount_payable": 50964.0,
                    "annual_mileage_limit": 15000,
                    "offer_valid_until": "2026-06-30",
                    "subsidy_amount": 2000.0,
                    "net_price_after_subsidy": 35000.0,
                    "finance_currency": "EUR",
                },
            )
        ],
    )
    _expect(
        finance_batch.get("financeObservationsCreated", 0) == 1,
        "Finance batch did not create a finance observation",
        finance_batch,
    )
    _expect(
        finance_batch.get("currentPricesTouched", 0) == 0,
        "Finance observation should not mutate current MSRP",
        finance_batch,
    )

    finance = _query(
        client,
        "/msrp/finance-observations",
        ctx,
        extra={
            "price_semantics": "lease_monthly",
            "finance_type": "private_lease",
            "has_monthly_payment": True,
            "has_subsidy": True,
            "has_net_price_after_subsidy": True,
        },
    )
    _expect(
        finance.get("total", 0) >= 1
        and finance.get("summary", {})
        .get("financeTypeCounts", {})
        .get("private_lease", 0)
        >= 1,
        "Finance observation query did not expose lease summary",
        finance,
    )
    _expect(
        finance.get("summary", {}).get("netPriceAfterSubsidyCount", 0) >= 1
        and finance.get("summary", {}).get("subsidyObservationCount", 0) >= 1,
        "Finance observation query did not expose subsidy/net-price summary",
        finance,
    )

    secondary_batch = _post_batch(
        client,
        ctx=ctx,
        batch_suffix="secondary_conflict",
        observed_at_utc="2026-05-20T08:00:00+00:00",
        observations=[
            _observation_payload(
                source_id=secondary_source_id,
                ctx=ctx,
                msrp_value=39000.0,
                observed_at_utc="2026-05-20T08:00:00+00:00",
            )
        ],
    )
    _expect(
        secondary_batch.get("currentPricesTouched", 0) >= 1,
        "Secondary MSRP batch did not materialize current price",
        secondary_batch,
    )

    reconciliation = _query(
        client,
        "/msrp/reconciliation",
        ctx,
        extra={"threshold_pct": 1.0},
    )
    reconciliation_items = [
        item
        for item in (reconciliation.get("items") or [])
        if isinstance(item, dict)
    ]
    _expect(
        any(item.get("status") == "conflict" for item in reconciliation_items),
        "Multi-source reconciliation did not flag the MSRP spread conflict",
        reconciliation,
    )

    review_queue = client.request_json(
        "POST",
        "/msrp/reconciliation/review-cases",
        query={
            "country": ctx.country,
            "brand": ctx.brand,
            "jato_model": ctx.jato_model,
            "limit": 50,
            "threshold_pct": 1.0,
        },
    ).get("item", {})
    _expect(
        review_queue.get("summary", {}).get("reviewCasesQueued", 0) >= 1,
        "Reconciliation conflict was not queued for review",
        review_queue,
    )

    snapshot = _query(
        client,
        "/msrp/current-prices/snapshot",
        ctx,
        extra={"threshold_pct": 3.0},
    )
    _expect(
        snapshot.get("schemaVersion") == "msrp_current_price_snapshot_v1"
        and snapshot.get("summary", {}).get("currentPriceCount", 0) >= 1
        and snapshot.get("summary", {}).get("priceAlertCount", 0) >= 1,
        "Current price snapshot did not include current price and alert counts",
        snapshot,
    )

    effectiveness = _query(
        client,
        "/msrp/effectiveness",
        ctx,
        extra={
            "threshold_pct": 3.0,
            "baseline_window_months": 2,
            "post_window_months": 2,
            "post_lag_months": 1,
            "min_months": 1,
        },
    )
    _expect(
        effectiveness.get("schemaVersion")
        == "msrp_price_sales_effectiveness_v1"
        and effectiveness.get("summary", {}).get("analyzedEventCount", 0) >= 1,
        "Sales effectiveness API did not analyze the smoke price event",
        effectiveness,
    )

    final_current_prices = _query(client, "/msrp/current-prices", ctx)
    return {
        "status": "ok",
        "runId": ctx.run_id,
        "filters": {
            "country": ctx.country,
            "brand": ctx.brand,
            "jatoModel": ctx.jato_model,
            "jatoTrim": ctx.jato_trim,
            "jatoPowertrain": ctx.jato_powertrain,
        },
        "sourceIds": {
            "primary": primary_source_id,
            "secondary": secondary_source_id,
            "finance": finance_source_id,
        },
        "checks": {
            "official_msrp_ingest": second_batch.get("observationRows"),
            "current_price_count": final_current_prices.get("total"),
            "price_history_rows": history_after_drop.get("rows"),
            "critical_price_alerts": len(alert_items),
            "finance_observations": finance.get("total"),
            "reconciliation_status_counts": reconciliation.get(
                "summary",
                {},
            ).get("statusCounts", {}),
            "review_cases_queued": review_queue.get("summary", {}).get(
                "reviewCasesQueued",
            ),
            "snapshot_week": snapshot.get("snapshotWeek"),
            "effectiveness_labels": effectiveness.get("summary", {}).get(
                "labelCounts",
            ),
        },
    }


def _read_only_filters(args: argparse.Namespace) -> dict[str, Any]:
    filters: dict[str, Any] = {
        "limit": 50,
        "threshold_pct": 3.0,
    }
    if args.country:
        filters["country"] = args.country
    if args.brand:
        filters["brand"] = args.brand
    if args.jato_model:
        filters["jato_model"] = args.jato_model
    return filters


def run_read_only_probe(
    client: ApiClient,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Probe deployed MSRP control-plane endpoints without writing rows."""
    probe_warnings: list[str] = []

    def safe_get(
        path: str,
        *,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        try:
            return client.request_json("GET", path, query=query)
        except SmokeFailure as exc:
            probe_warnings.append(f"{path}: {exc}")
            return {}

    overview = safe_get("/hermes/overview")
    country_progress = safe_get("/hermes/msrp-country-progress")
    dryrun_history = safe_get("/hermes/msrp-dryrun-history")
    current_snapshot = safe_get(
        "/msrp/current-prices/snapshot",
        query=_read_only_filters(args),
    )

    country_status = country_progress.get("status") or {}
    snapshot_summary = current_snapshot.get("summary") or {}
    dryrun_runs = dryrun_history.get("runs") or []
    warnings = [
        str(item)
        for item in (current_snapshot.get("warnings") or [])
        if str(item).strip()
    ]
    status = "ok"
    if str(country_progress.get("overall") or "") in {"critical", "failed"}:
        status = "degraded"
    if warnings or probe_warnings:
        status = "degraded"

    return {
        "status": status,
        "mode": "read_only",
        "apiBase": client.api_base,
        "filters": {
            "country": args.country,
            "brand": args.brand,
            "jatoModel": args.jato_model,
        },
        "checks": {
            "hermesOverviewRegistries": overview.get("registries", {}),
            "msrpCountryProgressOverall": country_progress.get("overall"),
            "msrpCountryProgressRunId": country_status.get("runId"),
            "msrpCountryProgressPassPct": country_status.get("overallPassPct"),
            "msrpCountryProgressGate": country_status.get("gateStatus"),
            "dryrunLatestRunId": dryrun_history.get("latestRunId"),
            "dryrunRunCount": len(dryrun_runs) if isinstance(dryrun_runs, list) else 0,
            "currentSnapshotSchema": current_snapshot.get("schemaVersion"),
            "currentSnapshotWeek": current_snapshot.get("snapshotWeek"),
            "currentPriceCount": snapshot_summary.get("currentPriceCount"),
            "priceAlertCount": snapshot_summary.get("priceAlertCount"),
            "warnings": warnings,
            "probeWarnings": probe_warnings,
        },
    }


def build_context(args: argparse.Namespace) -> SmokeContext:
    run_id = args.run_id or f"smoke_{_utc_stamp()}"
    jato_model = args.jato_model or f"{args.model_prefix} {run_id}"
    return SmokeContext(
        api_base=args.api_base,
        country=args.country or DEFAULT_COUNTRY,
        brand=args.brand or DEFAULT_BRAND,
        jato_model=jato_model,
        jato_trim=args.jato_trim,
        jato_powertrain=args.jato_powertrain,
        run_id=run_id,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an opt-in MSRP workflow smoke test through FastAPI.",
    )
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--country", default=None)
    parser.add_argument("--brand", default=None)
    parser.add_argument("--model-prefix", default=DEFAULT_MODEL_PREFIX)
    parser.add_argument("--jato-model", default=None)
    parser.add_argument("--jato-trim", default=DEFAULT_TRIM)
    parser.add_argument("--jato-powertrain", default=DEFAULT_POWERTRAIN)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--auth-token", default=None)
    parser.add_argument("--user-name", default="codex-smoke")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Required. Writes diagnostic rows to the target MSRP database.",
    )
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="Probe deployed MSRP endpoints without writing diagnostic rows.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.write and args.read_only:
        print("Choose only one of --write or --read-only.", file=sys.stderr)
        return 2
    if not args.write and not args.read_only:
        print(
            "Refusing to run: this smoke test writes diagnostic MSRP rows. "
            "Pass --write to continue, or --read-only to probe endpoints.",
            file=sys.stderr,
        )
        return 2

    client = ApiClient(
        api_base=args.api_base,
        timeout_seconds=max(1, int(args.timeout_seconds)),
        auth_token=args.auth_token,
        user_name=args.user_name,
    )
    started = time.time()
    try:
        if args.read_only:
            result = run_read_only_probe(client, args)
        else:
            ctx = build_context(args)
            result = run_smoke(client, ctx)
    except SmokeFailure as exc:
        print(f"MSRP workflow smoke failed: {exc}", file=sys.stderr)
        return 1
    result["elapsedSeconds"] = round(time.time() - started, 2)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
