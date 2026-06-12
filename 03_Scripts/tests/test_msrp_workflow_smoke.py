from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from typing import Any


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "diagnostics"
    / "msrp_workflow_smoke.py"
)


def load_module():
    module_name = "msrp_workflow_smoke_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


smoke_module = load_module()


class FakeClient:
    def __init__(self) -> None:
        self.api_base = "https://example.test/v1"
        self.sources: dict[str, dict[str, Any]] = {}
        self.history: list[dict[str, Any]] = []
        self.current_value: float | None = None
        self.finance_count = 0
        self.secondary_seen = False
        self.review_queued = False

    def request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if path == "/msrp/sources" and method == "GET":
            source_code = str((query or {}).get("source_code") or "")
            item = self.sources.get(source_code)
            return {"rows": 1 if item else 0, "items": [item] if item else []}
        if path == "/msrp/sources" and method == "POST":
            assert payload is not None
            source_code = str(payload["source_code"])
            item = {
                "sourceId": f"src-{len(self.sources) + 1}",
                "sourceCode": source_code,
                "priceSemantics": payload["price_semantics"],
            }
            self.sources[source_code] = item
            return {"item": item}
        if path == "/msrp/batches" and method == "POST":
            assert payload is not None
            observation = payload["observations"][0]
            semantics = observation.get("price_semantics") or "base_msrp"
            if semantics == "lease_monthly":
                self.finance_count += 1
                return {
                    "item": {
                        "observationRows": 1,
                        "currentPricesTouched": 0,
                        "financeObservationsCreated": 1,
                    }
                }
            value = float(observation["msrp_value"])
            self.current_value = value
            self.history.insert(0, {"sourceMsrpValue": value})
            if "secondary" in str(payload["batch_code"]):
                self.secondary_seen = True
            return {
                "item": {
                    "observationRows": 1,
                    "currentPricesTouched": 1,
                    "financeObservationsCreated": 0,
                }
            }
        if path == "/msrp/current-prices" and method == "GET":
            items = []
            if self.current_value is not None:
                items.append({"currentMsrpValue": self.current_value})
            return {"total": len(items), "rows": len(items), "items": items}
        if path == "/msrp/price-history" and method == "GET":
            return {"rows": len(self.history), "items": self.history}
        if path == "/msrp/current-prices/alerts" and method == "GET":
            items = []
            if len(self.history) >= 2:
                latest = float(self.history[0]["sourceMsrpValue"])
                previous = float(self.history[1]["sourceMsrpValue"])
                direction = "decrease" if latest < previous else "increase"
                items.append({
                    "direction": direction,
                    "severity": "critical",
                    "deltaPct": round(((latest - previous) / previous) * 100, 2),
                })
            return {"total": len(items), "rows": len(items), "items": items}
        if path == "/msrp/finance-observations" and method == "GET":
            assert (query or {}).get("has_monthly_payment") is True
            assert (query or {}).get("has_subsidy") is True
            assert (query or {}).get("has_net_price_after_subsidy") is True
            return {
                "total": self.finance_count,
                "rows": self.finance_count,
                "summary": {
                    "financeTypeCounts": {"private_lease": self.finance_count},
                    "netPriceAfterSubsidyCount": self.finance_count,
                    "subsidyObservationCount": self.finance_count,
                },
                "items": [{}] * self.finance_count,
            }
        if path == "/msrp/reconciliation" and method == "GET":
            items = [{"status": "conflict"}] if self.secondary_seen else []
            return {
                "summary": {"statusCounts": {"conflict": len(items)}},
                "items": items,
            }
        if path == "/msrp/reconciliation/review-cases" and method == "POST":
            self.review_queued = True
            return {"item": {"summary": {"reviewCasesQueued": 1}}}
        if path == "/msrp/current-prices/snapshot" and method == "GET":
            return {
                "schemaVersion": "msrp_current_price_snapshot_v1",
                "snapshotWeek": "2026-W24",
                "summary": {
                    "currentPriceCount": 1,
                    "priceAlertCount": 1,
                },
            }
        if path == "/msrp/effectiveness" and method == "GET":
            return {
                "schemaVersion": "msrp_price_sales_effectiveness_v1",
                "summary": {
                    "analyzedEventCount": 1,
                    "labelCounts": {"insufficient_data": 1},
                },
            }
        if path == "/hermes/overview" and method == "GET":
            return {"registries": {"source": 7}}
        if path == "/hermes/msrp-country-progress" and method == "GET":
            return {
                "overall": "ok",
                "status": {
                    "runId": "msrp-dryrun-test",
                    "overallPassPct": 96.4,
                    "gateStatus": "allowed",
                },
            }
        if path == "/hermes/msrp-dryrun-history" and method == "GET":
            return {
                "latestRunId": "msrp-dryrun-test",
                "runs": [{"runId": "msrp-dryrun-test"}],
            }
        if path == "/msrp/current-prices/snapshot" and method == "GET":
            return {
                "schemaVersion": "msrp_current_price_snapshot_v1",
                "snapshotWeek": "2026-W24",
                "summary": {
                    "currentPriceCount": 1,
                    "priceAlertCount": 1,
                },
                "warnings": [],
            }
        raise AssertionError(f"Unexpected request: {method} {path}")


class MissingSnapshotClient(FakeClient):
    def request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if path == "/msrp/current-prices/snapshot":
            raise smoke_module.SmokeFailure("GET /msrp/current-prices/snapshot failed with HTTP 404")
        return super().request_json(method, path, payload, query)


def test_main_requires_write_flag(capsys) -> None:
    assert smoke_module.main([]) == 2
    captured = capsys.readouterr()
    assert "Pass --write" in captured.err
    assert "--read-only" in captured.err


def test_main_rejects_write_and_read_only_together(capsys) -> None:
    assert smoke_module.main(["--write", "--read-only"]) == 2
    captured = capsys.readouterr()
    assert "Choose only one" in captured.err


def test_observation_payload_preserves_finance_fields() -> None:
    ctx = smoke_module.SmokeContext(
        api_base="http://example.test/v1",
        country="dk",
        brand="CODEX",
        jato_model="SMOKE MODEL",
        jato_trim="Smoke Trim",
        jato_powertrain="BEV",
        run_id="smoke_test",
    )

    payload = smoke_module._observation_payload(
        source_id="src-1",
        ctx=ctx,
        msrp_value=499,
        observed_at_utc="2026-05-01T08:00:00+00:00",
        price_semantics="lease_monthly",
        extra={"offer_valid_until": "2026-06-30"},
    )

    assert payload["price_semantics"] == "lease_monthly"
    assert payload["offer_valid_until"] == "2026-06-30"
    assert payload["source_context_json"] == {"runId": "smoke_test"}


def test_run_smoke_exercises_full_contract_with_fake_client() -> None:
    ctx = smoke_module.SmokeContext(
        api_base="http://example.test/v1",
        country="dk",
        brand="CODEX",
        jato_model="SMOKE MODEL smoke_test",
        jato_trim="Smoke Trim",
        jato_powertrain="BEV",
        run_id="smoke_test",
    )
    client = FakeClient()

    result = smoke_module.run_smoke(client, ctx)

    assert result["status"] == "ok"
    assert result["checks"]["price_history_rows"] >= 2
    assert result["checks"]["finance_observations"] == 1
    assert result["checks"]["reconciliation_status_counts"] == {"conflict": 1}
    assert result["checks"]["review_cases_queued"] == 1
    assert result["checks"]["effectiveness_labels"] == {"insufficient_data": 1}


def test_run_read_only_probe_summarizes_deployed_contract() -> None:
    client = FakeClient()
    args = smoke_module.parse_args([
        "--read-only",
        "--api-base",
        "https://example.test/v1",
    ])

    result = smoke_module.run_read_only_probe(client, args)

    assert result["status"] == "ok"
    assert result["mode"] == "read_only"
    assert result["checks"]["msrpCountryProgressRunId"] == "msrp-dryrun-test"
    assert result["checks"]["msrpCountryProgressGate"] == "allowed"
    assert result["checks"]["dryrunRunCount"] == 1
    assert result["checks"]["currentSnapshotSchema"] == "msrp_current_price_snapshot_v1"
    assert result["checks"]["probeWarnings"] == []


def test_run_read_only_probe_degrades_when_snapshot_endpoint_is_missing() -> None:
    client = MissingSnapshotClient()
    args = smoke_module.parse_args(["--read-only"])

    result = smoke_module.run_read_only_probe(client, args)

    assert result["status"] == "degraded"
    assert result["checks"]["currentSnapshotSchema"] is None
    assert result["checks"]["probeWarnings"]
