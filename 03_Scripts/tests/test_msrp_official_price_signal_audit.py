from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "msrp_official_price_signal_audit.py"


def load_module():
    module_name = "msrp_official_price_signal_audit_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


audit = load_module()


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        *,
        url: str = "https://example.test/model",
        headers: dict[str, str] | None = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self.url = url
        self.headers = headers or {"content-type": "text/html"}
        self.text = text


class _FakeSession:
    def __init__(self, responses=None, exc: Exception | None = None) -> None:
        self.headers = {}
        self.responses = list(responses or [])
        self.exc = exc
        self.calls: list[str] = []

    def get(self, url, allow_redirects, timeout):
        self.calls.append(url)
        if self.exc is not None:
            raise self.exc
        return self.responses.pop(0)


def _backlog() -> dict:
    return {
        "schemaVersion": "msrp_source_repair_backlog_v1",
        "runId": "msrp-dryrun-test",
        "sourceRepairIssueCount": 1,
        "transientRegressionCount": 0,
        "sourceIssues": [
            {
                "countryCode": "at",
                "sourceCode": "mg_zs_at_draft_scrapling",
                "brand": "MG",
                "sourceUrl": "https://www.mgmotor.at/modelle/mg-zs",
                "failureReason": "network_unavailable",
                "recommendedStrategy": "retry_network_or_proxy",
            }
        ],
    }


def test_classify_price_signal_marks_official_msrp_page_as_dryrun_candidate() -> None:
    html = """
    <html>
      <head><title>MG ZS Preisliste</title></head>
      <body>
        <h1>MG ZS</h1>
        <p>Unverbindliche Preisempfehlung inkl. MwSt ab € 24.990</p>
      </body>
    </html>
    """
    evidence = audit.build_page_evidence(html=html, url="https://example.test")
    heuristics = audit.analyze_page_heuristics(evidence)

    classification = audit.classify_price_signal(
        evidence=evidence,
        heuristics=heuristics,
    )

    assert classification["officialPriceSignalStatus"] == "price_signal_present"
    assert classification["recommendedAction"] == "repair_selector_and_run_dryrun"
    assert classification["dryrunCandidateEligible"] is True
    assert classification["officialIngestEligible"] is False


def test_classify_price_signal_rejects_page_without_price_values() -> None:
    html = """
    <html>
      <head><title>MG ZS Hybrid+</title></head>
      <body>
        <h1>MG ZS</h1>
        <p>Explore design, warranty and technology details.</p>
      </body>
    </html>
    """
    evidence = audit.build_page_evidence(html=html, url="https://example.test")
    heuristics = audit.analyze_page_heuristics(evidence)

    classification = audit.classify_price_signal(
        evidence=evidence,
        heuristics=heuristics,
    )

    assert classification["officialPriceSignalStatus"] == "no_price_signal"
    assert classification["recommendedAction"] == "do_not_promote_find_price_list_or_api"
    assert classification["dryrunCandidateEligible"] is False
    assert classification["officialIngestEligible"] is False


def test_build_price_signal_report_inspects_registered_and_candidate_urls() -> None:
    html_without_price = """
    <html><body><h1>MG ZS</h1><p>Design and warranty details.</p></body></html>
    """
    html_with_price = """
    <html><body>
      <h1>MG ZS</h1>
      <p>Unverbindliche Preisempfehlung inkl. MwSt ab € 24.990</p>
    </body></html>
    """
    session = _FakeSession([
        _FakeResponse(
            200,
            url="https://www.mgmotor.at/modelle/mg-zs",
            text=html_without_price,
        ),
        _FakeResponse(
            200,
            url="https://www.mgmotor.eu/model/zs-price-list",
            text=html_with_price,
        ),
    ])

    report = audit.build_price_signal_report(
        _backlog(),
        session=session,
        candidate_map={
            "mg_zs_at_draft_scrapling": [
                "https://www.mgmotor.eu/model/zs-price-list"
            ]
        },
    )

    assert session.calls == [
        "https://www.mgmotor.at/modelle/mg-zs",
        "https://www.mgmotor.eu/model/zs-price-list",
    ]
    assert report["summary"]["candidateUrlCount"] == 2
    assert report["summary"]["dryrunCandidateEligibleCount"] == 1
    assert report["summary"]["officialIngestEligibleCount"] == 0
    assert [
        item["officialPriceSignalStatus"] for item in report["items"]
    ] == ["no_price_signal", "price_signal_present"]
    assert report["items"][1]["candidateKind"] == "candidate_url"


def test_inspect_candidate_url_keeps_akamai_403_as_access_blocked() -> None:
    session = _FakeSession([
        _FakeResponse(
            403,
            url="https://www.tesla.com/de_at/modely",
            headers={"server": "AkamaiGHost", "content-type": "text/html"},
            text="Access Denied. You don't have permission to access this server.",
        )
    ])

    item = audit.inspect_candidate_url(
        {
            "countryCode": "at",
            "sourceCode": "tesla_model_y_at_draft_scrapling",
            "brand": "TESLA",
        },
        candidate={
            "kind": "registered_source",
            "url": "https://www.tesla.com/de_at/modely",
        },
        session=session,
    )

    assert item["fetchStatus"] == "anti_bot_blocked"
    assert item["officialPriceSignalStatus"] == "access_blocked"
    assert item["recommendedAction"] == "official_proxy_or_configurator_api"
    assert item["dryrunCandidateEligible"] is False


def test_run_writes_price_signal_report(tmp_path: Path) -> None:
    backlog_path = tmp_path / "backlog.json"
    backlog_path.write_text(json.dumps(_backlog()), encoding="utf-8")
    session = _FakeSession([
        _FakeResponse(
            200,
            url="https://www.mgmotor.at/modelle/mg-zs",
            text="<html><body><h1>MG ZS</h1></body></html>",
        )
    ])

    report = audit.run(
        str(backlog_path),
        str(tmp_path / "out"),
        session=session,
    )

    assert report["schemaVersion"] == audit.SCHEMA_VERSION
    assert report["summary"]["candidateUrlCount"] == 1
    assert report["summary"]["noPriceSignalCount"] == 1
    assert (tmp_path / "out" / "msrp_official_price_signal_audit.json").exists()
    assert (tmp_path / "out" / "msrp_official_price_signal_audit.md").exists()
