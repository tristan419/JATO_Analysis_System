from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "msrp_source_accessibility_audit.py"


def load_module():
    module_name = "msrp_source_accessibility_audit_test_module"
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
        self.headers = headers or {}
        self.text = text

    def __bool__(self) -> bool:
        return self.status_code < 400


class _FakeSession:
    def __init__(self, responses=None, exc: Exception | None = None) -> None:
        self.headers = {}
        self.responses = list(responses or [])
        self.exc = exc
        self.calls: list[tuple[str, str]] = []

    def head(self, url, allow_redirects, timeout):
        self.calls.append(("HEAD", url))
        if self.exc is not None:
            raise self.exc
        return self.responses.pop(0)

    def get(self, url, allow_redirects, timeout):
        self.calls.append(("GET", url))
        if self.exc is not None:
            raise self.exc
        return self.responses.pop(0)


def test_source_issues_from_backlog_dedupes_group_and_top_level_items() -> None:
    backlog = {
        "sourceIssues": [
            {
                "countryCode": "at",
                "sourceCode": "mg_zs_at_draft_scrapling",
                "sourceUrl": "https://www.mgmotor.at/modelle/mg-zs",
            }
        ],
        "groups": [
            {
                "sourceRepairIssues": [
                    {
                        "countryCode": "at",
                        "sourceCode": "mg_zs_at_draft_scrapling",
                        "sourceUrl": "https://www.mgmotor.at/modelle/mg-zs",
                    },
                    {
                        "countryCode": "at",
                        "sourceCode": "tesla_model_y_at_draft_scrapling",
                        "sourceUrl": "https://www.tesla.com/de_at/modely",
                    },
                ]
            }
        ],
        "transientSourceRegressions": [
            {
                "countryCode": "at",
                "sourceCode": "skoda_elroq_at_draft_scrapling",
                "sourceUrl": "https://www.skoda.at/elroq",
            }
        ],
    }

    assert [
        item["sourceCode"]
        for item in audit.source_issues_from_backlog(backlog)
    ] == [
        "mg_zs_at_draft_scrapling",
        "tesla_model_y_at_draft_scrapling",
    ]
    assert [
        item["sourceCode"]
        for item in audit.source_issues_from_backlog(
            backlog,
            include_transient=True,
        )
    ] == [
        "mg_zs_at_draft_scrapling",
        "tesla_model_y_at_draft_scrapling",
        "skoda_elroq_at_draft_scrapling",
    ]


def test_source_issues_from_legacy_backlog_samples_recover_source_repairs() -> None:
    backlog = {
        "sourceRepairIssueCount": 2,
        "transientRegressionCount": 10,
        "groups": [
            {
                "failureReason": "network_unavailable",
                "sourceRepairIssueCount": 2,
                "transientRegressionCount": 10,
                "recommendedStrategy": "retry_network_or_proxy",
                "recommendedAction": "repair_source_definition",
                "affectedCountries": ["at"],
                "sampleSources": [
                    "mg_zs_at_draft_scrapling",
                    "peugeot_3008_at_draft_scrapling",
                    "skoda_elroq_at_draft_scrapling",
                    "tesla_model_y_at_draft_scrapling",
                    "volkswagen_tiguan_at_draft_scrapling",
                ],
                "sampleTransientRegressions": [
                    {
                        "countryCode": "at",
                        "sourceCode": "peugeot_3008_at_draft_scrapling",
                        "recommendedAction": "recheck_before_source_repair",
                    },
                    {
                        "countryCode": "at",
                        "sourceCode": "skoda_elroq_at_draft_scrapling",
                        "recommendedAction": "recheck_before_source_repair",
                    },
                ],
                "topSourceHosts": [
                    {
                        "host": "mgmotor.at",
                        "sampleSources": ["mg_zs_at_draft_scrapling"],
                        "sampleUrls": ["https://www.mgmotor.at/modelle/mg-zs"],
                    },
                    {
                        "host": "tesla.com",
                        "sampleSources": ["tesla_model_y_at_draft_scrapling"],
                        "sampleUrls": ["https://www.tesla.com/de_at/modely"],
                    },
                    {
                        "host": "volkswagen.at",
                        "sampleSources": ["volkswagen_tiguan_at_draft_scrapling"],
                        "sampleUrls": ["https://www.volkswagen.at/tiguan/tiguan"],
                    },
                ],
            }
        ],
    }

    sources = audit.source_issues_from_backlog(backlog)

    assert [
        item["sourceCode"]
        for item in sources
    ] == [
        "mg_zs_at_draft_scrapling",
        "tesla_model_y_at_draft_scrapling",
    ]
    assert [item["sourceUrl"] for item in sources] == [
        "https://www.mgmotor.at/modelle/mg-zs",
        "https://www.tesla.com/de_at/modely",
    ]
    assert {item["brand"] for item in sources} == {"MG", "TESLA"}


def test_source_issues_from_source_drafts_reads_current_yaml_urls(tmp_path: Path) -> None:
    source_root = tmp_path / "source_drafts"
    target = source_root / "at" / "01_skoda_elroq_at.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "\n".join([
            "source_code: skoda_elroq_at_draft_scrapling",
            "country: Austria",
            "brand: SKODA",
            "source_url: https://www.skoda.at/elroq/elroq/overlay-elroq-preisliste",
            "profile:",
            "  url: https://www.skoda.at/elroq/elroq/overlay-elroq-preisliste",
        ]),
        encoding="utf-8",
    )
    ignored = source_root / "fi" / "01_tesla_model_y_fi.yaml"
    ignored.parent.mkdir(parents=True, exist_ok=True)
    ignored.write_text(
        "\n".join([
            "source_code: tesla_model_y_fi_draft_scrapling",
            "country: Finland",
            "brand: TESLA",
            "source_url: https://www.tesla.com/fi_FI/modely",
        ]),
        encoding="utf-8",
    )

    sources = audit.source_issues_from_source_drafts(
        source_root,
        countries={"at"},
        brands={"skoda"},
    )

    assert sources == [
        {
            "countryCode": "at",
            "sourceCode": "skoda_elroq_at_draft_scrapling",
            "sourceUrl": "https://www.skoda.at/elroq/elroq/overlay-elroq-preisliste",
            "brand": "SKODA",
            "failureReason": "source_draft_url_probe",
            "recommendedStrategy": "probe_current_source_url",
            "recommendedAction": "verify_current_source_draft_url",
            "sourceDraftPath": str(target),
        }
    ]


def test_probe_source_classifies_akamai_403_as_official_proxy_required() -> None:
    session = _FakeSession([
        _FakeResponse(
            403,
            url="https://www.tesla.com/de_at/modely",
            headers={"server": "AkamaiGHost"},
        ),
        _FakeResponse(
            403,
            url="https://www.tesla.com/de_at/modely",
            headers={"server": "AkamaiGHost"},
            text="Access Denied. You don't have permission to access this server.",
        )
    ])

    item = audit.probe_source(
        {
            "countryCode": "at",
            "sourceCode": "tesla_model_y_at_draft_scrapling",
            "sourceUrl": "https://www.tesla.com/de_at/modely",
            "failureReason": "network_unavailable",
        },
        session=session,
    )

    assert session.calls == [
        ("HEAD", "https://www.tesla.com/de_at/modely"),
        ("GET", "https://www.tesla.com/de_at/modely"),
    ]
    assert item["probeStatus"] == "anti_bot_blocked"
    assert item["recommendedAction"] == "official_proxy_or_configurator_api"
    assert item["officialProxyRequired"] is True


def test_probe_source_falls_back_to_get_when_head_not_allowed() -> None:
    session = _FakeSession([
        _FakeResponse(405),
        _FakeResponse(200, url="https://www.mgmotor.at/modelle/mg-zs"),
    ])

    item = audit.probe_source(
        {
            "countryCode": "at",
            "sourceCode": "mg_zs_at_draft_scrapling",
            "sourceUrl": "https://www.mgmotor.at/modelle/mg-zs",
        },
        session=session,
    )

    assert session.calls == [
        ("HEAD", "https://www.mgmotor.at/modelle/mg-zs"),
        ("GET", "https://www.mgmotor.at/modelle/mg-zs"),
    ]
    assert item["probeStatus"] == "fetchable"
    assert item["recommendedAction"] == "run_page_analyzer_or_selector_repair"


def test_probe_source_classifies_timeout_as_retryable_network() -> None:
    session = _FakeSession(exc=audit.requests.Timeout("timed out"))

    item = audit.probe_source(
        {
            "countryCode": "at",
            "sourceCode": "mg_zs_at_draft_scrapling",
            "sourceUrl": "https://www.mgmotor.at/modelle/mg-zs",
        },
        session=session,
    )

    assert item["probeStatus"] == "network_timeout"
    assert item["recommendedAction"] == "retry_network_or_proxy"
    assert item["retryable"] is True


def test_summarize_items_marks_multi_host_network_outage_as_inconclusive() -> None:
    summary = audit.summarize_items([
        {
            "sourceUrl": "https://www.volkswagen.de/model",
            "host": "volkswagen.de",
            "probeStatus": "network_timeout",
            "retryable": True,
            "httpStatus": None,
        },
        {
            "sourceUrl": "https://www.skoda.de/model",
            "host": "skoda.de",
            "probeStatus": "network_timeout",
            "retryable": True,
            "httpStatus": None,
        },
        {
            "sourceUrl": "https://www.opel.de/model",
            "host": "opel.de",
            "probeStatus": "network_timeout",
            "retryable": True,
            "httpStatus": None,
        },
    ])

    assert summary["environmentAssessment"] == {
        "status": "inconclusive_egress",
        "sourceRepairEligible": False,
        "probedSourceCount": 3,
        "distinctHostCount": 3,
        "probeStatuses": ["network_timeout"],
        "recommendedAction": "rerun_from_production_egress_or_known_good_network",
        "reason": (
            "All probes failed before HTTP across multiple official hosts; "
            "this run cannot prove individual source URLs are broken."
        ),
    }


def test_summarize_items_keeps_mixed_source_evidence_eligible_for_triage() -> None:
    summary = audit.summarize_items([
        {
            "sourceUrl": "https://www.volkswagen.de/model",
            "host": "volkswagen.de",
            "probeStatus": "network_timeout",
            "retryable": True,
            "httpStatus": None,
        },
        {
            "sourceUrl": "https://www.skoda.de/model",
            "host": "skoda.de",
            "probeStatus": "fetchable",
            "retryable": False,
            "httpStatus": 200,
        },
        {
            "sourceUrl": "https://www.opel.de/model",
            "host": "opel.de",
            "probeStatus": "source_url_not_found",
            "retryable": False,
            "httpStatus": 404,
        },
    ])

    assert summary["environmentAssessment"]["status"] == "not_detected"
    assert summary["environmentAssessment"]["sourceRepairEligible"] is True


def test_probe_source_classifies_tls_eof_as_tls_handshake_failed() -> None:
    session = _FakeSession(
        exc=audit.requests.exceptions.SSLError(
            "[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol",
        ),
    )

    item = audit.probe_source(
        {
            "countryCode": "at",
            "sourceCode": "mg_zs_at_draft_scrapling",
            "sourceUrl": "https://www.mgmotor.at/modelle/mg-zs",
        },
        session=session,
    )

    assert item["probeStatus"] == "tls_handshake_failed"
    assert item["recommendedAction"] == "try_official_alternative_url_or_proxy"
    assert item["retryable"] is True


def test_probe_source_uses_curl_fallback_when_requests_tls_fails() -> None:
    session = _FakeSession(
        exc=audit.requests.exceptions.SSLError(
            "[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol",
        ),
    )
    calls: list[tuple[str, float]] = []

    def fake_curl_probe(url: str, timeout_seconds: float):
        calls.append((url, timeout_seconds))
        return {
            "method": "CURL_HEAD",
            "statusCode": 200,
            "finalUrl": "https://www.dacia.pt/gama-hibrida-eletrica/duster-suv.html",
        }

    item = audit.probe_source(
        {
            "countryCode": "pt",
            "sourceCode": "dacia_duster_pt_draft_scrapling",
            "sourceUrl": "https://www.dacia.pt/gama-hibrida-eletrica/duster-suv.html",
        },
        session=session,
        timeout_seconds=7.0,
        curl_probe=fake_curl_probe,
    )

    assert calls == [
        ("https://www.dacia.pt/gama-hibrida-eletrica/duster-suv.html", 7.0)
    ]
    assert item["method"] == "CURL_HEAD"
    assert item["probeStatus"] == "fetchable"
    assert item["recommendedAction"] == "run_page_analyzer_or_selector_repair"
    assert item["retryable"] is False
    assert item["fallbackProbe"] == "curl"
    assert item["requestsError"]


def test_curl_probe_uses_default_curl_identity_first(monkeypatch) -> None:
    commands: list[list[str]] = []

    def fake_run(command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            "\n200\nhttps://www.peugeot.be/tarif.pdf",
            "",
        )

    monkeypatch.setattr(audit.subprocess, "run", fake_run)

    result = audit._probe_with_curl("https://www.peugeot.be/tarif.pdf", 9.0)

    assert result is not None
    assert result["method"] == "CURL_HEAD"
    assert result["statusCode"] == 200
    assert len(commands) == 1
    assert "--user-agent" not in commands[0]
    assert "--http1.1" in commands[0]


def test_curl_probe_falls_back_to_default_range_get_after_head_403(monkeypatch) -> None:
    commands: list[list[str]] = []
    responses = [
        subprocess.CompletedProcess(
            [],
            0,
            "\n403\nhttps://www.peugeot.be/tarif.pdf",
            "",
        ),
        subprocess.CompletedProcess(
            [],
            0,
            "\n206\nhttps://www.peugeot.be/tarif.pdf",
            "",
        ),
    ]

    def fake_run(command, **_kwargs):
        commands.append(command)
        response = responses.pop(0)
        response.args = command
        return response

    monkeypatch.setattr(audit.subprocess, "run", fake_run)

    result = audit._probe_with_curl("https://www.peugeot.be/tarif.pdf", 9.0)

    assert result is not None
    assert result["method"] == "CURL_GET_RANGE"
    assert result["statusCode"] == 206
    assert len(commands) == 2
    assert "--head" in commands[0]
    assert "--range" in commands[1]
    assert "--user-agent" not in commands[0]
    assert "--user-agent" not in commands[1]


def test_probe_source_classifies_dns_resolution_failures() -> None:
    session = _FakeSession(
        exc=audit.requests.ConnectionError(
            "NameResolutionError: Failed to resolve 'www.example.invalid'",
        ),
    )

    item = audit.probe_source(
        {
            "countryCode": "at",
            "sourceCode": "example_at_draft_scrapling",
            "sourceUrl": "https://www.example.invalid/model",
        },
        session=session,
    )

    assert item["probeStatus"] == "dns_unresolved"
    assert item["recommendedAction"] == "check_dns_or_source_domain"
    assert item["retryable"] is True


def test_run_writes_accessibility_report(tmp_path: Path) -> None:
    backlog_path = tmp_path / "backlog.json"
    backlog_path.write_text(
        json.dumps({
            "schemaVersion": "msrp_source_repair_backlog_v1",
            "runId": "msrp-dryrun-test",
            "sourceRepairIssueCount": 1,
            "transientRegressionCount": 0,
            "sourceIssues": [
                {
                    "countryCode": "at",
                    "sourceCode": "mg_zs_at_draft_scrapling",
                    "sourceUrl": "https://www.mgmotor.at/modelle/mg-zs",
                }
            ],
        }),
        encoding="utf-8",
    )
    session = _FakeSession([
        _FakeResponse(404, url="https://www.mgmotor.at/modelle/mg-zs")
    ])

    report = audit.run(
        str(backlog_path),
        str(tmp_path / "out"),
        session=session,
    )

    assert report["schemaVersion"] == audit.SCHEMA_VERSION
    assert report["summary"]["probedSourceCount"] == 1
    assert report["summary"]["probeStatusCounts"] == {
        "source_url_not_found": 1,
    }
    assert report["summary"]["curlFallbackFetchableCount"] == 0
    assert report["summary"]["tlsHandshakeFailedCount"] == 0
    assert report["summary"]["dnsUnresolvedCount"] == 0
    assert (tmp_path / "out" / "msrp_source_accessibility_audit.json").exists()
    assert (tmp_path / "out" / "msrp_source_accessibility_audit.md").exists()


def test_run_can_probe_current_source_drafts(tmp_path: Path) -> None:
    source_root = tmp_path / "source_drafts"
    target = source_root / "at" / "01_mg_zs_at.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "\n".join([
            "source_code: mg_zs_at_draft_scrapling",
            "country: Austria",
            "brand: MG",
            "source_url: https://www.mgmotor.at/modelle/mg-zs",
            "profile:",
            "  url: https://www.mgmotor.at/modelle/mg-zs",
        ]),
        encoding="utf-8",
    )
    session = _FakeSession([
        _FakeResponse(200, url="https://www.mgmotor.at/modelle/mg-zs"),
    ])

    report = audit.run(
        out_dir=str(tmp_path / "out"),
        source_draft_root=str(source_root),
        countries="at",
        brands="mg",
        session=session,
    )

    assert report["sourceMode"] == "source_drafts"
    assert report["summary"]["sourceDraftSourceCount"] == 1
    assert report["summary"]["probeStatusCounts"] == {"fetchable": 1}
    assert report["summary"]["curlFallbackFetchableCount"] == 0
    assert report["items"][0]["sourceDraftPath"] == str(target)
    assert (tmp_path / "out" / "msrp_source_accessibility_audit.json").exists()


def test_run_counts_curl_fallback_fetchable_source_drafts(tmp_path: Path) -> None:
    source_root = tmp_path / "source_drafts"
    target = source_root / "pt" / "04_dacia_duster_pt.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "\n".join([
            "source_code: dacia_duster_pt_draft_scrapling",
            "country: Portugal",
            "brand: DACIA",
            "source_url: https://www.dacia.pt/gama-hibrida-eletrica/duster-suv.html",
            "profile:",
            "  url: https://www.dacia.pt/gama-hibrida-eletrica/duster-suv.html",
        ]),
        encoding="utf-8",
    )
    session = _FakeSession(
        exc=audit.requests.exceptions.SSLError(
            "[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol",
        ),
    )

    def fake_curl_probe(url: str, timeout_seconds: float):
        return {
            "method": "CURL_HEAD",
            "statusCode": 200,
            "finalUrl": url,
        }

    report = audit.run(
        out_dir=str(tmp_path / "out"),
        source_draft_root=str(source_root),
        countries="pt",
        brands="dacia",
        session=session,
        curl_probe=fake_curl_probe,
    )

    assert report["summary"]["probeStatusCounts"] == {"fetchable": 1}
    assert report["summary"]["retryableNetworkCount"] == 0
    assert report["summary"]["tlsHandshakeFailedCount"] == 0
    assert report["summary"]["curlFallbackFetchableCount"] == 1
    assert report["items"][0]["fallbackProbe"] == "curl"
