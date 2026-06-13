from __future__ import annotations

import json
import time

from jato_scraper import registry
from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation
from jato_scraper import runner


class SlowTimeoutExtractor(BaseExtractor):
    def extract(self) -> list[RawObservation]:
        time.sleep(30)
        return []


class FastValidExtractor(BaseExtractor):
    def extract(self) -> list[RawObservation]:
        return [
            RawObservation(
                official_model="EX30",
                official_trim="Core",
                msrp_value=429_000,
                currency="SEK",
                tax_included=True,
                price_label="MSRP",
                source_url=self.config.source_url,
            )
        ]


class EmptyForbiddenExtractor(BaseExtractor):
    def extract(self) -> list[RawObservation]:
        self.record_audit_event(
            url=self.config.source_url,
            attempted_strategies=[{
                "strategy": "css_selectors",
                "status": "no_match",
                "observations_count": 0,
            }],
            winning_strategy=None,
            observations=[],
            extra={
                "httpStatus": 403,
                "finalUrl": self.config.source_url,
                "contentType": "text/html",
            },
        )
        return []


def test_run_scrape_times_out_one_source_and_continues(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("JATO_AUDIT_DIR", str(tmp_path))
    monkeypatch.setattr(runner, "load_all_sources", lambda *_, **__: [])
    monkeypatch.setattr(
        runner,
        "enrich_observations_with_eur",
        lambda observations: observations,
    )
    slow_code = "runner_timeout_slow_test"
    fast_code = "runner_timeout_fast_test"

    try:
        registry.register(
            ExtractorConfig(
                source_code=slow_code,
                country="瑞典",
                brand="VOLVO",
                source_url="https://example.invalid/slow",
            ),
            SlowTimeoutExtractor,
        )
    except ValueError:
        pass
    try:
        registry.register(
            ExtractorConfig(
                source_code=fast_code,
                country="瑞典",
                brand="VOLVO",
                source_url="https://example.invalid/fast",
            ),
            FastValidExtractor,
        )
    except ValueError:
        pass

    summary = runner.run_scrape(
        [slow_code, fast_code],
        dry_run=True,
        source_timeout_seconds=1,
    )

    slow = summary["sources"][slow_code]
    fast = summary["sources"][fast_code]
    assert summary["ok"] is False
    assert slow["status"] == "timeout"
    assert slow["extracted"] == 0
    assert slow["sourceTimeoutSeconds"] == 1
    assert "exceeded 1s extraction timeout" in slow["error"]
    assert fast["status"] == "dry_run"
    assert fast["valid"] == 1
    assert fast["rejected"] == 0

    audit_file = tmp_path / f"{summary['run_id']}.jsonl"
    audit_events = [
        json.loads(line)
        for line in audit_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    timeout_events = [
        event
        for event in audit_events
        if event["source_id"] == slow_code
    ]
    assert len(timeout_events) == 1
    assert timeout_events[0]["attempted_strategies"][0]["status"] == "error"
    assert "exceeded 1s extraction timeout" in timeout_events[0]["error"]


def test_run_scrape_returns_child_audit_diagnostics(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("JATO_AUDIT_DIR", str(tmp_path))
    monkeypatch.setattr(runner, "load_all_sources", lambda *_, **__: [])
    code = "runner_forbidden_empty_test"

    try:
        registry.register(
            ExtractorConfig(
                source_code=code,
                country="SE",
                brand="TESLA",
                source_url="https://www.tesla.com/sv_se/modely",
            ),
            EmptyForbiddenExtractor,
        )
    except ValueError:
        pass

    summary = runner.run_scrape(
        [code],
        dry_run=True,
        source_timeout_seconds=5,
    )

    source = summary["sources"][code]
    assert source["status"] == "empty"
    assert source["extracted"] == 0
    assert source["sourceUrl"] == "https://www.tesla.com/sv_se/modely"
    assert source["brand"] == "TESLA"
    assert source["httpStatus"] == 403
    assert source["finalUrl"] == "https://www.tesla.com/sv_se/modely"
    assert source["contentType"] == "text/html"
    assert source["coverageLevel"] == "L0_FAILED"
    assert source["auditStatus"] == "failed"
    assert source["winningStrategy"] is None
    assert source["attemptedStrategies"] == [{
        "strategy": "css_selectors",
        "status": "no_match",
        "observations_count": 0,
    }]
