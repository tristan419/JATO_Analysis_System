import json

from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation


class DummyExtractor(BaseExtractor):
    def extract(self) -> list[RawObservation]:
        return []


def test_base_extractor_records_strategy_audit(tmp_path, monkeypatch):
    monkeypatch.setenv("JATO_AUDIT_DIR", str(tmp_path))
    extractor = DummyExtractor(
        ExtractorConfig(
            source_code="dummy_source",
            country="SE",
            brand="VOLVO",
            source_url="https://example.invalid",
        )
    )
    extractor.run_id = "run_test"
    observation = RawObservation(
        official_model="XC60",
        official_trim="Core",
        msrp_value=499000,
        currency="SEK",
        tax_included=True,
        price_label="MSRP",
        source_url="https://example.invalid",
    )

    extractor.record_strategy_audit(
        url="https://example.invalid",
        strategy="json_script_selector",
        observations=[observation],
        winning_strategy="json_script_selector",
    )

    audit_file = tmp_path / "run_test.jsonl"
    event = json.loads(audit_file.read_text().strip())
    assert event["run_id"] == "run_test"
    assert event["source_id"] == "dummy_source"
    assert event["winning_strategy"] == "json_script_selector"
    assert event["attempted_strategies"] == [{
        "strategy": "json_script_selector",
        "status": "success",
        "observations_count": 1,
    }]
    assert event["observations_count"] == 1
    assert event["currency"] == "SEK"
    assert extractor.last_audit_event is not None
    assert extractor.last_audit_event["winning_strategy"] == "json_script_selector"
