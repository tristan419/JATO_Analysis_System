"""Tests for the scraper base types, registry, and HTTP JSON extractor."""

import json
from unittest.mock import patch, MagicMock

from app.scraper.base import BaseExtractor, ExtractorConfig, RawObservation
from app.scraper import registry as _registry_module
from app.scraper.extractors.http_json import (
    HttpJsonExtractor,
    HttpJsonProfile,
    FieldMapping,
)


# ── RawObservation tests ─────────────────────────────────────────────

class TestRawObservation:
    def test_payload_hash_deterministic(self):
        obs = RawObservation(
            official_model="X5",
            official_trim="xDrive40i",
            msrp_value=72_900.0,
            currency="EUR",
            tax_included=True,
            price_label="UVP",
            source_url="https://bmw.de",
        )
        assert obs.payload_hash == obs.payload_hash  # same value twice

    def test_payload_hash_changes_with_price(self):
        base = dict(
            official_model="X5", official_trim="xDrive40i",
            currency="EUR", tax_included=True,
            price_label="UVP", source_url="https://bmw.de",
        )
        obs_a = RawObservation(msrp_value=72_900.0, **base)
        obs_b = RawObservation(msrp_value=73_000.0, **base)
        assert obs_a.payload_hash != obs_b.payload_hash


# ── Registry tests ───────────────────────────────────────────────────

class TestRegistry:
    def setup_method(self):
        """Save and restore registry state per test."""
        self._backup = dict(_registry_module._REGISTRY)

    def teardown_method(self):
        _registry_module._REGISTRY.clear()
        _registry_module._REGISTRY.update(self._backup)

    def test_register_and_get(self):
        cfg = ExtractorConfig(
            source_code="test_src",
            country="德国",
            brand="Test",
            source_url="https://test.example",
        )

        class FakeExtractor(BaseExtractor):
            def extract(self):
                return []

        _registry_module.register(cfg, FakeExtractor)
        ext = _registry_module.get("test_src")
        assert isinstance(ext, FakeExtractor)
        assert ext.config.source_code == "test_src"

    def test_list_registered(self):
        assert isinstance(_registry_module.list_registered(), list)

    def test_get_unknown_raises(self):
        import pytest
        with pytest.raises(KeyError):
            _registry_module.get("nonexistent_source_xyz")


# ── HttpJsonExtractor tests ──────────────────────────────────────────

_MOCK_RESPONSE = {
    "data": {
        "models": [
            {
                "modelName": "3 Series",
                "trimName": "320i Sedan",
                "basePrice": 42_900.0,
                "currency": "EUR",
                "taxIncluded": True,
                "priceLabel": "UVP inkl. MwSt.",
            },
            {
                "modelName": "X5",
                "trimName": "xDrive40i",
                "basePrice": 72_900.0,
                "currency": "EUR",
                "taxIncluded": True,
                "priceLabel": "UVP inkl. MwSt.",
            },
        ]
    }
}


def _build_extractor() -> HttpJsonExtractor:
    cfg = ExtractorConfig(
        source_code="test_bmw",
        country="德国",
        brand="BMW",
        source_url="https://test.bmw.de",
    )
    profile = HttpJsonProfile(
        url="https://test.bmw.de/api/models",
        field_mapping=FieldMapping(
            model="modelName",
            trim="trimName",
            price="basePrice",
            currency="currency",
            tax_included="taxIncluded",
            price_label="priceLabel",
            vehicles_path="data.models",
        ),
    )
    return HttpJsonExtractor(cfg, profile)


class TestHttpJsonExtractor:
    @patch("app.scraper.extractors.http_json.requests.Session")
    def test_extract_maps_vehicles(self, MockSession):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _MOCK_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        session_instance = MagicMock()
        session_instance.get.return_value = mock_resp
        MockSession.return_value = session_instance

        ext = _build_extractor()
        ext._session = session_instance

        results = ext.extract()
        assert len(results) == 2
        assert results[0].official_model == "3 Series"
        assert results[0].msrp_value == 42_900.0
        assert results[1].official_model == "X5"

    @patch("app.scraper.extractors.http_json.requests.Session")
    def test_extract_handles_http_error(self, MockSession):
        import requests as _requests
        session_instance = MagicMock()
        session_instance.get.side_effect = _requests.ConnectionError("timeout")
        MockSession.return_value = session_instance

        ext = _build_extractor()
        ext._session = session_instance

        results = ext.extract()
        assert results == []

    def test_navigate_missing_path(self):
        ext = _build_extractor()
        result = ext._navigate({"wrong": "structure"})
        assert result is None

    def test_navigate_success(self):
        ext = _build_extractor()
        result = ext._navigate(_MOCK_RESPONSE)
        assert len(result) == 2

    def test_map_skips_bad_entries(self):
        ext = _build_extractor()
        vehicles = [
            {"modelName": "Valid", "trimName": "Trim", "basePrice": 30_000},
            {},  # missing fields but still maps with defaults
        ]
        results = ext._map(vehicles)
        assert len(results) == 2  # both map, validation catches bad ones


# ── Runner payload builder tests ─────────────────────────────────────

class TestBuildBatchPayload:
    def test_payload_structure(self):
        from app.scraper.runner import build_batch_payload
        from app.scraper.validation import BatchValidationReport

        cfg = ExtractorConfig(
            source_code="test_src",
            country="德国",
            brand="BMW",
            source_url="https://test.de",
        )

        class FakeExt(BaseExtractor):
            def extract(self):
                return []

        ext = FakeExt(cfg)
        obs = RawObservation(
            official_model="X1",
            official_trim="sDrive18i",
            msrp_value=38_900.0,
            currency="EUR",
            tax_included=True,
            price_label="UVP",
            source_url="https://test.de",
        )
        report = BatchValidationReport(valid=[obs], rejected=[])

        payload = build_batch_payload(ext, report, source_id="fake-uuid")
        assert payload["scope_country"] == "德国"
        assert payload["scope_brands"] == ["BMW"]
        assert len(payload["observations"]) == 1
        assert payload["observations"][0]["msrp_value"] == 38_900.0
        assert payload["observations"][0]["source_id"] == "fake-uuid"
        assert payload["trigger_type"] == "scheduled"
        assert "batch_code" in payload
