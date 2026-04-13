"""Tests for the YAML config loader."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from app.scraper import registry
from app.scraper.config_loader import load_all_sources, load_source_file


@pytest.fixture(autouse=True)
def _clean_registry():
    """Remove any sources registered during a test."""
    before = set(registry.list_registered())
    yield
    after = set(registry.list_registered())
    for code in after - before:
        registry._REGISTRY.pop(code, None)


def _write_yaml(tmp_path: Path, name: str, content: str) -> Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(content), encoding="utf-8")
    return path


class TestLoadSourceFile:
    def test_loads_scrapling_with_attr_json(self, tmp_path):
        path = _write_yaml(
            tmp_path,
            "brand_x.yaml",
            """\
            source_code: brand_x_test
            country: 德国
            brand: BrandX
            source_url: https://example.com
            source_type: manufacturer_official
            price_semantics: base_msrp
            extractor_type: scrapling
            profile:
              url: https://example.com/models
              tier: stealth
              attr_json:
                vehicle_container: ".card"
                filter_attr: "data-filter"
                tracking_attr: "data-track"
                price_key: "price"
                name_key: "name"
              css:
                vehicle_container: ".card"
                model: ".title::text"
            """,
        )

        code = load_source_file(path)

        assert code == "brand_x_test"
        cls = registry.get(code)
        assert cls._profile.attr_json is not None
        assert cls._profile.attr_json.vehicle_container == ".card"
        assert cls._profile.attr_json.filter_attr == "data-filter"
        assert cls._profile.tier == "stealth"

    def test_skips_invalid_yaml(self, tmp_path):
        path = _write_yaml(
            tmp_path,
            "bad.yaml",
            """\
            - this is a list, not a mapping
            """,
        )

        assert load_source_file(path) is None

    def test_skips_missing_keys(self, tmp_path):
        path = _write_yaml(
            tmp_path,
            "incomplete.yaml",
            """\
            source_code: incomplete_test
            country: 德国
            """,
        )

        assert load_source_file(path) is None

    def test_merges_profile_preset_relative_to_source(self, tmp_path):
        preset_dir = tmp_path / "_shared" / "presets"
        preset_dir.mkdir(parents=True)
        _write_yaml(
            preset_dir,
            "vw_base.yaml",
            """\
            tier: stealth
            headless: true
            structured_fields:
              edition_rules:
                - key: edition_special
                  label: Special Edition
                  keyword: special edition
                  special: true
            """,
        )
        source_dir = tmp_path / "se"
        source_dir.mkdir()
        path = _write_yaml(
            source_dir,
            "vw_tiguan.yaml",
            """\
            source_code: vw_tiguan_test
            country: 瑞典
            brand: VOLKSWAGEN
            source_url: https://example.com/tiguan
            extractor_type: scrapling
            profile_preset: ../_shared/presets/vw_base.yaml
            profile:
              url: https://example.com/tiguan
              fixed_model: TIGUAN
              structured_fields:
                powertrain_rules:
                  - key: powertrain_phev
                    powertrain: PHEV
                    keywords:
                      - eHybrid
            """,
        )

        code = load_source_file(path)

        assert code == "vw_tiguan_test"
        cls = registry.get(code)
        assert cls._profile.tier == "stealth"
        assert cls._profile.fixed_model == "TIGUAN"
        edition_rules = cls._profile.structured_fields["edition_rules"]
        powertrain_rules = cls._profile.structured_fields[
            "powertrain_rules"
        ]
        assert edition_rules[0]["key"] == "edition_special"
        assert powertrain_rules[0]["powertrain"] == "PHEV"


class TestLoadAllSources:
    def test_loads_from_directory(self, tmp_path):
        _write_yaml(
            tmp_path,
            "a.yaml",
            """\
            source_code: test_a
            country: 德国
            brand: A
            source_url: https://a.example.com
            extractor_type: scrapling
            profile:
              url: https://a.example.com
            """,
        )
        _write_yaml(
            tmp_path,
            "_template.yaml",
            """\
            source_code: should_skip
            country: 德国
            brand: Skip
            source_url: https://skip.example.com
            extractor_type: scrapling
            profile:
              url: https://skip.example.com
            """,
        )

        loaded = load_all_sources(tmp_path)

        assert "test_a" in loaded
        assert "should_skip" not in loaded

    def test_nonexistent_directory(self, tmp_path):
        result = load_all_sources(tmp_path / "does_not_exist")

        assert result == []

    def test_skips_hidden_directories(self, tmp_path):
        visible_dir = tmp_path / "se"
        visible_dir.mkdir()
        hidden_dir = tmp_path / "_shared"
        hidden_dir.mkdir()
        _write_yaml(
            visible_dir,
            "visible.yaml",
            """\
            source_code: visible_source
            country: 德国
            brand: Visible
            source_url: https://visible.example.com
            extractor_type: scrapling
            profile:
              url: https://visible.example.com
            """,
        )
        _write_yaml(
            hidden_dir,
            "hidden.yaml",
            """\
            source_code: hidden_source
            country: 德国
            brand: Hidden
            source_url: https://hidden.example.com
            extractor_type: scrapling
            profile:
              url: https://hidden.example.com
            """,
        )

        loaded = load_all_sources(tmp_path)

        assert "visible_source" in loaded
        assert "hidden_source" not in loaded
      # End of tests.

