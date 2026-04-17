from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
import sys


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "data_pipeline"
    / "seed_msrp_initial_links.py"
)


def load_module():
    module_name = "seed_msrp_initial_links_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


seed_module = load_module()
SUV_DRAFT_ROOT = (
    "07_ScrapingToolkit/source_drafts/suv_only_country_model_top30"
)


def test_official_source_file_seeds_include_seed_targets() -> None:
    actual = {
        item.relative_path
        for item in seed_module.OFFICIAL_SOURCE_FILE_SEEDS
    }

    assert {
        f"{SUV_DRAFT_ROOT}/se/09_volkswagen_tiguan_se.yaml",
        f"{SUV_DRAFT_ROOT}/se/12_volkswagen_t_roc_se.yaml",
        f"{SUV_DRAFT_ROOT}/se/06_skoda_kodiaq_se.yaml",
        f"{SUV_DRAFT_ROOT}/de/02_volkswagen_tiguan_de.yaml",
        f"{SUV_DRAFT_ROOT}/de/01_volkswagen_t_roc_de.yaml",
        f"{SUV_DRAFT_ROOT}/de/04_skoda_kodiaq_de.yaml",
    } <= actual


def test_link_seeds_cover_seeded_suv_models() -> None:
    actual = {
        (item.country, item.brand, item.jato_model)
        for item in seed_module.LINK_SEEDS
    }

    assert {
        (seed_module.SWEDEN, "VOLKSWAGEN", "Tiguan"),
        (seed_module.SWEDEN, "VOLKSWAGEN", "T-Roc"),
        (seed_module.SWEDEN, "SKODA", "Kodiaq"),
        (seed_module.GERMANY, "VOLKSWAGEN", "Tiguan"),
        (seed_module.GERMANY, "VOLKSWAGEN", "T-Roc"),
        (seed_module.GERMANY, "SKODA", "Kodiaq"),
    } <= actual


def test_sweden_t_roc_link_seeds_cover_current_mhev_trims() -> None:
    actual = {
        (item.jato_trim, item.jato_powertrain, item.official_trim)
        for item in seed_module.LINK_SEEDS
        if item.country == seed_module.SWEDEN
        and item.brand == "VOLKSWAGEN"
        and item.jato_model == "T-Roc"
    }

    assert actual == {
        ("LIFE", "MHEV", "Life"),
        ("R-LINE", "MHEV", "R-Line"),
    }


def test_dedupe_observation_payloads_removes_duplicates() -> None:
    payload = {
        "country": "Germany",
        "brand": "VOLKSWAGEN",
        "jato_model": "Tiguan",
        "jato_trim": "ENERGY",
        "jato_powertrain": "PHEV",
        "official_model": "TIGUAN",
        "official_trim": "ENERGY",
        "official_edition": "",
        "official_powertrain": "PHEV",
        "msrp_value": 54680.0,
        "currency": "EUR",
        "source_url": "https://example.com/tiguan",
        "source_id": "source-1",
    }

    deduped = seed_module.dedupe_observation_payloads([payload, dict(payload)])

    assert len(deduped) == 1
    assert deduped[0]["official_trim"] == "ENERGY"


def test_dedupe_observation_payloads_collapses_business_key_duplicates(
) -> None:
    base_payload = {
        "country": "Germany",
        "brand": "VOLKSWAGEN",
        "jato_model": "TIGUAN",
        "jato_trim": (
            "ENERGY | 1.5 eTSI OPF "
            "7-Gang-Doppelkupplungsgetriebe DSG"
        ),
        "jato_powertrain": "",
        "official_model": "TIGUAN",
        "official_trim": (
            "ENERGY | 1.5 eTSI OPF "
            "7-Gang-Doppelkupplungsgetriebe DSG"
        ),
        "official_edition": "",
        "official_powertrain": "",
        "currency": "EUR",
        "source_id": "source-1",
        "match_confidence": 0.84,
        "match_reason_json": {"source": "official_source_file"},
    }

    deduped = seed_module.dedupe_observation_payloads(
        [
            {
                **base_payload,
                "msrp_value": 46775.0,
                "source_url": "https://example.com/high",
            },
            {
                **base_payload,
                "msrp_value": 44695.0,
                "source_url": "https://example.com/low",
            },
        ]
    )

    assert len(deduped) == 1
    assert deduped[0]["msrp_value"] == 44695.0


def test_build_batch_code_uses_microseconds() -> None:
    created_at = datetime(
        2026,
        4,
        17,
        12,
        30,
        16,
        123456,
        tzinfo=timezone.utc,
    )

    batch_code = seed_module.build_batch_code(
        seed_module.GERMANY,
        created_at=created_at,
    )

    assert (
        batch_code
        == "w2-initial-links-2026-04-17-germany-20260417123016123456"
    )


def test_build_link_seed_row_uses_seed_label_and_timestamp() -> None:
    seed = seed_module.LinkSeed(
        country=seed_module.GERMANY,
        brand="SKODA",
        jato_model="Kodiaq",
        jato_trim="SELECTION",
        jato_powertrain="MHEV",
        official_model="KODIAQ",
        official_trim="Kodiaq",
        official_edition="",
        official_powertrain="",
        confidence=84,
        notes=(
            "Germany Kodiaq Selection MHEV collapsed to official "
            "entry MSRP row"
        ),
    )
    created_at = datetime(2026, 4, 17, tzinfo=timezone.utc)

    row = seed_module.build_link_seed_row(seed, created_at=created_at)

    assert row["link_source"] == seed_module.SEED_LABEL
    assert row["created_at_utc"] == created_at
    assert row["updated_at_utc"] == created_at
    assert row["is_active"] is True
    assert row["official_trim"] == "Kodiaq"
