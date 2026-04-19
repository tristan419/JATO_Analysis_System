from pathlib import Path

from jato_scraper.msrp_batch_config import (
    load_msrp_batch_config,
    resolve_msrp_batch_source_refs,
)


def test_load_msrp_batch_config_reads_batch_a_country_packs() -> None:
    batch = load_msrp_batch_config(
        Path(__file__).resolve().parents[1] / "msrp_batches" / "batch_a.yaml"
    )

    assert batch.batch_code == "country_msrp_batch_a"
    assert [country.country_code for country in batch.countries] == [
        "SE",
        "FI",
        "NO",
        "DK",
        "HU",
        "HR",
        "AT",
        "CZ",
    ]
    assert batch.countries[0].country_label == "Sweden / 瑞典"
    assert len(batch.countries[0].source_refs) == 1
    assert Path(batch.countries[0].source_refs[0]).is_dir()
    assert Path(batch.countries[0].source_refs[0]).name == "se"


def test_resolve_msrp_batch_source_refs_filters_countries() -> None:
    refs = resolve_msrp_batch_source_refs(
        [
            Path(__file__).resolve().parents[1]
            / "msrp_batches"
            / "batch_a.yaml"
        ],
        country_filter={"FI", "NO"},
    )

    assert [Path(ref).name for ref in refs] == ["fi", "no"]


def test_load_msrp_batch_config_rejects_boolean_like_country_code(
    tmp_path: Path,
) -> None:
    country_dir = tmp_path / "fi"
    country_dir.mkdir()

    batch_path = tmp_path / "batch.yaml"
    batch_path.write_text(
        """
batch_code: demo_batch
description: Demo batch
countries:
  - country_code: no
    country_label: Norway
    source_path: ./fi
""".strip(),
        encoding="utf-8",
    )

    try:
        load_msrp_batch_config(batch_path)
    except ValueError as exc:
        assert "country_code must be quoted text" in str(exc)
    else:
        raise AssertionError("Expected boolean-like country_code to be rejected")
