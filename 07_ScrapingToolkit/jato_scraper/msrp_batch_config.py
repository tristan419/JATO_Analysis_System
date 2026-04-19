"""Load country-aware MSRP batch definitions from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from jato_scraper.msrp_batch_base import (
    CountryMsrpBatchConfig,
    MsrpBatchConfig,
)

MSRP_BATCHES_DIR = Path(__file__).resolve().parent.parent / "msrp_batches"


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path} did not parse into a YAML mapping")
    return data


def _require_text_scalar(value: Any, *, field_name: str) -> str:
    if isinstance(value, bool):
        raise ValueError(
            f"{field_name} must be quoted text in YAML, got boolean {value!r}",
        )
    rendered = str(value).strip()
    if not rendered:
        raise ValueError(f"{field_name} must not be empty")
    return rendered


def _resolve_batch_path(batch_file: str | Path) -> Path:
    candidate = Path(batch_file).expanduser()
    if candidate.exists():
        return candidate.resolve()
    if candidate.is_absolute():
        raise FileNotFoundError(f"MSRP batch file does not exist: {candidate}")

    fallbacks = [MSRP_BATCHES_DIR / candidate]
    if not candidate.suffix:
        fallbacks.append(MSRP_BATCHES_DIR / f"{candidate.name}.yaml")

    for fallback in fallbacks:
        if fallback.exists():
            return fallback.resolve()

    raise FileNotFoundError(f"MSRP batch file does not exist: {batch_file}")


def _resolve_source_refs(
    batch_path: Path,
    country_code: str,
    raw_country: dict[str, Any],
) -> tuple[str, ...]:
    refs_raw = raw_country.get("source_refs")
    if refs_raw is None and raw_country.get("source_path") is not None:
        refs_raw = [raw_country["source_path"]]

    if isinstance(refs_raw, str):
        refs_iterable: list[Any] = [refs_raw]
    elif isinstance(refs_raw, list):
        refs_iterable = refs_raw
    else:
        refs_iterable = []

    source_refs: list[str] = []
    for index, raw_ref in enumerate(refs_iterable):
        ref_text = _require_text_scalar(
            raw_ref,
            field_name=f"source_refs[{index}]",
        )
        ref_path = Path(ref_text).expanduser()
        if not ref_path.is_absolute():
            ref_path = (batch_path.parent / ref_path).resolve()
        else:
            ref_path = ref_path.resolve()
        if not ref_path.exists():
            raise FileNotFoundError(
                f"{batch_path}: source ref for {country_code} does not exist: "
                f"{ref_text}",
            )
        source_refs.append(str(ref_path))

    if not source_refs:
        raise ValueError(
            f"{batch_path}: country {country_code} must define source_path or "
            "source_refs",
        )
    return tuple(source_refs)


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def load_msrp_batch_config(batch_file: str | Path) -> MsrpBatchConfig:
    path = _resolve_batch_path(batch_file)
    data = _load_yaml_mapping(path)
    countries: list[CountryMsrpBatchConfig] = []
    for raw_country in data.get("countries") or []:
        if not isinstance(raw_country, dict):
            continue
        country_code = _require_text_scalar(
            raw_country["country_code"],
            field_name="country_code",
        ).upper()
        country_label = _require_text_scalar(
            raw_country["country_label"],
            field_name="country_label",
        )
        countries.append(
            CountryMsrpBatchConfig(
                country_code=country_code,
                country_label=country_label,
                source_refs=_resolve_source_refs(
                    path,
                    country_code,
                    raw_country,
                ),
                notes=str(raw_country.get("notes") or "").strip(),
            )
        )
    return MsrpBatchConfig(
        batch_code=_require_text_scalar(
            data["batch_code"],
            field_name="batch_code",
        ),
        description=str(data.get("description") or "").strip(),
        countries=tuple(countries),
    )


def load_msrp_batch_configs(
    batches_dir: str | Path = MSRP_BATCHES_DIR,
) -> list[MsrpBatchConfig]:
    base = Path(batches_dir).expanduser().resolve()
    return [
        load_msrp_batch_config(path)
        for path in sorted(base.glob("*.y*ml"))
    ]


def resolve_msrp_batch_source_refs(
    batch_files: list[str | Path],
    country_filter: set[str] | None = None,
) -> list[str]:
    source_refs: list[str] = []
    for batch_file in batch_files:
        batch = load_msrp_batch_config(batch_file)
        for country in batch.countries:
            if country_filter and country.country_code.upper() not in country_filter:
                continue
            source_refs.extend(country.source_refs)
    return _dedupe_preserve_order(source_refs)
