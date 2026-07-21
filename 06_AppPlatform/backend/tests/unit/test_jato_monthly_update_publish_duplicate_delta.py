from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from app.services import jato_monthly_update_service


ACTIVE_FINGERPRINT = "a" * 64
CANDIDATE_FINGERPRINT = "b" * 64


def _row(
    *,
    country: str,
    model: str,
    registration_type: str = "Business",
    jan: int = 0,
    jun: int = 0,
) -> dict[str, object]:
    return {
        "Country": country,
        "Make": "BRAND",
        "Model": model,
        "Version name": "VERSION",
        "Registration type": registration_type,
        "2026 Jan": jan,
        "2026 Jun": jun,
    }


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> Path:
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def _country_frame(path: Path, country: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    return frame.loc[
        frame["Country"].astype("string").str.strip().str.casefold()
        == country.casefold()
    ].reset_index(drop=True)


def _partial_contract(
    *,
    tmp_path: Path,
    active_path: Path,
    country_scope: list[str] | None = None,
    job_type: str = "partial_country",
) -> tuple[dict[str, Any], Path]:
    countries = ["Germany"] if country_scope is None else country_scope
    route = (
        "single_country" if job_type == "single_country" else "partial_country"
    )
    source_scope = (
        "target_country_partition_only"
        if job_type == "single_country"
        else "target_country_partitions_only"
    )
    active = pd.read_parquet(active_path)
    target_keys = {country.casefold() for country in countries}
    untouched_checks: dict[str, dict[str, object]] = {}
    for country in active["Country"].drop_duplicates().tolist():
        if str(country).strip().casefold() in target_keys:
            continue
        frame = _country_frame(active_path, str(country))
        columns = [str(column) for column in frame.columns]
        untouched_checks[str(country)] = {
            "status": "pass",
            "rowCount": len(frame),
            "canonicalSignature": (
                jato_monthly_update_service
                ._canonical_country_content_signature(frame, columns)
            ),
            "candidateOnlyColumnsNull": True,
        }

    refresh_path = tmp_path / "refresh-report.json"
    refresh_path.write_text(
        json.dumps(
            {
                "incremental": {
                    "scope": "full_smart_merge",
                    "sourceCandidateScope": source_scope,
                }
            }
        ),
        encoding="utf-8",
    )
    payload: dict[str, Any] = {
        "jobType": job_type,
        "countryScope": countries,
        "activeBaseFingerprint": ACTIVE_FINGERPRINT,
        "ingestDigest": {
            "route": route,
            "candidateScope": source_scope,
            "countries": countries,
            "activeDatasetVersion": ACTIVE_FINGERPRINT,
        },
        "uploadInspection": {"countries": countries},
        "reviewApproval": {
            "decision": "approved",
            "activeBaseFingerprint": ACTIVE_FINGERPRINT,
            "candidateFingerprint": CANDIDATE_FINGERPRINT,
        },
        "artifacts": {
            "candidateScope": "full_smart_merge",
            "untouchedPartitionCheck": {"status": "pass"},
        },
        "summaries": {
            "smartMerge": {
                "deprecatedStaticCarryForward": {
                    "untouchedCountryChecks": untouched_checks,
                }
            }
        },
    }
    return payload, refresh_path


def _assess(
    *,
    tmp_path: Path,
    active_path: Path,
    candidate_path: Path,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if payload is None:
        payload, refresh_path = _partial_contract(
            tmp_path=tmp_path,
            active_path=active_path,
        )
    else:
        refresh_path = tmp_path / "refresh-report.json"
        if not refresh_path.exists():
            refresh_path.write_text(
                json.dumps(
                    {
                        "incremental": {
                            "scope": "full_smart_merge",
                            "sourceCandidateScope": (
                                "target_country_partitions_only"
                            ),
                        }
                    }
                ),
                encoding="utf-8",
            )
    return (
        jato_monthly_update_service
        ._publish_duplicate_configuration_assessment(
            payload=payload,
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
            refresh_report_path=refresh_path,
        )
    )


def test_unchanged_untouched_active_duplicates_are_inherited(
    tmp_path: Path,
) -> None:
    active_path = _write_parquet(
        tmp_path / "active.parquet",
        [
            _row(country="Austria", model="LEGACY", jan=1),
            _row(country="Austria", model="LEGACY", jan=2),
            _row(country="Germany", model="TARGET", jan=3),
        ],
    )
    candidate_path = _write_parquet(
        tmp_path / "candidate.parquet",
        [
            _row(country="Germany", model="TARGET", jan=3, jun=4),
            _row(country="Austria", model="LEGACY", jan=2),
            _row(country="Austria", model="LEGACY", jan=1),
        ],
    )

    result = _assess(
        tmp_path=tmp_path,
        active_path=active_path,
        candidate_path=candidate_path,
    )

    assert result["blocking"] == []
    assert result["guard"]["status"] == "pass"
    assert result["guard"]["targetCountries"] == ["Germany"]
    assert result["guard"]["untouchedCountries"] == ["Austria"]
    assert len(result["inherited"]) == 1
    inherited = result["inherited"][0]
    assert inherited["country"] == "Austria"
    assert inherited["duplicateRows"] == 2
    assert inherited["duplicateStatus"] == "unchanged_active_duplicate"
    assert len(inherited["contentFingerprint"]) == 64


def test_candidate_only_all_null_column_does_not_change_untouched_content(
    tmp_path: Path,
) -> None:
    rows = [
        _row(country="Austria", model="LEGACY", jan=1),
        _row(country="Austria", model="LEGACY", jan=2),
        _row(country="Germany", model="TARGET", jan=3),
    ]
    active_path = _write_parquet(tmp_path / "active.parquet", rows)
    candidate = pd.DataFrame(list(reversed(rows)))
    candidate["New optional field"] = pd.NA
    candidate_path = tmp_path / "candidate.parquet"
    candidate.to_parquet(candidate_path, index=False)

    result = _assess(
        tmp_path=tmp_path,
        active_path=active_path,
        candidate_path=candidate_path,
    )

    assert result["blocking"] == []
    assert result["inherited"][0]["country"] == "Austria"


def test_untouched_raw_label_case_or_whitespace_change_is_blocked(
    tmp_path: Path,
) -> None:
    active_path = _write_parquet(
        tmp_path / "active.parquet",
        [
            _row(country="Austria", model="LEGACY", jan=1),
            _row(country="Austria", model="LEGACY", jan=2),
            _row(country="Germany", model="TARGET", jan=3),
        ],
    )
    candidate_rows = [
        _row(country="Austria", model=" legacy ", jan=1),
        _row(country="Austria", model=" legacy ", jan=2),
        _row(country="Germany", model="TARGET", jan=3, jun=4),
    ]
    candidate_rows[0]["Make"] = "brand"
    candidate_rows[1]["Make"] = "brand"
    candidate_path = _write_parquet(
        tmp_path / "candidate.parquet",
        candidate_rows,
    )

    result = _assess(
        tmp_path=tmp_path,
        active_path=active_path,
        candidate_path=candidate_path,
    )

    assert result["inherited"] == []
    blocker = result["blocking"][0]
    assert blocker["duplicateStatus"] == "untouched_country_content_changed"
    assert (
        blocker["contentEvidence"]["activeContentFingerprint"]
        != blocker["contentEvidence"]["candidateContentFingerprint"]
    )


def test_target_country_duplicate_stays_blocked_even_if_active_had_it(
    tmp_path: Path,
) -> None:
    rows = [
        _row(country="Germany", model="TARGET", jan=1),
        _row(country="Germany", model="TARGET", jan=2),
        _row(country="Austria", model="CLEAN", jan=3),
    ]
    active_path = _write_parquet(tmp_path / "active.parquet", rows)
    candidate_path = _write_parquet(tmp_path / "candidate.parquet", rows)

    result = _assess(
        tmp_path=tmp_path,
        active_path=active_path,
        candidate_path=candidate_path,
    )

    assert result["inherited"] == []
    assert result["blocking"][0]["duplicateStatus"] == (
        "target_country_duplicate"
    )


def test_target_normalized_duplicate_cannot_hide_with_case_or_whitespace(
    tmp_path: Path,
) -> None:
    active_path = _write_parquet(
        tmp_path / "active.parquet",
        [
            _row(country="Austria", model="CLEAN", jan=1),
            _row(country="Germany", model="TARGET", jan=2),
        ],
    )
    candidate_rows = [
        _row(country="Austria", model="CLEAN", jan=1),
        _row(country="Germany", model="TARGET", jun=3),
        _row(country="Germany", model=" target ", jun=4),
    ]
    candidate_rows[-1]["Make"] = "brand"
    candidate_path = _write_parquet(
        tmp_path / "candidate.parquet",
        candidate_rows,
    )

    result = _assess(
        tmp_path=tmp_path,
        active_path=active_path,
        candidate_path=candidate_path,
    )

    assert result["inherited"] == []
    blocker = result["blocking"][0]
    assert blocker["country"] == "Germany"
    assert blocker["duplicateRows"] == 2
    assert blocker["duplicateStatus"] == "target_country_duplicate"


@pytest.mark.parametrize(
    "candidate_austria_rows",
    [
        [
            _row(country="Austria", model="LEGACY", jan=1),
            _row(country="Austria", model="LEGACY", jan=2),
            _row(country="Austria", model="LEGACY", jan=3),
        ],
        [
            _row(country="Austria", model="CHANGED", jan=1),
            _row(country="Austria", model="CHANGED", jan=2),
        ],
        [_row(country="Austria", model="LEGACY", jan=3)],
    ],
)
def test_untouched_duplicate_increase_change_or_disappearance_is_blocked(
    tmp_path: Path,
    candidate_austria_rows: list[dict[str, object]],
) -> None:
    active_path = _write_parquet(
        tmp_path / "active.parquet",
        [
            _row(country="Austria", model="LEGACY", jan=1),
            _row(country="Austria", model="LEGACY", jan=2),
            _row(country="Germany", model="TARGET", jan=3),
        ],
    )
    candidate_path = _write_parquet(
        tmp_path / "candidate.parquet",
        [
            *candidate_austria_rows,
            _row(country="Germany", model="TARGET", jan=3, jun=4),
        ],
    )

    result = _assess(
        tmp_path=tmp_path,
        active_path=active_path,
        candidate_path=candidate_path,
    )

    assert result["inherited"] == []
    assert any(
        entry["duplicateStatus"] == "untouched_country_content_changed"
        for entry in result["blocking"]
    )


def test_new_untouched_duplicate_without_active_proof_is_blocked(
    tmp_path: Path,
) -> None:
    active_path = _write_parquet(
        tmp_path / "active.parquet",
        [
            _row(country="Austria", model="LEGACY", jan=1),
            _row(country="Germany", model="TARGET", jan=3),
        ],
    )
    candidate_path = _write_parquet(
        tmp_path / "candidate.parquet",
        [
            _row(country="Austria", model="LEGACY", jan=1),
            _row(country="Austria", model="LEGACY", jan=2),
            _row(country="Germany", model="TARGET", jan=3, jun=4),
        ],
    )

    result = _assess(
        tmp_path=tmp_path,
        active_path=active_path,
        candidate_path=candidate_path,
    )

    blocker = result["blocking"][0]
    assert blocker["duplicateStatus"] == "untouched_country_content_changed"
    assert blocker["activeDuplicateRows"] == 0
    assert blocker["candidateDuplicateRows"] == 2


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        ("digest_country", "target_country_lineage_mismatch"),
        ("digest_route", "ingest_route_mismatch"),
        ("job_type", "job_type_not_partial"),
        ("candidate_scope", "candidate_scope_not_full_smart_merge"),
        ("partition_proof", "untouched_partition_check_not_pass"),
        ("stored_proof_missing", "smart_merge_untouched_proof_coverage_mismatch"),
        ("refresh_scope", "refresh_source_scope_mismatch"),
    ],
)
def test_lineage_or_smart_merge_proof_mismatch_fails_closed(
    tmp_path: Path,
    mutation: str,
    expected_error: str,
) -> None:
    rows = [
        _row(country="Austria", model="LEGACY", jan=1),
        _row(country="Austria", model="LEGACY", jan=2),
        _row(country="Germany", model="TARGET", jan=3),
    ]
    active_path = _write_parquet(tmp_path / "active.parquet", rows)
    candidate_path = _write_parquet(tmp_path / "candidate.parquet", rows)
    payload, refresh_path = _partial_contract(
        tmp_path=tmp_path,
        active_path=active_path,
    )
    if mutation == "digest_country":
        payload["ingestDigest"]["countries"] = ["Austria"]
    elif mutation == "digest_route":
        payload["ingestDigest"]["route"] = "single_country"
    elif mutation == "job_type":
        payload["jobType"] = "batch"
    elif mutation == "candidate_scope":
        payload["artifacts"]["candidateScope"] = "full_archive"
    elif mutation == "partition_proof":
        payload["artifacts"]["untouchedPartitionCheck"]["status"] = "fail"
    elif mutation == "stored_proof_missing":
        payload["summaries"]["smartMerge"][
            "deprecatedStaticCarryForward"
        ]["untouchedCountryChecks"] = {}
    elif mutation == "refresh_scope":
        refresh_path.write_text(
            json.dumps(
                {
                    "incremental": {
                        "scope": "full_smart_merge",
                        "sourceCandidateScope": "target_country_partition_only",
                    }
                }
            ),
            encoding="utf-8",
        )

    result = (
        jato_monthly_update_service
        ._publish_duplicate_configuration_assessment(
            payload=payload,
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
            refresh_report_path=refresh_path,
        )
    )

    assert result["inherited"] == []
    assert result["blocking"][0]["duplicateStatus"] == (
        "duplicate_guard_scope_invalid"
    )
    assert expected_error in result["guard"]["errors"]


def test_stored_smart_merge_proof_value_mismatch_blocks_untouched_country(
    tmp_path: Path,
) -> None:
    rows = [
        _row(country="Austria", model="LEGACY", jan=1),
        _row(country="Austria", model="LEGACY", jan=2),
        _row(country="Germany", model="TARGET", jan=3),
    ]
    active_path = _write_parquet(tmp_path / "active.parquet", rows)
    candidate_path = _write_parquet(tmp_path / "candidate.parquet", rows)
    payload, refresh_path = _partial_contract(
        tmp_path=tmp_path,
        active_path=active_path,
    )
    check = payload["summaries"]["smartMerge"][
        "deprecatedStaticCarryForward"
    ]["untouchedCountryChecks"]["Austria"]
    check["rowCount"] = 999

    result = (
        jato_monthly_update_service
        ._publish_duplicate_configuration_assessment(
            payload=payload,
            active_parquet_path=active_path,
            candidate_parquet_path=candidate_path,
            refresh_report_path=refresh_path,
        )
    )

    assert result["inherited"] == []
    blocker = result["blocking"][0]
    assert blocker["duplicateStatus"] == "untouched_country_content_changed"
    assert blocker["contentEvidence"]["storedSmartMergeProofPass"] is False


@pytest.mark.parametrize("candidate_only_value", ["unexpected", "", "   "])
def test_candidate_only_non_null_column_blocks_untouched_country(
    tmp_path: Path,
    candidate_only_value: str,
) -> None:
    rows = [
        _row(country="Austria", model="LEGACY", jan=1),
        _row(country="Austria", model="LEGACY", jan=2),
        _row(country="Germany", model="TARGET", jan=3),
    ]
    active_path = _write_parquet(tmp_path / "active.parquet", rows)
    candidate = pd.DataFrame(rows)
    candidate["New optional field"] = [
        candidate_only_value,
        candidate_only_value,
        None,
    ]
    candidate_path = tmp_path / "candidate.parquet"
    candidate.to_parquet(candidate_path, index=False)

    result = _assess(
        tmp_path=tmp_path,
        active_path=active_path,
        candidate_path=candidate_path,
    )

    blocker = result["blocking"][0]
    assert blocker["duplicateStatus"] == "untouched_country_content_changed"
    assert blocker["contentEvidence"]["nonNullCandidateOnlyColumns"] == [
        "New optional field"
    ]


@pytest.mark.parametrize(
    "candidate_scope",
    ["full_archive", "full_smart_merge"],
)
def test_full_batch_duplicates_keep_the_absolute_zero_duplicate_gate(
    tmp_path: Path,
    candidate_scope: str,
) -> None:
    rows = [
        _row(country="Austria", model="LEGACY", jan=1),
        _row(country="Austria", model="LEGACY", jan=2),
    ]
    active_path = _write_parquet(tmp_path / "active.parquet", rows)
    candidate_path = _write_parquet(tmp_path / "candidate.parquet", rows)
    payload = {
        "jobType": "batch",
        "countryScope": ["Austria"],
        "artifacts": {"candidateScope": candidate_scope},
    }

    result = _assess(
        tmp_path=tmp_path,
        active_path=active_path,
        candidate_path=candidate_path,
        payload=payload,
    )

    assert result["inherited"] == []
    assert result["blocking"][0]["duplicateStatus"] == "candidate_duplicate"
    assert result["guard"]["policy"] == "full_candidate_zero_duplicates"


def test_full_batch_smart_merge_without_duplicates_is_not_blocked(
    tmp_path: Path,
) -> None:
    rows = [
        _row(country="Austria", model="LEGACY", jan=1),
        _row(country="Germany", model="TARGET", jan=2),
    ]
    active_path = _write_parquet(tmp_path / "active.parquet", rows)
    candidate_path = _write_parquet(tmp_path / "candidate.parquet", rows)
    payload = {
        "jobType": "batch",
        "countryScope": ["Austria", "Germany"],
        "ingestDigest": {"route": "full_batch"},
        "artifacts": {"candidateScope": "full_smart_merge"},
    }

    result = _assess(
        tmp_path=tmp_path,
        active_path=active_path,
        candidate_path=candidate_path,
        payload=payload,
    )

    assert result["blocking"] == []
    assert result["inherited"] == []
    assert result["guard"] == {
        "status": "not_applicable",
        "policy": "full_candidate_zero_duplicates",
    }
