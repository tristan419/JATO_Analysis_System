from __future__ import annotations

import json
import hashlib
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import pytest
from fastapi import HTTPException

from app.services import jato_monthly_update_service


def _configure_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    project_root = tmp_path / "project"
    monkeypatch.setattr(jato_monthly_update_service, "PROJECT_ROOT", project_root)
    return project_root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _make_complete_candidate_bundle(project_root: Path) -> dict[str, Path]:
    staging_root = project_root / "04_Processed_data" / "staging" / "bundle-test"
    candidate_path = staging_root / "jato_full_archive.parquet"
    manifest_path = staging_root / "manifest.json"
    partition_root = staging_root / "partitioned_dataset_v1"
    fingerprint_path = staging_root / "dataset_fingerprint.json"
    refresh_report_path = staging_root / "refresh_job_report.json"
    summaries_path = staging_root / "summaries"

    candidate = pd.DataFrame(
        [
            {"国家": "匈牙利", "Model": "T5 EVO", "2026 Jun": 9},
            {"国家": "捷克", "Model": "Enyaq", "2026 Jun": 12},
        ]
    )
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    candidate.to_parquet(candidate_path, index=False)
    candidate_sha256 = hashlib.sha256(candidate_path.read_bytes()).hexdigest()
    _write_json(
        manifest_path,
        {
            "rows": len(candidate),
            "columns": len(candidate.columns),
            "fileSizeBytes": candidate_path.stat().st_size,
            "sha256": candidate_sha256,
        },
    )

    partition_stats: dict[str, dict[str, object]] = {}
    for country, group in candidate.groupby("国家", sort=False):
        partition_dir_name = f"国家={quote(country, safe='')}"
        payload = group.drop(columns=["国家"])
        partition_dir = partition_root / partition_dir_name
        partition_dir.mkdir(parents=True, exist_ok=True)
        payload.to_parquet(partition_dir / "part-00000.parquet", index=False)
        partition_stats[partition_dir_name] = {
            "rows": len(payload),
            "signature": jato_monthly_update_service._partition_payload_signature(
                payload
            ),
        }

    _write_json(
        partition_root / "manifest.json",
        {
            "rows": len(candidate),
            "columns": len(candidate.columns),
            "partitionColumns": ["国家"],
            "parquetFileCount": len(partition_stats),
            "partitionDirectoryCount": len(partition_stats),
            "partitionStats": partition_stats,
        },
    )
    _write_json(
        fingerprint_path,
        {
            "sha256": candidate_sha256,
            "rowCount": len(candidate),
            "columnCount": len(candidate.columns),
        },
    )
    _write_json(
        refresh_report_path,
        {"jobStatus": "success", "fullManifest": {"rows": len(candidate)}},
    )
    summaries_path.mkdir(parents=True, exist_ok=True)
    (summaries_path / "summary-marker.txt").write_text(
        "candidate summaries",
        encoding="utf-8",
    )
    return {
        "parquet": candidate_path,
        "manifest": manifest_path,
        "partition": partition_root,
        "fingerprint": fingerprint_path,
        "refreshReport": refresh_report_path,
        "summaries": summaries_path,
    }


def _validate_bundle(
    paths: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, object]:
    monkeypatch.setattr(
        jato_monthly_update_service,
        "_validate_candidate_summaries_bundle",
        lambda *, summaries_path, candidate_manifest_path: {
            "totalSummaryRows": 0,
            "summariesPath": str(summaries_path),
            "candidateManifestPath": str(candidate_manifest_path),
        },
    )
    return jato_monthly_update_service._validate_candidate_full_bundle(
        parquet_path=paths["parquet"],
        manifest_path=paths["manifest"],
        partition_path=paths["partition"],
        fingerprint_path=paths["fingerprint"],
        refresh_report_path=paths["refreshReport"],
        summaries_path=paths["summaries"],
    )


def test_complete_candidate_bundle_validates_as_one_dataset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = _configure_project(tmp_path, monkeypatch)
    paths = _make_complete_candidate_bundle(project_root)

    result = _validate_bundle(paths, monkeypatch)

    assert result == {
        "rows": 2,
        "columns": 3,
        "partitionCount": 2,
        "summaryRows": 0,
    }


def test_stale_partition_content_fails_closed_even_when_file_set_is_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = _configure_project(tmp_path, monkeypatch)
    paths = _make_complete_candidate_bundle(project_root)
    stale_partition = (
        paths["partition"]
        / f"国家={quote('匈牙利', safe='')}"
        / "part-00000.parquet"
    )
    pd.DataFrame({"Model": ["T5 EVO"], "2026 Jun": [999]}).to_parquet(
        stale_partition,
        index=False,
    )

    with pytest.raises(HTTPException) as exc_info:
        _validate_bundle(paths, monkeypatch)

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["blockerType"] == "candidate_bundle_invalid"
    assert exc_info.value.detail["message"] == "candidate partition 内容与 parquet 不一致。"


def test_refresh_report_row_mismatch_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = _configure_project(tmp_path, monkeypatch)
    paths = _make_complete_candidate_bundle(project_root)
    _write_json(
        paths["refreshReport"],
        {"jobStatus": "success", "fullManifest": {"rows": 1}},
    )

    with pytest.raises(HTTPException) as exc_info:
        _validate_bundle(paths, monkeypatch)

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["blockerType"] == "candidate_bundle_invalid"
    assert exc_info.value.detail["message"] == "candidate refresh report 与 parquet 不一致。"


def test_candidate_fingerprint_binds_every_full_bundle_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = _configure_project(tmp_path, monkeypatch)
    paths = _make_complete_candidate_bundle(project_root)
    artifacts = {
        "candidateScope": "full_smart_merge",
        "stagingOutputPath": str(paths["parquet"].relative_to(project_root)),
        "manifestPath": str(paths["manifest"].relative_to(project_root)),
        "partitionOutputPath": str(paths["partition"].relative_to(project_root)),
        "fingerprintPath": str(paths["fingerprint"].relative_to(project_root)),
        "refreshReportPath": str(
            paths["refreshReport"].relative_to(project_root)
        ),
        "summariesOutputPath": str(paths["summaries"].relative_to(project_root)),
    }
    original = jato_monthly_update_service._candidate_fingerprint_id(artifacts)

    mutation_paths = [
        paths["parquet"],
        paths["manifest"],
        paths["partition"] / "extra-tree-marker.txt",
        paths["fingerprint"],
        paths["refreshReport"],
        paths["summaries"] / "extra-summary-marker.txt",
    ]
    for mutation_path in mutation_paths:
        if mutation_path.suffix == ".parquet":
            frame = pd.read_parquet(mutation_path)
            frame.loc[0, "2026 Jun"] = int(frame.loc[0, "2026 Jun"]) + 1
            frame.to_parquet(mutation_path, index=False)
        else:
            previous = (
                mutation_path.read_text(encoding="utf-8")
                if mutation_path.exists()
                else ""
            )
            mutation_path.write_text(
                previous + "\nchanged",
                encoding="utf-8",
            )
        assert jato_monthly_update_service._candidate_fingerprint_id(artifacts) != original
        original = jato_monthly_update_service._candidate_fingerprint_id(artifacts)
