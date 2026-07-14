from __future__ import annotations

import hashlib
import importlib.util
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.core.config import resolve_msrp_governance_evidence_root
from app.services import msrp_evidence_integrity_service as integrity_service
from app.services.msrp_source_governance.service import MsrpSourceGovernanceService


@dataclass(frozen=True)
class _EvidenceRow:
    evidence_asset_id: object
    evidence_type: str
    storage_key: str | None
    size_bytes: int | None
    sha256: str


def _row(
    content: bytes,
    *,
    evidence_type: str = "uploaded_pdf",
    storage_key: str | None = None,
    size_bytes: int | None = None,
    sha256: str | None = None,
) -> _EvidenceRow:
    digest = hashlib.sha256(content).hexdigest()
    return _EvidenceRow(
        evidence_asset_id=uuid4(),
        evidence_type=evidence_type,
        storage_key=storage_key or f"assets/{digest[:2]}/{digest}.pdf",
        size_bytes=len(content) if size_bytes is None else size_bytes,
        sha256=sha256 or digest,
    )


def _write_object(root: Path, row: _EvidenceRow, content: bytes) -> Path:
    assert row.storage_key is not None
    path = root / row.storage_key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def test_evidence_root_uses_durable_project_path_and_preserves_override(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    monkeypatch.delenv("MSRP_GOVERNANCE_EVIDENCE_ROOT", raising=False)
    assert resolve_msrp_governance_evidence_root(project_root=project_root) == (
        project_root / "04_Processed_data" / "ops" / "msrp_source_evidence"
    ).resolve()

    override = tmp_path / "external-evidence"
    monkeypatch.setenv("MSRP_GOVERNANCE_EVIDENCE_ROOT", str(override))
    service = MsrpSourceGovernanceService(object())
    assert service.evidence_root == override.resolve()

    monkeypatch.setenv("MSRP_GOVERNANCE_EVIDENCE_ROOT", "durable/msrp")
    assert resolve_msrp_governance_evidence_root(project_root=project_root) == (
        project_root / "durable" / "msrp"
    ).resolve()


def test_integrity_audit_accepts_healthy_objects_and_ignores_url_and_screenshot(
    tmp_path: Path,
) -> None:
    content = b"official immutable pdf"
    replayable = _row(content)
    _write_object(tmp_path, replayable, content)
    url_only = _row(
        b"https://example.test/official",
        evidence_type="official_url",
        storage_key=None,
    )
    screenshot_content = b"visual corroboration only"
    screenshot = _row(screenshot_content, evidence_type="screenshot")
    _write_object(tmp_path, screenshot, screenshot_content)

    report = integrity_service.audit_msrp_evidence_integrity(
        [replayable, url_only, screenshot],
        tmp_path,
        checked_at=datetime(2026, 7, 14, tzinfo=timezone.utc),
    )

    assert report["status"] == "healthy"
    assert report["summary"] == {
        "databaseAssetRowCount": 3,
        "replayableAssetRowCount": 1,
        "ignoredNonReplayableRowCount": 2,
        "expectedObjectCount": 1,
        "healthyObjectCount": 1,
        "verifiedObjectBytes": len(content),
        "missingObjectCount": 0,
        "mismatchedObjectCount": 0,
        "unreadableObjectCount": 0,
        "notRegularFileCount": 0,
        "invalidPathCount": 0,
        "invalidMetadataCount": 0,
        "invalidContentAddressCount": 0,
        "orphanObjectCount": 0,
    }
    assert [item["storageKey"] for item in report["objects"]] == [
        replayable.storage_key
    ]
    assert {item["reason"] for item in report["ignoredAssets"]} == {
        "non_replayable_reference"
    }


def test_integrity_audit_reports_missing_mismatch_and_orphan(tmp_path: Path) -> None:
    missing = _row(b"missing")
    expected = b"right"
    mismatch = _row(expected)
    _write_object(tmp_path, mismatch, b"wrong")
    size_content = b"size"
    size_mismatch = _row(size_content, size_bytes=len(size_content) + 1)
    _write_object(tmp_path, size_mismatch, size_content)
    orphan_content = b"orphan"
    orphan_hash = hashlib.sha256(orphan_content).hexdigest()
    orphan_path = tmp_path / "assets" / orphan_hash[:2] / f"{orphan_hash}.bin"
    orphan_path.parent.mkdir(parents=True)
    orphan_path.write_bytes(orphan_content)

    report = integrity_service.audit_msrp_evidence_integrity(
        [missing, mismatch, size_mismatch],
        tmp_path,
    )

    assert report["status"] == "unhealthy"
    assert report["summary"]["missingObjectCount"] == 1
    assert report["summary"]["mismatchedObjectCount"] == 2
    assert report["summary"]["orphanObjectCount"] == 1
    assert report["orphans"][0]["sha256"] == orphan_hash


def test_integrity_audit_rejects_traversal_symlink_non_regular_and_unreadable(
    monkeypatch,
    tmp_path: Path,
) -> None:
    traversal = _row(b"escape", storage_key="assets/../../outside.pdf")
    bad_address = _row(b"bad address", storage_key="assets/00/not-a-hash.pdf")

    symlink_content = b"symlink escape"
    symlink_row = _row(symlink_content)
    assert symlink_row.storage_key is not None
    symlink_prefix = tmp_path / "assets" / symlink_row.storage_key.split("/")[1]
    outside = tmp_path.parent / f"outside-{uuid4()}"
    outside.mkdir()
    symlink_prefix.parent.mkdir(parents=True)
    symlink_prefix.symlink_to(outside, target_is_directory=True)

    directory_row = _row(b"directory")
    assert directory_row.storage_key is not None
    (tmp_path / directory_row.storage_key).mkdir(parents=True)

    unreadable_content = b"unreadable"
    unreadable_row = _row(unreadable_content)
    unreadable_path = _write_object(tmp_path, unreadable_row, unreadable_content)
    original_access = integrity_service.os.access
    monkeypatch.setattr(
        integrity_service.os,
        "access",
        lambda path, mode: False if Path(path) == unreadable_path else original_access(path, mode),
    )

    report = integrity_service.audit_msrp_evidence_integrity(
        [traversal, bad_address, symlink_row, directory_row, unreadable_row],
        tmp_path,
    )

    assert report["status"] == "unhealthy"
    assert report["summary"]["invalidPathCount"] == 2
    assert report["summary"]["notRegularFileCount"] == 2
    assert report["summary"]["unreadableObjectCount"] == 1
    assert report["summary"]["invalidContentAddressCount"] == 1


def test_integrity_command_emits_json_object_list_and_nonzero_when_unhealthy(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    project_root = Path(__file__).resolve().parents[4]
    script_path = project_root / "03_Scripts" / "ops" / "msrp_evidence_integrity.py"
    spec = importlib.util.spec_from_file_location("msrp_evidence_integrity_cli", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    content = b"cli object"
    healthy_row = _row(content)
    _write_object(tmp_path, healthy_row, content)
    monkeypatch.setattr(module, "_load_evidence_assets", lambda: [healthy_row])
    report_path = tmp_path / "report.json"
    object_list_path = tmp_path / "objects.txt"

    assert module.main(
        [
            "--evidence-root",
            str(tmp_path),
            "--output",
            str(report_path),
            "--object-list-output",
            str(object_list_path),
        ]
    ) == 0
    assert json.loads(report_path.read_text())["status"] == "healthy"
    assert object_list_path.read_text() == f"{healthy_row.storage_key}\n"
    assert json.loads(capsys.readouterr().out)["status"] == "healthy"

    (tmp_path / str(healthy_row.storage_key)).unlink()
    assert module.main(["--evidence-root", str(tmp_path)]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "unhealthy"
