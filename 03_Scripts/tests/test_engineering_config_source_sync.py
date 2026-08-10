from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
import sys
from uuid import uuid4


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "engineering_config_source_sync.py"
)


def load_module():
    module_name = "engineering_config_source_sync_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


sync_module = load_module()


class FakeSession:
    def __init__(self) -> None:
        self.flush_count = 0
        self.commit_count = 0
        self.rollback_count = 0

    def flush(self) -> None:
        self.flush_count += 1

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        pass


def _digest() -> dict:
    return {
        "status": "ready",
        "summary": {
            "candidateTrimCount": 4,
            "comparableGroupCount": 2,
            "featureCount": 3,
        },
        "compareGroups": [
            {
                "groupId": "group-a",
                "modelName": "Model A",
                "trims": [
                    {"fullTrimName": "Model A / Base"},
                    {"fullTrimName": "Model A / Premium"},
                ],
                "rows": [{"featureCode": "feature-a"}],
            },
            {
                "groupId": "group-b",
                "modelName": "Model B",
                "trims": [
                    {"trimName": "Comfort", "modelName": "Model B"},
                    {"trimName": "Luxury", "modelName": "Model B"},
                ],
                "rows": [{"featureCode": "feature-b"}, {"featureCode": "feature-c"}],
            },
        ],
    }


def test_run_sync_dry_run_selects_requested_group(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "source.xlsx"
    source_path.write_bytes(b"fake workbook")
    monkeypatch.setattr(
        sync_module.config_api,
        "build_source_digest",
        lambda _path, _name: _digest(),
    )

    report = sync_module.run_sync(
        session=FakeSession(),
        source_file=source_path,
        group_id="group-b",
        dry_run=True,
    )

    assert report["schemaVersion"] == sync_module.SCHEMA_VERSION
    assert report["status"] == "dry_run"
    assert report["selectedGroups"] == [{
        "index": 1,
        "groupId": "group-b",
        "modelName": "Model B",
        "trimCount": 2,
        "featureCount": 2,
        "importable": True,
    }]
    assert report["imports"] == []


def test_run_sync_skips_duplicate_source_when_trims_already_exist(
    tmp_path,
    monkeypatch,
) -> None:
    source_path = tmp_path / "source.xlsx"
    source_path.write_bytes(b"fake workbook")
    source_batch = SimpleNamespace(
        import_batch_id=uuid4(),
        domain=sync_module.config_api.SOURCE_IMPORT_DOMAIN,
        source_file_name=source_path.name,
        source_file_path=str(source_path),
        source_file_hash="hash-1",
    )
    monkeypatch.setattr(
        sync_module.config_api,
        "build_source_digest",
        lambda _path, _name: _digest(),
    )
    monkeypatch.setattr(
        sync_module.config_api,
        "_validate_source_file_content",
        lambda _path, _name: None,
    )
    monkeypatch.setattr(
        sync_module.config_api,
        "_sha256_for_path",
        lambda _path: "hash-1",
    )
    monkeypatch.setattr(
        sync_module.config_api.repo,
        "get_import_batch_by_hash",
        lambda *_args: source_batch,
    )
    monkeypatch.setattr(
        sync_module.config_api.repo,
        "get_vehicle_trim_by_full_name",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        sync_module.config_api,
        "create_draft_from_source_digest_group",
        lambda *_args: (_ for _ in ()).throw(AssertionError("unexpected draft")),
    )

    report = sync_module.run_sync(
        session=FakeSession(),
        source_file=source_path,
        group_index=0,
    )

    assert report["status"] == "passed"
    assert report["source"]["uploadStatus"] == "duplicate"
    assert report["imports"][0]["status"] == "skipped_existing_trims"


def test_run_sync_force_draft_imports_duplicate_source(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "source.xlsx"
    source_path.write_bytes(b"fake workbook")
    source_batch = SimpleNamespace(
        import_batch_id=uuid4(),
        domain=sync_module.config_api.SOURCE_IMPORT_DOMAIN,
        source_file_name=source_path.name,
        source_file_path=str(source_path),
        source_file_hash="hash-1",
    )
    draft_calls: list[str] = []
    monkeypatch.setattr(
        sync_module.config_api,
        "build_source_digest",
        lambda _path, _name: _digest(),
    )
    monkeypatch.setattr(
        sync_module.config_api,
        "_validate_source_file_content",
        lambda _path, _name: None,
    )
    monkeypatch.setattr(
        sync_module.config_api,
        "_sha256_for_path",
        lambda _path: "hash-1",
    )
    monkeypatch.setattr(
        sync_module.config_api.repo,
        "get_import_batch_by_hash",
        lambda *_args: source_batch,
    )
    monkeypatch.setattr(
        sync_module.config_api,
        "create_draft_from_source_digest_group",
        lambda _source_id, group_id, *_args: (
            draft_calls.append(group_id)
            or {"trimCount": 2, "valueRecordCount": 2}
        ),
    )

    report = sync_module.run_sync(
        session=FakeSession(),
        source_file=source_path,
        group_id="group-a",
        force_draft=True,
    )

    assert draft_calls == ["group-a"]
    assert report["imports"][0]["status"] == "draft_created"
    assert report["imports"][0]["result"]["valueRecordCount"] == 2


def test_main_writes_pipeline_status_for_success(tmp_path, monkeypatch, capsys) -> None:
    source_path = tmp_path / "source.xlsx"
    source_path.write_bytes(b"fake workbook")
    session = FakeSession()
    status_records: list[dict] = []

    monkeypatch.setattr(
        sync_module,
        "get_session_factory",
        lambda: lambda: session,
    )
    monkeypatch.setattr(
        sync_module,
        "run_sync",
        lambda **_kwargs: {
            "schemaVersion": sync_module.SCHEMA_VERSION,
            "status": "passed",
            "source": {
                "sourceFileName": source_path.name,
                "sourceFilePath": str(source_path),
                "sourceFileHash": "hash-1",
                "uploadStatus": "registered",
            },
            "selectedGroups": [{"groupId": "group-a"}],
            "imports": [
                {
                    "groupId": "group-a",
                    "status": "draft_created",
                    "result": {"trimCount": 2, "valueRecordCount": 5},
                }
            ],
        },
    )
    monkeypatch.setattr(
        sync_module,
        "write_pipeline_status",
        lambda **kwargs: status_records.append(kwargs) or kwargs,
    )

    rc = sync_module.main(["--source-file", str(source_path), "--group-id", "group-a"])

    assert rc == 0
    assert session.rollback_count == 0
    stdout = capsys.readouterr().out
    assert '"status": "passed"' in stdout
    assert len(status_records) == 1
    record = status_records[0]
    assert record["pipeline_id"] == sync_module.PIPELINE_ID
    assert record["status"] == "success"
    assert record["records_processed"] == 5
    assert record["warning_count"] == 0
    assert record["artifact_refs"] == [str(source_path)]
    assert record["extra"]["draftCreatedCount"] == 1
    assert record["extra"]["valueRecordCount"] == 5
    assert record["extra"]["sourceUploadStatus"] == "registered"


def test_main_writes_pipeline_status_for_failure(tmp_path, monkeypatch, capsys) -> None:
    source_path = tmp_path / "missing.xlsx"
    session = FakeSession()
    status_records: list[dict] = []

    monkeypatch.setattr(
        sync_module,
        "get_session_factory",
        lambda: lambda: session,
    )
    monkeypatch.setattr(
        sync_module,
        "run_sync",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("digest failed")),
    )
    monkeypatch.setattr(
        sync_module,
        "write_pipeline_status",
        lambda **kwargs: status_records.append(kwargs) or kwargs,
    )

    rc = sync_module.main(["--source-file", str(source_path)])

    assert rc == 1
    assert session.rollback_count == 1
    stderr = capsys.readouterr().err
    assert '"status": "failed"' in stderr
    record = status_records[0]
    assert record["pipeline_id"] == sync_module.PIPELINE_ID
    assert record["status"] == "failed"
    assert record["exit_code"] == 1
    assert record["failed_count"] == 1
    assert record["extra"]["error"] == "digest failed"
