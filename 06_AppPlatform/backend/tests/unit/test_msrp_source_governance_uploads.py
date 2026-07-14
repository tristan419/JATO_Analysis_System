from datetime import datetime, timezone
import hashlib
from pathlib import Path
import subprocess
from uuid import uuid4

from app.api.msrp_source_governance_schemas import (
    EvidenceUploadComplete,
    EvidenceUploadInitiate,
)
from app.db.msrp_source_governance_models import (
    MsrpEvidenceUploadSession,
    MsrpMonitoringTarget,
    MsrpSourceEvidenceAsset,
)
from app.services.msrp_source_governance import service as governance_service


class _FakeSession:
    def __init__(self) -> None:
        self.added: list[object] = []

    def flush(self) -> None:
        now = datetime(2026, 7, 14, 4, 0, tzinfo=timezone.utc)
        for item in self.added:
            if hasattr(item, "created_at_utc") and getattr(
                item, "created_at_utc", None
            ) is None:
                item.created_at_utc = now
            if hasattr(item, "updated_at_utc") and getattr(
                item, "updated_at_utc", None
            ) is None:
                item.updated_at_utc = now

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def refresh(self, _item: object) -> None:
        pass

    def get(self, _model, _identity):
        return None


def _target() -> MsrpMonitoringTarget:
    now = datetime(2026, 7, 14, 3, 0, tzinfo=timezone.utc)
    return MsrpMonitoringTarget(
        target_id=uuid4(),
        target_key="SE::volvo::xc60::::",
        country="SE",
        brand="Volvo",
        model="XC60",
        roster_type="manual",
        monitoring_status="manual_evidence_required",
        row_version=1,
        created_at_utc=now,
        updated_at_utc=now,
    )


def test_resumable_pdf_upload_validates_hash_and_creates_immutable_asset(
    monkeypatch,
    tmp_path: Path,
) -> None:
    content = b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\n"
    content_hash = hashlib.sha256(content).hexdigest()
    target = _target()
    session = _FakeSession()
    state: dict[str, object] = {"upload": None, "evidence": None}

    def add(_session, item):
        session.added.append(item)
        if isinstance(item, MsrpEvidenceUploadSession):
            state["upload"] = item
        if isinstance(item, MsrpSourceEvidenceAsset):
            state["evidence"] = item
        return item

    monkeypatch.setattr(governance_service.repo, "add", add)
    monkeypatch.setattr(
        governance_service.repo,
        "get_audit_event_by_idempotency",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_target",
        lambda *_args, **_kwargs: target,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_repair_case",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_evidence_by_sha256",
        lambda *_args, **_kwargs: state["evidence"],
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_evidence_upload_session",
        lambda *_args, **_kwargs: state["upload"],
    )

    fixture_repo = tmp_path / "release-fixture"
    evidence_root = (
        fixture_repo / "04_Processed_data" / "ops" / "msrp_source_evidence"
    )
    service = governance_service.MsrpSourceGovernanceService(
        session,
        evidence_root=evidence_root,
    )
    initiated = service.initiate_evidence_upload(
        EvidenceUploadInitiate(
            target_id=target.target_id,
            source_url="https://www.volvocars.com/se/pricelists/xc60.pdf",
            official_domain="volvocars.com",
            original_filename="XC60-pricelist.pdf",
            expected_size_bytes=len(content),
            expected_sha256=content_hash,
            chunk_size_bytes=256 * 1024,
            source_type="official_pdf",
            semantic_lane="msrp",
        ),
        actor="editor@example.test",
        actor_role="editor",
        idempotency_key="init-upload-1",
    )
    assert initiated["uploadStatus"] == "initiated"
    upload = state["upload"]
    assert isinstance(upload, MsrpEvidenceUploadSession)

    part = service.upload_evidence_part(
        upload.upload_session_id,
        1,
        content,
        content_hash,
        actor="editor@example.test",
        actor_role="editor",
    )
    assert part["receivedParts"][0]["sha256"] == content_hash

    completed = service.complete_evidence_upload(
        upload.upload_session_id,
        EvidenceUploadComplete(row_version=upload.row_version),
        actor="editor@example.test",
        actor_role="editor",
        idempotency_key="complete-upload-1",
    )

    evidence = state["evidence"]
    assert isinstance(evidence, MsrpSourceEvidenceAsset)
    assert completed["upload"]["uploadStatus"] == "completed"
    assert completed["evidence"]["sha256"] == content_hash
    assert completed["evidence"]["mimeSignature"] == "%PDF-"
    assert target.monitoring_status == "degraded"
    assert evidence.storage_key is not None
    object_path = evidence_root / evidence.storage_key
    assert object_path.read_bytes() == content
    assert not (evidence_root / upload.staging_key).exists()

    before_release_hash = hashlib.sha256(object_path.read_bytes()).hexdigest()
    release_tree = tmp_path / "new-release"
    (fixture_repo / "06_AppPlatform" / "backend").mkdir(parents=True)
    (fixture_repo / "06_AppPlatform" / "backend" / "old.txt").write_text("old")
    (release_tree / "06_AppPlatform" / "backend").mkdir(parents=True)
    (release_tree / "06_AppPlatform" / "backend" / "new.txt").write_text("new")
    project_root = Path(__file__).resolve().parents[4]
    helper = project_root / "03_Scripts" / "deploy" / "lib" / "release_paths.sh"
    subprocess.run(
        [
            "bash",
            "-c",
            (
                'source "$1"; '
                'assert_path_outside_release_roots "$2" "$3" 06_AppPlatform; '
                'replace_release_paths "$2" "$4" 06_AppPlatform'
            ),
            "_",
            str(helper),
            str(fixture_repo),
            str(evidence_root),
            str(release_tree),
        ],
        check=True,
    )

    assert (fixture_repo / "06_AppPlatform" / "backend" / "new.txt").is_file()
    assert not (fixture_repo / "06_AppPlatform" / "backend" / "old.txt").exists()
    assert object_path.is_file()
    assert hashlib.sha256(object_path.read_bytes()).hexdigest() == before_release_hash
