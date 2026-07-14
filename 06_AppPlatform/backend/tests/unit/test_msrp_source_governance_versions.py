from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.api.msrp_source_governance_schemas import PublishSourceVersionRequest
from app.db.msrp_source_governance_models import MsrpGovernanceAuditEvent
from app.services.msrp_source_governance import service as governance_service


class _FakeSession:
    def __init__(self) -> None:
        self.added: list[object] = []
        self.commits = 0

    def add(self, item) -> None:
        self.added.append(item)

    def flush(self) -> None:
        return None

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        return None

    def refresh(self, _item) -> None:
        return None


def _version(
    *,
    source_id,
    target_id,
    number: int,
    status: str,
    previous_version_id=None,
    validation_status: str = "passed",
    dryrun_status: str = "passed",
):
    now = datetime(2026, 7, 14, 9, 0, tzinfo=timezone.utc)
    return SimpleNamespace(
        source_version_id=uuid4(),
        source_id=source_id,
        target_id=target_id,
        version_number=number,
        profile_json={"source": "official"},
        profile_yaml="source: official\n",
        profile_sha256="a" * 64,
        evidence_refs_json=[],
        extractor_name="scrapling",
        extractor_type="html",
        extractor_version="v1",
        semantic_lane="msrp",
        currency="SEK",
        tax_mode="tax_included",
        valid_from=None,
        valid_until=None,
        previous_version_id=previous_version_id,
        validation_summary_json={"status": validation_status},
        dryrun_summary_json={"status": dryrun_status},
        replay_summary_json=None,
        conflict_summary_json=None,
        gate_result_json=None,
        version_status=status,
        created_by="editor@example.test",
        approved_by=None,
        approved_at_utc=None,
        published_at_utc=None,
        decision_reason=None,
        created_at_utc=now,
        updated_at_utc=now,
    )


def _target(target_id, active_version_id, row_version: int = 3):
    return SimpleNamespace(
        target_id=target_id,
        active_source_version_id=active_version_id,
        fallback_source_version_id=active_version_id,
        monitoring_status="active",
        row_version=row_version,
        updated_at_utc=datetime(2026, 7, 14, 9, 0, tzinfo=timezone.utc),
    )


def _patch_common(monkeypatch) -> None:
    monkeypatch.setattr(
        governance_service.repo,
        "get_audit_event_by_idempotency",
        lambda *_args, **_kwargs: None,
    )


def test_publish_source_version_atomically_updates_active_and_fallback(
    monkeypatch,
) -> None:
    source_id = uuid4()
    target_id = uuid4()
    current = _version(
        source_id=source_id,
        target_id=target_id,
        number=1,
        status="published",
    )
    candidate = _version(
        source_id=source_id,
        target_id=target_id,
        number=2,
        status="dryrun_passed",
        previous_version_id=current.source_version_id,
    )
    target = _target(target_id, current.source_version_id)
    session = _FakeSession()
    _patch_common(monkeypatch)
    monkeypatch.setattr(
        governance_service.repo,
        "get_source_version",
        lambda *_args, **_kwargs: candidate,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_target",
        lambda *_args, **_kwargs: target,
    )
    monkeypatch.setattr(
        governance_service.repo,
        "get_published_source_version",
        lambda *_args, **_kwargs: current,
    )

    result = governance_service.MsrpSourceGovernanceService(
        session
    ).publish_source_version(
        candidate.source_version_id,
        PublishSourceVersionRequest(
            target_row_version=3,
            decision_reason="Validated official source replacement",
        ),
        actor="admin@example.test",
        actor_role="admin",
        idempotency_key="source-version-publish-1",
    )

    assert current.version_status == "superseded"
    assert candidate.version_status == "published"
    assert target.active_source_version_id == candidate.source_version_id
    assert target.fallback_source_version_id == current.source_version_id
    assert result["versionStatus"] == "published"
    assert any(isinstance(item, MsrpGovernanceAuditEvent) for item in session.added)
    assert session.commits == 1


def test_rollback_source_version_restores_validated_last_known_good(
    monkeypatch,
) -> None:
    source_id = uuid4()
    target_id = uuid4()
    candidate = _version(
        source_id=source_id,
        target_id=target_id,
        number=1,
        status="superseded",
    )
    current = _version(
        source_id=source_id,
        target_id=target_id,
        number=2,
        status="published",
        previous_version_id=candidate.source_version_id,
    )
    target = _target(target_id, current.source_version_id)
    session = _FakeSession()
    _patch_common(monkeypatch)

    def get_version(_session, version_id, **_kwargs):
        if version_id == current.source_version_id:
            return current
        if version_id == candidate.source_version_id:
            return candidate
        return None

    monkeypatch.setattr(governance_service.repo, "get_source_version", get_version)
    monkeypatch.setattr(
        governance_service.repo,
        "get_target",
        lambda *_args, **_kwargs: target,
    )

    result = governance_service.MsrpSourceGovernanceService(
        session
    ).rollback_source_version(
        current.source_version_id,
        PublishSourceVersionRequest(
            target_row_version=3,
            decision_reason="Regression detected in monitored replay",
        ),
        actor="admin@example.test",
        actor_role="admin",
        idempotency_key="source-version-rollback-1",
    )

    assert current.version_status == "rolled_back"
    assert candidate.version_status == "published"
    assert target.active_source_version_id == candidate.source_version_id
    assert result["sourceVersionId"] == str(candidate.source_version_id)
    assert session.commits == 1


def test_rollback_rejects_unvalidated_candidate(monkeypatch) -> None:
    source_id = uuid4()
    target_id = uuid4()
    candidate = _version(
        source_id=source_id,
        target_id=target_id,
        number=1,
        status="superseded",
        validation_status="failed",
    )
    current = _version(
        source_id=source_id,
        target_id=target_id,
        number=2,
        status="published",
        previous_version_id=candidate.source_version_id,
    )
    target = _target(target_id, current.source_version_id)
    session = _FakeSession()
    _patch_common(monkeypatch)

    def get_version(_session, version_id, **_kwargs):
        return current if version_id == current.source_version_id else candidate

    monkeypatch.setattr(governance_service.repo, "get_source_version", get_version)
    monkeypatch.setattr(
        governance_service.repo,
        "get_target",
        lambda *_args, **_kwargs: target,
    )

    with pytest.raises(HTTPException, match="Rollback candidate is invalid") as exc_info:
        governance_service.MsrpSourceGovernanceService(
            session
        ).rollback_source_version(
            current.source_version_id,
            PublishSourceVersionRequest(
                target_row_version=3,
                decision_reason="Attempt invalid rollback",
            ),
            actor="admin@example.test",
            actor_role="admin",
            idempotency_key="source-version-rollback-invalid",
        )

    assert exc_info.value.status_code == 409
    assert current.version_status == "published"
    assert session.commits == 0
