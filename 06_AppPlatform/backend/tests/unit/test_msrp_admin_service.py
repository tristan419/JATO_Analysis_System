from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from app.services import msrp_admin_service


class _DummySession:
    def __init__(self, *, fail_commit: bool = False) -> None:
        self.deleted: list[object] = []
        self.flush_count = 0
        self.commit_count = 0
        self.rollback_count = 0
        self.fail_commit = fail_commit

    def delete(self, obj: object) -> None:
        self.deleted.append(obj)

    def flush(self) -> None:
        self.flush_count += 1

    def commit(self) -> None:
        self.commit_count += 1
        if self.fail_commit:
            raise IntegrityError("DELETE", {}, Exception("fk violation"))

    def rollback(self) -> None:
        self.rollback_count += 1


def test_delete_observation_flushes_review_case_before_observation(
    monkeypatch,
) -> None:
    session = _DummySession()
    observation = SimpleNamespace(observation_id=uuid4())
    review_case = SimpleNamespace(review_case_id=uuid4())

    monkeypatch.setattr(
        msrp_admin_service.repo,
        "get_observation",
        lambda *_args, **_kwargs: observation,
    )
    monkeypatch.setattr(
        msrp_admin_service,
        "_assert_observation_mutable",
        lambda *_args, **_kwargs: review_case,
    )
    monkeypatch.setattr(
        msrp_admin_service,
        "_serialize_observation_row",
        lambda *_args, **_kwargs: {"observationId": str(observation.observation_id)},
    )

    payload = msrp_admin_service.delete_observation(
        session,
        str(observation.observation_id),
    )

    assert payload["observationId"] == str(observation.observation_id)
    assert session.deleted == [review_case, observation]
    assert session.flush_count == 1
    assert session.commit_count == 1


def test_delete_observation_returns_conflict_when_delete_is_still_referenced(
    monkeypatch,
) -> None:
    session = _DummySession(fail_commit=True)
    observation = SimpleNamespace(observation_id=uuid4())

    monkeypatch.setattr(
        msrp_admin_service.repo,
        "get_observation",
        lambda *_args, **_kwargs: observation,
    )
    monkeypatch.setattr(
        msrp_admin_service,
        "_assert_observation_mutable",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        msrp_admin_service,
        "_serialize_observation_row",
        lambda *_args, **_kwargs: {"observationId": str(observation.observation_id)},
    )

    with pytest.raises(HTTPException) as exc_info:
        msrp_admin_service.delete_observation(
            session,
            str(observation.observation_id),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Observation is still referenced by review workflow"
    assert session.rollback_count == 1
