from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.api.routes import engineering_config
from app.core.security import UserContext
from app.db.session import get_db_session
from app.infra import engineering_config_repository
from app.main import app


@dataclass
class FakeFeature:
    feature_id: UUID
    feature_code: str
    category: str
    standard_field_name: str
    display_order: int
    aliases: list[str] | None = None
    seq: int = 0
    is_active: bool = True


@dataclass
class FakeTrim:
    trim_id: UUID
    full_trim_name: str
    brand: str
    model_name: str
    trim_name: str
    market: str | None
    model_year: str | None
    energy_type: str | None
    drivetrain: str | None
    engine: str | None
    vehicle_code: str | None
    material_no: str | None
    identity_key: str | None
    source_upload_id: UUID | None = None
    status: str = "active"


@dataclass
class FakeValue:
    value_id: UUID
    feature_id: UUID
    raw_value: str
    normalized_value: str | None
    availability: str
    unit: str | None
    version: int = 1
    trim_id: UUID | None = None


@dataclass
class FakeImportBatch:
    import_batch_id: UUID
    domain: str
    source_file_name: str
    source_file_path: str
    source_file_hash: str | None
    import_status: str
    row_count: int
    error_count: int
    triggered_by: str | None
    started_at_utc: datetime | None
    finished_at_utc: datetime | None
    created_at_utc: datetime | None


@dataclass
class FakeConfigVersion:
    version_id: UUID
    trim_id: UUID
    version_no: int
    status: str
    source_upload_id: UUID | None
    snapshot_values: list[dict]
    created_at_utc: datetime | None = None


@dataclass
class FakeSourceContextLink:
    id: UUID
    source_id: UUID
    batch_id: UUID
    brand: str | None
    model_name: str | None
    model_year: str | None
    market: str | None
    country: str | None
    trim_ids: list[str]
    sales_version_ids: list[str]
    context_type: str
    created_by: str | None
    created_at_utc: datetime | None
    powertrain: str | None = None
    segment: str | None = None
    scenario: str | None = None
    identity_anchor: str | None = None
    status: str = "active"


class FakeSession:
    def __init__(self, features: dict[UUID, FakeFeature]) -> None:
        self.features = features

    def get(self, _model: object, feature_id: UUID) -> FakeFeature | None:
        return self.features.get(feature_id)


class FakeImportSession:
    def __init__(self) -> None:
        self.added: list[object] = []
        self.committed = False
        self.rolled_back = False

    def add(self, item: object) -> None:
        self.added.append(item)

    def flush(self) -> None:
        for item in self.added:
            if hasattr(item, "import_batch_id") and getattr(item, "import_batch_id", None) is None:
                setattr(item, "import_batch_id", uuid4())
            if hasattr(item, "trim_id") and getattr(item, "trim_id", None) is None:
                setattr(item, "trim_id", uuid4())
            if hasattr(item, "feature_id") and getattr(item, "feature_id", None) is None:
                setattr(item, "feature_id", uuid4())
            if hasattr(item, "value_id") and getattr(item, "value_id", None) is None:
                setattr(item, "value_id", uuid4())
            if hasattr(item, "version_id") and getattr(item, "version_id", None) is None:
                setattr(item, "version_id", uuid4())
            if hasattr(item, "id") and getattr(item, "id", None) is None:
                setattr(item, "id", uuid4())
            if getattr(item, "created_at_utc", None) is None:
                setattr(item, "created_at_utc", datetime.now(timezone.utc))

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def refresh(self, _item: object) -> None:
        return None


@pytest.fixture(autouse=True)
def _source_scoped_trim_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep lightweight unit fakes aligned with the source-scoped repository API."""
    monkeypatch.setattr(
        engineering_config.repo,
        "get_vehicle_trim_by_source_full_name",
        lambda session, _source_id, full_name: engineering_config.repo.get_vehicle_trim_by_full_name(session, full_name),
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "get_vehicle_trim_by_config_version_source_full_name",
        lambda session, _source_id, full_name: engineering_config.repo.get_vehicle_trim_by_full_name(session, full_name),
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "get_latest_config_version_for_trim",
        lambda _session, _trim_id, **_kwargs: None,
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "list_config_versions_for_trims",
        lambda _session, trim_ids: {trim_id: [] for trim_id in trim_ids},
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "delete_trim_feature_values_not_in",
        lambda _session, _trim_id, _feature_ids: 0,
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "list_vehicle_trims_by_ids",
        lambda session, trim_ids: [
            trim
            for trim_id in trim_ids
            if (trim := engineering_config.repo.get_vehicle_trim(session, trim_id)) is not None
        ],
    )

    def list_values_with_features(session: object, trim_ids: list[UUID]) -> list[tuple[object, object]]:
        rows: list[tuple[object, object]] = []
        for trim_id in trim_ids:
            for value in engineering_config.repo.list_trim_feature_values(session, trim_id):
                if getattr(value, "trim_id", None) is None:
                    value.trim_id = trim_id
                feature = session.get(engineering_config.FeatureCatalog, value.feature_id)
                if feature is not None:
                    rows.append((value, feature))
        return rows

    monkeypatch.setattr(engineering_config.repo, "list_trim_feature_values_with_features", list_values_with_features)

    def list_batches(session: object, source_ids: list[UUID]) -> dict[UUID, object]:
        batches: dict[UUID, object] = {}
        for source_id in source_ids:
            batch = engineering_config.repo.get_import_batch(session, source_id)
            if batch is not None:
                batches[source_id] = batch
        return batches

    monkeypatch.setattr(engineering_config.repo, "list_import_batches_by_ids", list_batches)


def _value(feature: FakeFeature, raw_value: str, availability: str) -> FakeValue:
    return FakeValue(
        value_id=uuid4(),
        feature_id=feature.feature_id,
        raw_value=raw_value,
        normalized_value=raw_value.lower(),
        availability=availability,
        unit=None,
    )


def _fake_trim(*, trim_id: UUID | None = None, status: str = "active") -> FakeTrim:
    return FakeTrim(
        trim_id=trim_id or uuid4(),
        full_trim_name="Draft Basic",
        brand="OMODA",
        model_name="T19C",
        trim_name="Basic",
        market="EU",
        model_year="2026",
        energy_type=None,
        drivetrain=None,
        engine=None,
        vehicle_code=None,
        material_no=None,
        identity_key="draft-basic",
        status=status,
    )


def test_update_feature_value_writes_audit_and_returns_display_state(monkeypatch) -> None:
    fake_session = FakeImportSession()
    value_id = uuid4()
    trim = _fake_trim()
    current = FakeValue(
        value_id=value_id,
        feature_id=uuid4(),
        raw_value="●",
        normalized_value="standard",
        availability="STANDARD",
        unit=None,
        version=1,
        trim_id=trim.trim_id,
    )
    current.source_upload_id = uuid4()
    current.source_row = 42
    current.source_column = "D"
    update_calls: list[dict[str, object]] = []

    def fake_update(_session: object, updated_value_id: UUID, **kwargs: object) -> bool:
        update_calls.append({"value_id": updated_value_id, **kwargs})
        return True

    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value", lambda _session, _value_id: current)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(engineering_config.repo, "update_trim_feature_value", fake_update)

    payload = engineering_config.ConfigFeatureValueUpdate(
        raw_value="O",
        expected_version=1,
        comment="reviewed source",
    )

    result = engineering_config.update_feature_value(
        value_id=str(value_id),
        payload=payload,
        session=fake_session,
        user=UserContext(role="editor", name="alice"),
    )

    assert result["valueId"] == str(value_id)
    assert result["availability"] == "OPTIONAL"
    assert result["displayValue"] == "选装"
    assert result["version"] == 2
    assert result["manualOverride"] is True
    assert update_calls == [
        {
            "value_id": value_id,
            "raw_value": "O",
            "normalized_value": "选装",
            "availability": "OPTIONAL",
            "unit": None,
            "updated_by": "alice",
            "expected_version": 1,
        }
    ]
    audits = [item for item in fake_session.added if item.__class__.__name__ == "ConfigAuditLog"]
    assert [(audit.field_name, audit.old_value, audit.new_value, audit.comment) for audit in audits] == [
        ("raw_value", "●", "O", "reviewed source"),
        ("availability", "STANDARD", "OPTIONAL", "reviewed source"),
        ("source_upload_id", str(current.source_upload_id), None, "reviewed source"),
        ("source_row", "42", "0", "reviewed source"),
        ("source_column", "D", "manual", "reviewed source"),
    ]
    assert {audit.changed_by for audit in audits} == {"alice"}
    assert fake_session.committed is True
    assert fake_session.rolled_back is False


def test_update_feature_value_updates_latest_draft_snapshot(monkeypatch) -> None:
    feature = FakeFeature(uuid4(), "seat_heat", "Comfort", "Seat heating", 1)
    trim = _fake_trim()
    current = FakeValue(
        value_id=uuid4(),
        feature_id=feature.feature_id,
        raw_value="●",
        normalized_value="标配",
        availability="STANDARD",
        unit=None,
        version=1,
        trim_id=trim.trim_id,
    )
    current.source_upload_id = uuid4()
    current.source_row = 12
    current.source_column = "D"
    draft_version = engineering_config.ConfigVersion(
        version_id=uuid4(),
        trim_id=trim.trim_id,
        identity_key="draft",
        brand="OMODA",
        model_name="T19C",
        trim_name="Basic",
        status="draft",
        version_no=1,
        snapshot_values=[{
            "featureId": str(feature.feature_id),
            "featureCode": feature.feature_code,
            "rawValue": "●",
            "sourceUploadId": str(current.source_upload_id),
        }],
        snapshot_feature_count=1,
    )

    class DraftEditSession(FakeImportSession):
        def get(self, model: object, object_id: UUID) -> object | None:
            if model is engineering_config.FeatureCatalog and object_id == feature.feature_id:
                return feature
            return None

    session = DraftEditSession()
    locked_trim_ids: list[UUID] = []
    latest_for_update: list[bool] = []
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value", lambda _session, _value_id: current)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(
        engineering_config.repo,
        "acquire_config_trim_lock",
        lambda _session, trim_id: locked_trim_ids.append(trim_id),
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "get_latest_config_version_for_trim",
        lambda _session, _trim_id, *, for_update=False: (
            latest_for_update.append(for_update) or draft_version
        ),
    )
    monkeypatch.setattr(engineering_config.repo, "update_trim_feature_value", lambda *_args, **_kwargs: True)

    engineering_config.update_feature_value(
        value_id=str(current.value_id),
        payload=engineering_config.ConfigFeatureValueUpdate(
            raw_value="O",
            expected_version=1,
            comment="verified option",
        ),
        session=session,
        user=UserContext(role="editor", name="alice"),
    )

    assert locked_trim_ids == [trim.trim_id]
    assert latest_for_update == [True]
    assert draft_version.snapshot_feature_count == 1
    assert draft_version.snapshot_values == [{
        "featureId": str(feature.feature_id),
        "featureCode": "seat_heat",
        "featureName": "Seat heating",
        "category": "Comfort",
        "rawValue": "O",
        "normalizedValue": "选装",
        "availability": "OPTIONAL",
        "unit": None,
        "sourceRow": 0,
        "sourceColumn": "manual",
        "sourceUploadId": None,
        "source": None,
        "inferred": False,
        "inferenceReason": None,
        "confidence": None,
    }]


def test_update_feature_value_rejects_published_configuration(monkeypatch) -> None:
    trim = _fake_trim()
    current = FakeValue(
        value_id=uuid4(),
        feature_id=uuid4(),
        raw_value="●",
        normalized_value="标配",
        availability="STANDARD",
        unit=None,
        version=1,
        trim_id=trim.trim_id,
    )
    published_version = type("PublishedVersion", (), {"status": "published"})()
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value", lambda _session, _value_id: current)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(
        engineering_config.repo,
        "get_latest_config_version_for_trim",
        lambda _session, _trim_id, **_kwargs: published_version,
    )

    with pytest.raises(HTTPException) as exc_info:
        engineering_config.update_feature_value(
            value_id=str(current.value_id),
            payload=engineering_config.ConfigFeatureValueUpdate(raw_value="O", expected_version=1),
            session=FakeImportSession(),
            user=UserContext(role="editor", name="alice"),
        )

    assert exc_info.value.status_code == 409
    assert "immutable" in exc_info.value.detail


def test_update_feature_value_noop_preserves_source_evidence(monkeypatch) -> None:
    fake_session = FakeImportSession()
    value_id = uuid4()
    trim = _fake_trim()
    current = FakeValue(
        value_id=value_id,
        feature_id=uuid4(),
        raw_value="●",
        normalized_value="standard",
        availability="STANDARD",
        unit=None,
        version=3,
        trim_id=trim.trim_id,
    )
    current.source_upload_id = uuid4()
    current.source_row = 18
    current.source_column = "F"
    update_called = False

    def fake_update(*_args, **_kwargs) -> bool:
        nonlocal update_called
        update_called = True
        return True

    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value", lambda _session, _value_id: current)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(engineering_config.repo, "update_trim_feature_value", fake_update)

    result = engineering_config.update_feature_value(
        value_id=str(value_id),
        payload=engineering_config.ConfigFeatureValueUpdate(
            raw_value="●",
            expected_version=3,
            comment="duplicate save",
        ),
        session=fake_session,
        user=UserContext(role="editor", name="alice"),
    )

    assert result["unchanged"] is True
    assert result["version"] == 3
    assert result["manualOverride"] is False
    assert update_called is False
    assert fake_session.added == []
    assert fake_session.committed is False


def test_update_feature_value_rejects_version_conflict(monkeypatch) -> None:
    fake_session = FakeImportSession()
    value_id = uuid4()
    trim = _fake_trim()
    current = FakeValue(
        value_id=value_id,
        feature_id=uuid4(),
        raw_value="●",
        normalized_value="standard",
        availability="STANDARD",
        unit=None,
        version=3,
        trim_id=trim.trim_id,
    )

    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value", lambda _session, _value_id: current)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(engineering_config.repo, "update_trim_feature_value", lambda *_args, **_kwargs: False)

    with pytest.raises(HTTPException) as exc_info:
        engineering_config.update_feature_value(
            value_id=str(value_id),
            payload=engineering_config.ConfigFeatureValueUpdate(raw_value="O", expected_version=1),
            session=fake_session,
                user=UserContext(role="editor", name="alice"),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Version conflict — refresh and retry"
    assert fake_session.rolled_back is True
    assert fake_session.committed is False
    assert [item for item in fake_session.added if item.__class__.__name__ == "ConfigAuditLog"] == []


def test_update_feature_value_rejects_trashed_trim(monkeypatch) -> None:
    fake_session = FakeImportSession()
    value_id = uuid4()
    trim = _fake_trim(status="trashed")
    current = FakeValue(
        value_id=value_id,
        feature_id=uuid4(),
        raw_value="●",
        normalized_value="standard",
        availability="STANDARD",
        unit=None,
        version=1,
        trim_id=trim.trim_id,
    )
    update_called = False

    def fake_update(*_args, **_kwargs) -> bool:
        nonlocal update_called
        update_called = True
        return True

    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value", lambda _session, _value_id: current)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(engineering_config.repo, "update_trim_feature_value", fake_update)

    with pytest.raises(HTTPException) as exc_info:
        engineering_config.update_feature_value(
            value_id=str(value_id),
            payload=engineering_config.ConfigFeatureValueUpdate(raw_value="O", expected_version=1),
            session=fake_session,
                user=UserContext(role="editor", name="alice"),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Config trim is in trash; restore it before editing"
    assert update_called is False
    assert fake_session.committed is False


def test_create_feature_value_flushes_before_audit(monkeypatch) -> None:
    feature = FakeFeature(
        feature_id=uuid4(),
        feature_code="seat_heat",
        category="Comfort",
        standard_field_name="Seat heating",
        display_order=1,
    )
    trim = FakeTrim(
        trim_id=uuid4(),
        full_trim_name="Draft Basic",
        brand="OMODA",
        model_name="T19C",
        trim_name="Basic",
        market="EU",
        model_year="2026",
        energy_type=None,
        drivetrain=None,
        engine=None,
        vehicle_code=None,
        material_no=None,
        identity_key="draft-basic",
    )

    class FakeEditSession(FakeImportSession):
        def get(self, _model: object, object_id: UUID) -> FakeFeature | None:
            return feature if object_id == feature.feature_id else None

    fake_session = FakeEditSession()
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)

    result = engineering_config.create_feature_value(
        payload=engineering_config.ConfigFeatureValueCreate(
            trim_id=str(trim.trim_id),
            feature_id=str(feature.feature_id),
            raw_value="●",
        ),
        session=fake_session,
        user=UserContext(role="editor", name="alice"),
    )

    values = [item for item in fake_session.added if item.__class__.__name__ == "TrimFeatureValue"]
    audits = [item for item in fake_session.added if item.__class__.__name__ == "ConfigAuditLog"]
    assert len(values) == 1
    assert len(audits) == 1
    assert values[0].value_id is not None
    assert values[0].source_column == "manual"
    assert values[0].source_upload_id is None
    assert values[0].updated_by == "alice"
    assert audits[0].entity_id == values[0].value_id
    assert audits[0].field_name == "raw_value"
    assert audits[0].new_value == "●"
    assert result["valueId"] == str(values[0].value_id)
    assert result["displayValue"] == "标配"
    assert fake_session.committed is True


def test_create_feature_value_rejects_trashed_trim(monkeypatch) -> None:
    trim = _fake_trim(status="trashed")
    fake_session = FakeImportSession()
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)

    with pytest.raises(HTTPException) as exc_info:
        engineering_config.create_feature_value(
            payload=engineering_config.ConfigFeatureValueCreate(
                trim_id=str(trim.trim_id),
                feature_id=str(uuid4()),
                raw_value="●",
            ),
            session=fake_session,
            user=UserContext(role="editor", name="alice"),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Config trim is in trash; restore it before editing"
    assert fake_session.added == []
    assert fake_session.committed is False


def test_delete_feature_value_rejects_trashed_trim(monkeypatch) -> None:
    fake_session = FakeImportSession()
    value_id = uuid4()
    trim = _fake_trim(status="trashed")
    current = FakeValue(
        value_id=value_id,
        feature_id=uuid4(),
        raw_value="●",
        normalized_value="standard",
        availability="STANDARD",
        unit=None,
        version=1,
        trim_id=trim.trim_id,
    )
    delete_called = False

    def fake_delete(*_args, **_kwargs) -> bool:
        nonlocal delete_called
        delete_called = True
        return True

    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value", lambda _session, _value_id: current)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(engineering_config.repo, "delete_trim_feature_value", fake_delete)

    with pytest.raises(HTTPException) as exc_info:
        engineering_config.delete_feature_value(
            value_id=str(value_id),
            comment=None,
            session=fake_session,
            user=UserContext(role="editor", name="alice"),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Config trim is in trash; restore it before editing"
    assert delete_called is False
    assert fake_session.committed is False


def test_delete_feature_value_writes_value_and_source_audit(monkeypatch) -> None:
    fake_session = FakeImportSession()
    value_id = uuid4()
    trim = _fake_trim()
    current = FakeValue(
        value_id=value_id,
        feature_id=uuid4(),
        raw_value="O",
        normalized_value="optional",
        availability="OPTIONAL",
        unit=None,
        version=2,
        trim_id=trim.trim_id,
    )
    current.source_upload_id = uuid4()
    current.source_row = 25
    current.source_column = "G"
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value", lambda _session, _value_id: current)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(engineering_config.repo, "delete_trim_feature_value", lambda _session, _value_id: True)

    result = engineering_config.delete_feature_value(
        value_id=str(value_id),
        comment="duplicate row removed",
        session=fake_session,
        user=UserContext(role="editor", name="alice"),
    )

    assert result == {"valueId": str(value_id), "deleted": True}
    audits = [item for item in fake_session.added if item.__class__.__name__ == "ConfigAuditLog"]
    assert [(item.field_name, item.old_value, item.changed_by, item.comment) for item in audits] == [
        ("raw_value", "O", "alice", "duplicate row removed"),
        ("availability", "OPTIONAL", "alice", "duplicate row removed"),
        ("source_upload_id", str(current.source_upload_id), "alice", "duplicate row removed"),
        ("source_row", "25", "alice", "duplicate row removed"),
        ("source_column", "G", "alice", "duplicate row removed"),
    ]
    assert fake_session.committed is True


def test_feature_value_edit_routes_reject_unauthenticated_request(monkeypatch) -> None:
    from app.core import security

    monkeypatch.setattr(security, "AUTH_ENABLED", True)
    monkeypatch.setattr(security.session_store, "lookup", lambda _token: None)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {})
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.patch(
            f"/v1/engineering-config/values/{uuid4()}",
            json={"raw_value": "O", "expected_version": 1, "updated_by": "viewer"},
            headers={"X-User-Name": "viewer"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 401
    assert response.json()["detail"] == "Authentication required"


def test_feature_value_edit_routes_reject_authenticated_viewer(monkeypatch) -> None:
    from app.core import security

    monkeypatch.setattr(security, "AUTH_ENABLED", True)
    monkeypatch.setattr(security.session_store, "lookup", lambda _token: None)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {"viewer-token": "viewer"})
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.patch(
            f"/v1/engineering-config/values/{uuid4()}",
            json={"raw_value": "O", "expected_version": 1},
            headers={"X-Auth-Token": "viewer-token", "X-User-Name": "viewer"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 403
    assert response.json()["detail"] == "Forbidden"


def test_feature_value_edit_routes_allow_editor(monkeypatch) -> None:
    from app.core import security

    fake_session = FakeImportSession()
    value_id = uuid4()
    trim = _fake_trim()
    current = FakeValue(
        value_id=value_id,
        feature_id=uuid4(),
        raw_value="●",
        normalized_value="standard",
        availability="STANDARD",
        unit=None,
        version=1,
        trim_id=trim.trim_id,
    )
    update_calls: list[dict[str, object]] = []

    def fake_update(_session: object, updated_value_id: UUID, **kwargs: object) -> bool:
        update_calls.append({"value_id": updated_value_id, **kwargs})
        return True

    monkeypatch.setattr(security, "AUTH_ENABLED", True)
    monkeypatch.setattr(security.session_store, "lookup", lambda _token: None)
    monkeypatch.setattr(security, "TOKEN_ROLE_MAP", {"editor-token": "editor"})
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value", lambda _session, _value_id: current)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(engineering_config.repo, "update_trim_feature_value", fake_update)
    app.dependency_overrides[get_db_session] = lambda: fake_session
    try:
        client = TestClient(app)
        response = client.patch(
            f"/v1/engineering-config/values/{value_id}",
            json={"raw_value": "O", "expected_version": 1, "updated_by": "spoofed-user"},
            headers={"X-Auth-Token": "editor-token", "X-User-Name": "editor"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["availability"] == "OPTIONAL"
    assert payload["displayValue"] == "选装"
    assert payload["manualOverride"] is True
    assert update_calls == [
        {
            "value_id": value_id,
            "raw_value": "O",
            "normalized_value": "选装",
            "availability": "OPTIONAL",
            "unit": None,
                "updated_by": "static-editor",
            "expected_version": 1,
        }
    ]
    assert fake_session.committed is True


def test_manual_override_has_no_file_coordinate() -> None:
    value = FakeValue(
        value_id=uuid4(),
        feature_id=uuid4(),
        raw_value="O",
        normalized_value="选装",
        availability="OPTIONAL",
        unit=None,
    )
    value.source_upload_id = uuid4()
    value.source_row = 12
    value.source_column = "manual"

    assert engineering_config._source_ref_payload_for_value(value, FakeImportSession()) is None


def test_compare_trims_marks_manual_values_without_source_evidence(monkeypatch) -> None:
    feature = FakeFeature(uuid4(), "seat_heat", "Comfort", "Seat heating", 1)
    base_trim = _fake_trim(trim_id=uuid4())
    target_trim = FakeTrim(
        trim_id=uuid4(),
        full_trim_name="Draft Premium",
        brand="OMODA",
        model_name="T19C",
        trim_name="Premium",
        market="EU",
        model_year="2026",
        energy_type=None,
        drivetrain=None,
        engine=None,
        vehicle_code=None,
        material_no=None,
        identity_key="draft-premium",
    )
    manual_value = _value(feature, "O", "OPTIONAL")
    manual_value.trim_id = target_trim.trim_id
    manual_value.source_column = "manual"
    manual_value.source_row = 0
    manual_value.source_upload_id = None
    base_value = _value(feature, "●", "STANDARD")
    base_value.trim_id = base_trim.trim_id
    trims = {base_trim.trim_id: base_trim, target_trim.trim_id: target_trim}
    values_by_trim = {
        base_trim.trim_id: [base_value],
        target_trim.trim_id: [manual_value],
    }
    session = FakeSession({feature.feature_id: feature})

    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, trim_id: trims.get(trim_id))
    monkeypatch.setattr(engineering_config.repo, "list_trim_feature_values", lambda _session, trim_id, **_kwargs: values_by_trim[trim_id])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, code: feature if code == feature.feature_code else None)

    payload = engineering_config.compare_trims(
        trim_ids=f"{base_trim.trim_id},{target_trim.trim_id}",
        session=session,
        differences_only=False,
        _=None,
    )

    manual_cell = payload["rows"][0]["values"][1]
    assert manual_cell["manualOverride"] is True
    assert manual_cell["source"] is None
    assert manual_cell["inferred"] is False


def test_compare_trims_returns_union_summary_and_business_types(monkeypatch) -> None:
    core_id = uuid4()
    ultra_id = uuid4()
    features = [
        FakeFeature(uuid4(), "common_led", "Lighting", "LED headlamps", 1),
        FakeFeature(uuid4(), "blind_spot", "Safety", "Blind Spot Information System", 2),
        FakeFeature(uuid4(), "wheel_size", "Wheel", "Wheel size", 3),
        FakeFeature(uuid4(), "premium_audio", "Infotainment", "Premium audio", 4),
    ]
    features_by_id = {feature.feature_id: feature for feature in features}
    features_by_code = {feature.feature_code: feature for feature in features}
    trims = {
        core_id: FakeTrim(
            trim_id=core_id,
            full_trim_name="Volvo XC60 Core",
            brand="Volvo",
            model_name="XC60",
            trim_name="Core",
            market="Germany",
            model_year="2026",
            energy_type="PHEV",
            drivetrain="AWD",
            engine=None,
            vehicle_code="CORE-SV",
            material_no=None,
            identity_key="volvo-xc60-core",
        ),
        ultra_id: FakeTrim(
            trim_id=ultra_id,
            full_trim_name="Volvo XC60 Ultra",
            brand="Volvo",
            model_name="XC60",
            trim_name="Ultra",
            market="Germany",
            model_year="2026",
            energy_type="PHEV",
            drivetrain="AWD",
            engine=None,
            vehicle_code="ULTRA-SV",
            material_no=None,
            identity_key="volvo-xc60-ultra",
        ),
    }
    values_by_trim = {
        core_id: [
            _value(features_by_code["common_led"], "Yes", "STANDARD"),
            _value(features_by_code["blind_spot"], "No", "NOT_AVAILABLE"),
            _value(features_by_code["wheel_size"], "18 inch", "VALUE"),
        ],
        ultra_id: [
            _value(features_by_code["common_led"], "Yes", "STANDARD"),
            _value(features_by_code["blind_spot"], "Yes", "STANDARD"),
            _value(features_by_code["wheel_size"], "20 inch", "VALUE"),
            _value(features_by_code["premium_audio"], "Harman Kardon", "VALUE"),
        ],
    }

    monkeypatch.setattr(
        engineering_config.repo,
        "get_vehicle_trim",
        lambda _session, trim_id: trims.get(trim_id),
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "list_trim_feature_values",
        lambda _session, trim_id: values_by_trim[trim_id],
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "get_feature_catalog_by_code",
        lambda _session, code: features_by_code.get(code),
    )

    payload = engineering_config.compare_trims(
        trim_ids=f"{core_id},{ultra_id}",
        differences_only=True,
        session=FakeSession(features_by_id),
        _=None,
    )

    rows_by_code = {row["featureCode"]: row for row in payload["rows"]}
    assert set(rows_by_code) == {"blind_spot", "wheel_size", "premium_audio"}
    assert rows_by_code["blind_spot"]["comparisonType"] == "UNIQUE_TO_TRIM"
    assert rows_by_code["blind_spot"]["uniqueTrimIds"] == [str(ultra_id)]
    assert rows_by_code["wheel_size"]["comparisonType"] == "DIFFERENT_VALUE"
    assert rows_by_code["wheel_size"]["featureId"] == str(features_by_code["wheel_size"].feature_id)
    assert rows_by_code["premium_audio"]["comparisonType"] == "MISSING_OR_UNKNOWN"
    assert rows_by_code["premium_audio"]["values"][0] is None
    assert payload["summary"] == {
        "totalFeatures": 4,
        "shownFeatures": 3,
        "commonSameCount": 1,
        "differentValueCount": 1,
        "uniqueFeatureCount": 1,
        "partialAvailableCount": 0,
        "uniqueOrPartialCount": 1,
        "missingOrUnknownCount": 1,
        "confirmedDifferenceCount": 2,
        "rawConfirmedDifferenceCount": 2,
        "inferredDifferenceCount": 0,
        "differenceCount": 3,
        "categoryCounts": {"Infotainment": 1, "Lighting": 1, "Safety": 1, "Wheel": 1},
        "differenceCategories": ["Infotainment", "Safety", "Wheel"],
    }
    assert rows_by_code["blind_spot"]["values"][0]["source"] is None
    assert rows_by_code["blind_spot"]["values"][0]["inferred"] is False
    assert rows_by_code["blind_spot"]["values"][0]["displayValue"] == "不配备"
    assert rows_by_code["blind_spot"]["values"][0]["valueId"] is None
    assert rows_by_code["blind_spot"]["values"][0]["version"] is None
    assert payload["trims"][0]["salesVersion"] == "CORE-SV"
    assert payload["trims"][0]["msrp"] is None
    assert payload["groups"][0]["category"] == "Infotainment"


def test_compare_trims_separates_published_and_latest_version_snapshots(monkeypatch) -> None:
    feature = FakeFeature(uuid4(), "seat_heat", "Comfort", "Seat heating", 1)
    base_trim = _fake_trim(trim_id=uuid4())
    target_trim = _fake_trim(trim_id=uuid4())
    target_trim.trim_name = "Premium"
    target_trim.full_trim_name = "Draft Premium"
    trims = {base_trim.trim_id: base_trim, target_trim.trim_id: target_trim}
    live_values: dict[UUID, list[FakeValue]] = {}
    for trim in (base_trim, target_trim):
        value = _value(feature, "●", "STANDARD")
        value.trim_id = trim.trim_id
        value.source_upload_id = None
        value.source_row = 0
        value.source_column = "manual"
        live_values[trim.trim_id] = [value]

    def snapshot(raw_value: str, availability: str) -> list[dict]:
        return [{
            "featureId": str(feature.feature_id),
            "featureCode": feature.feature_code,
            "featureName": feature.standard_field_name,
            "category": feature.category,
            "rawValue": raw_value,
            "normalizedValue": raw_value,
            "availability": availability,
            "unit": None,
            "source": None,
        }]

    versions_by_trim = {
        base_trim.trim_id: [
            FakeConfigVersion(uuid4(), base_trim.trim_id, 2, "draft", None, snapshot("●", "STANDARD")),
            FakeConfigVersion(uuid4(), base_trim.trim_id, 1, "published", None, snapshot("●", "STANDARD")),
        ],
        target_trim.trim_id: [
            FakeConfigVersion(uuid4(), target_trim.trim_id, 2, "draft", None, snapshot("●", "STANDARD")),
            FakeConfigVersion(uuid4(), target_trim.trim_id, 1, "published", None, snapshot("-", "NOT_AVAILABLE")),
        ],
    }
    published_target_version = versions_by_trim[target_trim.trim_id][1]
    published_target_version.identity_key = "published-premium-de-2025"
    published_target_version.material_no = "MAT-PUBLISHED"
    published_target_version.vehicle_code = "SV-PUBLISHED"
    published_target_version.market = "Germany"
    published_target_version.model_year = "2025"
    published_target_version.brand = "Published Brand"
    published_target_version.model_name = "Published Model"
    published_target_version.trim_name = "Published Premium"
    target_trim.identity_key = "live-premium-fr-2027"
    target_trim.material_no = "MAT-LIVE"
    target_trim.vehicle_code = "SV-LIVE"
    target_trim.market = "France"
    target_trim.model_year = "2027"
    target_trim.brand = "Live Brand"
    target_trim.model_name = "Live Model"
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, trim_id: trims.get(trim_id))
    monkeypatch.setattr(
        engineering_config.repo,
        "list_trim_feature_values",
        lambda _session, trim_id, **_kwargs: live_values[trim_id],
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "list_config_versions_for_trims",
        lambda _session, _trim_ids: versions_by_trim,
    )
    session = FakeSession({feature.feature_id: feature})
    trim_ids = f"{base_trim.trim_id},{target_trim.trim_id}"

    published = engineering_config.compare_trims(
        trim_ids=trim_ids,
        differences_only=False,
        version_scope="published",
        session=session,
        _=None,
    )
    assert published["versionScope"] == "published"
    assert published["usesDraft"] is False
    assert published["versionFallbackCount"] == 0
    assert [trim["configVersionStatus"] for trim in published["trims"]] == ["published", "published"]
    assert published["rows"][0]["values"][1]["displayValue"] == "不配备"
    assert published["rows"][0]["values"][1]["valueId"] is None
    assert published["rows"][0]["values"][1]["version"] is None
    assert published["trims"][1]["identityKey"] == "published-premium-de-2025"
    assert published["trims"][1]["materialNo"] == "MAT-PUBLISHED"
    assert published["trims"][1]["market"] == "Germany"
    assert published["trims"][1]["modelYear"] == "2025"
    assert published["trims"][1]["brand"] == "Published Brand"
    assert published["trims"][1]["modelName"] == "Published Model"
    assert published["trims"][1]["trimName"] == "Published Premium"

    latest = engineering_config.compare_trims(
        trim_ids=trim_ids,
        differences_only=False,
        version_scope="latest",
        session=session,
        _=None,
    )
    assert latest["versionScope"] == "latest"
    assert latest["usesDraft"] is True
    assert [trim["configVersionStatus"] for trim in latest["trims"]] == ["draft", "draft"]
    assert latest["rows"][0]["values"][1]["displayValue"] == "标配"
    assert latest["rows"][0]["values"][1]["valueId"] == str(live_values[target_trim.trim_id][0].value_id)
    assert latest["rows"][0]["values"][1]["version"] == 1


def test_compare_published_scope_does_not_treat_legacy_live_values_as_published(
    monkeypatch,
) -> None:
    feature = FakeFeature(uuid4(), "seat_heat", "Comfort", "Seat heating", 1)
    base_trim = _fake_trim(trim_id=uuid4())
    target_trim = _fake_trim(trim_id=uuid4())
    target_trim.trim_name = "Premium"
    target_trim.full_trim_name = "Premium"
    trims = {base_trim.trim_id: base_trim, target_trim.trim_id: target_trim}
    live_values: dict[UUID, list[FakeValue]] = {}
    for trim in (base_trim, target_trim):
        value = _value(feature, "●", "STANDARD")
        value.trim_id = trim.trim_id
        live_values[trim.trim_id] = [value]

    def snapshot(raw_value: str, availability: str) -> list[dict]:
        return [{
            "featureId": str(feature.feature_id),
            "featureCode": feature.feature_code,
            "featureName": feature.standard_field_name,
            "category": feature.category,
            "rawValue": raw_value,
            "availability": availability,
            "source": None,
        }]

    versions_by_trim = {
        base_trim.trim_id: [
            FakeConfigVersion(uuid4(), base_trim.trim_id, 2, "draft", None, snapshot("●", "STANDARD")),
            FakeConfigVersion(uuid4(), base_trim.trim_id, 1, "published", None, []),
        ],
        target_trim.trim_id: [
            FakeConfigVersion(uuid4(), target_trim.trim_id, 2, "draft", None, snapshot("-", "NOT_AVAILABLE")),
            FakeConfigVersion(uuid4(), target_trim.trim_id, 1, "published", None, []),
        ],
    }
    monkeypatch.setattr(engineering_config.repo, "list_vehicle_trims_by_ids", lambda _session, _ids: list(trims.values()))
    monkeypatch.setattr(engineering_config.repo, "list_config_versions_for_trims", lambda _session, _ids: versions_by_trim)
    monkeypatch.setattr(
        engineering_config.repo,
        "list_trim_feature_values_with_features",
        lambda _session, _ids: [
            (value, feature)
            for values in live_values.values()
            for value in values
        ],
    )
    monkeypatch.setattr(engineering_config.repo, "list_import_batches_by_ids", lambda _session, _ids: {})

    result = engineering_config.build_compare_facts(
        FakeSession({feature.feature_id: feature}),
        [base_trim.trim_id, target_trim.trim_id],
        differences_only=False,
        version_scope="published",
    )

    assert result["usesDraft"] is True
    assert result["versionFallbackCount"] == 2
    assert [trim["configVersionStatus"] for trim in result["trims"]] == ["draft", "draft"]
    assert [trim["publishedVersionAvailable"] for trim in result["trims"]] == [False, False]
    assert result["rows"][0]["values"][1]["displayValue"] == "不配备"
    assert result["rows"][0]["values"][1]["valueId"] is None
    assert result["rows"][0]["values"][1]["version"] is None


def test_compare_trims_keeps_distinct_catalog_rows_with_same_feature_code(monkeypatch) -> None:
    base_trim = _fake_trim(trim_id=uuid4())
    target_trim = _fake_trim(trim_id=uuid4())
    target_trim.full_trim_name = "Draft Premium"
    target_trim.trim_name = "Premium"
    features = [
        FakeFeature(uuid4(), "shared_code", "Exterior", "Roof rack", 1),
        FakeFeature(uuid4(), "shared_code", "Comfort", "Seat heating", 2),
    ]
    features_by_id = {feature.feature_id: feature for feature in features}
    values_by_trim = {
        base_trim.trim_id: [_value(feature, "-", "NOT_AVAILABLE") for feature in features],
        target_trim.trim_id: [_value(feature, "●", "STANDARD") for feature in features],
    }
    trims = {base_trim.trim_id: base_trim, target_trim.trim_id: target_trim}

    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, trim_id: trims.get(trim_id))
    monkeypatch.setattr(
        engineering_config.repo,
        "list_trim_feature_values",
        lambda _session, trim_id: values_by_trim[trim_id],
    )

    payload = engineering_config.compare_trims(
        trim_ids=f"{base_trim.trim_id},{target_trim.trim_id}",
        differences_only=False,
        session=FakeSession(features_by_id),
        _=None,
    )

    assert payload["summary"]["totalFeatures"] == 2
    assert [row["featureCode"] for row in payload["rows"]] == ["shared_code", "shared_code"]
    assert {row["featureName"] for row in payload["rows"]} == {"Roof rack", "Seat heating"}


def test_compare_trims_does_not_truncate_more_than_one_thousand_features(monkeypatch) -> None:
    base_trim = _fake_trim(trim_id=uuid4())
    target_trim = _fake_trim(trim_id=uuid4())
    target_trim.full_trim_name = "Draft Premium"
    target_trim.trim_name = "Premium"
    features = [
        FakeFeature(uuid4(), f"feature_{index}", "Long catalog", f"Feature {index}", index)
        for index in range(1001)
    ]
    features_by_id = {feature.feature_id: feature for feature in features}
    values_by_trim = {
        base_trim.trim_id: [_value(feature, "●", "STANDARD") for feature in features],
        target_trim.trim_id: [_value(feature, "●", "STANDARD") for feature in features],
    }
    trims = {base_trim.trim_id: base_trim, target_trim.trim_id: target_trim}

    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, trim_id: trims.get(trim_id))
    monkeypatch.setattr(
        engineering_config.repo,
        "list_trim_feature_values",
        lambda _session, trim_id: values_by_trim[trim_id],
    )

    payload = engineering_config.compare_trims(
        trim_ids=f"{base_trim.trim_id},{target_trim.trim_id}",
        differences_only=False,
        session=FakeSession(features_by_id),
        _=None,
    )

    assert payload["summary"]["totalFeatures"] == 1001
    assert payload["summary"]["shownFeatures"] == 1001
    assert len(payload["rows"]) == 1001


def test_compare_trims_rejects_duplicate_trim_ids() -> None:
    trim_id = uuid4()

    with pytest.raises(HTTPException) as exc_info:
        engineering_config.compare_trims(
            trim_ids=f"{trim_id},{trim_id}",
            differences_only=False,
            session=FakeSession({}),
            _=None,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Compare requires 2-4 distinct trim IDs"


def test_compare_trims_restores_digest_evidence_for_persisted_config(monkeypatch, tmp_path) -> None:
    basic_id = uuid4()
    premium_id = uuid4()
    source_id = uuid4()
    source_path = tmp_path / "config.xlsx"
    source_path.write_bytes(b"PK\x03\x04")
    feature = FakeFeature(uuid4(), "rear_camera", "Comfort", "Rear camera", 1)
    trims = {
        basic_id: FakeTrim(
            trim_id=basic_id,
            full_trim_name="T19C Basic",
            brand="OMODA",
            model_name="T19C",
            trim_name="Basic",
            market="EU",
            model_year="2026",
            energy_type="ICE",
            drivetrain="FWD",
            engine=None,
            vehicle_code=None,
            material_no="MM001",
            identity_key="basic",
        ),
        premium_id: FakeTrim(
            trim_id=premium_id,
            full_trim_name="T19C Premium",
            brand="OMODA",
            model_name="T19C",
            trim_name="Premium",
            market="EU",
            model_year="2026",
            energy_type="ICE",
            drivetrain="FWD",
            engine=None,
            vehicle_code=None,
            material_no="MM002",
            identity_key="premium",
        ),
    }
    basic_value = _value(feature, "●", "STANDARD")
    premium_value = _value(feature, "", "NOT_AVAILABLE")
    premium_value.source_upload_id = source_id
    premium_value.source_row = 12
    premium_value.source_column = "F12"
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="config.xlsx",
        source_file_path=str(source_path),
        source_file_hash="hash-1",
        import_status="draft_created",
        row_count=2,
        error_count=0,
        triggered_by="alice",
        started_at_utc=datetime(2026, 7, 13, tzinfo=timezone.utc),
        finished_at_utc=datetime(2026, 7, 13, tzinfo=timezone.utc),
        created_at_utc=datetime(2026, 7, 13, tzinfo=timezone.utc),
    )
    digest = {
        "digestType": "workbook",
        "status": "ready",
        "compareGroups": [
            {
                "groupId": "group-1",
                "rows": [
                    {
                        "featureCode": feature.feature_code,
                        "values": [
                            {
                                "rawValue": "●",
                                "source": {
                                    "sheetName": "T19C",
                                    "rowNumber": 12,
                                    "columnNumber": 4,
                                    "columnLetter": "D",
                                    "cell": "D12",
                                    "sourceCell": "D12",
                                    "mergedRange": None,
                                },
                            },
                            {
                                "rawValue": "",
                                "inferred": True,
                                "inferenceReason": "blank_as_not_equipped_by_eu_matrix_policy",
                                "confidence": 0.7,
                                "source": {
                                    "sheetName": "T19C",
                                    "rowNumber": 12,
                                    "columnNumber": 6,
                                    "columnLetter": "F",
                                    "cell": "F12",
                                    "sourceCell": "D12",
                                    "mergedRange": "D12:F12",
                                },
                            },
                        ],
                    }
                ],
            }
        ],
    }

    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, trim_id: trims.get(trim_id))
    monkeypatch.setattr(
        engineering_config.repo,
        "list_trim_feature_values",
        lambda _session, trim_id: [basic_value] if trim_id == basic_id else [premium_value],
    )
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: feature)
    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)
    monkeypatch.setattr(engineering_config, "build_source_digest", lambda _path, _name: digest)
    engineering_config._cached_source_evidence_index.cache_clear()
    engineering_config._cached_source_digest.cache_clear()

    payload = engineering_config.compare_trims(
        trim_ids=f"{basic_id},{premium_id}",
        differences_only=False,
        session=FakeSession({feature.feature_id: feature}),
        _=None,
    )

    cell = payload["rows"][0]["values"][1]
    assert cell["displayValue"] == "不配备*"
    assert cell["inferred"] is True
    assert cell["inferenceReason"] == "blank_as_not_equipped_by_eu_matrix_policy"
    assert cell["confidence"] == 0.7
    assert cell["source"]["sheetName"] == "T19C"
    assert cell["source"]["sourceCell"] == "D12"
    assert cell["source"]["mergedRange"] == "D12:F12"
    assert payload["summary"]["confirmedDifferenceCount"] == 1
    assert payload["summary"]["rawConfirmedDifferenceCount"] == 0
    assert payload["summary"]["inferredDifferenceCount"] == 1


def test_compare_trims_rejects_trashed_config_columns(monkeypatch) -> None:
    active_id = uuid4()
    trashed_id = uuid4()
    active_trim = FakeTrim(
        trim_id=active_id,
        full_trim_name="Active Basic",
        brand="OMODA",
        model_name="T19C",
        trim_name="Basic",
        market="Germany",
        model_year="2026",
        energy_type="ICE",
        drivetrain="FWD",
        engine=None,
        vehicle_code=None,
        material_no="MM001",
        identity_key="active-basic",
    )
    trashed_trim = FakeTrim(
        trim_id=trashed_id,
        full_trim_name="Trashed Premium",
        brand="OMODA",
        model_name="T19C",
        trim_name="Premium",
        market="Germany",
        model_year="2026",
        energy_type="ICE",
        drivetrain="FWD",
        engine=None,
        vehicle_code=None,
        material_no="MM002",
        identity_key="trashed-premium",
        status="trashed",
    )
    trims = {
        active_id: active_trim,
        trashed_id: trashed_trim,
    }

    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, trim_id: trims.get(trim_id))
    monkeypatch.setattr(engineering_config.repo, "list_trim_feature_values", lambda _session, _trim_id: [])

    with pytest.raises(HTTPException) as exc_info:
        engineering_config.compare_trims(
            trim_ids=f"{active_id},{trashed_id}",
            differences_only=False,
            session=FakeSession({}),
            _=None,
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == f"Trim not found: {trashed_id}"


def test_compare_trims_allows_cross_market_and_cross_model_selection(monkeypatch) -> None:
    own_id = uuid4()
    rival_id = uuid4()
    features = [
        FakeFeature(uuid4(), "seat_heat", "Comfort", "Seat heating", 1),
        FakeFeature(uuid4(), "premium_audio", "Infotainment", "Premium audio", 2),
    ]
    features_by_id = {feature.feature_id: feature for feature in features}
    features_by_code = {feature.feature_code: feature for feature in features}
    trims = {
        own_id: FakeTrim(
            trim_id=own_id,
            full_trim_name="OMODA T19C Comfort Germany",
            brand="OMODA",
            model_name="T19C",
            trim_name="Comfort",
            market="Germany",
            model_year="2026",
            energy_type="ICE",
            drivetrain="FWD",
            engine=None,
            vehicle_code="T19C-COMFORT-DE",
            material_no="T71607V**MM0002",
            identity_key="t19c-comfort-de",
        ),
        rival_id: FakeTrim(
            trim_id=rival_id,
            full_trim_name="Volvo EX30 Plus France",
            brand="Volvo",
            model_name="EX30",
            trim_name="Plus",
            market="France",
            model_year="2026",
            energy_type="BEV",
            drivetrain="RWD",
            engine=None,
            vehicle_code="EX30-PLUS-FR",
            material_no=None,
            identity_key="ex30-plus-fr",
        ),
    }
    values_by_trim = {
        own_id: [
            _value(features_by_code["seat_heat"], "●", "STANDARD"),
        ],
        rival_id: [
            _value(features_by_code["seat_heat"], "-", "NOT_AVAILABLE"),
            _value(features_by_code["premium_audio"], "●", "STANDARD"),
        ],
    }

    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, trim_id: trims.get(trim_id))
    monkeypatch.setattr(engineering_config.repo, "list_trim_feature_values", lambda _session, trim_id: values_by_trim[trim_id])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, code: features_by_code.get(code))

    payload = engineering_config.compare_trims(
        trim_ids=f"{own_id},{rival_id}",
        differences_only=True,
        session=FakeSession(features_by_id),
        _=None,
    )

    assert [trim["modelName"] for trim in payload["trims"]] == ["T19C", "EX30"]
    assert [trim["market"] for trim in payload["trims"]] == ["Germany", "France"]
    rows_by_code = {row["featureCode"]: row for row in payload["rows"]}
    assert rows_by_code["seat_heat"]["comparisonType"] == "UNIQUE_TO_TRIM"
    assert rows_by_code["seat_heat"]["uniqueTrimIds"] == [str(own_id)]
    assert rows_by_code["premium_audio"]["comparisonType"] == "MISSING_OR_UNKNOWN"
    assert payload["summary"]["differenceCount"] == 2


def test_compare_trims_returns_source_evidence_for_uploaded_values(monkeypatch) -> None:
    core_id = uuid4()
    ultra_id = uuid4()
    source_id = uuid4()
    feature = FakeFeature(uuid4(), "seat_heat", "Comfort", "Seat heating", 1)
    features_by_id = {feature.feature_id: feature}
    trims = {
        core_id: FakeTrim(
            trim_id=core_id,
            full_trim_name="Basic",
            brand="OMODA",
            model_name="T19C",
            trim_name="Basic",
            market="EU",
            model_year="2026",
            energy_type=None,
            drivetrain=None,
            engine=None,
            vehicle_code="Basic",
            material_no="MM001",
            identity_key="basic",
            source_upload_id=source_id,
        ),
        ultra_id: FakeTrim(
            trim_id=ultra_id,
            full_trim_name="Premium",
            brand="OMODA",
            model_name="T19C",
            trim_name="Premium",
            market="EU",
            model_year="2026",
            energy_type=None,
            drivetrain=None,
            engine=None,
            vehicle_code="Premium",
            material_no="MM002",
            identity_key="premium",
            source_upload_id=source_id,
        ),
    }
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="config.xlsx",
        source_file_path="/tmp/config.xlsx",
        source_file_hash="hash-1",
        import_status="draft_created",
        row_count=2,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime(2026, 7, 13, tzinfo=timezone.utc),
        finished_at_utc=datetime(2026, 7, 13, tzinfo=timezone.utc),
        created_at_utc=datetime(2026, 7, 13, tzinfo=timezone.utc),
    )
    core_value = _value(feature, "●", "STANDARD")
    core_value.source_upload_id = source_id
    core_value.source_row = 12
    core_value.source_column = "D12"
    premium_value = _value(feature, "O", "OPTIONAL")
    premium_value.source_upload_id = source_id
    premium_value.source_row = 12
    premium_value.source_column = "E12"

    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, trim_id: trims.get(trim_id))
    monkeypatch.setattr(
        engineering_config.repo,
        "list_trim_feature_values",
        lambda _session, trim_id: [core_value] if trim_id == core_id else [premium_value],
    )
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: feature)
    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)

    payload = engineering_config.compare_trims(
        trim_ids=f"{core_id},{ultra_id}",
        differences_only=False,
        session=FakeSession(features_by_id),
        _=None,
    )

    source = payload["rows"][0]["values"][0]["source"]
    assert payload["trims"][0]["sourceUploadId"] == str(source_id)
    assert payload["trims"][0]["sourceFileName"] == "config.xlsx"
    assert payload["trims"][0]["sourceCreatedAt"] == "2026-07-13T00:00:00+00:00"
    assert source == {
        "sheetName": "config.xlsx",
        "rowNumber": 12,
        "columnNumber": 0,
        "columnLetter": "D",
        "cell": "D12",
        "sourceCell": "D12",
        "mergedRange": None,
    }


def test_compare_trims_returns_text_and_ocr_source_coordinates(monkeypatch) -> None:
    core_id = uuid4()
    ultra_id = uuid4()
    source_id = uuid4()
    feature = FakeFeature(uuid4(), "rear_camera", "Drive assist", "Rear camera", 1)
    features_by_id = {feature.feature_id: feature}
    trims = {
        core_id: FakeTrim(
            trim_id=core_id,
            full_trim_name="Basic",
            brand="OMODA",
            model_name="T19C",
            trim_name="Basic",
            market="EU",
            model_year="2026",
            energy_type=None,
            drivetrain=None,
            engine=None,
            vehicle_code="Basic",
            material_no="MM001",
            identity_key="basic",
        ),
        ultra_id: FakeTrim(
            trim_id=ultra_id,
            full_trim_name="Premium",
            brand="OMODA",
            model_name="T19C",
            trim_name="Premium",
            market="EU",
            model_year="2026",
            energy_type=None,
            drivetrain=None,
            engine=None,
            vehicle_code="Premium",
            material_no="MM002",
            identity_key="premium",
        ),
    }
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="scanned-config.pdf",
        source_file_path="/tmp/scanned-config.pdf",
        source_file_hash="hash-ocr",
        import_status="draft_created",
        row_count=2,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    core_value = _value(feature, "●", "STANDARD")
    core_value.source_upload_id = source_id
    core_value.source_row = 3
    core_value.source_column = "P1OCRR3C4"
    premium_value = _value(feature, "O", "OPTIONAL")
    premium_value.source_upload_id = source_id
    premium_value.source_row = 4
    premium_value.source_column = "P1R4C5"

    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, trim_id: trims.get(trim_id))
    monkeypatch.setattr(
        engineering_config.repo,
        "list_trim_feature_values",
        lambda _session, trim_id: [core_value] if trim_id == core_id else [premium_value],
    )
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: feature)
    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)

    payload = engineering_config.compare_trims(
        trim_ids=f"{core_id},{ultra_id}",
        differences_only=False,
        session=FakeSession(features_by_id),
        _=None,
    )

    ocr_source = payload["rows"][0]["values"][0]["source"]
    text_source = payload["rows"][0]["values"][1]["source"]
    assert ocr_source == {
        "sheetName": "scanned-config.pdf",
        "rowNumber": 3,
        "columnNumber": 4,
        "columnLetter": "D",
        "cell": "P1OCRR3C4",
        "sourceCell": "P1OCRR3C4",
        "sourceType": "pdf_ocr",
        "pageNumber": 1,
        "mergedRange": None,
    }
    assert text_source == {
        "sheetName": "scanned-config.pdf",
        "rowNumber": 4,
        "columnNumber": 5,
        "columnLetter": "E",
        "cell": "P1R4C5",
        "sourceCell": "P1R4C5",
        "sourceType": "pdf_text",
        "pageNumber": 1,
        "mergedRange": None,
    }


def test_source_ref_payload_parses_image_ocr_coordinates(monkeypatch) -> None:
    source_id = uuid4()
    feature = FakeFeature(uuid4(), "wheel_size", "Exterior", "Wheel size", 1)
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="competitor-card.png",
        source_file_path="/tmp/competitor-card.png",
        source_file_hash="hash-image",
        import_status="draft_created",
        row_count=1,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    value = _value(feature, "18 inch", "VALUE")
    value.source_upload_id = source_id
    value.source_row = 5
    value.source_column = "OCRR5C2"
    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)

    source = engineering_config._source_ref_payload_for_value(value, FakeSession({}))

    assert source == {
        "sheetName": "competitor-card.png",
        "rowNumber": 5,
        "columnNumber": 2,
        "columnLetter": "B",
        "cell": "OCRR5C2",
        "sourceCell": "OCRR5C2",
        "sourceType": "image_ocr",
        "mergedRange": None,
    }


def test_list_trims_passes_query_for_floating_deck_search(monkeypatch) -> None:
    trim_id = uuid4()
    captured: dict[str, object] = {}
    count_captured: dict[str, object] = {}
    trim = FakeTrim(
        trim_id=trim_id,
        full_trim_name="Smart #1 Premium",
        brand="Smart",
        model_name="#1",
        trim_name="Premium",
        market="Germany",
        model_year="2026",
        energy_type="BEV",
        drivetrain="RWD",
        engine=None,
        vehicle_code="SV-PREMIUM",
        material_no=None,
        identity_key="SMART-1-PREMIUM-DE",
        source_upload_id=trim_id,
    )
    source_batch = FakeImportBatch(
        import_batch_id=trim_id,
        domain="engineering_config_source",
        source_file_name="smart-config.pdf",
        source_file_path="/tmp/smart-config.pdf",
        source_file_hash="hash-smart",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="alice",
        started_at_utc=datetime(2026, 7, 13, tzinfo=timezone.utc),
        finished_at_utc=datetime(2026, 7, 13, tzinfo=timezone.utc),
        created_at_utc=datetime(2026, 7, 13, tzinfo=timezone.utc),
    )

    def fake_list_vehicle_trims(_session, **kwargs):
        captured.update(kwargs)
        return [trim]

    def fake_count_vehicle_trims(_session, **kwargs):
        count_captured.update(kwargs)
        return 9

    monkeypatch.setattr(engineering_config.repo, "list_vehicle_trims", fake_list_vehicle_trims)
    monkeypatch.setattr(engineering_config.repo, "count_vehicle_trims", fake_count_vehicle_trims)
    monkeypatch.setattr(
        engineering_config.repo,
        "list_import_batches_by_ids",
        lambda _session, source_ids: {source_id: source_batch for source_id in source_ids},
    )

    payload = engineering_config.list_trims(
        q="smart-config.pdf",
        status=None,
        limit=80,
        session=FakeSession({}),
        _=None,
    )

    assert captured["query"] == "smart-config.pdf"
    assert captured["limit"] == 80
    assert captured["status"] is None
    assert count_captured["query"] == "smart-config.pdf"
    assert count_captured["status"] is None
    assert "limit" not in count_captured
    assert payload["rows"] == 9
    assert payload["items"][0]["trimId"] == str(trim_id)
    assert payload["items"][0]["salesVersion"] == "SV-PREMIUM"
    assert payload["items"][0]["sourceFileName"] == "smart-config.pdf"
    assert payload["items"][0]["sourceCreatedBy"] == "alice"
    assert payload["items"][0]["sourceCreatedAt"] == "2026-07-13T00:00:00+00:00"


def test_vehicle_trim_library_search_can_match_source_uploader() -> None:
    query_sql = str(
        engineering_config_repository._vehicle_trim_filtered_stmt(query="alice").compile(
            compile_kwargs={"literal_binds": True},
        )
    )
    source_sql = str(
        engineering_config_repository._vehicle_trim_filtered_stmt(source_query="alice").compile(
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "triggered_by" in query_sql
    assert "triggered_by" in source_sql


def test_feature_catalog_mapping_upsert_updates_aliases_and_creates_missing(monkeypatch) -> None:
    existing = FakeFeature(
        uuid4(),
        "driver_assist.camera_360",
        "驾驶辅助 Drive assist",
        "360 round view camera / 360度高清全景影像",
        10,
        aliases=["360 view"],
        seq=10,
    )
    fake_session = FakeImportSession()
    parsed_mapping = {
        "features": [
            {
                "seq": 20,
                "category": "驾驶辅助 Drive assist",
                "standard_field_name": "360 round view camera / 360度高清全景影像",
                "feature_code": "driver_assist.camera_360",
                "aliases": ["360 camera", "360 view"],
                "display_order": 20,
            },
            {
                "seq": 21,
                "category": "内饰 Interior",
                "standard_field_name": "Seat ventilation / 座椅通风",
                "feature_code": "interior.seat_ventilation",
                "aliases": ["Seat ventilation"],
                "display_order": 21,
            },
        ],
        "warnings": [],
        "categories": ["驾驶辅助 Drive assist", "内饰 Interior"],
    }

    def fake_get_by_category(_session, category, standard_field_name):
        if category == existing.category and standard_field_name == existing.standard_field_name:
            return existing
        return None

    def fake_get_by_code(_session, feature_code):
        if feature_code == existing.feature_code:
            return existing
        return None

    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_category_field", fake_get_by_category)
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", fake_get_by_code)

    summary = engineering_config._upsert_feature_catalog_from_mapping(fake_session, parsed_mapping)

    assert summary["totalFeatures"] == 2
    assert summary["updatedFeatureCount"] == 1
    assert summary["createdFeatureCount"] == 1
    assert existing.aliases == ["360 view", "360 camera"]
    assert existing.seq == 20
    assert existing.display_order == 20
    created_features = [item for item in fake_session.added if item.__class__.__name__ == "FeatureCatalog"]
    assert len(created_features) == 1
    assert created_features[0].standard_field_name == "Seat ventilation / 座椅通风"
    assert created_features[0].aliases == ["Seat ventilation"]


def test_feature_catalog_mapping_audit_records_upload_summary() -> None:
    summary = {
        "totalFeatures": 2,
        "updatedFeatureCount": 1,
        "createdFeatureCount": 1,
        "unchangedFeatureCount": 0,
        "warningCount": 1,
        "warnings": ["第 8 行缺少标准字段英文名。"],
        "categories": ["驾驶辅助 Drive assist"],
    }
    audit = engineering_config._build_feature_catalog_mapping_audit(
        {
            "uploadId": "feature-upload-1",
            "fileName": "配置字段映射表.xlsx",
        },
        summary,
        engineering_config.UserContext(role="editor", name="alice"),
        imported_at_utc="2026-07-05T12:00:00+00:00",
    )

    assert audit["uploadId"] == "feature-upload-1"
    assert audit["fileName"] == "配置字段映射表.xlsx"
    assert audit["importedBy"] == "alice"
    assert audit["importedRole"] == "editor"
    assert audit["artifactRef"] == "eng_config_uploads/feature-upload-1/session.json"
    assert audit["persistedIn"] == "upload_session_meta"
    assert audit["summary"] == summary


def test_list_trims_returns_shared_editable_columns_from_other_users(monkeypatch) -> None:
    source_id = uuid4()
    trim_id = uuid4()
    trim = FakeTrim(
        trim_id=trim_id,
        full_trim_name="XC60 Ultra",
        brand="Volvo",
        model_name="XC60",
        trim_name="Ultra",
        market="Germany",
        model_year="2026",
        energy_type="PHEV",
        drivetrain="AWD",
        engine=None,
        vehicle_code=None,
        material_no=None,
        identity_key="VOLVO-XC60-ULTRA-DE",
        source_upload_id=source_id,
        status="draft",
    )
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="alice-xc60-config.xlsx",
        source_file_path="/tmp/alice-xc60-config.xlsx",
        source_file_hash="hash-xc60",
        import_status="stored",
        row_count=12,
        error_count=0,
        triggered_by="alice",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    captured: dict[str, object] = {}
    count_captured: dict[str, object] = {}

    def fake_list_vehicle_trims(_session, **kwargs):
        captured.update(kwargs)
        return [trim]

    def fake_count_vehicle_trims(_session, **kwargs):
        count_captured.update(kwargs)
        return 1

    monkeypatch.setattr(engineering_config.repo, "list_vehicle_trims", fake_list_vehicle_trims)
    monkeypatch.setattr(engineering_config.repo, "count_vehicle_trims", fake_count_vehicle_trims)
    monkeypatch.setattr(
        engineering_config.repo,
        "list_import_batches_by_ids",
        lambda _session, source_ids: {source_id: source_batch for source_id in source_ids},
    )
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/engineering-config/trims?market=Germany&limit=10",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "bob"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert captured["market"] == "Germany"
        assert captured["status"] is None
        assert "created_by" not in captured
        assert count_captured["market"] == "Germany"
        assert "created_by" not in count_captured
        assert payload["rows"] == 1
        assert payload["items"][0]["trimId"] == str(trim_id)
        assert payload["items"][0]["sourceUploadId"] == str(source_id)
        assert payload["items"][0]["sourceFileName"] == "alice-xc60-config.xlsx"
        assert payload["items"][0]["sourceCreatedBy"] == "alice"
        assert payload["items"][0]["dataOrigin"] == "external_or_scraped"
    finally:
        app.dependency_overrides.clear()


def test_list_trims_passes_trashed_status_only_for_trash_scope(monkeypatch) -> None:
    trim = FakeTrim(
        trim_id=uuid4(),
        full_trim_name="Trashed Premium",
        brand="Smart",
        model_name="#1",
        trim_name="Premium",
        market="Germany",
        model_year="2026",
        energy_type="BEV",
        drivetrain="RWD",
        engine=None,
        vehicle_code=None,
        material_no=None,
        identity_key="SMART-1-PREMIUM-DE",
        status="trashed",
    )
    captured: dict[str, object] = {}
    count_captured: dict[str, object] = {}

    def fake_list_vehicle_trims(_session, **kwargs):
        captured.update(kwargs)
        return [trim]

    def fake_count_vehicle_trims(_session, **kwargs):
        count_captured.update(kwargs)
        return 1

    monkeypatch.setattr(engineering_config.repo, "list_vehicle_trims", fake_list_vehicle_trims)
    monkeypatch.setattr(engineering_config.repo, "count_vehicle_trims", fake_count_vehicle_trims)

    payload = engineering_config.list_trims(
        market="Germany",
        status="trashed",
        session=FakeSession({}),
        _=None,
    )

    assert captured["market"] == "Germany"
    assert captured["status"] == "trashed"
    assert count_captured["market"] == "Germany"
    assert count_captured["status"] == "trashed"
    assert payload["rows"] == 1
    assert payload["items"][0]["status"] == "trashed"


def test_list_trims_rows_report_total_before_limit(monkeypatch) -> None:
    trims = [
        FakeTrim(
            trim_id=uuid4(),
            full_trim_name=f"Model Trim {index}",
            brand="Brand",
            model_name="Model",
            trim_name=f"Trim {index}",
            market="Germany",
            model_year="2026",
            energy_type="BEV",
            drivetrain="RWD",
            engine=None,
            vehicle_code=None,
            material_no=f"MAT-{index}",
            identity_key=None,
        )
        for index in range(2)
    ]

    monkeypatch.setattr(engineering_config.repo, "list_vehicle_trims", lambda _session, **_kwargs: trims)
    monkeypatch.setattr(engineering_config.repo, "count_vehicle_trims", lambda _session, **_kwargs: 12)

    payload = engineering_config.list_trims(
        brand="Brand",
        limit=2,
        session=FakeSession({}),
        _=None,
    )

    assert payload["rows"] == 12
    assert len(payload["items"]) == 2
    assert [item["materialNo"] for item in payload["items"]] == ["MAT-0", "MAT-1"]


def test_update_vehicle_trim_can_soft_trash_config_column(monkeypatch) -> None:
    trim_id = uuid4()
    trim = FakeTrim(
        trim_id=trim_id,
        full_trim_name="Smart #1 Premium",
        brand="Smart",
        model_name="#1",
        trim_name="Premium",
        market="Germany",
        model_year="2026",
        energy_type="BEV",
        drivetrain="RWD",
        engine=None,
        vehicle_code="SV-PREMIUM",
        material_no=None,
        identity_key="SMART-1-PREMIUM-DE",
        status="active",
    )
    fake_session = FakeImportSession()

    def fake_update_vehicle_trim(_session, updated_trim_id, **kwargs):
        assert updated_trim_id == trim_id
        assert kwargs == {"status": "trashed"}
        trim.status = "trashed"
        return trim

    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(engineering_config.repo, "update_vehicle_trim", fake_update_vehicle_trim)

    payload = engineering_config.update_vehicle_trim(
        trim_id=str(trim_id),
        payload=engineering_config.VehicleTrimUpdate(
            status="trashed",
            comment="Move duplicate column to trash",
        ),
        session=fake_session,
        user=UserContext(role="editor", name="config-editor"),
    )

    assert fake_session.committed is True
    assert payload["trimId"] == str(trim_id)
    assert payload["status"] == "trashed"
    audit_rows = [item for item in fake_session.added if item.__class__.__name__ == "ConfigAuditLog"]
    assert len(audit_rows) == 1
    assert audit_rows[0].field_name == "status"
    assert audit_rows[0].old_value == "active"
    assert audit_rows[0].new_value == "trashed"
    assert audit_rows[0].changed_by == "config-editor"
    assert audit_rows[0].comment == "Move duplicate column to trash"


def test_update_vehicle_trim_can_edit_identity_fields(monkeypatch) -> None:
    trim_id = uuid4()
    trim = FakeTrim(
        trim_id=trim_id,
        full_trim_name="Omoda C9 Premium AWD",
        brand="Omoda",
        model_name="C9",
        trim_name="Premium AWD",
        market="Germany",
        model_year="2026",
        energy_type="PHEV",
        drivetrain="AWD",
        engine=None,
        vehicle_code="SV-PREMIUM",
        material_no=None,
        identity_key="C9-PREMIUM-DE",
        status="active",
    )
    fake_session = FakeImportSession()
    draft_version = type("DraftVersion", (), {
        "identity_key": trim.identity_key,
        "material_no": trim.material_no,
        "vehicle_code": trim.vehicle_code,
        "market": trim.market,
        "model_year": trim.model_year,
        "brand": trim.brand,
        "model_name": trim.model_name,
        "trim_name": trim.trim_name,
        "status": "draft",
    })()

    def fake_update_vehicle_trim(_session, updated_trim_id, **kwargs):
        assert updated_trim_id == trim_id
        assert kwargs == {
            "full_trim_name": "Omoda C9 Premium AWD 2026",
            "market": "France",
            "model_year": "2027",
            "material_no": "T71607V**MM0003",
            "vehicle_code": "SV-PREMIUM-2027",
            "identity_key": "C9-PREMIUM-FR",
        }
        trim.full_trim_name = kwargs["full_trim_name"]
        trim.market = kwargs["market"]
        trim.model_year = kwargs["model_year"]
        trim.material_no = kwargs["material_no"]
        trim.vehicle_code = kwargs["vehicle_code"]
        trim.identity_key = kwargs["identity_key"]
        return trim

    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(engineering_config.repo, "update_vehicle_trim", fake_update_vehicle_trim)
    monkeypatch.setattr(
        engineering_config.repo,
        "get_latest_config_version_for_trim",
        lambda _session, _trim_id, **_kwargs: draft_version,
    )

    payload = engineering_config.update_vehicle_trim(
        trim_id=str(trim_id),
        payload=engineering_config.VehicleTrimUpdate(
            full_trim_name="Omoda C9 Premium AWD 2026",
            market="France",
            model_year="2027",
            material_no="T71607V**MM0003",
            vehicle_code="SV-PREMIUM-2027",
            identity_key="C9-PREMIUM-FR",
            comment="Correct 2027 France identity",
        ),
        session=fake_session,
        user=UserContext(role="editor", name="identity-editor"),
    )

    assert fake_session.committed is True
    assert payload["trimId"] == str(trim_id)
    assert payload["market"] == "France"
    assert payload["country"] == "France"
    assert payload["modelYear"] == "2027"
    assert payload["materialNo"] == "T71607V**MM0003"
    assert payload["vehicleCode"] == "SV-PREMIUM-2027"
    assert payload["identityKey"] == "C9-PREMIUM-FR"
    assert payload["hasMaterialNo"] is True
    assert payload["dataOrigin"] == "own_catalog"
    assert draft_version.market == "France"
    assert draft_version.model_year == "2027"
    assert draft_version.material_no == "T71607V**MM0003"
    assert draft_version.vehicle_code == "SV-PREMIUM-2027"
    assert draft_version.identity_key == "C9-PREMIUM-FR"
    audit_rows = [item for item in fake_session.added if item.__class__.__name__ == "ConfigAuditLog"]
    assert {item.field_name for item in audit_rows} == {
        "full_trim_name",
        "market",
        "model_year",
        "material_no",
        "vehicle_code",
        "identity_key",
    }
    assert {item.changed_by for item in audit_rows} == {"identity-editor"}
    assert {item.comment for item in audit_rows} == {"Correct 2027 France identity"}


def test_update_vehicle_trim_returns_unchanged_without_audit(monkeypatch) -> None:
    trim_id = uuid4()
    trim = FakeTrim(
        trim_id=trim_id,
        full_trim_name="Omoda C9 Premium AWD",
        brand="Omoda",
        model_name="C9",
        trim_name="Premium AWD",
        market="Germany",
        model_year="2026",
        energy_type="PHEV",
        drivetrain="AWD",
        engine=None,
        vehicle_code=None,
        material_no=None,
        identity_key=None,
        status="draft",
    )
    fake_session = FakeImportSession()
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(
        engineering_config.repo,
        "update_vehicle_trim",
        lambda *_args, **_kwargs: pytest.fail("No-op update must not reach repository"),
    )

    payload = engineering_config.update_vehicle_trim(
        trim_id=str(trim_id),
        payload=engineering_config.VehicleTrimUpdate(
            model_year="2026",
            comment="Confirm current model year",
        ),
        session=fake_session,
        user=UserContext(role="editor", name="identity-editor"),
    )

    assert payload["unchanged"] is True
    assert fake_session.committed is False
    assert fake_session.added == []


def test_update_vehicle_trim_can_clear_optional_identity_field(monkeypatch) -> None:
    trim_id = uuid4()
    trim = FakeTrim(
        trim_id=trim_id,
        full_trim_name="Omoda C9 Premium AWD",
        brand="Omoda",
        model_name="C9",
        trim_name="Premium AWD",
        market="Germany",
        model_year="2026",
        energy_type="PHEV",
        drivetrain="AWD",
        engine=None,
        vehicle_code="SV-PREMIUM",
        material_no="MAT-PREMIUM",
        identity_key="C9-PREMIUM-DE",
        status="draft",
    )
    fake_session = FakeImportSession()

    def fake_update_vehicle_trim(_session, updated_trim_id, **kwargs):
        assert updated_trim_id == trim_id
        assert kwargs == {"material_no": None}
        trim.material_no = kwargs["material_no"]
        return trim

    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim", lambda _session, _trim_id: trim)
    monkeypatch.setattr(engineering_config.repo, "update_vehicle_trim", fake_update_vehicle_trim)

    payload = engineering_config.update_vehicle_trim(
        trim_id=str(trim_id),
        payload=engineering_config.VehicleTrimUpdate(
            material_no="",
            comment="Remove invalid material number",
        ),
        session=fake_session,
        user=UserContext(role="editor", name="identity-editor"),
    )

    assert payload["materialNo"] is None
    audit_rows = [item for item in fake_session.added if item.__class__.__name__ == "ConfigAuditLog"]
    assert len(audit_rows) == 1
    assert audit_rows[0].field_name == "material_no"
    assert audit_rows[0].old_value == "MAT-PREMIUM"
    assert audit_rows[0].new_value is None


def test_local_digest_ai_facts_are_rebuilt_from_server_workbook(monkeypatch) -> None:
    digest = {
        "modelName": "T19C",
        "compareGroups": [
            {
                "groupId": "t19c",
                "modelName": "T19C",
                "summary": {"totalFeatures": 1, "shownFeatures": 1},
                "trims": [
                    {"trimId": "basic", "trimName": "Basic", "materialNo": "MAT-BASIC"},
                    {"trimId": "premium", "trimName": "Premium", "materialNo": "MAT-PREMIUM"},
                ],
                "rows": [
                    {
                        "featureCode": "camera_360",
                        "featureName": "360 camera",
                        "category": "Parking",
                        "comparisonType": "UNIQUE_TO_TRIM",
                        "uniqueTrimIds": ["premium"],
                        "businessNote": "Premium adds 360 camera",
                        "values": [
                            {
                                "rawValue": "-",
                                "displayValue": "Not available",
                                "availability": "NOT_AVAILABLE",
                                "valueState": "NOT_AVAILABLE",
                                "source": {"sheetName": "T19C", "cell": "D10"},
                            },
                            {
                                "rawValue": "●",
                                "displayValue": "Standard",
                                "availability": "STANDARD",
                                "valueState": "STANDARD",
                                "source": {"sheetName": "T19C", "cell": "E10"},
                            },
                        ],
                    }
                ],
            }
        ],
    }
    captured_facts: list[dict] = []
    monkeypatch.setattr(engineering_config, "_load_local_source_workbook_digest", lambda _file_name: digest)
    monkeypatch.setattr(
        engineering_config,
        "compose_engineering_config_business_summary",
        lambda facts: captured_facts.append(facts) or {"summaries": [], "usage": {"status": "ok"}},
    )
    payload = engineering_config.EngineeringConfigBusinessSummaryComposeRequest(
        trimIds=["basic", "premium"],
        baseTrimId="basic",
        versionScope="published",
        factSource={
            "kind": "local_workbook_digest",
            "fileName": "compare-sample.xlsx",
            "groupId": "t19c",
        },
    )

    result = engineering_config.compose_business_summary(
        payload=payload,
        session=FakeImportSession(),
        _=None,
    )

    assert result["usage"]["status"] == "ok"
    assert len(captured_facts) == 1
    assert captured_facts[0]["baseTrim"]["trimId"] == "basic"
    assert captured_facts[0]["targets"][0]["targetTrimId"] == "premium"
    assert captured_facts[0]["targets"][0]["addedFeatures"] == ["360 camera"]
    assert captured_facts[0]["targets"][0]["evidenceFacts"][0]["evidenceKey"] == "premium:ADDED:camera_360"


def test_local_digest_export_facts_use_server_rows(monkeypatch) -> None:
    digest = {
        "modelName": "T19C",
        "compareGroups": [
            {
                "groupId": "t19c",
                "modelName": "T19C",
                "summary": {"totalFeatures": 1, "shownFeatures": 1},
                "trims": [
                    {"trimId": "basic", "trimName": "Basic"},
                    {"trimId": "premium", "trimName": "Premium"},
                ],
                "rows": [
                    {
                        "featureCode": "seat_heat",
                        "featureName": "Seat heating",
                        "category": "Comfort",
                        "comparisonType": "UNIQUE_TO_TRIM",
                        "uniqueTrimIds": ["premium"],
                        "values": [
                            {"rawValue": "-", "availability": "NOT_AVAILABLE", "displayValue": "Not available"},
                            {"rawValue": "●", "availability": "STANDARD", "displayValue": "Standard"},
                        ],
                    }
                ],
            }
        ],
    }
    monkeypatch.setattr(engineering_config, "_load_local_source_workbook_digest", lambda _file_name: digest)
    payload = engineering_config.EngineeringConfigCompareFactRequest(
        trimIds=["basic", "premium"],
        baseTrimId="basic",
        versionScope="published",
        factSource={
            "kind": "local_workbook_digest",
            "fileName": "compare-sample.xlsx",
            "groupId": "t19c",
        },
        filters={"includeBusinessSummary": False},
    )

    facts = engineering_config._canonical_export_facts(FakeImportSession(), payload)

    assert [trim["trimId"] for trim in facts["trims"]] == ["basic", "premium"]
    assert facts["rows"][0]["featureCode"] == "seat_heat"
    assert [value["rawValue"] for value in facts["rows"][0]["values"]] == ["-", "●"]


def test_clear_vehicle_trim_trash_requires_market_and_clears_country_scope(monkeypatch) -> None:
    fake_session = FakeImportSession()
    cleared_markets: list[str] = []

    def fake_clear_vehicle_trim_trash(_session, *, market: str) -> int:
        cleared_markets.append(market)
        return 3

    monkeypatch.setattr(engineering_config.repo, "clear_vehicle_trim_trash", fake_clear_vehicle_trim_trash)

    with pytest.raises(HTTPException) as missing_market:
        engineering_config.clear_trim_trash(
            market=None,
            session=fake_session,
            user=UserContext(role="editor", name="tester"),
        )

    assert missing_market.value.status_code == 400
    assert missing_market.value.detail == "market is required to clear trim trash"
    assert cleared_markets == []

    payload = engineering_config.clear_trim_trash(
        market=" Germany ",
        session=fake_session,
        user=UserContext(role="editor", name="tester"),
    )

    assert fake_session.committed is True
    assert payload["cleared"] == 3
    assert payload["market"] == "Germany"
    assert cleared_markets == ["Germany"]


def test_source_upload_stores_pdf_as_import_batch(tmp_path, monkeypatch) -> None:
    fake_session = FakeImportSession()
    monkeypatch.setattr(engineering_config, "UPLOAD_SESSION_DIR", tmp_path)
    monkeypatch.setattr(
        engineering_config.repo,
        "get_import_batch_by_hash",
        lambda _session, _domain, _source_file_hash: None,
    )
    app.dependency_overrides[get_db_session] = lambda: fake_session
    try:
        client = TestClient(app)
        headers = {"X-Auth-Token": "change-me", "X-User-Name": "tester"}
        init = client.post(
            "/v1/engineering-config/source/upload/initiate?file_name=omoda9.pdf&total_size=11&chunk_size=1024",
            headers=headers,
        )
        assert init.status_code == 200
        upload_id = init.json()["uploadId"]
        part = client.put(
            f"/v1/engineering-config/source/upload/{upload_id}/parts/0",
            headers=headers,
            content=b"%PDF-1.4 ok",
        )
        assert part.status_code == 200
        complete = client.post(
            f"/v1/engineering-config/source/upload/{upload_id}/complete",
            headers=headers,
            json={
                "relatedContext": {
                    "brand": "Volvo",
                    "model": "XC60",
                    "market": "Germany",
                    "powertrain": "PHEV",
                    "modelYear": "2026",
                    "trimIds": ["core", "ultra"],
                    "salesVersionIds": ["core-sv", "ultra-sv"],
                    "scenario": "own_vs_competitor",
                    "identityAnchor": "brand_model_market",
                }
            },
        )
        assert complete.status_code == 200
        payload = complete.json()
        assert payload["fileType"] == "pdf"
        assert payload["uploadStatus"] == "registered"
        assert payload["extractStatus"] == "pending"
        assert payload["nextAction"] == "extractor_pending"
        assert payload["createdBy"] == "static-editor"
        assert payload["linkedToCurrentContext"] is True
        assert payload["relatedContext"] == {
            "brand": "Volvo",
            "model": "XC60",
            "market": "Germany",
            "country": "Germany",
            "powertrain": "PHEV",
            "segment": None,
            "modelYear": "2026",
            "trimIds": ["core", "ultra"],
            "salesVersionIds": ["core-sv", "ultra-sv"],
                "contextType": "compare",
                "scenario": "own_vs_competitor",
                "identityAnchor": "brand_model_market",
                "sourceUrl": None,
                "sourceRole": None,
                "documentType": None,
                "effectiveFrom": None,
                "effectiveTo": None,
            }
        assert len(payload["contexts"]) == 1
        assert payload["contexts"][0]["contextType"] == "compare"
        assert payload["contexts"][0]["scenario"] == "own_vs_competitor"
        assert payload["contexts"][0]["identityAnchor"] == "brand_model_market"
        assert payload["parseMode"] == "stored_source"
        assert fake_session.committed is True
        assert len(fake_session.added) == 2
        batch = fake_session.added[0]
        link = fake_session.added[1]
        assert batch.domain == "engineering_config_source"
        assert batch.source_file_name == "omoda9.pdf"
        assert batch.import_status == "stored"
        assert batch.triggered_by == "static-editor"
        assert link.brand == "Volvo"
        assert link.model_name == "XC60"
        assert link.trim_ids == ["core", "ultra"]
        assert link.scenario == "own_vs_competitor"
        assert link.identity_anchor == "brand_model_market"
    finally:
        app.dependency_overrides.clear()


def test_create_draft_from_source_digest_group_creates_editable_config(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "config.xlsx"
    source_path.write_bytes(b"PK\x03\x04")
    source_id = uuid4()
    fake_session = FakeImportSession()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="config.xlsx",
        source_file_path=str(source_path),
        source_file_hash="hash-1",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    digest = {
        "digestType": "workbook",
        "status": "ready",
        "compareGroups": [
            {
                "groupId": "group-1",
                "modelName": "T19C",
                "trims": [
                    {
                        "trimId": "digest-trim-1",
                        "trimName": "Basic",
                        "fullTrimName": "Basic / MM001",
                        "modelName": "T19C",
                        "market": "EU",
                        "modelYear": "2026",
                        "materialNo": "MM001",
                        "salesVersion": "Basic",
                        "profile": {"brand": "OMODA", "country": "EU", "powertrain": "BEV", "drivetrain": "FWD", "engine": "100kWh"},
                    },
                    {
                        "trimId": "digest-trim-2",
                        "trimName": "Premium",
                        "fullTrimName": "Premium / MM002",
                        "modelName": "T19C",
                        "market": "EU",
                        "modelYear": "2026",
                        "materialNo": "MM002",
                        "salesVersion": "Premium",
                        "profile": {"brand": "OMODA", "country": "EU", "powertrain": "BEV", "drivetrain": "AWD", "engine": "100kWh"},
                    },
                ],
                "rows": [
                    {
                        "category": "舒适便利",
                        "featureCode": "digest_seat_heat",
                        "featureName": "Seat heating / 座椅加热",
                        "values": [
                            {
                                "rawValue": "●",
                                "normalizedValue": "standard",
                                "availability": "STANDARD",
                                "unit": None,
                                "source": {"rowNumber": 12, "cell": "D12", "columnLetter": "D"},
                            },
                            {
                                "rawValue": "O",
                                "normalizedValue": "optional",
                                "availability": "OPTIONAL",
                                "unit": None,
                                "source": {"rowNumber": 12, "cell": "E12", "columnLetter": "E"},
                            },
                        ],
                    }
                ],
            }
        ],
    }

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)
    monkeypatch.setattr(engineering_config, "build_source_digest", lambda _path, _name: digest)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config.repo, "list_feature_catalog", lambda _session, **_kwargs: [])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: None)
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_category_field", lambda _session, _category, _field: None)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_full_name", lambda _session, _full_name: None)
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value_by_trim_feature", lambda _session, _trim_id, _feature_id: None)

    payload = engineering_config.create_draft_from_source_digest_group(
        source_id=source_id,
        group_id="group-1",
        payload=None,
        session=fake_session,
        user=UserContext(role="editor", name="tester"),
    )

    assert payload["sourceId"] == str(source_id)
    assert payload["groupId"] == "group-1"
    assert payload["sourceFileName"] == "config.xlsx"
    assert payload["groupTitle"] == "T19C"
    assert payload["sourceDigestType"] == "workbook"
    assert payload["sourceFormat"] == "workbook"
    assert payload["sourceKind"] == "config_matrix"
    assert payload["trimCount"] == 2
    assert payload["createdTrimCount"] == 2
    assert payload["featureCount"] == 1
    assert payload["createdFeatureCount"] == 1
    assert payload["aliasMatchedFeatureCount"] == 0
    assert payload["semanticAliasMatchedFeatureCount"] == 0
    assert payload["featureMatchReasonCounts"] == {"created": 1}
    assert payload["featureMatchSamples"] == []
    assert payload["valueRecordCount"] == 2
    assert payload["insertedValueCount"] == 2
    assert len(payload["compareTrimIds"]) == 2
    assert fake_session.committed is True
    assert any(item.__class__.__name__ == "FeatureCatalog" for item in fake_session.added)
    assert sum(1 for item in fake_session.added if item.__class__.__name__ == "VehicleTrim") == 2
    created_trims = [item for item in fake_session.added if item.__class__.__name__ == "VehicleTrim"]
    assert [(trim.energy_type, trim.drivetrain, trim.engine) for trim in created_trims] == [
        ("BEV", "FWD", "100kWh"),
        ("BEV", "AWD", "100kWh"),
    ]
    assert sum(1 for item in fake_session.added if item.__class__.__name__ == "TrimFeatureValue") == 2
    versions = [item for item in fake_session.added if item.__class__.__name__ == "ConfigVersion"]
    assert len(versions) == 2
    assert {version.version_no for version in versions} == {1}
    assert {version.snapshot_feature_count for version in versions} == {1}
    assert {version.snapshot_values[0]["featureCode"] for version in versions} == {"digest_seat_heat"}
    assert {version.snapshot_values[0]["sourceUploadId"] for version in versions} == {str(source_id)}


def test_create_draft_from_source_digest_group_uses_selected_trim_subset(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "config.xlsx"
    source_path.write_bytes(b"PK\x03\x04")
    source_id = uuid4()
    fake_session = FakeImportSession()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="config.xlsx",
        source_file_path=str(source_path),
        source_file_hash="hash-1",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    digest = {
        "digestType": "workbook",
        "status": "ready",
        "compareGroups": [
            {
                "groupId": "group-1",
                "modelName": "T19C",
                "trims": [
                    {"trimId": "digest-basic", "trimName": "Basic", "fullTrimName": "Basic / MM001", "modelName": "T19C", "market": "EU", "materialNo": "MM001"},
                    {"trimId": "digest-comfort", "trimName": "Comfort", "fullTrimName": "Comfort / MM002", "modelName": "T19C", "market": "EU", "materialNo": "MM002"},
                    {"trimId": "digest-premium", "trimName": "Premium", "fullTrimName": "Premium / MM003", "modelName": "T19C", "market": "EU", "materialNo": "MM003"},
                ],
                "rows": [
                    {
                        "category": "舒适便利",
                        "featureCode": "digest_camera",
                        "featureName": "Camera",
                        "values": [
                            {"rawValue": "-", "normalizedValue": None, "availability": "NOT_AVAILABLE", "unit": None, "source": {"rowNumber": 12, "cell": "D12", "columnLetter": "D"}},
                            {"rawValue": "O", "normalizedValue": "optional", "availability": "OPTIONAL", "unit": None, "source": {"rowNumber": 12, "cell": "E12", "columnLetter": "E"}},
                            {"rawValue": "●", "normalizedValue": "standard", "availability": "STANDARD", "unit": None, "source": {"rowNumber": 12, "cell": "F12", "columnLetter": "F"}},
                        ],
                    }
                ],
            }
        ],
    }

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)
    monkeypatch.setattr(engineering_config, "build_source_digest", lambda _path, _name: digest)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config.repo, "list_feature_catalog", lambda _session, **_kwargs: [])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: None)
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_category_field", lambda _session, _category, _field: None)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_full_name", lambda _session, _full_name: None)
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value_by_trim_feature", lambda _session, _trim_id, _feature_id: None)

    payload = engineering_config.create_draft_from_source_digest_group(
        source_id=source_id,
        group_id="group-1",
        payload=engineering_config.SourceDigestDraftCreate(trim_ids=["digest-basic", "digest-premium"]),
        session=fake_session,
        user=UserContext(role="editor", name="tester"),
    )

    assert payload["trimCount"] == 2
    assert len(payload["compareTrimIds"]) == 2
    assert payload["valueRecordCount"] == 2
    created_trims = [item for item in fake_session.added if item.__class__.__name__ == "VehicleTrim"]
    assert [trim.trim_name for trim in created_trims] == ["Basic", "Premium"]
    created_values = [item for item in fake_session.added if item.__class__.__name__ == "TrimFeatureValue"]
    assert [value.raw_value for value in created_values] == ["-", "●"]
    assert [value.source_column for value in created_values] == ["D12", "F12"]


def test_create_draft_from_ocr_headerless_group_applies_trim_identity_overrides(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "ocr-config.png"
    source_path.write_bytes(b"\x89PNG\r\n\x1a\n")
    source_id = uuid4()
    fake_session = FakeImportSession()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="ocr-config.png",
        source_file_path=str(source_path),
        source_file_hash="hash-ocr",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    digest = {
        "digestType": "image_ocr",
        "sourceFormat": "image_ocr",
        "status": "ready",
        "compareGroups": [
            {
                "groupId": "ocr-headerless",
                "title": "OCR Headerless",
                "sourceKind": "ocr_headerless",
                "identityStatus": "temporary_ocr_column",
                "modelName": "OCR Headerless",
                "trims": [
                    {
                        "trimId": "ocr-column-1",
                        "trimName": "OCR Column 1",
                        "fullTrimName": "OCR Column 1 · 待补配置列身份",
                        "modelName": "OCR Headerless",
                        "identityStatus": "temporary_ocr_column",
                    },
                    {
                        "trimId": "ocr-column-2",
                        "trimName": "OCR Column 2",
                        "fullTrimName": "OCR Column 2 · 待补配置列身份",
                        "modelName": "OCR Headerless",
                        "identityStatus": "temporary_ocr_column",
                    },
                ],
                "rows": [
                    {
                        "category": "Safety",
                        "featureCode": "digest_camera",
                        "featureName": "360 camera",
                        "values": [
                            {"rawValue": "-", "normalizedValue": None, "availability": "NOT_AVAILABLE", "unit": None, "source": {"rowNumber": 2, "cell": "B2", "columnLetter": "B"}},
                            {"rawValue": "●", "normalizedValue": "standard", "availability": "STANDARD", "unit": None, "source": {"rowNumber": 2, "cell": "C2", "columnLetter": "C"}},
                        ],
                    }
                ],
            }
        ],
    }

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)
    monkeypatch.setattr(engineering_config, "build_source_digest", lambda _path, _name: digest)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config.repo, "list_feature_catalog", lambda _session, **_kwargs: [])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: None)
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_category_field", lambda _session, _category, _field: None)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_full_name", lambda _session, _full_name: None)
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value_by_trim_feature", lambda _session, _trim_id, _feature_id: None)

    payload = engineering_config.create_draft_from_source_digest_group(
        source_id=source_id,
        group_id="ocr-headerless",
        payload=engineering_config.SourceDigestDraftCreate(
            trim_ids=["ocr-column-1", "ocr-column-2"],
            trim_identity_overrides=[
                {
                    "trim_id": "ocr-column-1",
                    "brand": "OMODA",
                    "model_name": "T19C MY ICE",
                    "trim_name": "Comfort-FWD",
                    "full_trim_name": "OMODA T19C MY ICE Comfort-FWD",
                    "market": "Germany",
                    "model_year": "2026",
                    "energy_type": "ICE",
                    "drivetrain": "FWD",
                    "engine": "1.6TGDI",
                    "sales_version": "Comfort-FWD",
                },
                {
                    "trim_id": "ocr-column-2",
                    "brand": "OMODA",
                    "model_name": "T19C MY ICE",
                    "trim_name": "Premium-FWD",
                    "full_trim_name": "OMODA T19C MY ICE Premium-FWD",
                    "market": "Germany",
                    "model_year": "2026",
                    "energy_type": "ICE",
                    "drivetrain": "FWD",
                    "engine": "1.6TGDI",
                    "sales_version": "Premium-FWD",
                },
            ],
        ),
        session=fake_session,
        user=UserContext(role="editor", name="tester"),
    )

    assert payload["trimCount"] == 2
    created_trims = [item for item in fake_session.added if item.__class__.__name__ == "VehicleTrim"]
    assert [trim.trim_name for trim in created_trims] == ["Comfort-FWD", "Premium-FWD"]
    assert [trim.full_trim_name for trim in created_trims] == [
        "OMODA T19C MY ICE Comfort-FWD",
        "OMODA T19C MY ICE Premium-FWD",
    ]
    assert [trim.brand for trim in created_trims] == ["OMODA", "OMODA"]
    assert [trim.model_name for trim in created_trims] == ["T19C MY ICE", "T19C MY ICE"]
    assert [trim.market for trim in created_trims] == ["Germany", "Germany"]
    assert [trim.model_year for trim in created_trims] == ["2026", "2026"]
    assert [(trim.energy_type, trim.drivetrain, trim.engine) for trim in created_trims] == [
        ("ICE", "FWD", "1.6TGDI"),
        ("ICE", "FWD", "1.6TGDI"),
    ]
    assert [trim.vehicle_code for trim in created_trims] == ["Comfort-FWD", "Premium-FWD"]


def test_create_draft_from_tabular_source_digest_group(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "competitor.csv"
    source_path.write_text("Category,Feature,Basic,Premium\nSafety,Blind spot,-,S\n", encoding="utf-8")
    source_id = uuid4()
    fake_session = FakeImportSession()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="competitor.csv",
        source_file_path=str(source_path),
        source_file_hash="hash-csv",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    digest = {
        "digestType": "tabular",
        "status": "ready",
        "compareGroups": [
            {
                "groupId": "tabular-simple-competitor",
                "modelName": "competitor",
                "trims": [
                    {
                        "trimId": "csv-basic",
                        "trimName": "Basic",
                        "fullTrimName": "Basic",
                        "modelName": "competitor",
                        "market": "Germany",
                    },
                    {
                        "trimId": "csv-premium",
                        "trimName": "Premium",
                        "fullTrimName": "Premium",
                        "modelName": "competitor",
                        "market": "Germany",
                    },
                ],
                "rows": [
                    {
                        "category": "Safety",
                        "featureCode": "digest_blind_spot",
                        "featureName": "Blind spot",
                        "values": [
                            {
                                "rawValue": "-",
                                "normalizedValue": None,
                                "availability": "NOT_AVAILABLE",
                                "unit": None,
                                "source": {"rowNumber": 2, "cell": "C2", "columnLetter": "C"},
                            },
                            {
                                "rawValue": "S",
                                "normalizedValue": "standard",
                                "availability": "STANDARD",
                                "unit": None,
                                "source": {"rowNumber": 2, "cell": "D2", "columnLetter": "D"},
                            },
                        ],
                    }
                ],
            }
        ],
    }
    context_link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="Rival",
        model_name="C-SUV",
        model_year="2026",
        market="Germany",
        country="Germany",
        trim_ids=[],
        sales_version_ids=[],
        context_type="compare",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
    )

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)
    monkeypatch.setattr(engineering_config, "build_source_digest", lambda _path, _name: digest)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [context_link])
    monkeypatch.setattr(engineering_config.repo, "list_feature_catalog", lambda _session, **_kwargs: [])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: None)
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_category_field", lambda _session, _category, _field: None)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_full_name", lambda _session, _full_name: None)
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value_by_trim_feature", lambda _session, _trim_id, _feature_id: None)

    payload = engineering_config.create_draft_from_source_digest_group(
        source_id=source_id,
        group_id="tabular-simple-competitor",
        payload=None,
        session=fake_session,
        user=UserContext(role="editor", name="tester"),
    )

    assert payload["trimCount"] == 2
    assert payload["featureCount"] == 1
    assert payload["insertedValueCount"] == 2
    assert fake_session.committed is True
    created_trims = [item for item in fake_session.added if item.__class__.__name__ == "VehicleTrim"]
    assert [(trim.brand, trim.model_name, trim.market, trim.model_year, trim.full_trim_name) for trim in created_trims] == [
        ("Rival", "C-SUV", "Germany", "2026", "C-SUV / Basic"),
        ("Rival", "C-SUV", "Germany", "2026", "C-SUV / Premium"),
    ]


def test_create_draft_from_trashed_source_snapshot_is_rejected(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "trashed-config.xlsx"
    source_path.write_bytes(b"PK\x03\x04")
    source_id = uuid4()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="trashed-config.xlsx",
        source_file_path=str(source_path),
        source_file_hash="hash-trashed",
        import_status="trashed",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)

    with pytest.raises(HTTPException) as exc_info:
        engineering_config.create_draft_from_source_digest_group(
            source_id=source_id,
            group_id="group-1",
            payload=None,
            session=FakeImportSession(),
            user=UserContext(role="editor", name="tester"),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Source snapshot is in trash; restore it before creating editable config columns"


def test_create_draft_from_trashed_source_context_is_rejected_before_digest(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "country-trashed-config.xlsx"
    source_path.write_bytes(b"PK\x03\x04")
    source_id = uuid4()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="country-trashed-config.xlsx",
        source_file_path=str(source_path),
        source_file_hash="hash-country-trashed",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    context_link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="Rival",
        model_name="C-SUV",
        model_year="2026",
        market="Germany",
        country="Germany",
        trim_ids=[],
        sales_version_ids=[],
        context_type="compare",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
        status="trashed",
    )

    def fail_digest(_path, _name):
        raise AssertionError("trashed source context must be rejected before digest parsing")

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [context_link])
    monkeypatch.setattr(engineering_config, "build_source_digest", fail_digest)

    with pytest.raises(HTTPException) as exc_info:
        engineering_config.create_draft_from_source_digest_group(
            source_id=source_id,
            group_id="group-1",
            payload=None,
            session=FakeImportSession(),
            user=UserContext(role="editor", name="tester"),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Source snapshot is in trash; restore it before creating editable config columns"


def test_create_draft_from_price_list_keeps_source_identity(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "competitor-price-list.csv"
    source_path.write_text(
        "\n".join(
            [
                "Brand,Model,Trim,Market,Model Year,Powertrain,MSRP,Currency",
                "OMODA,T19C,Basic,Germany,2026,ICE,23000,EUR",
                "OMODA,T19C,Premium,Germany,2026,ICE,28000,EUR",
            ]
        ),
        encoding="utf-8",
    )
    source_id = uuid4()
    fake_session = FakeImportSession()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="competitor-price-list.csv",
        source_file_path=str(source_path),
        source_file_hash="hash-price",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    context_link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="Rival",
        model_name="Wrong Context Model",
        model_year="2024",
        market="Spain",
        country="Spain",
        trim_ids=[],
        sales_version_ids=[],
        context_type="compare",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
    )

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [context_link])
    monkeypatch.setattr(engineering_config.repo, "list_feature_catalog", lambda _session, **_kwargs: [])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: None)
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_category_field", lambda _session, _category, _field: None)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_full_name", lambda _session, _full_name: None)
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value_by_trim_feature", lambda _session, _trim_id, _feature_id: None)

    payload = engineering_config.create_draft_from_source_digest_group(
        source_id=source_id,
        group_id="price-list-competitor-price-list-1",
        payload=None,
        session=fake_session,
        user=UserContext(role="editor", name="tester"),
    )

    assert payload["trimCount"] == 2
    assert payload["featureCount"] == 2
    assert payload["insertedValueCount"] == 4
    assert payload["sourceDigestType"] == "tabular"
    assert payload["sourceKind"] == "price_list"
    assert payload["groupTitle"] == "OMODA / T19C / Germany / 2026 / ICE / 价格单"
    created_trims = [item for item in fake_session.added if item.__class__.__name__ == "VehicleTrim"]
    assert [(trim.brand, trim.model_name, trim.market, trim.model_year, trim.full_trim_name) for trim in created_trims] == [
        ("OMODA", "T19C", "Germany", "2026", "OMODA / T19C / Basic"),
        ("OMODA", "T19C", "Germany", "2026", "OMODA / T19C / Premium"),
    ]


def test_create_draft_from_external_digest_keeps_same_trim_from_different_sources_separate(monkeypatch, tmp_path) -> None:
    source_a_path = tmp_path / "site-a-config.csv"
    source_b_path = tmp_path / "site-b-config.csv"
    source_a_path.write_text("site a", encoding="utf-8")
    source_b_path.write_text("site b", encoding="utf-8")
    source_a_id = uuid4()
    source_b_id = uuid4()
    batches = {
        source_a_id: FakeImportBatch(
            import_batch_id=source_a_id,
            domain=engineering_config.SOURCE_IMPORT_DOMAIN,
            source_file_name="site-a-config.csv",
            source_file_path=str(source_a_path),
            source_file_hash="hash-site-a",
            import_status="stored",
            row_count=0,
            error_count=0,
            triggered_by="tester",
            started_at_utc=datetime.now(timezone.utc),
            finished_at_utc=datetime.now(timezone.utc),
            created_at_utc=datetime.now(timezone.utc),
        ),
        source_b_id: FakeImportBatch(
            import_batch_id=source_b_id,
            domain=engineering_config.SOURCE_IMPORT_DOMAIN,
            source_file_name="site-b-config.csv",
            source_file_path=str(source_b_path),
            source_file_hash="hash-site-b",
            import_status="stored",
            row_count=0,
            error_count=0,
            triggered_by="tester",
            started_at_utc=datetime.now(timezone.utc),
            finished_at_utc=datetime.now(timezone.utc),
            created_at_utc=datetime.now(timezone.utc),
        ),
    }
    digest = {
        "digestType": "tabular",
        "status": "ready",
        "compareGroups": [
            {
                "groupId": "rival-config",
                "title": "OMODA / T19C / Germany / 2026 / 来源配置",
                "modelName": "T19C",
                "trims": [
                    {
                        "trimId": "comfort",
                        "trimName": "Comfort",
                        "fullTrimName": "OMODA / T19C / Comfort",
                        "modelName": "T19C",
                        "market": "Germany",
                        "modelYear": "2026",
                        "materialNo": "T19C-COMFORT-M001",
                        "dataOrigin": "external_or_scraped",
                        "profile": {"brand": "OMODA", "country": "Germany", "powertrain": "ICE"},
                    },
                    {
                        "trimId": "premium",
                        "trimName": "Premium",
                        "fullTrimName": "OMODA / T19C / Premium",
                        "modelName": "T19C",
                        "market": "Germany",
                        "modelYear": "2026",
                        "materialNo": "T19C-PREMIUM-M002",
                        "dataOrigin": "external_or_scraped",
                        "profile": {"brand": "OMODA", "country": "Germany", "powertrain": "ICE"},
                    },
                ],
                "rows": [
                    {
                        "category": "Comfort",
                        "featureCode": "digest_seat_heat",
                        "featureName": "Seat heating",
                        "values": [
                            {"rawValue": "-", "normalizedValue": None, "availability": "NOT_AVAILABLE", "unit": None},
                            {"rawValue": "S", "normalizedValue": "standard", "availability": "STANDARD", "unit": None},
                        ],
                    }
                ],
            }
        ],
    }
    existing_by_source_and_full_name: dict[tuple[UUID, str], object] = {}

    def fake_get_by_source_and_full_name(_session: object, source_id: UUID, full_name: str) -> object | None:
        return existing_by_source_and_full_name.get((source_id, full_name))

    def fake_add_trim(session: FakeImportSession, trim: object) -> None:
        session.add(trim)
        session.flush()
        existing_by_source_and_full_name[(getattr(trim, "source_upload_id"), getattr(trim, "full_trim_name"))] = trim

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, source_id: batches.get(source_id))
    monkeypatch.setattr(engineering_config, "build_source_digest", lambda _path, _name: digest)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config.repo, "list_feature_catalog", lambda _session, **_kwargs: [])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: None)
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_category_field", lambda _session, _category, _field: None)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_source_full_name", fake_get_by_source_and_full_name)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_config_version_source_full_name", lambda _session, _source_id, _full_name: None)
    monkeypatch.setattr(engineering_config.repo, "add_vehicle_trim", fake_add_trim)
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value_by_trim_feature", lambda _session, _trim_id, _feature_id: None)

    first_session = FakeImportSession()
    first_payload = engineering_config.create_draft_from_source_digest_group(
        source_id=source_a_id,
        group_id="rival-config",
        payload=None,
        session=first_session,
        user=UserContext(role="editor", name="tester"),
    )
    second_session = FakeImportSession()
    second_payload = engineering_config.create_draft_from_source_digest_group(
        source_id=source_b_id,
        group_id="rival-config",
        payload=None,
        session=second_session,
        user=UserContext(role="editor", name="tester"),
    )

    assert first_payload["createdTrimCount"] == 2
    assert second_payload["createdTrimCount"] == 2
    assert first_payload["reusedTrimCount"] == 0
    assert second_payload["reusedTrimCount"] == 0
    assert set(first_payload["trimIds"]).isdisjoint(set(second_payload["trimIds"]))
    first_full_names = [
        trim.full_trim_name
        for trim in first_session.added
        if trim.__class__.__name__ == "VehicleTrim"
    ]
    second_full_names = [
        trim.full_trim_name
        for trim in second_session.added
        if trim.__class__.__name__ == "VehicleTrim"
    ]
    assert first_full_names == ["OMODA / T19C / Comfort", "OMODA / T19C / Premium"]
    assert second_full_names == ["OMODA / T19C / Comfort", "OMODA / T19C / Premium"]
    assert {trim.source_upload_id for trim in first_session.added if trim.__class__.__name__ == "VehicleTrim"} == {source_a_id}
    assert {trim.source_upload_id for trim in second_session.added if trim.__class__.__name__ == "VehicleTrim"} == {source_b_id}


def test_create_draft_from_pdf_text_source_digest_group(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "competitor.pdf"
    source_path.write_bytes(b"%PDF-1.4 text table")
    source_id = uuid4()
    fake_session = FakeImportSession()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="competitor.pdf",
        source_file_path=str(source_path),
        source_file_hash="hash-pdf",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    digest = {
        "digestType": "pdf_text",
        "status": "ready",
        "compareGroups": [
            {
                "groupId": "tabular-simple-pdf-page-1",
                "modelName": "competitor",
                "trims": [
                    {
                        "trimId": "pdf-basic",
                        "trimName": "Basic",
                        "fullTrimName": "Basic",
                        "modelName": "competitor",
                        "market": "Germany",
                    },
                    {
                        "trimId": "pdf-premium",
                        "trimName": "Premium",
                        "fullTrimName": "Premium",
                        "modelName": "competitor",
                        "market": "Germany",
                    },
                ],
                "rows": [
                    {
                        "category": "Comfort",
                        "featureCode": "digest_seat_heating",
                        "featureName": "Seat heating",
                        "values": [
                            {
                                "rawValue": "-",
                                "normalizedValue": None,
                                "availability": "NOT_AVAILABLE",
                                "unit": None,
                                "source": {
                                    "sourceType": "pdf_text",
                                    "pageNumber": 1,
                                    "rowNumber": 3,
                                    "cell": "P1R3C3",
                                    "columnLetter": "C",
                                },
                            },
                            {
                                "rawValue": "S",
                                "normalizedValue": "standard",
                                "availability": "STANDARD",
                                "unit": None,
                                "source": {
                                    "sourceType": "pdf_text",
                                    "pageNumber": 1,
                                    "rowNumber": 3,
                                    "cell": "P1R3C4",
                                    "columnLetter": "D",
                                },
                            },
                        ],
                    }
                ],
            }
        ],
    }
    context_link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="Runtime",
        model_name="PDF Demo",
        model_year="2027",
        market="Germany",
        country="Germany",
        trim_ids=[],
        sales_version_ids=[],
        context_type="compare",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
    )

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)
    monkeypatch.setattr(engineering_config, "build_source_digest", lambda _path, _name: digest)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [context_link])
    monkeypatch.setattr(engineering_config.repo, "list_feature_catalog", lambda _session, **_kwargs: [])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: None)
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_category_field", lambda _session, _category, _field: None)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_full_name", lambda _session, _full_name: None)
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value_by_trim_feature", lambda _session, _trim_id, _feature_id: None)

    payload = engineering_config.create_draft_from_source_digest_group(
        source_id=source_id,
        group_id="tabular-simple-pdf-page-1",
        payload=None,
        session=fake_session,
        user=UserContext(role="editor", name="tester"),
    )

    assert payload["trimCount"] == 2
    assert payload["featureCount"] == 1
    assert payload["insertedValueCount"] == 2
    assert payload["sourceDigestType"] == "pdf_text"
    assert payload["sourceFormat"] == "pdf_text"
    assert fake_session.committed is True
    created_trims = [item for item in fake_session.added if item.__class__.__name__ == "VehicleTrim"]
    assert [(trim.brand, trim.model_name, trim.market, trim.model_year, trim.full_trim_name) for trim in created_trims] == [
        ("Runtime", "PDF Demo", "Germany", "2027", "PDF Demo / Basic"),
        ("Runtime", "PDF Demo", "Germany", "2027", "PDF Demo / Premium"),
    ]


def test_create_draft_from_pdf_ocr_source_digest_group(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "scanned-competitor.pdf"
    source_path.write_bytes(b"%PDF-1.4 scanned table")
    source_id = uuid4()
    fake_session = FakeImportSession()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="scanned-competitor.pdf",
        source_file_path=str(source_path),
        source_file_hash="hash-pdf-ocr",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    digest = {
        "digestType": "pdf_ocr",
        "status": "ready",
        "compareGroups": [
            {
                "groupId": "tabular-simple-pdf-ocr-page-1",
                "modelName": "scanned-competitor",
                "trims": [
                    {
                        "trimId": "pdf-ocr-basic",
                        "trimName": "Basic",
                        "fullTrimName": "Basic",
                        "modelName": "scanned-competitor",
                    },
                    {
                        "trimId": "pdf-ocr-premium",
                        "trimName": "Premium",
                        "fullTrimName": "Premium",
                        "modelName": "scanned-competitor",
                    },
                ],
                "rows": [
                    {
                        "category": "Safety",
                        "featureCode": "digest_360_camera",
                        "featureName": "360 camera",
                        "values": [
                            {
                                "rawValue": "-",
                                "normalizedValue": None,
                                "availability": "NOT_AVAILABLE",
                                "unit": None,
                                "source": {
                                    "sourceType": "pdf_ocr",
                                    "ocrEngine": "paddleocr",
                                    "pageNumber": 1,
                                    "rowNumber": 3,
                                    "cell": "P1OCRR3C3",
                                    "columnLetter": "C",
                                },
                            },
                            {
                                "rawValue": "S",
                                "normalizedValue": "standard",
                                "availability": "STANDARD",
                                "unit": None,
                                "source": {
                                    "sourceType": "pdf_ocr",
                                    "ocrEngine": "paddleocr",
                                    "pageNumber": 1,
                                    "rowNumber": 3,
                                    "cell": "P1OCRR3C4",
                                    "columnLetter": "D",
                                },
                            },
                        ],
                    }
                ],
            }
        ],
    }
    context_link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="ScanBrand",
        model_name="Scan Model",
        model_year="2028",
        market="Spain",
        country="Spain",
        trim_ids=[],
        sales_version_ids=[],
        context_type="compare",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
    )

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)
    monkeypatch.setattr(engineering_config, "build_source_digest", lambda _path, _name: digest)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [context_link])
    monkeypatch.setattr(engineering_config.repo, "list_feature_catalog", lambda _session, **_kwargs: [])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: None)
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_category_field", lambda _session, _category, _field: None)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_full_name", lambda _session, _full_name: None)
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value_by_trim_feature", lambda _session, _trim_id, _feature_id: None)

    payload = engineering_config.create_draft_from_source_digest_group(
        source_id=source_id,
        group_id="tabular-simple-pdf-ocr-page-1",
        payload=None,
        session=fake_session,
        user=UserContext(role="editor", name="tester"),
    )

    assert payload["trimCount"] == 2
    assert payload["featureCount"] == 1
    assert payload["insertedValueCount"] == 2
    assert payload["sourceDigestType"] == "pdf_ocr"
    assert payload["sourceFormat"] == "pdf_ocr"
    assert payload["ocrEngine"] == "paddleocr"
    assert fake_session.committed is True
    created_trims = [item for item in fake_session.added if item.__class__.__name__ == "VehicleTrim"]
    assert [(trim.brand, trim.model_name, trim.market, trim.model_year, trim.full_trim_name) for trim in created_trims] == [
        ("ScanBrand", "Scan Model", "Spain", "2028", "Scan Model / Basic"),
        ("ScanBrand", "Scan Model", "Spain", "2028", "Scan Model / Premium"),
    ]


def test_create_draft_from_image_ocr_source_digest_group(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "competitor.png"
    source_path.write_bytes(b"\x89PNG\r\n\x1a\n")
    source_id = uuid4()
    fake_session = FakeImportSession()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="competitor.png",
        source_file_path=str(source_path),
        source_file_hash="hash-image",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    digest = {
        "digestType": "image_ocr",
        "status": "ready",
        "compareGroups": [
            {
                "groupId": "tabular-simple-ocr-image-1",
                "modelName": "competitor",
                "trims": [
                    {
                        "trimId": "image-basic",
                        "trimName": "Basic",
                        "fullTrimName": "Basic",
                        "modelName": "competitor",
                    },
                    {
                        "trimId": "image-premium",
                        "trimName": "Premium",
                        "fullTrimName": "Premium",
                        "modelName": "competitor",
                    },
                ],
                "rows": [
                    {
                        "category": "Comfort",
                        "featureCode": "digest_wireless_charging",
                        "featureName": "Wireless charging",
                        "values": [
                            {
                                "rawValue": "-",
                                "normalizedValue": None,
                                "availability": "NOT_AVAILABLE",
                                "unit": None,
                                "source": {
                                    "sourceType": "image_ocr",
                                    "ocrEngine": "unit_ocr",
                                    "rowNumber": 3,
                                    "cell": "OCRR3C3",
                                    "columnLetter": "C",
                                },
                            },
                            {
                                "rawValue": "S",
                                "normalizedValue": "standard",
                                "availability": "STANDARD",
                                "unit": None,
                                "source": {
                                    "sourceType": "image_ocr",
                                    "ocrEngine": "unit_ocr",
                                    "rowNumber": 3,
                                    "cell": "OCRR3C4",
                                    "columnLetter": "D",
                                },
                            },
                        ],
                    }
                ],
            }
        ],
    }
    context_link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="ImageBrand",
        model_name="Image Model",
        model_year="2028",
        market="France",
        country="France",
        trim_ids=[],
        sales_version_ids=[],
        context_type="compare",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
    )

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)
    monkeypatch.setattr(engineering_config, "build_source_digest", lambda _path, _name: digest)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [context_link])
    monkeypatch.setattr(engineering_config.repo, "list_feature_catalog", lambda _session, **_kwargs: [])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: None)
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_category_field", lambda _session, _category, _field: None)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_full_name", lambda _session, _full_name: None)
    monkeypatch.setattr(engineering_config.repo, "get_trim_feature_value_by_trim_feature", lambda _session, _trim_id, _feature_id: None)

    payload = engineering_config.create_draft_from_source_digest_group(
        source_id=source_id,
        group_id="tabular-simple-ocr-image-1",
        payload=None,
        session=fake_session,
        user=UserContext(role="editor", name="tester"),
    )

    assert payload["trimCount"] == 2
    assert payload["featureCount"] == 1
    assert payload["insertedValueCount"] == 2
    assert fake_session.committed is True
    created_trims = [item for item in fake_session.added if item.__class__.__name__ == "VehicleTrim"]
    assert [(trim.brand, trim.model_name, trim.market, trim.model_year, trim.full_trim_name) for trim in created_trims] == [
        ("ImageBrand", "Image Model", "France", "2028", "Image Model / Basic"),
        ("ImageBrand", "Image Model", "France", "2028", "Image Model / Premium"),
    ]


def test_create_draft_from_source_digest_group_reuses_and_updates_existing_values(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "config.xlsx"
    source_path.write_bytes(b"PK\x03\x04")
    source_id = uuid4()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="config.xlsx",
        source_file_path=str(source_path),
        source_file_hash="hash-1",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    feature = FakeFeature(
        feature_id=uuid4(),
        feature_code="digest_seat_heat",
        category="舒适便利",
        standard_field_name="Seat heating / 座椅加热",
        display_order=8,
    )
    basic_trim = FakeTrim(
        trim_id=uuid4(),
        full_trim_name="Basic / MM001",
        brand="OMODA",
        model_name="T19C",
        trim_name="Basic",
        market="EU",
        model_year="2026",
        energy_type=None,
        drivetrain=None,
        engine=None,
        vehicle_code="Basic",
        material_no="MM001",
        identity_key="old-basic",
    )
    premium_trim = FakeTrim(
        trim_id=uuid4(),
        full_trim_name="Premium / MM002",
        brand="OMODA",
        model_name="T19C",
        trim_name="Premium",
        market="EU",
        model_year="2026",
        energy_type=None,
        drivetrain=None,
        engine=None,
        vehicle_code="Premium",
        material_no="MM002",
        identity_key="old-premium",
    )
    existing_values = {
        (basic_trim.trim_id, feature.feature_id): FakeValue(
            value_id=uuid4(),
            feature_id=feature.feature_id,
            raw_value="-",
            normalized_value=None,
            availability="NOT_AVAILABLE",
            unit=None,
            version=2,
        ),
        (premium_trim.trim_id, feature.feature_id): FakeValue(
            value_id=uuid4(),
            feature_id=feature.feature_id,
            raw_value="-",
            normalized_value=None,
            availability="NOT_AVAILABLE",
            unit=None,
            version=4,
        ),
    }
    digest = {
        "digestType": "workbook",
        "status": "ready",
        "compareGroups": [
            {
                "groupId": "group-1",
                "modelName": "T19C",
                "trims": [
                    {
                        "trimId": "digest-trim-1",
                        "trimName": "Basic",
                        "fullTrimName": basic_trim.full_trim_name,
                        "modelName": "T19C",
                        "market": "EU",
                        "modelYear": "2026",
                        "materialNo": "MM001",
                        "salesVersion": "Basic",
                        "profile": {"brand": "OMODA", "country": "EU"},
                    },
                    {
                        "trimId": "digest-trim-2",
                        "trimName": "Premium",
                        "fullTrimName": premium_trim.full_trim_name,
                        "modelName": "T19C",
                        "market": "EU",
                        "modelYear": "2026",
                        "materialNo": "MM002",
                        "salesVersion": "Premium",
                        "profile": {"brand": "OMODA", "country": "EU"},
                    },
                ],
                "rows": [
                    {
                        "category": feature.category,
                        "featureCode": feature.feature_code,
                        "featureName": feature.standard_field_name,
                        "values": [
                            {"rawValue": "●", "normalizedValue": "standard", "availability": "STANDARD", "unit": None},
                            {"rawValue": "O", "normalizedValue": "optional", "availability": "OPTIONAL", "unit": None},
                        ],
                    }
                ],
            }
        ],
    }
    fake_session = FakeImportSession()
    basic_parent_id = uuid4()
    premium_parent_id = uuid4()
    latest_versions = {
        basic_trim.trim_id: type("LatestVersion", (), {"version_id": basic_parent_id, "version_no": 2})(),
        premium_trim.trim_id: type("LatestVersion", (), {"version_id": premium_parent_id, "version_no": 4})(),
    }
    trims_by_name = {
        basic_trim.full_trim_name: basic_trim,
        premium_trim.full_trim_name: premium_trim,
    }
    deleted_projection_calls: list[tuple[UUID, set[UUID]]] = []

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, _source_id: source_batch)
    monkeypatch.setattr(engineering_config, "build_source_digest", lambda _path, _name: digest)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config.repo, "list_feature_catalog", lambda _session, **_kwargs: [feature])
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_code", lambda _session, _code: feature)
    monkeypatch.setattr(engineering_config.repo, "get_feature_catalog_by_category_field", lambda _session, _category, _field: feature)
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_full_name", lambda _session, full_name: trims_by_name.get(full_name))
    monkeypatch.setattr(
        engineering_config.repo,
        "get_latest_config_version_for_trim",
        lambda _session, trim_id, **_kwargs: latest_versions.get(trim_id),
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "get_trim_feature_value_by_trim_feature",
        lambda _session, trim_id, feature_id: existing_values.get((trim_id, feature_id)),
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "delete_trim_feature_values_not_in",
        lambda _session, trim_id, feature_ids: (
            deleted_projection_calls.append((trim_id, set(feature_ids))) or 1
        ),
    )

    payload = engineering_config.create_draft_from_source_digest_group(
        source_id=source_id,
        group_id="group-1",
        payload=None,
        session=fake_session,
        user=UserContext(role="editor", name="tester"),
    )

    assert payload["createdTrimCount"] == 0
    assert payload["reusedTrimCount"] == 2
    assert payload["createdFeatureCount"] == 0
    assert payload["reusedFeatureCount"] == 1
    assert payload["aliasMatchedFeatureCount"] == 0
    assert payload["featureMatchReasonCounts"] == {"feature_code": 1}
    assert payload["insertedValueCount"] == 0
    assert payload["updatedValueCount"] == 2
    assert payload["deletedValueCount"] == 2
    assert deleted_projection_calls == [
        (basic_trim.trim_id, {feature.feature_id}),
        (premium_trim.trim_id, {feature.feature_id}),
    ]
    assert existing_values[(basic_trim.trim_id, feature.feature_id)].raw_value == "●"
    assert existing_values[(basic_trim.trim_id, feature.feature_id)].version == 3
    assert existing_values[(premium_trim.trim_id, feature.feature_id)].raw_value == "O"
    assert existing_values[(premium_trim.trim_id, feature.feature_id)].version == 5
    assert sum(1 for item in fake_session.added if item.__class__.__name__ == "VehicleTrim") == 0
    assert sum(1 for item in fake_session.added if item.__class__.__name__ == "TrimFeatureValue") == 0
    versions = [item for item in fake_session.added if item.__class__.__name__ == "ConfigVersion"]
    version_by_trim_id = {version.trim_id: version for version in versions}
    assert version_by_trim_id[basic_trim.trim_id].version_no == 3
    assert version_by_trim_id[basic_trim.trim_id].parent_version_id == basic_parent_id
    assert version_by_trim_id[premium_trim.trim_id].version_no == 5
    assert version_by_trim_id[premium_trim.trim_id].parent_version_id == premium_parent_id
    assert {version.snapshot_feature_count for version in versions} == {1}


def test_confirm_matrix_upload_as_draft_is_disabled_in_favour_of_source_digest(monkeypatch, tmp_path) -> None:
    fake_session = FakeImportSession()
    upload_id = "matrix-audit-user"
    source_path = tmp_path / "matrix.xlsx"
    source_path.write_bytes(b"matrix")
    monkeypatch.setattr(engineering_config, "UPLOAD_SESSION_DIR", tmp_path / "uploads")
    engineering_config._save_session_meta(
        upload_id,
        {
            "uploadId": upload_id,
            "fileName": "matrix.xlsx",
            "status": "parsed",
            "assembledPath": str(source_path),
        },
    )
    feature = FakeFeature(
        feature_id=uuid4(),
        feature_code="seat_heat",
        category="Comfort",
        standard_field_name="Seat heating",
        display_order=1,
    )
    parse_result = {
        "summary": {"value_record_count": 1},
        "trims": [
            {
                "brand": "OMODA",
                "model_name": "T19C",
                "trim_name": "Basic",
                "full_trim_name": "T19C Basic",
            }
        ],
        "values": [
            {
                "full_trim_name": "T19C Basic",
                "feature_code": feature.feature_code,
                "raw_value": "●",
                "normalized_value": "standard",
                "availability": "STANDARD",
                "unit": None,
                "source_row": 12,
                "source_column": "D",
            }
        ],
        "warnings": [],
        "unmatched_features": [],
        "categories": [],
    }

    monkeypatch.setattr(engineering_config, "parse_config_matrix", lambda _path, feature_catalog=None: parse_result)
    monkeypatch.setattr(engineering_config.repo, "list_feature_catalog", lambda _session, **_kwargs: [feature])
    monkeypatch.setattr(engineering_config.repo, "get_vehicle_trim_by_full_name", lambda _session, _full_name: None)

    with pytest.raises(HTTPException) as exc_info:
        engineering_config.confirm_upload_as_draft(
            upload_id=upload_id,
            session=fake_session,
            user=UserContext(role="editor", name="alice"),
        )

    assert exc_info.value.status_code == 410
    assert "Source Snapshot" in exc_info.value.detail
    assert fake_session.added == []
    assert fake_session.committed is False


def test_publish_version_archives_all_previous_versions_and_records_actor(monkeypatch) -> None:
    version_id = uuid4()
    identity_key = "T19C Premium source:one"
    version = engineering_config.ConfigVersion(
        version_id=version_id,
        trim_id=uuid4(),
        identity_key=identity_key,
        brand="OMODA",
        model_name="T19C",
        trim_name="Premium",
        status="draft",
        version_no=3,
        snapshot_values=[{"featureCode": "seat_heat", "rawValue": "●"}],
        snapshot_feature_count=1,
    )
    previous_versions = [
        engineering_config.ConfigVersion(
            version_id=uuid4(),
            trim_id=version.trim_id,
            identity_key=identity_key,
            brand="OMODA",
            model_name="T19C",
            trim_name="Premium",
            status="published",
            version_no=number,
            snapshot_values=[{"featureCode": "seat_heat", "rawValue": value}],
            snapshot_feature_count=1,
        )
        for number, value in ((1, "-"), (2, "O"))
    ]

    class PublishSession(FakeImportSession):
        def __init__(self) -> None:
            super().__init__()
            self.statuses_at_flush: list[list[str]] = []

        def get(self, _model: object, object_id: UUID) -> object | None:
            return version if object_id == version_id else None

        def flush(self) -> None:
            self.statuses_at_flush.append([item.status for item in previous_versions])
            super().flush()

    session = PublishSession()
    monkeypatch.setattr(
        engineering_config.repo,
        "list_published_config_versions_by_identity",
        lambda _session, _identity_key: previous_versions,
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "get_latest_config_version_for_trim",
        lambda _session, _trim_id, **_kwargs: version,
    )

    result = engineering_config.publish_version(
        version_id=str(version_id),
        session=session,
        user=UserContext(role="admin", name="release-admin"),
    )

    assert version.status == "published"
    assert version.published_by == "release-admin"
    assert all(item.status == "archived" for item in previous_versions)
    assert session.statuses_at_flush == [["archived", "archived"]]
    assert result["publishedBy"] == "release-admin"
    assert result["archivedPreviousVersionIds"] == [str(item.version_id) for item in previous_versions]
    assert session.committed is True


def test_publish_version_rejects_non_latest_draft(monkeypatch) -> None:
    version_id = uuid4()
    version = engineering_config.ConfigVersion(
        version_id=version_id,
        trim_id=uuid4(),
        identity_key="T19C Basic source:one",
        brand="OMODA",
        model_name="T19C",
        trim_name="Basic",
        status="draft",
        version_no=1,
        snapshot_values=[{"featureId": str(uuid4()), "rawValue": "●"}],
        snapshot_feature_count=1,
    )
    newer_version = type("LatestVersion", (), {"version_id": uuid4()})()

    class PublishSession(FakeImportSession):
        def get(self, _model: object, object_id: UUID) -> object | None:
            return version if object_id == version_id else None

    session = PublishSession()
    monkeypatch.setattr(
        engineering_config.repo,
        "get_latest_config_version_for_trim",
        lambda _session, _trim_id, **_kwargs: newer_version,
    )

    with pytest.raises(HTTPException) as exc_info:
        engineering_config.publish_version(
            version_id=str(version_id),
            session=session,
            user=UserContext(role="admin", name="release-admin"),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Only the latest draft version can be published"
    assert session.committed is False


def test_publish_version_rejects_draft_without_snapshot() -> None:
    version_id = uuid4()
    version = engineering_config.ConfigVersion(
        version_id=version_id,
        trim_id=uuid4(),
        identity_key="legacy",
        brand="OMODA",
        model_name="T19C",
        trim_name="Basic",
        status="draft",
        version_no=1,
        snapshot_values=None,
        snapshot_feature_count=0,
    )

    class PublishSession(FakeImportSession):
        def get(self, _model: object, object_id: UUID) -> object | None:
            return version if object_id == version_id else None

    session = PublishSession()
    with pytest.raises(HTTPException) as exc_info:
        engineering_config.publish_version(
            version_id=str(version_id),
            session=session,
            user=UserContext(role="admin", name="release-admin"),
        )

    assert exc_info.value.status_code == 409
    assert "snapshot is missing" in exc_info.value.detail
    assert session.committed is False


def test_source_upload_rejects_unsupported_extension(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(engineering_config, "UPLOAD_SESSION_DIR", tmp_path)
    client = TestClient(app)
    response = client.post(
        "/v1/engineering-config/source/upload/initiate?file_name=bad.exe&total_size=10&chunk_size=1024",
        headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported source file type"


def test_source_upload_rejects_pdf_signature_mismatch(tmp_path, monkeypatch) -> None:
    fake_session = FakeImportSession()
    monkeypatch.setattr(engineering_config, "UPLOAD_SESSION_DIR", tmp_path)
    app.dependency_overrides[get_db_session] = lambda: fake_session
    try:
        client = TestClient(app)
        headers = {"X-Auth-Token": "change-me", "X-User-Name": "tester"}
        init = client.post(
            "/v1/engineering-config/source/upload/initiate?file_name=omoda9.pdf&total_size=10&chunk_size=1024",
            headers=headers,
        )
        upload_id = init.json()["uploadId"]
        client.put(
            f"/v1/engineering-config/source/upload/{upload_id}/parts/0",
            headers=headers,
            content=b"not a pdf!",
        )
        complete = client.post(
            f"/v1/engineering-config/source/upload/{upload_id}/complete",
            headers=headers,
        )
        assert complete.status_code == 400
        assert complete.json()["detail"] == "PDF file signature mismatch"
        assert fake_session.committed is False
        assert fake_session.added == []
    finally:
        app.dependency_overrides.clear()


def test_source_upload_html_signature_accepts_table_fragments(tmp_path) -> None:
    source_path = tmp_path / "rival-table.html"
    source_path.write_text(
        "<table><tr><td>Feature</td><td>Basic</td><td>Premium</td></tr></table>",
        encoding="utf-8",
    )

    engineering_config._validate_source_file_content(source_path, source_path.name)


def test_source_upload_stores_html_table_fragment_with_digest(tmp_path, monkeypatch) -> None:
    fake_session = FakeImportSession()
    html_content = (
        b"<table><tr><td>Feature</td><td>Basic</td><td>Premium</td></tr>"
        b"<tr><td>Seat heating</td><td>-</td><td>S</td></tr></table>"
    )
    digest = {
        "digestType": "tabular",
        "sourceFormat": "html",
        "status": "ready",
        "fileName": "rival-table.html",
        "summary": {
            "candidateTrimCount": 2,
            "comparableGroupCount": 1,
            "featureCount": 1,
            "differenceCount": 1,
        },
        "sheets": [],
        "compareGroups": [
            {
                "groupId": "html_rival_table",
                "modelName": "Rival Model",
                "trimColumns": [
                    {"trimId": "basic", "displayName": "Basic"},
                    {"trimId": "premium", "displayName": "Premium"},
                ],
                "features": [
                    {
                        "featureId": "seat_heating",
                        "displayName": "Seat heating",
                        "values": {
                            "basic": {"rawValue": "-", "availability": "NOT_AVAILABLE"},
                            "premium": {"rawValue": "S", "availability": "STANDARD"},
                        },
                    }
                ],
            }
        ],
    }
    digest_calls: list[tuple[str, str]] = []

    def fake_build_source_digest(source_file_path: str, source_file_name: str) -> dict:
        digest_calls.append((source_file_path, source_file_name))
        assert Path(source_file_path).read_bytes() == html_content
        return digest

    engineering_config._cached_source_digest.cache_clear()
    monkeypatch.setattr(engineering_config, "UPLOAD_SESSION_DIR", tmp_path)
    monkeypatch.setattr(
        engineering_config.repo,
        "get_import_batch_by_hash",
        lambda _session, _domain, _source_file_hash: None,
    )
    monkeypatch.setattr(engineering_config, "build_source_digest", fake_build_source_digest)
    app.dependency_overrides[get_db_session] = lambda: fake_session
    try:
        client = TestClient(app)
        headers = {"X-Auth-Token": "change-me", "X-User-Name": "tester"}
        init = client.post(
            f"/v1/engineering-config/source/upload/initiate?file_name=rival-table.html&total_size={len(html_content)}&chunk_size=1024",
            headers=headers,
        )
        assert init.status_code == 200
        upload_id = init.json()["uploadId"]
        part = client.put(
            f"/v1/engineering-config/source/upload/{upload_id}/parts/0",
            headers=headers,
            content=html_content,
        )
        assert part.status_code == 200
        complete = client.post(
            f"/v1/engineering-config/source/upload/{upload_id}/complete",
            headers=headers,
        )
        assert complete.status_code == 200
        payload = complete.json()
        assert payload["fileType"] == "html"
        assert payload["uploadStatus"] == "registered"
        assert payload["extractStatus"] == "digest_ready"
        assert payload["nextAction"] == "review_digest"
        assert payload["createdBy"] == "static-editor"
        assert payload["sourceDigestStatus"]["sourceFormat"] == "html"
        assert payload["sourceDigestStatus"]["summary"]["comparableGroupCount"] == 1
        assert payload["sourceDigest"]["compareGroups"][0]["groupId"] == "html_rival_table"
        assert payload["parseMode"] == "stored_source"
        assert fake_session.committed is True
        assert len(fake_session.added) == 1
        batch = fake_session.added[0]
        assert batch.domain == "engineering_config_source"
        assert batch.source_file_name == "rival-table.html"
        assert batch.import_status == "stored"
        assert batch.triggered_by == "static-editor"
        assert digest_calls == [(batch.source_file_path, "rival-table.html")]
    finally:
        engineering_config._cached_source_digest.cache_clear()
        app.dependency_overrides.clear()


def test_source_upload_rejects_html_signature_mismatch(tmp_path) -> None:
    source_path = tmp_path / "rival-table.html"
    source_path.write_text("Feature,Basic,Premium\nSeat heating,-,S\n", encoding="utf-8")

    with pytest.raises(HTTPException) as exc:
        engineering_config._validate_source_file_content(source_path, source_path.name)

    assert exc.value.status_code == 400
    assert exc.value.detail == "HTML file signature mismatch"


def test_source_upload_rejects_fake_xlsx_archive(tmp_path, monkeypatch) -> None:
    fake_session = FakeImportSession()
    monkeypatch.setattr(engineering_config, "UPLOAD_SESSION_DIR", tmp_path)
    app.dependency_overrides[get_db_session] = lambda: fake_session
    try:
        client = TestClient(app)
        headers = {"X-Auth-Token": "change-me", "X-User-Name": "tester"}
        init = client.post(
            "/v1/engineering-config/source/upload/initiate?file_name=spoof.xlsx&total_size=12&chunk_size=1024",
            headers=headers,
        )
        upload_id = init.json()["uploadId"]
        client.put(
            f"/v1/engineering-config/source/upload/{upload_id}/parts/0",
            headers=headers,
            content=b"PK\x03\x04notzip!!",
        )
        complete = client.post(
            f"/v1/engineering-config/source/upload/{upload_id}/complete",
            headers=headers,
        )
        assert complete.status_code == 400
        assert complete.json()["detail"] == "XLSX file is not a valid workbook archive"
        assert fake_session.committed is False
        assert fake_session.added == []
    finally:
        app.dependency_overrides.clear()


def test_source_upload_returns_existing_snapshot_for_duplicate(tmp_path, monkeypatch) -> None:
    fake_session = FakeImportSession()
    source_path = tmp_path / "existing.pdf"
    source_path.write_bytes(b"%PDF-1.4 ok")
    source_hash = engineering_config._sha256_for_path(source_path)
    existing = FakeImportBatch(
        import_batch_id=uuid4(),
        domain="engineering_config_source",
        source_file_name="existing.pdf",
        source_file_path=str(source_path),
        source_file_hash=source_hash,
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(engineering_config, "UPLOAD_SESSION_DIR", tmp_path / "uploads")
    monkeypatch.setattr(
        engineering_config.repo,
        "get_import_batch_by_hash",
        lambda _session, _domain, _source_file_hash: existing,
    )
    app.dependency_overrides[get_db_session] = lambda: fake_session
    try:
        client = TestClient(app)
        headers = {"X-Auth-Token": "change-me", "X-User-Name": "tester"}
        init = client.post(
            "/v1/engineering-config/source/upload/initiate?file_name=duplicate.pdf&total_size=11&chunk_size=1024",
            headers=headers,
        )
        upload_id = init.json()["uploadId"]
        client.put(
            f"/v1/engineering-config/source/upload/{upload_id}/parts/0",
            headers=headers,
            content=b"%PDF-1.4 ok",
        )
        complete = client.post(
            f"/v1/engineering-config/source/upload/{upload_id}/complete",
            headers=headers,
            json={
                "relatedContext": {
                    "brand": "Volvo",
                    "model": "XC60",
                    "market": "Austria",
                    "powertrain": "PHEV",
                    "segment": "SUV C",
                    "modelYear": "2026",
                    "trimIds": ["core"],
                }
            },
        )
        assert complete.status_code == 200
        payload = complete.json()
        assert payload["sourceId"] == str(existing.import_batch_id)
        assert payload["uploadStatus"] == "duplicate"
        assert payload["duplicate"] is True
        assert payload["deduplicated"] is True
        assert payload["linkedToCurrentContext"] is True
        assert payload["relatedContext"]["market"] == "Austria"
        assert payload["relatedContext"]["powertrain"] == "PHEV"
        assert payload["relatedContext"]["segment"] == "SUV C"
        assert payload["sourceFileName"] == "existing.pdf"
        assert fake_session.committed is True
        assert len(fake_session.added) == 1
        link = fake_session.added[0]
        assert link.source_id == existing.import_batch_id
        assert link.market == "Austria"
        assert link.powertrain == "PHEV"
        assert link.segment == "SUV C"
        assert link.trim_ids == ["core"]
    finally:
        app.dependency_overrides.clear()


def test_list_source_snapshots_returns_shared_uploads_from_other_users(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "source.pdf"
    source_path.write_bytes(b"%PDF-1.4 ok")
    source_id = uuid4()
    existing = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="source.pdf",
        source_file_path=str(source_path),
        source_file_hash=engineering_config._sha256_for_path(source_path),
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="alice",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="Volvo",
        model_name="XC60",
        model_year="2026",
        market="Germany",
        country="Germany",
        segment="SUV C",
        trim_ids=["core", "ultra"],
        sales_version_ids=[],
        context_type="compare",
        created_by="alice",
        created_at_utc=datetime.now(timezone.utc),
    )
    captured: dict[str, object] = {}

    def fake_list_source_snapshot_batches(_session, domain, **kwargs):
        captured["domain"] = domain
        captured.update(kwargs)
        return [existing]

    monkeypatch.setattr(engineering_config.repo, "list_source_snapshot_batches", fake_list_source_snapshot_batches)
    monkeypatch.setattr(
        engineering_config.repo,
        "list_source_context_links",
        lambda _session, _source_id: [link],
    )
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "bob"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert captured["domain"] == "engineering_config_source"
        assert captured["include_trash"] is False
        assert captured["trash_only"] is False
        assert payload["rows"] == 1
        assert payload["items"][0]["sourceId"] == str(existing.import_batch_id)
        assert payload["items"][0]["createdBy"] == "alice"
        assert payload["items"][0]["fileType"] == "pdf"
        assert payload["items"][0]["uploadStatus"] == "registered"
        assert payload["items"][0]["relatedContext"]["model"] == "XC60"
        assert payload["items"][0]["relatedContext"]["segment"] == "SUV C"
        assert payload["items"][0]["relatedContext"]["trimIds"] == ["core", "ultra"]
        assert payload["items"][0]["contexts"][0]["createdBy"] == "alice"
        assert payload["items"][0]["contexts"][0]["status"] == "active"
    finally:
        app.dependency_overrides.clear()


def test_list_source_snapshots_rows_report_total_before_limit(tmp_path, monkeypatch) -> None:
    source_batches = []
    for index in range(3):
        source_path = tmp_path / f"source-{index + 1}.pdf"
        source_path.write_bytes(b"%PDF-1.4 ok")
        source_id = uuid4()
        source_batches.append(
            FakeImportBatch(
                import_batch_id=source_id,
                domain="engineering_config_source",
                source_file_name=f"source-{index + 1}.pdf",
                source_file_path=str(source_path),
                source_file_hash=engineering_config._sha256_for_path(source_path),
                import_status="stored",
                row_count=0,
                error_count=0,
                triggered_by="api",
                started_at_utc=datetime.now(timezone.utc),
                finished_at_utc=datetime.now(timezone.utc),
                created_at_utc=datetime.now(timezone.utc),
            )
        )
    captured: dict[str, object] = {}

    def fake_list_source_snapshot_batches(_session, domain, **kwargs):
        captured["domain"] = domain
        captured.update(kwargs)
        return source_batches

    monkeypatch.setattr(engineering_config.repo, "list_source_snapshot_batches", fake_list_source_snapshot_batches)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/engineering-config/source/snapshots?limit=2",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert captured["domain"] == "engineering_config_source"
        assert captured["limit"] == engineering_config.SOURCE_SNAPSHOT_LIST_CANDIDATE_LIMIT
        assert captured["include_trash"] is False
        assert captured["trash_only"] is False
        assert payload["rows"] == 3
        assert [item["sourceFileName"] for item in payload["items"]] == ["source-1.pdf", "source-2.pdf"]
    finally:
        app.dependency_overrides.clear()


def test_list_source_snapshots_scopes_context_by_brand_year_and_powertrain(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "multi-context-source.xlsx"
    source_path.write_bytes(b"PK\x03\x04")
    source_id = uuid4()
    batch = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="multi-context-source.xlsx",
        source_file_path=str(source_path),
        source_file_hash="hash",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    matched_context = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="OMODA",
        model_name="T19C",
        model_year="2026",
        market="Germany",
        country="Germany",
        powertrain="BEV",
        segment="SUV C",
        trim_ids=["basic", "premium"],
        sales_version_ids=[],
        context_type="compare",
        created_by="alice",
        created_at_utc=datetime.now(timezone.utc),
    )
    other_context = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="MG",
        model_name="ZS",
        model_year="2025",
        market="Germany",
        country="Germany",
        powertrain="ICE",
        segment="SUV B",
        trim_ids=["comfort"],
        sales_version_ids=[],
        context_type="compare",
        created_by="bob",
        created_at_utc=datetime.now(timezone.utc),
    )
    captured: dict[str, object] = {}

    def fake_list_source_snapshot_batches(_session, domain, **kwargs):
        captured["domain"] = domain
        captured.update(kwargs)
        return [batch]

    monkeypatch.setattr(engineering_config.repo, "list_source_snapshot_batches", fake_list_source_snapshot_batches)
    monkeypatch.setattr(
        engineering_config.repo,
        "list_source_context_links",
        lambda _session, _source_id: [other_context, matched_context],
    )
    monkeypatch.setattr(engineering_config, "_safe_source_digest", lambda _batch: None)
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5&brand=OMODA&country=Germany&modelYear=2026&powertrain=BEV&segment=SUV%20C",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert captured["domain"] == engineering_config.SOURCE_IMPORT_DOMAIN
        assert captured["brand"] == "OMODA"
        assert captured["country"] == "Germany"
        assert captured["model_year"] == "2026"
        assert captured["powertrain"] == "BEV"
        assert captured["segment"] == "SUV C"
        assert payload["rows"] == 1
        assert payload["items"][0]["relatedContext"]["brand"] == "OMODA"
        assert payload["items"][0]["relatedContext"]["model"] == "T19C"
        assert payload["items"][0]["relatedContext"]["modelYear"] == "2026"
        assert payload["items"][0]["relatedContext"]["powertrain"] == "BEV"
        assert [context["relatedContext"]["brand"] for context in payload["items"][0]["contexts"]] == ["OMODA"]
    finally:
        app.dependency_overrides.clear()


def test_list_source_snapshots_reports_digest_ready_without_returning_digest(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "source.xlsx"
    source_path.write_bytes(b"PK\x03\x04")
    source_id = uuid4()
    existing = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="source.xlsx",
        source_file_path=str(source_path),
        source_file_hash=engineering_config._sha256_for_path(source_path),
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "list_source_snapshot_batches",
        lambda _session, _domain, **_kwargs: [existing],
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "list_source_context_links",
        lambda _session, _source_id: [],
    )
    monkeypatch.setattr(
        engineering_config,
        "build_source_digest",
        lambda _path, _name: {
            "digestType": "workbook",
            "status": "ready",
            "fileName": "source.xlsx",
            "modelName": "EX30",
            "summary": {
                "sheetCount": 1,
                "tableCount": 1,
                "candidateTrimCount": 2,
                "comparableGroupCount": 1,
                "featureCount": 10,
                "differenceCount": 3,
            },
            "sheets": [],
            "compareGroups": [{"groupId": "ex30", "trimCount": 2}],
        },
    )
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert response.status_code == 200
        item = response.json()["items"][0]
        assert item["extractStatus"] == "digest_ready"
        assert item["nextAction"] == "review_digest"
        assert item["sourceDigest"] is None
        assert item["sourceDigestStatus"]["digestType"] == "workbook"
        assert item["sourceDigestStatus"]["status"] == "ready"
        assert item["sourceDigestStatus"]["summary"]["candidateTrimCount"] == 2
        assert item["sourceDigestStatus"]["summary"]["differenceCount"] == 3
    finally:
        app.dependency_overrides.clear()


def test_list_source_snapshots_search_rows_report_total_matches_before_limit(tmp_path, monkeypatch) -> None:
    batches = []
    for file_name in ["T19C-source-a.xlsx", "T19C-source-b.xlsx", "EX30-source.xlsx"]:
        source_path = tmp_path / file_name
        source_path.write_bytes(b"PK\x03\x04")
        source_id = uuid4()
        batches.append(
            FakeImportBatch(
                import_batch_id=source_id,
                domain="engineering_config_source",
                source_file_name=file_name,
                source_file_path=str(source_path),
                source_file_hash="hash",
                import_status="stored",
                row_count=0,
                error_count=0,
                triggered_by="api",
                started_at_utc=datetime.now(timezone.utc),
                finished_at_utc=datetime.now(timezone.utc),
                created_at_utc=datetime.now(timezone.utc),
            )
        )

    monkeypatch.setattr(
        engineering_config.repo,
        "list_source_snapshot_batches",
        lambda _session, _domain, **_kwargs: batches,
    )
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/engineering-config/source/snapshots?limit=1&q=T19C",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["rows"] == 2
        assert [item["sourceFileName"] for item in payload["items"]] == ["T19C-source-a.xlsx"]
        assert "文件 T19C-source-a.xlsx" in payload["items"][0]["sourceSearchMatches"]
    finally:
        app.dependency_overrides.clear()


def test_list_source_snapshots_uses_metadata_prefilter_for_source_file_query(tmp_path, monkeypatch) -> None:
    file_name = "欧盟在售车型可控资源表20260226.xlsx"
    source_path = tmp_path / file_name
    source_path.write_bytes(b"PK\x03\x04")
    source_id = uuid4()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name=file_name,
        source_file_path=str(source_path),
        source_file_hash="hash",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    captured_calls: list[dict[str, object]] = []

    def fake_list_source_snapshot_batches(_session, _domain, **kwargs):
        captured_calls.append(kwargs)
        return [source_batch] if kwargs.get("query") == file_name else []

    monkeypatch.setattr(engineering_config.repo, "list_source_snapshot_batches", fake_list_source_snapshot_batches)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config, "_safe_source_digest", lambda _batch: None)
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            f"/v1/engineering-config/source/snapshots?limit=5&q={file_name}",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert captured_calls[0]["query"] == file_name
        assert len(captured_calls) == 1
        assert payload["rows"] == 1
        assert payload["items"][0]["sourceFileName"] == file_name
        assert f"文件 {file_name}" in payload["items"][0]["sourceSearchMatches"]
    finally:
        app.dependency_overrides.clear()


def test_list_source_snapshots_search_matches_source_uploader(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "shared-source.xlsx"
    source_path.write_bytes(b"PK\x03\x04")
    alice_source_id = uuid4()
    bob_source_id = uuid4()
    batches = [
        FakeImportBatch(
            import_batch_id=alice_source_id,
            domain="engineering_config_source",
            source_file_name="shared-source-a.xlsx",
            source_file_path=str(source_path),
            source_file_hash="hash-a",
            import_status="stored",
            row_count=0,
            error_count=0,
            triggered_by="alice",
            started_at_utc=datetime.now(timezone.utc),
            finished_at_utc=datetime.now(timezone.utc),
            created_at_utc=datetime.now(timezone.utc),
        ),
        FakeImportBatch(
            import_batch_id=bob_source_id,
            domain="engineering_config_source",
            source_file_name="shared-source-b.xlsx",
            source_file_path=str(source_path),
            source_file_hash="hash-b",
            import_status="stored",
            row_count=0,
            error_count=0,
            triggered_by="bob",
            started_at_utc=datetime.now(timezone.utc),
            finished_at_utc=datetime.now(timezone.utc),
            created_at_utc=datetime.now(timezone.utc),
        ),
    ]

    monkeypatch.setattr(
        engineering_config.repo,
        "list_source_snapshot_batches",
        lambda _session, _domain, **_kwargs: batches,
    )
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config, "_safe_source_digest", lambda _batch: None)
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5&q=alice",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["rows"] == 1
        assert payload["items"][0]["sourceFileName"] == "shared-source-a.xlsx"
        assert payload["items"][0]["createdBy"] == "alice"
        assert "上传人 alice" in payload["items"][0]["sourceSearchMatches"]
    finally:
        app.dependency_overrides.clear()


def test_list_source_snapshots_search_scans_beyond_default_recent_window(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "source-placeholder.xlsx"
    source_path.write_bytes(b"PK\x03\x04")
    batches = []
    for index in range(120):
        source_id = uuid4()
        file_name = "hidden-rival-config.xlsx" if index == 104 else f"source-{index + 1:03}.xlsx"
        batches.append(
            FakeImportBatch(
                import_batch_id=source_id,
                domain="engineering_config_source",
                source_file_name=file_name,
                source_file_path=str(source_path),
                source_file_hash="hash",
                import_status="stored",
                row_count=0,
                error_count=0,
                triggered_by="api",
                started_at_utc=datetime.now(timezone.utc),
                finished_at_utc=datetime.now(timezone.utc),
                created_at_utc=datetime.now(timezone.utc),
            )
        )
    captured: dict[str, object] = {}

    def fake_list_source_snapshot_batches(_session, _domain, **kwargs):
        captured.update(kwargs)
        return batches[: int(kwargs["limit"])]

    monkeypatch.setattr(engineering_config.repo, "list_source_snapshot_batches", fake_list_source_snapshot_batches)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config, "_safe_source_digest", lambda _batch: None)
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5&q=hidden-rival",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert captured["limit"] == engineering_config.SOURCE_SNAPSHOT_SEARCH_CANDIDATE_LIMIT
        assert payload["rows"] == 1
        assert payload["items"][0]["sourceFileName"] == "hidden-rival-config.xlsx"
        assert "文件 hidden-rival-config.xlsx" in payload["items"][0]["sourceSearchMatches"]
    finally:
        app.dependency_overrides.clear()


def test_list_source_snapshots_search_matches_context_scenario_and_identity_anchor(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "missing-rival.pdf"
    source_path.write_bytes(b"%PDF-1.4 ok")
    source_id = uuid4()
    batch = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="missing-rival.pdf",
        source_file_path=str(source_path),
        source_file_hash="hash",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="MissingBrand",
        model_name="Missing Rival",
        model_year=None,
        market="Germany",
        country="Germany",
        powertrain="PHEV",
        segment="SUV C",
        trim_ids=[],
        sales_version_ids=[],
        context_type="competitor_recommendation_upload",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
        scenario="recommended_competitor_config_gap",
        identity_anchor="brand_model_market",
    )

    monkeypatch.setattr(
        engineering_config.repo,
        "list_source_snapshot_batches",
        lambda _session, _domain, **_kwargs: [batch],
    )
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [link])
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        scenario_response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5&q=recommended_competitor_config_gap",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert scenario_response.status_code == 200
        scenario_payload = scenario_response.json()
        assert scenario_payload["rows"] == 1
        assert "场景 recommended_competitor_config_gap" in scenario_payload["items"][0]["sourceSearchMatches"]

        identity_response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5&q=brand_model_market",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert identity_response.status_code == 200
        identity_payload = identity_response.json()
        assert identity_payload["rows"] == 1
        assert "身份锚点 brand_model_market" in identity_payload["items"][0]["sourceSearchMatches"]

        powertrain_response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5&q=PHEV",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert powertrain_response.status_code == 200
        powertrain_payload = powertrain_response.json()
        assert powertrain_payload["rows"] == 1
        assert "上下文 MissingBrand · Missing Rival · Germany · PHEV · SUV C" in powertrain_payload["items"][0]["sourceSearchMatches"]
        assert powertrain_payload["items"][0]["relatedContext"]["powertrain"] == "PHEV"
    finally:
        app.dependency_overrides.clear()


def test_list_source_snapshots_search_matches_recommended_competitor_source_digest_available(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "digest-rival.xlsx"
    source_path.write_bytes(b"PK\x03\x04 ok")
    source_id = uuid4()
    batch = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="digest-rival.xlsx",
        source_file_path=str(source_path),
        source_file_hash="digest-rival-hash",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="DigestBrand",
        model_name="Digest Rival",
        model_year=None,
        market="Germany",
        country="Germany",
        powertrain="PHEV",
        segment="SUV C",
        trim_ids=[],
        sales_version_ids=[],
        context_type="competitor_recommendation_source_digest",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
        scenario="recommended_competitor_source_digest_available",
        identity_anchor="brand_model_market",
    )

    monkeypatch.setattr(
        engineering_config.repo,
        "list_source_snapshot_batches",
        lambda _session, _domain, **_kwargs: [batch],
    )
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [link])
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        scenario_response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5&q=recommended_competitor_source_digest_available",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert scenario_response.status_code == 200
        scenario_payload = scenario_response.json()
        assert scenario_payload["rows"] == 1
        assert scenario_payload["items"][0]["relatedContext"]["contextType"] == "competitor_recommendation_source_digest"
        assert "场景 recommended_competitor_source_digest_available" in scenario_payload["items"][0]["sourceSearchMatches"]

        model_response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5&q=Digest%20Rival",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert model_response.status_code == 200
        model_payload = model_response.json()
        assert model_payload["rows"] == 1
        assert "上下文 DigestBrand · Digest Rival · Germany · PHEV · SUV C" in model_payload["items"][0]["sourceSearchMatches"]
    finally:
        app.dependency_overrides.clear()


def test_list_source_snapshots_filters_country_and_trash(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "country-source.pdf"
    source_path.write_bytes(b"%PDF-1.4 ok")
    source_id = uuid4()
    existing = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="country-source.pdf",
        source_file_path=str(source_path),
        source_file_hash=engineering_config._sha256_for_path(source_path),
        import_status="trashed",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="Volvo",
        model_name="XC60",
        model_year="2026",
        market="Germany",
        country="Germany",
        segment="SUV C",
        trim_ids=["core", "ultra"],
        sales_version_ids=[],
        context_type="compare",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
        status="trashed",
    )
    captured: dict[str, object] = {}

    def fake_list_source_snapshot_batches(_session, domain, **kwargs):
        captured["domain"] = domain
        captured.update(kwargs)
        return [existing]

    monkeypatch.setattr(engineering_config.repo, "list_source_snapshot_batches", fake_list_source_snapshot_batches)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [link])
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5&country=Germany&segment=SUV%20C&q=XC60&trashOnly=true",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert captured["domain"] == "engineering_config_source"
        assert captured["country"] == "Germany"
        assert captured["segment"] == "SUV C"
        assert captured["query"] == "XC60"
        assert captured["limit"] == engineering_config.SOURCE_SNAPSHOT_SEARCH_CANDIDATE_LIMIT
        assert captured["trash_only"] is True
        assert payload["items"][0]["uploadStatus"] == "trashed"
        assert payload["items"][0]["libraryStatus"] == "trashed"
        assert payload["items"][0]["inTrash"] is True
    finally:
        app.dependency_overrides.clear()


def test_source_snapshot_search_matches_digest_internal_models(monkeypatch, tmp_path) -> None:
    source_id = uuid4()
    source_path = tmp_path / "mixed-source.xlsx"
    source_path.write_bytes(b"placeholder")
    existing = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="mixed-source.xlsx",
        source_file_path=str(source_path),
        source_file_hash="hash",
        import_status="registered",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="Omoda",
        model_name="EU resource table",
        model_year=None,
        market="EU",
        country="EU",
        segment=None,
        trim_ids=[],
        sales_version_ids=[],
        context_type="source_snapshot",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
    )
    digest = {
        "digestType": "workbook",
        "status": "ready",
        "fileName": "mixed-source.xlsx",
        "modelName": "EU resource table",
        "summary": {
            "sheetCount": 1,
            "tableCount": 1,
            "candidateTrimCount": 2,
            "comparableGroupCount": 1,
            "featureCount": 1,
            "differenceCount": 1,
        },
        "sheets": [],
        "compareGroups": [
            {
                "groupId": "bev-group",
                "title": "T19C-BEV（2025款）",
                "sourceSheet": "BEV",
                "modelName": "T19C-BEV（2025款）",
                "trimCount": 2,
                "featureCount": 1,
                "differenceCount": 1,
                "trims": [
                    {
                        "trimId": "bev-comfort",
                        "trimName": "Comfort-FWD",
                        "fullTrimName": "两驱长续航舒适型 Comfort-FWD",
                        "modelName": "T19C-BEV（2025款）",
                    }
                ],
                "rows": [
                    {
                        "category": "灯光配置",
                        "featureCode": "dynamic-cornering-light",
                        "featureName": "Dynamic Cornering Light / 动态转向灯",
                    }
                ],
            }
        ],
    }

    monkeypatch.setattr(
        engineering_config.repo,
        "list_source_snapshot_batches",
        lambda _session, _domain, **_kwargs: [existing],
    )
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [link])
    monkeypatch.setattr(engineering_config, "_safe_source_digest", lambda _batch: digest)
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5&q=T19C-BEV",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["rows"] == 1
        assert payload["items"][0]["sourceFileName"] == "mixed-source.xlsx"
        assert payload["items"][0]["extractStatus"] == "digest_ready"
        assert "Model T19C-BEV（2025款）" in payload["items"][0]["sourceSearchMatches"]
    finally:
        app.dependency_overrides.clear()


def test_source_snapshot_search_matches_digest_identity_and_ocr(monkeypatch, tmp_path) -> None:
    source_id = uuid4()
    source_path = tmp_path / "opaque-source.pdf"
    source_path.write_bytes(b"%PDF-1.4 placeholder")
    existing = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="opaque-source.pdf",
        source_file_path=str(source_path),
        source_file_hash="hash",
        import_status="registered",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    digest = {
        "digestType": "pdf_ocr",
        "sourceFormat": "pdf_ocr",
        "status": "ready",
        "fileName": "opaque-source.pdf",
        "modelName": "Scanned config",
        "ocrEngine": "paddleocr",
        "ocrEvaluation": {
            "strategy": "highest_config_semantic_score",
            "reason": "highest_config_semantic_score",
            "candidateCount": 2,
            "selectedEngine": "paddleocr",
            "selectedEngines": ["paddleocr"],
        },
        "ocrEngineCandidates": [
            {"engine": "legacy_pdf_ocr", "sourceType": "pdf_ocr", "selected": False},
            {"engine": "paddleocr", "sourceType": "pdf_ocr", "selected": True},
        ],
        "summary": {
            "sheetCount": 1,
            "tableCount": 1,
            "candidateTrimCount": 2,
            "comparableGroupCount": 1,
            "featureCount": 1,
            "differenceCount": 1,
        },
        "sheets": [],
        "compareGroups": [
            {
                "groupId": "ocr-group",
                "title": "Scanned config group",
                "sourceSheet": "PDF OCR Page 1",
                "modelName": "Scanned config",
                "sourceKind": "price_list",
                "trimCount": 2,
                "featureCount": 1,
                "differenceCount": 1,
                "trims": [
                    {
                        "trimId": "ocr-basic",
                        "trimName": "Basic",
                        "fullTrimName": "Basic",
                        "modelName": "Scanned config",
                        "profile": {
                            "country": "Germany",
                            "modelYear": "2026",
                            "powertrain": "BEV",
                            "drivetrain": "RWD",
                            "materialNo": "T19CFL**MM0001",
                        },
                    },
                    {
                        "trimId": "ocr-premium",
                        "trimName": "Premium",
                        "fullTrimName": "Premium",
                        "modelName": "Scanned config",
                        "profile": {
                            "country": "Germany",
                            "modelYear": "2026",
                            "powertrain": "BEV",
                            "drivetrain": "AWD",
                            "configurationVersion": "PREMIUM-SV",
                        },
                    },
                ],
                "rows": [],
            }
        ],
    }

    monkeypatch.setattr(engineering_config.repo, "list_source_snapshot_batches", lambda _session, _domain, **_kwargs: [existing])
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config, "_safe_source_digest", lambda _batch: digest)
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        headers = {"X-Auth-Token": "change-me", "X-User-Name": "tester"}

        year_response = client.get("/v1/engineering-config/source/snapshots?limit=5&q=2026", headers=headers)
        assert year_response.status_code == 200
        year_payload = year_response.json()
        assert year_payload["rows"] == 1
        assert "年款 2026" in year_payload["items"][0]["sourceSearchMatches"]

        sales_version_response = client.get("/v1/engineering-config/source/snapshots?limit=5&q=PREMIUM-SV", headers=headers)
        assert sales_version_response.status_code == 200
        sales_version_payload = sales_version_response.json()
        assert sales_version_payload["rows"] == 1
        assert "Sales version PREMIUM-SV" in sales_version_payload["items"][0]["sourceSearchMatches"]

        ocr_response = client.get("/v1/engineering-config/source/snapshots?limit=5&q=paddleocr", headers=headers)
        assert ocr_response.status_code == 200
        ocr_payload = ocr_response.json()
        assert ocr_payload["rows"] == 1
        assert "OCR paddleocr" in ocr_payload["items"][0]["sourceSearchMatches"]
        ocr_status = ocr_payload["items"][0]["sourceDigestStatus"]
        assert ocr_status["ocrEngine"] == "paddleocr"
        assert ocr_status["ocrEvaluation"]["selectedEngine"] == "paddleocr"
        assert ocr_status["ocrEngineCandidates"] == [
            {"engine": "legacy_pdf_ocr", "sourceType": "pdf_ocr", "selected": False},
            {"engine": "paddleocr", "sourceType": "pdf_ocr", "selected": True},
        ]

        powertrain_response = client.get("/v1/engineering-config/source/snapshots?limit=5&q=BEV", headers=headers)
        assert powertrain_response.status_code == 200
        powertrain_payload = powertrain_response.json()
        assert powertrain_payload["rows"] == 1
        assert "动力 BEV" in powertrain_payload["items"][0]["sourceSearchMatches"]

        drivetrain_response = client.get("/v1/engineering-config/source/snapshots?limit=5&q=AWD", headers=headers)
        assert drivetrain_response.status_code == 200
        drivetrain_payload = drivetrain_response.json()
        assert drivetrain_payload["rows"] == 1
        assert "动力 AWD" in drivetrain_payload["items"][0]["sourceSearchMatches"]
    finally:
        app.dependency_overrides.clear()


def test_source_snapshot_search_reports_digest_matches_with_file_match(monkeypatch, tmp_path) -> None:
    source_id = uuid4()
    source_path = tmp_path / "T19C-mixed-source.xlsx"
    source_path.write_bytes(b"placeholder")
    existing = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="T19C-mixed-source.xlsx",
        source_file_path=str(source_path),
        source_file_hash="hash",
        import_status="registered",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    digest = {
        "digestType": "workbook",
        "status": "ready",
        "fileName": "T19C-mixed-source.xlsx",
        "modelName": "EU resource table",
        "summary": {
            "sheetCount": 1,
            "tableCount": 1,
            "candidateTrimCount": 2,
            "comparableGroupCount": 1,
            "featureCount": 1,
            "differenceCount": 1,
        },
        "sheets": [],
        "compareGroups": [
            {
                "groupId": "t19c-bev",
                "title": "T19C-BEV（2025款）",
                "sourceSheet": "T19C-BEV",
                "modelName": "T19C-BEV（2025款）",
                "trimCount": 2,
                "trims": [
                    {
                        "trimId": "premium",
                        "trimName": "Premium-FWD",
                        "fullTrimName": "两驱尊贵型 Premium-FWD",
                        "modelName": "T19C-BEV（2025款）",
                    }
                ],
                "rows": [],
            }
        ],
    }

    monkeypatch.setattr(engineering_config.repo, "list_source_snapshot_batches", lambda _session, _domain, **_kwargs: [existing])
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config, "_safe_source_digest", lambda _batch: digest)
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/engineering-config/source/snapshots?limit=5&q=T19C",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert response.status_code == 200
        matches = response.json()["items"][0]["sourceSearchMatches"]
        assert "文件 T19C-mixed-source.xlsx" in matches
        assert "Model T19C-BEV（2025款）" in matches
    finally:
        app.dependency_overrides.clear()


def test_source_snapshot_trash_restore_and_clear(monkeypatch) -> None:
    source_id = uuid4()
    existing = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="source.pdf",
        source_file_path="/tmp/source.pdf",
        source_file_hash="hash",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    statuses: list[str] = []

    def fake_set_status(_session, import_batch_id, status):
        assert import_batch_id == source_id
        statuses.append(status)
        existing.import_status = status
        return 1

    cleared_countries: list[str | None] = []

    def fake_clear_source_snapshot_trash(_session, _domain, country=None):
        cleared_countries.append(country)
        return 2

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, import_batch_id: existing if import_batch_id == source_id else None)
    monkeypatch.setattr(engineering_config.repo, "set_import_batch_status", fake_set_status)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config.repo, "clear_source_snapshot_trash", fake_clear_source_snapshot_trash)
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        trash_response = client.delete(
            f"/v1/engineering-config/source/snapshots/{source_id}",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert trash_response.status_code == 200
        assert trash_response.json()["uploadStatus"] == "trashed"
        assert statuses[-1] == "trashed"

        restore_response = client.post(
            f"/v1/engineering-config/source/snapshots/{source_id}/restore",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert restore_response.status_code == 200
        assert restore_response.json()["uploadStatus"] == "registered"
        assert statuses[-1] == "stored"

        missing_country_clear_response = client.delete(
            "/v1/engineering-config/source/trash",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert missing_country_clear_response.status_code == 400
        assert missing_country_clear_response.json()["detail"] == "country is required to clear source trash"
        assert cleared_countries == []

        clear_response = client.delete(
            "/v1/engineering-config/source/trash?country=Germany",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert clear_response.status_code == 200
        assert clear_response.json()["cleared"] == 2
        assert clear_response.json()["country"] == "Germany"
        assert cleared_countries == ["Germany"]
    finally:
        app.dependency_overrides.clear()


def test_source_snapshot_country_trash_restore_uses_context_status(monkeypatch) -> None:
    source_id = uuid4()
    existing = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="source.pdf",
        source_file_path="/tmp/source.pdf",
        source_file_hash="hash",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    germany_link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="Volvo",
        model_name="XC60",
        model_year="2026",
        market="Germany",
        country="Germany",
        segment="SUV C",
        trim_ids=["core"],
        sales_version_ids=[],
        context_type="compare",
        created_by="alice",
        created_at_utc=datetime.now(timezone.utc),
    )
    france_link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="Volvo",
        model_name="XC60",
        model_year="2026",
        market="France",
        country="France",
        segment="SUV C",
        trim_ids=["core"],
        sales_version_ids=[],
        context_type="compare",
        created_by="bob",
        created_at_utc=datetime.now(timezone.utc),
    )
    context_updates: list[tuple[str, str]] = []

    def fail_global_status_update(*_args, **_kwargs):
        raise AssertionError("country-scoped trash must not update the source snapshot globally")

    def fake_set_source_context_status(_session, updated_source_id, status, *, country=None):
        assert updated_source_id == source_id
        assert country == "Germany"
        context_updates.append((country, status))
        germany_link.status = status
        return 1

    monkeypatch.setattr(engineering_config.repo, "get_import_batch", lambda _session, import_batch_id: existing if import_batch_id == source_id else None)
    monkeypatch.setattr(engineering_config.repo, "set_import_batch_status", fail_global_status_update)
    monkeypatch.setattr(engineering_config.repo, "set_source_context_status", fake_set_source_context_status)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [germany_link, france_link])
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        trash_response = client.delete(
            f"/v1/engineering-config/source/snapshots/{source_id}?country=Germany",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert trash_response.status_code == 200
        trash_payload = trash_response.json()
        assert trash_payload["uploadStatus"] == "trashed"
        assert trash_payload["libraryStatus"] == "stored"
        assert trash_payload["inTrash"] is True
        assert trash_payload["relatedContext"]["country"] == "Germany"
        assert trash_payload["contexts"][0]["status"] == "trashed"
        assert france_link.status == "active"

        restore_response = client.post(
            f"/v1/engineering-config/source/snapshots/{source_id}/restore?country=Germany",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert restore_response.status_code == 200
        restore_payload = restore_response.json()
        assert restore_payload["uploadStatus"] == "registered"
        assert restore_payload["libraryStatus"] == "stored"
        assert restore_payload["inTrash"] is False
        assert restore_payload["relatedContext"]["country"] == "Germany"
        assert restore_payload["contexts"][0]["status"] == "active"
        assert context_updates == [("Germany", "trashed"), ("Germany", "active")]
    finally:
        app.dependency_overrides.clear()


def test_source_snapshot_detail_returns_contexts(tmp_path, monkeypatch) -> None:
    source_id = uuid4()
    source_path = tmp_path / "source.pdf"
    source_path.write_bytes(b"%PDF-1.4 ok")
    existing = FakeImportBatch(
        import_batch_id=source_id,
        domain="engineering_config_source",
        source_file_name="source.pdf",
        source_file_path=str(source_path),
        source_file_hash=engineering_config._sha256_for_path(source_path),
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="Volvo",
        model_name="XC60",
        model_year="2026",
        market="Germany",
        country="Germany",
        trim_ids=["core", "ultra"],
        sales_version_ids=[],
        context_type="compare",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
    )
    purged_link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="Volvo",
        model_name="XC60",
        model_year="2026",
        market="France",
        country="France",
        trim_ids=["core-fr"],
        sales_version_ids=[],
        context_type="compare",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
        status="purged",
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "get_import_batch",
        lambda _session, import_batch_id: existing if import_batch_id == source_id else None,
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "list_source_context_links",
        lambda _session, _source_id: [purged_link, link],
    )
    app.dependency_overrides[get_db_session] = lambda: FakeImportSession()
    try:
        client = TestClient(app)
        response = client.get(
            f"/v1/engineering-config/source/snapshots/{source_id}",
            headers={"X-Auth-Token": "change-me", "X-User-Name": "tester"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["sourceId"] == str(source_id)
        assert payload["relatedContext"]["trimIds"] == ["core", "ultra"]
        assert payload["relatedContext"]["country"] == "Germany"
        assert len(payload["contexts"]) == 1
        assert payload["contexts"][0]["contextType"] == "compare"
        assert payload["contexts"][0]["model"] == "XC60"
        assert payload["contexts"][0]["status"] == "active"
    finally:
        app.dependency_overrides.clear()


def test_competitor_recommendations_join_advanced_analysis_and_config_library(monkeypatch) -> None:
    from app.services import advanced_analysis_service

    rival_trim_id = uuid4()
    rival_trim = FakeTrim(
        trim_id=rival_trim_id,
        full_trim_name="Rival Premium AWD",
        brand="RivalBrand",
        model_name="Rival C-SUV",
        trim_name="Premium AWD",
        market="Germany",
        model_year="2026",
        energy_type="PHEV",
        drivetrain="AWD",
        engine=None,
        vehicle_code="RIVAL-PREMIUM",
        material_no=None,
        identity_key="rival-premium-awd",
    )
    captured: dict[str, object] = {}

    def fake_compute_competitor_set(**kwargs):
        captured.update(kwargs)
        return {
            "analysis_mode": "target_model",
            "target_period": "2026-05",
            "base_period": "2025-05",
            "scope_model_count": 21,
            "competitors": [
                {
                    "model": "Rival C-SUV",
                    "make": "RivalBrand",
                    "profile": {"make": "RivalBrand", "segment": "SUV C", "powertrain": "PHEV"},
                    "role": "likely_source",
                    "similarity_score": 84.2,
                    "sales_tgt": 1200.0,
                    "sales_base": 980.0,
                    "dV": 220.0,
                    "share_tgt": 0.12,
                    "share_change": 0.02,
                    "pure_share_shift": 80.0,
                    "estimated_flow": 35.0,
                    "shared_dims": ["powertrain", "segment"],
                    "match_evidence": [
                        {"field": "segment", "label": "Segment", "detail": "Segment: same SUV C", "score": 100.0}
                    ],
                },
                {
                    "model": "Missing Rival",
                    "make": "MissingBrand",
                    "profile": {"make": "MissingBrand", "segment": "SUV C", "powertrain": "PHEV"},
                    "role": "adjacent",
                    "similarity_score": 72.0,
                    "sales_tgt": 800.0,
                    "sales_base": 900.0,
                    "dV": -100.0,
                    "share_tgt": 0.08,
                    "share_change": -0.01,
                    "pure_share_shift": -40.0,
                    "estimated_flow": 0.0,
                    "shared_dims": ["powertrain", "segment"],
                    "match_evidence": [],
                },
            ],
        }

    def fake_list_vehicle_trims(_session, **kwargs):
        if kwargs.get("model_name") == "Rival C-SUV":
            return [rival_trim]
        return []

    monkeypatch.setattr(advanced_analysis_service, "compute_competitor_set", fake_compute_competitor_set)
    monkeypatch.setattr(engineering_config.repo, "list_vehicle_trims", fake_list_vehicle_trims)

    payload = engineering_config.list_competitor_recommendations(
        country="Germany",
        model_name="Target C-SUV",
        powertrain="PHEV",
        segment="SUV C",
        limit=10,
        session=FakeSession({}),
        _=None,
    )

    assert captured["country"] == "德国"
    assert captured["target_model"] == "Target C-SUV"
    assert captured["fuel_types"] == ["PHEV"]
    assert captured["segments"] == ["SUV C及以上"]
    assert payload["rows"] == 2
    assert payload["source"]["type"] == "advanced_analysis_competitor_set"
    assert payload["source"]["advancedAnalysisCountry"] == "德国"
    assert payload["source"]["advancedAnalysisSegment"] == "SUV C及以上"
    assert payload["items"][0]["modelName"] == "Rival C-SUV"
    assert payload["items"][0]["configAvailable"] is True
    assert payload["items"][0]["configTrimCount"] == 1
    assert payload["items"][0]["trims"][0]["trimId"] == str(rival_trim_id)
    assert payload["items"][0]["nextAction"] == "select_config_trim"
    assert payload["items"][1]["modelName"] == "Missing Rival"
    assert payload["items"][1]["configAvailable"] is False
    assert payload["items"][1]["nextAction"] == "upload_source"


def test_competitor_recommendations_report_source_digest_waiting_for_draft(monkeypatch) -> None:
    from app.services import advanced_analysis_service

    source_id = uuid4()
    source_snapshot_queries: list[dict[str, object]] = []
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="uploaded-config.xlsx",
        source_file_path="/tmp/uploaded-config.xlsx",
        source_file_hash="missing-rival-hash",
        import_status="completed",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )

    def fake_compute_competitor_set(**_kwargs):
        return {
            "analysis_mode": "target_model",
            "target_period": "2026-05",
            "base_period": "2025-05",
            "scope_model_count": 9,
            "competitors": [
                {
                    "model": "Missing Rival",
                    "make": "MissingBrand",
                    "profile": {"make": "MissingBrand", "segment": "SUV C", "powertrain": "PHEV"},
                    "shared_dims": ["powertrain", "segment"],
                },
            ],
        }

    def fake_list_source_snapshot_batches(_session, _domain, **kwargs):
        source_snapshot_queries.append({"domain": _domain, **kwargs})
        assert _domain == engineering_config.SOURCE_IMPORT_DOMAIN
        assert kwargs["country"] == "Germany"
        assert kwargs["segment"] == "SUV C"
        assert kwargs["query"] is None
        assert kwargs["limit"] == engineering_config.SOURCE_SNAPSHOT_LIST_CANDIDATE_LIMIT
        return [source_batch]

    def fake_source_digest(_batch):
        return {
            "digestType": "workbook",
            "status": "ready",
            "compareGroups": [
                {
                    "groupId": "missing-rival-sheet",
                    "modelName": "Missing Rival",
                    "title": "Missing Rival · Config",
                    "trimCount": 2,
                    "featureCount": 18,
                    "trims": [
                        {
                            "trimId": "missing-basic",
                            "trimName": "Basic",
                            "modelName": "Missing Rival",
                            "brand": "MissingBrand",
                            "profile": {"powertrain": "PHEV"},
                        },
                        {
                            "trimId": "missing-premium",
                            "trimName": "Premium",
                            "modelName": "Missing Rival",
                            "brand": "MissingBrand",
                            "profile": {"powertrain": "PHEV"},
                        },
                    ],
                }
            ],
        }

    monkeypatch.setattr(advanced_analysis_service, "compute_competitor_set", fake_compute_competitor_set)
    monkeypatch.setattr(engineering_config.repo, "list_vehicle_trims", lambda _session, **_kwargs: [])
    monkeypatch.setattr(engineering_config.repo, "list_source_snapshot_batches", fake_list_source_snapshot_batches)
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [])
    monkeypatch.setattr(engineering_config, "_safe_source_digest", fake_source_digest)

    payload = engineering_config.list_competitor_recommendations(
        country="Germany",
        model_name="Target C-SUV",
        powertrain="PHEV",
        segment="SUV C",
        limit=10,
        session=FakeSession({}),
        _=None,
    )

    assert payload["rows"] == 1
    item = payload["items"][0]
    assert item["modelName"] == "Missing Rival"
    assert item["configAvailable"] is False
    assert item["configTrimCount"] == 0
    assert item["sourceDigestAvailable"] is True
    assert item["sourceDigestSourceCount"] == 1
    assert item["sourceDigestGroupCount"] == 1
    assert item["sourceDigestTrimCount"] == 2
    assert item["sourceDigestSearchQuery"] == "MissingBrand Missing Rival Germany PHEV SUV C"
    assert item["sourceDigestMatches"][0]["sourceId"] == str(source_id)
    assert item["sourceDigestMatches"][0]["sourceFileName"] == "uploaded-config.xlsx"
    assert item["nextAction"] == "create_from_source_digest"
    assert item["trims"] == []
    assert len(source_snapshot_queries) == 1


def test_competitor_recommendations_use_source_context_when_digest_model_is_inferred(monkeypatch) -> None:
    from app.services import advanced_analysis_service

    source_id = uuid4()
    source_batch = FakeImportBatch(
        import_batch_id=source_id,
        domain=engineering_config.SOURCE_IMPORT_DOMAIN,
        source_file_name="bmw-x7-competitor-config.csv",
        source_file_path="/tmp/bmw-x7-competitor-config.csv",
        source_file_hash="bmw-x7-hash",
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by="tester",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
        created_at_utc=datetime.now(timezone.utc),
    )
    context_link = FakeSourceContextLink(
        id=uuid4(),
        source_id=source_id,
        batch_id=source_id,
        brand="BMW",
        model_name="BMW X7",
        model_year=None,
        market="Germany",
        country="Germany",
        powertrain="ICE",
        segment="SUV C",
        trim_ids=[],
        sales_version_ids=[],
        context_type="competitor_recommendation_upload",
        created_by="tester",
        created_at_utc=datetime.now(timezone.utc),
        scenario="advanced_analysis_competitor_top10_smoke",
        identity_anchor="brand_model_market_powertrain",
    )

    def fake_compute_competitor_set(**_kwargs):
        return {
            "analysis_mode": "target_model",
            "target_period": "2026-05",
            "base_period": "2025-05",
            "scope_model_count": 9,
            "competitors": [
                {
                    "model": "BMW X7",
                    "make": "BMW",
                    "profile": {"make": "BMW", "segment": "SUV C", "powertrain": "ICE"},
                    "shared_dims": ["powertrain", "segment"],
                },
            ],
        }

    def fake_source_digest(_batch):
        return {
            "digestType": "tabular",
            "status": "ready",
            "compareGroups": [
                {
                    "groupId": "tabular-file-name-derived",
                    "modelName": "bmw x7 competitor 1783136016",
                    "title": "bmw x7 competitor 1783136016 · upload",
                    "trimCount": 2,
                    "featureCount": 8,
                    "trims": [
                        {"trimId": "xdrive40i", "trimName": "xDrive40i"},
                        {"trimId": "m60i", "trimName": "M60i"},
                    ],
                }
            ],
        }

    monkeypatch.setattr(advanced_analysis_service, "compute_competitor_set", fake_compute_competitor_set)
    monkeypatch.setattr(engineering_config.repo, "list_vehicle_trims", lambda _session, **_kwargs: [])
    monkeypatch.setattr(engineering_config.repo, "list_source_snapshot_batches", lambda _session, _domain, **_kwargs: [source_batch])
    monkeypatch.setattr(engineering_config.repo, "list_source_context_links", lambda _session, _source_id: [context_link])
    monkeypatch.setattr(engineering_config, "_safe_source_digest", fake_source_digest)

    payload = engineering_config.list_competitor_recommendations(
        country="Germany",
        model_name="Target C-SUV",
        powertrain="ICE",
        segment="SUV C",
        limit=10,
        session=FakeSession({}),
        _=None,
    )

    item = payload["items"][0]
    assert item["modelName"] == "BMW X7"
    assert item["configAvailable"] is False
    assert item["sourceDigestAvailable"] is True
    assert item["sourceDigestSourceCount"] == 1
    assert item["sourceDigestGroupCount"] == 1
    assert item["sourceDigestTrimCount"] == 2
    assert item["nextAction"] == "create_from_source_digest"


def test_digest_feature_alias_match_prefers_existing_canonical_feature(monkeypatch) -> None:
    canonical = FakeFeature(
        uuid4(),
        "digest_df9a8c8d2a1dc6e5",
        "驾驶辅助 Drive assist",
        "360 round view camera / 360度高清全景影像",
        12,
    )
    stale_source_feature = FakeFeature(
        uuid4(),
        "digest_stale_360_camera",
        "Comfort",
        "360 camera",
        409,
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "list_feature_catalog",
        lambda _session, **_kwargs: [stale_source_feature, canonical],
    )

    matched = engineering_config._feature_catalog_match_for_digest_row(
        FakeSession({}),
        "360 camera",
    )

    assert matched is canonical


def test_digest_feature_alias_match_prefers_business_matrix_category(monkeypatch) -> None:
    older_generic_feature = FakeFeature(
        uuid4(),
        "digest_0f8f3e35cc018ac4",
        "驾驶辅助系统",
        "360 round view camera / 360度高清全景影像",
        113,
    )
    canonical = FakeFeature(
        uuid4(),
        "digest_df9a8c8d2a1dc6e5",
        "驾驶辅助 Drive assist",
        "360 round view camera / 360度高清全景影像",
        270,
    )
    stale_source_feature = FakeFeature(
        uuid4(),
        "digest_8de2da4cbbb44aad",
        "Comfort",
        "360 camera",
        784,
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "list_feature_catalog",
        lambda _session, **_kwargs: [older_generic_feature, canonical, stale_source_feature],
    )

    matched = engineering_config._feature_catalog_match_for_digest_row(
        FakeSession({}),
        "360 camera",
    )

    assert matched is canonical


def test_digest_feature_for_row_allows_semantic_alias_to_override_stale_code_match(monkeypatch) -> None:
    canonical = FakeFeature(
        uuid4(),
        "digest_df9a8c8d2a1dc6e5",
        "驾驶辅助 Drive assist",
        "360 round view camera / 360度高清全景影像",
        12,
    )
    stale_source_feature = FakeFeature(
        uuid4(),
        "digest_8de2da4cbbb44aad",
        "Comfort",
        "360 camera",
        409,
    )
    row = {
        "category": "Comfort",
        "featureName": "360 camera",
        "featureCode": "digest_8de2da4cbbb44aad",
    }

    monkeypatch.setattr(
        engineering_config.repo,
        "get_feature_catalog_by_code",
        lambda _session, _feature_code: stale_source_feature,
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "get_feature_catalog_by_category_field",
        lambda _session, _category, _field_name: None,
    )

    matched, created, match_reason = engineering_config._digest_feature_for_row(
        FakeSession({}),
        row,
        next_order=410,
        catalog_features=[stale_source_feature, canonical],
    )

    assert matched is canonical
    assert created is False
    assert match_reason in {"alias", "semantic_alias"}


@pytest.mark.parametrize(
    ("canonical_name", "source_name"),
    [
        ("Panoramic skylight / 全景天窗", "Panoramadach (Ausrüstungscode – 006)"),
        (
            "Induction electric tailgate (key induction) / 感应电动尾门（钥匙感应）",
            "Heckklappe elektrisch",
        ),
        (
            "Ventilated front seats (cushions + backrests) / 前排座椅通风（坐垫+靠背）",
            "Sitzbelüftung vorne",
        ),
        ("Driver seat with memory function / 座椅记忆", "Fahrersitz mit Memory"),
    ],
)
def test_digest_feature_for_row_reuses_governed_multilingual_product_feature(
    monkeypatch,
    canonical_name: str,
    source_name: str,
) -> None:
    canonical = FakeFeature(
        uuid4(),
        "governed_product_feature",
        "Product configuration",
        canonical_name,
        12,
        aliases=[canonical_name],
    )
    row = {
        "category": "Official competitor PDF",
        "featureName": source_name,
        "featureCode": "source_digest_feature",
    }

    monkeypatch.setattr(
        engineering_config.repo,
        "get_feature_catalog_by_code",
        lambda _session, _feature_code: None,
    )
    monkeypatch.setattr(
        engineering_config.repo,
        "get_feature_catalog_by_category_field",
        lambda _session, _category, _field_name: None,
    )

    matched, created, match_reason = engineering_config._digest_feature_for_row(
        FakeSession({}),
        row,
        next_order=99,
        catalog_features=[canonical],
    )

    assert matched is canonical
    assert created is False
    assert match_reason == "semantic_alias"


def test_competitor_recommendations_skip_target_model_and_respect_limit(monkeypatch) -> None:
    from app.services import advanced_analysis_service

    captured: dict[str, object] = {}
    trim_queries: list[dict[str, object]] = []

    def fake_compute_competitor_set(**kwargs):
        captured.update(kwargs)
        return {
            "analysis_mode": "target_model",
            "target_period": "2026-05",
            "base_period": "2025-05",
            "scope_model_count": 12,
            "competitors": [
                {"model": "Target C-SUV", "make": "TargetBrand", "profile": {"segment": "SUV C"}},
                {"model": "Rival One", "make": "RivalBrand", "profile": {"segment": "SUV C"}},
                {"model": "Rival Two", "make": "RivalBrand", "profile": {"segment": "SUV C"}},
                {"model": "Rival Three", "make": "RivalBrand", "profile": {"segment": "SUV C"}},
            ],
        }

    def fake_list_vehicle_trims(_session, **kwargs):
        trim_queries.append(kwargs)
        return []

    monkeypatch.setattr(advanced_analysis_service, "compute_competitor_set", fake_compute_competitor_set)
    monkeypatch.setattr(engineering_config.repo, "list_vehicle_trims", fake_list_vehicle_trims)

    payload = engineering_config.list_competitor_recommendations(
        country="Germany",
        model_name="Target C-SUV",
        powertrain="PHEV",
        segment="SUV C",
        limit=2,
        session=FakeSession({}),
        _=None,
    )

    assert captured["top_n"] == 5
    assert payload["rows"] == 2
    assert [item["modelName"] for item in payload["items"]] == ["Rival One", "Rival Two"]
    assert [item["sourceRank"] for item in payload["items"]] == [2, 3]
    assert all(query["status"] == "active" for query in trim_queries)
    assert all(query["limit"] == 8 for query in trim_queries)


def test_competitor_recommendations_fetch_extra_candidates_for_top_ten(monkeypatch) -> None:
    from app.services import advanced_analysis_service

    captured: dict[str, object] = {}
    source_snapshot_queries: list[dict[str, object]] = []
    competitors = [
        {"model": "Target C-SUV", "make": "TargetBrand", "profile": {"segment": "SUV C"}},
        {"model": "Rival 01", "make": "RivalBrand", "profile": {"segment": "SUV C"}},
        {"model": "Rival 01", "make": "RivalBrand", "profile": {"segment": "SUV C"}},
    ]
    competitors.extend(
        {"model": f"Rival {index:02d}", "make": "RivalBrand", "profile": {"segment": "SUV C"}}
        for index in range(2, 12)
    )

    def fake_compute_competitor_set(**kwargs):
        captured.update(kwargs)
        return {
            "analysis_mode": "target_model",
            "target_period": "2026-05",
            "base_period": "2025-05",
            "scope_model_count": 42,
            "competitors": competitors,
        }

    monkeypatch.setattr(advanced_analysis_service, "compute_competitor_set", fake_compute_competitor_set)
    monkeypatch.setattr(engineering_config.repo, "list_vehicle_trims", lambda _session, **_kwargs: [])
    monkeypatch.setattr(
        engineering_config.repo,
        "list_source_snapshot_batches",
        lambda _session, domain, **kwargs: source_snapshot_queries.append({"domain": domain, **kwargs}) or [],
    )

    payload = engineering_config.list_competitor_recommendations(
        country="Germany",
        model_name="Target C-SUV",
        powertrain="PHEV",
        segment="SUV C",
        limit=10,
        session=FakeSession({}),
        _=None,
    )

    assert captured["top_n"] == 20
    assert payload["rows"] == 10
    assert [item["modelName"] for item in payload["items"]] == [f"Rival {index:02d}" for index in range(1, 11)]
    assert all(item["modelName"] != "Target C-SUV" for item in payload["items"])
    assert len({item["modelName"] for item in payload["items"]}) == 10
    assert source_snapshot_queries == [
        {
            "domain": engineering_config.SOURCE_IMPORT_DOMAIN,
            "country": "Germany",
            "segment": "SUV C",
            "query": None,
            "limit": engineering_config.SOURCE_SNAPSHOT_LIST_CANDIDATE_LIMIT,
        }
    ]
