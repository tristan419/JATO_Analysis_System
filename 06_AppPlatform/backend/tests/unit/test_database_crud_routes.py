from fastapi.testclient import TestClient

from app.api.routes import engineering, msrp, review
from app.db.session import get_db_session
from app.main import app


def _headers() -> dict[str, str]:
    return {
        "X-Auth-Token": "change-me",
        "X-User-Name": "tester",
    }


def test_delete_msrp_source_route_returns_deactivated_item(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp,
        "deactivate_msrp_source",
        lambda session, source_id: {
            "sourceId": source_id,
            "sourceCode": "cn_byd",
            "enabled": False,
        },
    )
    app.dependency_overrides[get_db_session] = lambda: None
    try:
        client = TestClient(app)
        response = client.delete("/v1/msrp/sources/source-1", headers=_headers())
        assert response.status_code == 200
        assert response.json()["item"]["enabled"] is False
    finally:
        app.dependency_overrides.clear()


def test_delete_engineering_project_route_returns_archived_item(monkeypatch) -> None:
    monkeypatch.setattr(
        engineering,
        "archive_config_project",
        lambda session, project_id: {
            "projectId": project_id,
            "projectCode": "proj-1",
            "status": "archived",
        },
    )
    app.dependency_overrides[get_db_session] = lambda: None
    try:
        client = TestClient(app)
        response = client.delete("/v1/engineering/projects/project-1", headers=_headers())
        assert response.status_code == 200
        assert response.json()["item"]["status"] == "archived"
    finally:
        app.dependency_overrides.clear()


def test_delete_match_override_route_returns_deleted_item(monkeypatch) -> None:
    monkeypatch.setattr(
        review,
        "delete_match_override",
        lambda session, override_id: {
            "overrideId": override_id,
            "jatoModel": "Seal",
        },
    )
    app.dependency_overrides[get_db_session] = lambda: None
    try:
        client = TestClient(app)
        response = client.delete("/v1/review/overrides/override-1", headers=_headers())
        assert response.status_code == 200
        assert response.json()["item"]["overrideId"] == "override-1"
    finally:
        app.dependency_overrides.clear()
