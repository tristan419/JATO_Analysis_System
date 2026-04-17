from fastapi.testclient import TestClient

from app.api.routes import data_management
from app.main import app


def _headers() -> dict[str, str]:
    return {
        "X-Auth-Token": "change-me",
        "X-User-Name": "tester",
    }


def test_get_data_management_overview_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        data_management,
        "read_data_management_overview",
        lambda: {
            "generatedAt": "2026-04-16T00:00:00+00:00",
            "database": {"enabled": True, "connected": True, "detail": "ok"},
            "domains": [],
            "fileInventory": [],
            "databaseTables": [],
            "activity": {
                "days": [],
                "maxCount": 0,
                "totalCount": 0,
                "rangeStart": "2026-02-01",
                "rangeEnd": "2026-04-16",
                "sourceCounts": [],
                "databaseConnected": True,
            },
        },
    )

    client = TestClient(app)
    response = client.get("/v1/data-management/overview", headers=_headers())

    assert response.status_code == 200
    payload = response.json()["item"]
    assert payload["database"]["connected"] is True
    assert payload["generatedAt"] == "2026-04-16T00:00:00+00:00"
