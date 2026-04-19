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
            "airflow": {
                "available": True,
                "mode": "running",
                "detail": "ok",
                "uiUrl": "http://127.0.0.1:8080",
                "running": True,
                "runningServices": 3,
                "totalServices": 3,
                "updatedAt": "2026-04-16T00:00:00+00:00",
                "services": [],
                "actions": {
                    "canStart": False,
                    "canStop": True,
                    "canOpenUi": True,
                },
            },
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


def test_get_airflow_status_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        data_management,
        "read_airflow_ops_status",
        lambda: {
            "available": True,
            "mode": "running",
            "detail": "Airflow running",
            "uiUrl": "http://127.0.0.1:8080",
            "running": True,
            "runningServices": 3,
            "totalServices": 3,
            "updatedAt": "2026-04-16T00:00:00+00:00",
            "services": [],
            "actions": {
                "canStart": False,
                "canStop": True,
                "canOpenUi": True,
            },
        },
    )

    client = TestClient(app)
    response = client.get("/v1/data-management/airflow/status", headers=_headers())

    assert response.status_code == 200
    assert response.json()["item"]["mode"] == "running"


def test_start_airflow_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        data_management,
        "start_airflow_stack",
        lambda: {
            "action": "start",
            "detail": "started",
            "status": {
                "available": True,
                "mode": "running",
                "detail": "Airflow running",
                "uiUrl": "http://127.0.0.1:8080",
                "running": True,
                "runningServices": 3,
                "totalServices": 3,
                "updatedAt": "2026-04-16T00:00:00+00:00",
                "services": [],
                "actions": {
                    "canStart": False,
                    "canStop": True,
                    "canOpenUi": True,
                },
            },
        },
    )

    client = TestClient(app)
    response = client.post("/v1/data-management/airflow/start", headers=_headers())

    assert response.status_code == 200
    assert response.json()["item"]["action"] == "start"


def test_start_airflow_route_maps_value_error(monkeypatch) -> None:
    monkeypatch.setattr(
        data_management,
        "start_airflow_stack",
        lambda: (_ for _ in ()).throw(ValueError("already stopped")),
    )

    client = TestClient(app)
    response = client.post(
        "/v1/data-management/airflow/start",
        headers=_headers(),
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "already stopped"


def test_start_airflow_route_maps_runtime_error(monkeypatch) -> None:
    monkeypatch.setattr(
        data_management,
        "start_airflow_stack",
        lambda: (_ for _ in ()).throw(RuntimeError("start failed")),
    )

    client = TestClient(app)
    response = client.post(
        "/v1/data-management/airflow/start",
        headers=_headers(),
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "start failed"


def test_stop_airflow_route_maps_runtime_error(monkeypatch) -> None:
    monkeypatch.setattr(
        data_management,
        "stop_airflow_stack",
        lambda: (_ for _ in ()).throw(RuntimeError("stop failed")),
    )

    client = TestClient(app)
    response = client.post("/v1/data-management/airflow/stop", headers=_headers())

    assert response.status_code == 500
    assert response.json()["detail"] == "stop failed"


def test_stop_airflow_route_maps_value_error(monkeypatch) -> None:
    monkeypatch.setattr(
        data_management,
        "stop_airflow_stack",
        lambda: (_ for _ in ()).throw(ValueError("already stopped")),
    )

    client = TestClient(app)
    response = client.post(
        "/v1/data-management/airflow/stop",
        headers=_headers(),
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "already stopped"


def test_sync_voc_raw_route_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        data_management,
        "sync_voc_raw_to_store",
        lambda: {
            "root": "04_Processed_data/voc",
            "countryCount": 2,
            "sourceRunCount": 6,
            "documentCount": 14,
            "errorCount": 1,
        },
    )

    client = TestClient(app)
    response = client.post("/v1/data-management/voc/sync", headers=_headers())

    assert response.status_code == 200
    assert response.json()["item"]["documentCount"] == 14


def test_sync_voc_raw_route_maps_runtime_error(monkeypatch) -> None:
    monkeypatch.setattr(
        data_management,
        "sync_voc_raw_to_store",
        lambda: (_ for _ in ()).throw(RuntimeError("sync failed")),
    )

    client = TestClient(app)
    response = client.post("/v1/data-management/voc/sync", headers=_headers())

    assert response.status_code == 500
    assert response.json()["detail"] == "sync failed"
