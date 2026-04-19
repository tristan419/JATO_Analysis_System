from datetime import date
from uuid import uuid4

from fastapi.testclient import TestClient

from app.api.routes import engineering
from app.db.session import get_db_session
from app.main import app


def _headers() -> dict[str, str]:
    return {
        "X-Auth-Token": "change-me",
        "X-User-Name": "tester",
    }


def test_get_config_base_variants_route_returns_payload(monkeypatch) -> None:
    project_id = uuid4()
    monkeypatch.setattr(
        engineering,
        "list_config_base_variants",
        lambda session, incoming_project_id, model, limit: {
            "rows": 1,
            "items": [
                {
                    "projectId": str(incoming_project_id),
                    "model": model,
                    "limit": limit,
                }
            ],
        },
    )
    app.dependency_overrides[get_db_session] = lambda: None
    try:
        client = TestClient(app)
        response = client.get(
            (
                "/v1/engineering/projects/base-variants"
                f"?project_id={project_id}&model=XC60&limit=25"
            ),
            headers=_headers(),
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["rows"] == 1
        assert payload["items"][0]["projectId"] == str(project_id)
        assert payload["items"][0]["model"] == "XC60"
        assert payload["items"][0]["limit"] == 25
    finally:
        app.dependency_overrides.clear()


def test_get_config_market_variants_route_returns_payload(monkeypatch) -> None:
    project_id = uuid4()
    base_variant_id = uuid4()
    monkeypatch.setattr(
        engineering,
        "list_config_market_variants",
        lambda session,
        incoming_project_id,
        incoming_base_variant_id,
        market_country,
        limit: {
            "rows": 1,
            "items": [
                {
                    "projectId": str(incoming_project_id),
                    "baseVariantId": str(incoming_base_variant_id),
                    "marketCountry": market_country,
                    "limit": limit,
                }
            ],
        },
    )
    app.dependency_overrides[get_db_session] = lambda: None
    try:
        client = TestClient(app)
        response = client.get(
            (
                "/v1/engineering/projects/market-variants"
                f"?project_id={project_id}"
                f"&base_variant_id={base_variant_id}"
                "&market_country=Sweden&limit=40"
            ),
            headers=_headers(),
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["rows"] == 1
        assert payload["items"][0]["projectId"] == str(project_id)
        assert payload["items"][0]["baseVariantId"] == str(base_variant_id)
        assert payload["items"][0]["marketCountry"] == "Sweden"
        assert payload["items"][0]["limit"] == 40
    finally:
        app.dependency_overrides.clear()


def test_get_config_feature_overrides_route_returns_payload(
    monkeypatch,
) -> None:
    project_id = uuid4()
    base_variant_id = uuid4()
    market_variant_id = uuid4()
    monkeypatch.setattr(
        engineering,
        "list_config_market_feature_overrides",
        lambda session,
        incoming_project_id,
        incoming_base_variant_id,
        incoming_market_variant_id,
        market_country,
        feature_code,
        limit: {
            "rows": 1,
            "items": [
                {
                    "projectId": str(incoming_project_id),
                    "baseVariantId": str(incoming_base_variant_id),
                    "marketVariantId": str(incoming_market_variant_id),
                    "marketCountry": market_country,
                    "featureCode": feature_code,
                    "limit": limit,
                }
            ],
        },
    )
    app.dependency_overrides[get_db_session] = lambda: None
    try:
        client = TestClient(app)
        response = client.get(
            (
                "/v1/engineering/projects/feature-overrides"
                f"?project_id={project_id}"
                f"&base_variant_id={base_variant_id}"
                f"&market_variant_id={market_variant_id}"
                "&market_country=Sweden"
                "&feature_code=heated_steering_wheel&limit=60"
            ),
            headers=_headers(),
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["rows"] == 1
        assert payload["items"][0]["projectId"] == str(project_id)
        assert payload["items"][0]["baseVariantId"] == str(base_variant_id)
        assert payload["items"][0]["marketVariantId"] == str(market_variant_id)
        assert payload["items"][0]["marketCountry"] == "Sweden"
        assert payload["items"][0]["featureCode"] == "heated_steering_wheel"
        assert payload["items"][0]["limit"] == 60
    finally:
        app.dependency_overrides.clear()


def test_post_config_normalization_route_returns_payload(monkeypatch) -> None:
    project_id = uuid4()
    monkeypatch.setattr(
        engineering,
        "normalize_config_project",
        lambda session, incoming_project_id: {
            "projectId": incoming_project_id,
            "baseVariantCount": 3,
            "marketVariantCount": 5,
            "featureOverrideCount": 8,
        },
    )
    app.dependency_overrides[get_db_session] = lambda: None
    try:
        client = TestClient(app)
        response = client.post(
            f"/v1/engineering/projects/{project_id}/normalize",
            headers=_headers(),
        )
        assert response.status_code == 200
        assert response.json()["item"] == {
            "projectId": str(project_id),
            "baseVariantCount": 3,
            "marketVariantCount": 5,
            "featureOverrideCount": 8,
        }
    finally:
        app.dependency_overrides.clear()


def test_post_config_import_route_passes_full_payload_model_dump(
    monkeypatch,
) -> None:
    project_id = uuid4()
    captured: dict[str, object] = {}

    def _run_import(_session, incoming_project_id, payload):
        captured["project_id"] = incoming_project_id
        captured["payload"] = payload
        return {"projectId": incoming_project_id, "rawRows": 10}

    monkeypatch.setattr(engineering, "run_config_import", _run_import)
    app.dependency_overrides[get_db_session] = lambda: None
    try:
        client = TestClient(app)
        response = client.post(
            f"/v1/engineering/projects/{project_id}/imports",
            headers=_headers(),
            json={
                "source_file_path": "imports/xc60.xlsx",
                "sheet_name": "Data Export",
                "source_schema_version": "v2",
                "replace_mode": "incremental",
                "valid_from_date": "2026-04-01",
                "notes": "nightly import",
            },
        )
        assert response.status_code == 200
        assert captured == {
            "project_id": str(project_id),
            "payload": {
                "source_file_path": "imports/xc60.xlsx",
                "sheet_name": "Data Export",
                "source_schema_version": "v2",
                "replace_mode": "incremental",
                "valid_from_date": date(2026, 4, 1),
                "notes": "nightly import",
            },
        }
        assert response.json()["item"] == {
            "projectId": str(project_id),
            "rawRows": 10,
        }
    finally:
        app.dependency_overrides.clear()
