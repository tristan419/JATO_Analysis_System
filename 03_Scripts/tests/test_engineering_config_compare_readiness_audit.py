from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "diagnostics" / "engineering_config_compare_readiness_audit.py"
FRONTEND_ARTIFACT_ROOT = "06_AppPlatform/frontend/artifacts"
COMPETITOR_ENTRY_ARTIFACT_DIR = f"{FRONTEND_ARTIFACT_ROOT}/product-config-competitor-entry-smoke/run"
EDIT_EXPORT_ARTIFACT_DIR = f"{FRONTEND_ARTIFACT_ROOT}/product-config-edit-export-smoke/run"
MULTISOURCE_ARTIFACT_DIR = f"{FRONTEND_ARTIFACT_ROOT}/product-config-multisource-same-model-smoke/run"
CROSS_SCOPE_ARTIFACT_DIR = f"{FRONTEND_ARTIFACT_ROOT}/product-config-cross-scope-direct-picker-smoke/run"
CROSS_USER_ARTIFACT_DIR = f"{FRONTEND_ARTIFACT_ROOT}/product-config-cross-user-source-library-smoke/run"
REVIEW_ROW_ARTIFACT_DIR = f"{FRONTEND_ARTIFACT_ROOT}/product-config-review-row-smoke/run"
COMPETITOR_RECOMMENDATION_PATH = (
    "/engineering-config/recommendations/competitors"
    "?country=Germany&model_name=T19C+MY+ICE&powertrain=ICE&segment=SUV+C&limit=10"
)


def load_module():
    spec = importlib.util.spec_from_file_location("engineering_config_compare_readiness_audit", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


audit = load_module()


class FakeClient:
    api_base = "http://fake.test"

    def __init__(
        self,
        responses: dict[str, dict[str, Any] | Exception | list[dict[str, Any] | Exception]],
        post_responses: dict[str, tuple[bytes, str] | Exception] | None = None,
        json_post_responses: dict[str, dict[str, Any] | Exception] | None = None,
        put_responses: dict[str, dict[str, Any] | Exception] | None = None,
        patch_responses: dict[str, dict[str, Any] | Exception] | None = None,
        delete_responses: dict[str, dict[str, Any] | Exception] | None = None,
    ) -> None:
        self.responses = responses
        self.post_responses = post_responses or {}
        self.json_post_responses = json_post_responses or {}
        self.put_responses = put_responses or {}
        self.patch_responses = patch_responses or {}
        self.delete_responses = delete_responses or {}
        self.calls: list[str] = []
        self.post_calls: list[tuple[str, dict[str, Any]]] = []
        self.json_post_calls: list[tuple[str, dict[str, Any] | None]] = []
        self.put_calls: list[tuple[str, bytes]] = []
        self.patch_calls: list[tuple[str, dict[str, Any]]] = []
        self.delete_calls: list[str] = []

    @staticmethod
    def _resolve_response(response: dict[str, Any] | Exception | list[dict[str, Any] | Exception]) -> dict[str, Any]:
        if isinstance(response, list):
            if not response:
                raise AssertionError("fake response queue is empty")
            item = response.pop(0)
        else:
            item = response
        if isinstance(item, Exception):
            raise item
        return item

    def get_json(self, path: str) -> dict[str, Any]:
        self.calls.append(path)
        return self._resolve_response(self.responses[path])

    def post_json_bytes(self, path: str, payload: dict[str, Any]) -> tuple[bytes, str | None]:
        self.post_calls.append((path, payload))
        response = self.post_responses[path]
        if isinstance(response, Exception):
            raise response
        return response

    def post_json(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        self.json_post_calls.append((path, payload))
        response = self.json_post_responses[path]
        if isinstance(response, Exception):
            raise response
        return response

    def put_bytes(self, path: str, body: bytes, content_type: str = "application/octet-stream") -> dict[str, Any]:
        self.put_calls.append((path, body))
        response = self.put_responses[path]
        if isinstance(response, Exception):
            raise response
        return response

    def patch_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.patch_calls.append((path, payload))
        response = self.patch_responses[path]
        if isinstance(response, Exception):
            raise response
        return response

    def delete_json(self, path: str) -> dict[str, Any]:
        self.delete_calls.append(path)
        response = self.delete_responses[path]
        if isinstance(response, Exception):
            raise response
        return response


def _ready_responses() -> dict[str, dict[str, Any] | Exception]:
    return {
        "/healthz": {"ok": True},
        "/engineering-config/ocr/readiness": {
            "status": "ready",
            "defaultEngine": "paddleocr",
            "imageOcrReady": True,
            "pdfOcrReady": True,
            "legacyOcrReady": False,
        },
        "/engineering-config/business-summary/readiness": {
            "ready": True,
            "status": "ready",
            "provider": "deepseek",
            "model": "deepseek-chat",
            "pipeline": "compare_runtime_compose",
            "persisted": False,
            "cacheSize": 0,
            "cacheLimit": 64,
        },
        "/engineering-config/source/snapshots?limit=1": {"items": [{"sourceId": "s1"}]},
        "/engineering-config/trims?limit=4": {"items": [{"trimId": "t1"}, {"trimId": "t2"}]},
        "/engineering-config/compare?trim_ids=t1%2Ct2": {
            "trims": [{"trimId": "t1"}, {"trimId": "t2"}],
            "rows": [{"featureCode": "feature-a"}],
            "summary": {"totalFeatures": 1, "shownFeatures": 1, "differenceCount": 1},
        },
    }


def _write_ocr_quality_artifact(
    tmp_path: Path,
    *,
    decision: str,
    recommended_engine: str | None,
    confidence: str,
    candidate_engine_count: int,
    comparable_candidate_count: int,
    file_name: str = "engineering_config_ocr_quality_audit_20260706t010203z.json",
    runtime: dict[str, Any] | None = None,
) -> Path:
    artifact = tmp_path / file_name
    payload = {
        "schemaVersion": "engineering_config_ocr_quality_audit_v1",
        "generatedAtUtc": "2026-07-06T01:02:03Z",
        "summary": {
            "fileCount": 3,
            "readyComparableFileCount": 2,
            "pendingOrFailedFileCount": 1,
            "statusCounts": {"ready": 2, "pending": 1},
            "ocrComparisonStatusCounts": {"selected_compared_with_alternates": 2},
            "engineRecommendation": {
                "decision": decision,
                "recommendedEngine": recommended_engine,
                "confidence": confidence,
                "reason": "fixture recommendation",
                "candidateEngineCount": candidate_engine_count,
                "candidateCount": 4,
                "comparableCandidateCount": comparable_candidate_count,
                "runnerUpEngine": (
                    "legacy_image_ocr" if recommended_engine == "paddleocr" and candidate_engine_count > 1 else None
                ),
                "engineMetrics": [
                    {
                        "engine": recommended_engine or "paddleocr",
                        "candidateCount": 2,
                        "comparableCandidateCount": comparable_candidate_count,
                        "selectedCount": 2,
                        "featureCount": 40,
                        "candidateTrimCount": 6,
                        "nonEmptyCount": 180,
                    }
                ],
            },
        },
    }
    if runtime is not None:
        payload["runtime"] = runtime
    artifact.write_text(
        audit.json.dumps(
            payload,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return artifact


def test_mint_local_auth_token_encodes_expected_role_and_user() -> None:
    token = audit._mint_local_auth_token(
        username="config-editor-smoke",
        role="editor",
        jwt_secret="test-secret",
        ttl_seconds=3600,
    )

    header, body, signature = token.split(".")
    assert header
    assert signature
    padded_body = body + "=" * ((4 - len(body) % 4) % 4)
    payload = audit.json.loads(audit.base64.urlsafe_b64decode(padded_body))
    assert payload["username"] == "config-editor-smoke"
    assert payload["role"] == "editor"
    assert payload["exp"] > payload["iat"]


def test_build_readiness_report_passes_when_runtime_contract_is_ready() -> None:
    client = FakeClient(_ready_responses())

    report = audit.build_readiness_report(client)

    assert report["schemaVersion"] == audit.SCHEMA_VERSION
    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 6, "degraded": 0, "failed": 0}
    assert "/engineering-config/business-summary/readiness" in client.calls
    assert "/engineering-config/compare?trim_ids=t1%2Ct2" in client.calls
    ocr_check = next(check for check in report["checks"] if check["key"] == "ocr")
    assert ocr_check["status"] == "passed"
    assert "Only PaddleOCR is available" in ocr_check["message"]
    assert ocr_check["details"]["paddleOcrReady"] is True
    assert ocr_check["details"]["legacyOcrReady"] is False
    assert ocr_check["details"]["ocrComparisonStatus"] == "paddle_only"
    assert ocr_check["details"]["ocrComparisonReady"] is False
    assert "JATO_CONFIG_OCR_COMMAND" in " ".join(ocr_check["details"]["nextActions"])
    ai_check = next(check for check in report["checks"] if check["key"] == "ai_summary")
    assert ai_check["details"]["pipeline"] == "compare_runtime_compose"
    assert ai_check["details"]["persisted"] is False
    compare_check = next(check for check in report["checks"] if check["key"] == "compare_api")
    assert compare_check["details"]["totalFeatures"] == 1
    assert report["includeExportSmoke"] is False
    assert report["goalCoverage"]["schemaVersion"] == "engineering_config_compare_goal_coverage_v1"
    assert report["goalCoverage"]["summary"] == {"proved": 0, "partial": 5, "unverified": 3, "risk": 0}
    simple_item = next(item for item in report["goalCoverage"]["items"] if item["key"] == "simple_excel_mode")
    assert simple_item["status"] == "partial"
    assert "Run --include-t19c-ai-ui-smoke" in simple_item["nextEvidence"]
    ocr_item = next(item for item in report["goalCoverage"]["items"] if item["key"] == "paddle_vs_legacy_ocr_decision")
    assert ocr_item["status"] == "partial"
    assert any("OCR readiness: passed" == evidence for evidence in ocr_item["currentEvidence"])


def test_goal_coverage_promotes_proved_items_from_smoke_checks(monkeypatch) -> None:
    def passed_check(key: str, label: str) -> dict[str, Any]:
        return {
            "key": key,
            "label": label,
            "status": "passed",
            "message": "fixture passed",
            "details": {},
        }

    monkeypatch.setattr(
        audit,
        "_t19c_ai_ui_smoke_check",
        lambda **_kwargs: [passed_check("t19c_ai_ui_smoke", "T19C simple-mode AI UI smoke")],
    )
    monkeypatch.setattr(
        audit,
        "_ui_edit_export_smoke_check",
        lambda **_kwargs: [passed_check("ui_edit_export_smoke", "UI edit-after-digest export smoke")],
    )
    monkeypatch.setattr(
        audit,
        "_source_review_row_smoke_check",
        lambda **_kwargs: [passed_check("source_review_row_smoke", "Source Digest review-row smoke")],
    )
    monkeypatch.setattr(
        audit,
        "_floatingdeck_multisource_smoke_checks",
        lambda **_kwargs: [
            passed_check("floatingdeck_multisource_same_model", "FloatingDeck same-model multi-source smoke"),
            passed_check(
                "floatingdeck_cross_scope_direct_picker", "FloatingDeck cross-country/cross-model picker smoke"
            ),
        ],
    )
    monkeypatch.setattr(
        audit,
        "_competitor_entry_ui_smoke_check",
        lambda **_kwargs: [passed_check("competitor_entry_ui_smoke", "Competitor recommendation entry UI smoke")],
    )
    monkeypatch.setattr(
        audit,
        "_competitor_workflow_smoke_check",
        lambda *_args, **_kwargs: [passed_check("competitor_workflow", "Competitor upload/digest workflow smoke")],
    )
    monkeypatch.setattr(
        audit,
        "_cross_user_source_library_smoke_check",
        lambda **_kwargs: [passed_check("cross_user_source_library_smoke", "Cross-user source-library smoke")],
    )
    monkeypatch.setattr(
        audit,
        "_auth_contract_check",
        lambda *_args, **_kwargs: passed_check("auth_contract", "Auth and role contract smoke"),
    )
    monkeypatch.setattr(
        audit,
        "_ocr_quality_artifact_check",
        lambda *_args, **_kwargs: passed_check("ocr_quality_recommendation", "OCR quality recommendation"),
    )
    monkeypatch.setattr(
        audit,
        "_ai_summary_smoke_check",
        lambda *_args, **_kwargs: [passed_check("ai_summary_compose", "Runtime AI summary compose smoke")],
    )

    client = FakeClient(
        _ready_responses(),
        json_post_responses={
            "/engineering-config/business-summary/compose": {
                "summaries": [
                    {
                        "targetTrimId": "t2",
                        "targetLabel": "t2",
                        "headline": "AI summary",
                        "mainUpgrades": ["upgrade"],
                        "replacementsOrReductions": ["none"],
                        "evidenceStatus": ["source evidence checked"],
                    }
                ],
                "usage": {"provider": "deepseek", "model": "deepseek-chat", "status": "ok"},
            }
        },
    )

    report = audit.build_readiness_report(
        client,
        include_auth_contract_smoke=True,
        include_ai_summary_smoke=True,
        include_ocr_quality_audit=True,
        ocr_quality_artifact=Path("/tmp/fake-ocr-quality.json"),
        include_t19c_ai_ui_smoke=True,
        include_ui_edit_export_smoke=True,
        include_source_review_row_smoke=True,
        include_floatingdeck_multisource_smoke=True,
        include_competitor_entry_ui_smoke=True,
        include_competitor_workflow_smoke=True,
        include_cross_user_source_library_smoke=True,
    )

    coverage = {item["key"]: item["status"] for item in report["goalCoverage"]["items"]}
    assert coverage["simple_excel_mode"] == "proved"
    assert coverage["multi_format_digest_to_editable_export"] == "proved"
    assert coverage["floatingdeck_multisource_direct_picker"] == "proved"
    assert coverage["competitor_recommendation_and_upload_gap"] == "proved"
    assert coverage["cross_user_country_source_library_trash"] == "proved"
    assert coverage["admin_editor_edit_governance"] == "proved"
    assert coverage["paddle_vs_legacy_ocr_decision"] == "proved"
    assert coverage["runtime_ai_business_summary"] == "proved"
    assert report["goalCoverage"]["summary"] == {"proved": 8, "partial": 0, "unverified": 0, "risk": 0}


def test_build_readiness_report_includes_passing_ocr_quality_recommendation(tmp_path: Path) -> None:
    artifact = _write_ocr_quality_artifact(
        tmp_path,
        decision="use_recommended_engine",
        recommended_engine="paddleocr",
        confidence="high",
        candidate_engine_count=2,
        comparable_candidate_count=3,
    )

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_ocr_quality_audit=True,
        ocr_quality_artifact=artifact,
    )

    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 7, "degraded": 0, "failed": 0}
    assert report["includeOcrQualityAudit"] is True
    assert report["ocrQualityArtifact"] == str(artifact)
    quality_check = next(check for check in report["checks"] if check["key"] == "ocr_quality_recommendation")
    assert quality_check["status"] == "passed"
    assert "recommends paddleocr with high confidence" in quality_check["message"]
    assert quality_check["details"]["decision"] == "use_recommended_engine"
    assert quality_check["details"]["candidateEngineCount"] == 2
    assert quality_check["details"]["readyComparableFileCount"] == 2


def test_build_readiness_report_degrades_for_explicit_mismatched_ocr_quality_runtime(tmp_path: Path) -> None:
    artifact = _write_ocr_quality_artifact(
        tmp_path,
        decision="insufficient_evidence",
        recommended_engine=None,
        confidence="low",
        candidate_engine_count=2,
        comparable_candidate_count=0,
        runtime={
            "pythonExecutable": "/usr/local/bin/python3",
            "sourceDigestImportOk": True,
            "modules": {
                "paddleocr": {"available": False, "version": None},
                "paddle": {"available": False, "version": None},
            },
        },
    )

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_ocr_quality_audit=True,
        ocr_quality_artifact=artifact,
    )

    assert report["status"] == "degraded"
    quality_check = next(check for check in report["checks"] if check["key"] == "ocr_quality_recommendation")
    assert quality_check["status"] == "degraded"
    assert "runtime does not match backend OCR readiness" in quality_check["message"]
    assert "missing paddleocr" in quality_check["details"]["runtimeIssue"]
    assert quality_check["details"]["artifactRuntime"]["pythonExecutable"] == "/usr/local/bin/python3"


def test_build_readiness_report_skips_latest_ocr_artifact_with_mismatched_runtime(tmp_path: Path) -> None:
    compatible_artifact = _write_ocr_quality_artifact(
        tmp_path,
        decision="use_recommended_engine",
        recommended_engine="paddleocr",
        confidence="high",
        candidate_engine_count=2,
        comparable_candidate_count=3,
        file_name="engineering_config_ocr_quality_audit_20260706t010203z.json",
        runtime={
            "pythonExecutable": "/repo/.venv/bin/python",
            "sourceDigestImportOk": True,
            "modules": {
                "paddleocr": {"available": True, "version": "3.7.0"},
                "paddle": {"available": True, "version": "3.3.1"},
            },
        },
    )
    mismatched_artifact = _write_ocr_quality_artifact(
        tmp_path,
        decision="insufficient_evidence",
        recommended_engine=None,
        confidence="low",
        candidate_engine_count=2,
        comparable_candidate_count=0,
        file_name="engineering_config_ocr_quality_audit_20260706t020304z.json",
        runtime={
            "pythonExecutable": "/usr/local/bin/python3",
            "sourceDigestImportOk": True,
            "modules": {
                "paddleocr": {"available": False, "version": None},
                "paddle": {"available": False, "version": None},
            },
        },
    )
    mismatched_artifact.touch()

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_ocr_quality_audit=True,
        ocr_quality_artifact_dir=tmp_path,
    )

    assert report["status"] == "passed"
    assert report["ocrQualityArtifact"] == str(compatible_artifact)
    quality_check = next(check for check in report["checks"] if check["key"] == "ocr_quality_recommendation")
    assert quality_check["status"] == "passed"
    assert quality_check["details"]["artifactPath"] == str(compatible_artifact)


def test_build_readiness_report_degrades_for_single_engine_ocr_quality_candidate(tmp_path: Path) -> None:
    artifact = _write_ocr_quality_artifact(
        tmp_path,
        decision="single_engine_candidate",
        recommended_engine="paddleocr",
        confidence="medium",
        candidate_engine_count=1,
        comparable_candidate_count=2,
    )

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_ocr_quality_audit=True,
        ocr_quality_artifact=artifact,
    )

    assert report["status"] == "degraded"
    quality_check = next(check for check in report["checks"] if check["key"] == "ocr_quality_recommendation")
    assert quality_check["status"] == "degraded"
    assert "single-engine candidate" in quality_check["message"]
    assert "legacy/custom OCR comparison proof is still missing" in quality_check["message"]
    assert quality_check["details"]["recommendedEngine"] == "paddleocr"
    assert "candidateEngineCount >= 2" in " ".join(quality_check["details"]["nextActions"])


def test_build_readiness_report_degrades_when_ocr_quality_artifact_is_missing(tmp_path: Path) -> None:
    missing_artifact = tmp_path / "missing.json"

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_ocr_quality_audit=True,
        ocr_quality_artifact=missing_artifact,
    )

    assert report["status"] == "degraded"
    quality_check = next(check for check in report["checks"] if check["key"] == "ocr_quality_recommendation")
    assert quality_check["status"] == "degraded"
    assert "artifact is missing" in quality_check["message"]
    assert quality_check["details"]["artifactFound"] is False
    assert "fresh JSON artifact" in " ".join(quality_check["details"]["nextActions"])


def _auth_role_client(
    *,
    role: str,
    source_delete_status: int,
    trim_patch_status: int,
    value_patch_status: int | None = None,
) -> FakeClient:
    value_patch_status = trim_patch_status if value_patch_status is None else value_patch_status
    source_delete_response: dict[str, Any] | Exception
    if source_delete_status >= 400:
        source_delete_response = audit.ReadinessHttpError(
            "http://fake.test/v1/engineering-config/source/snapshots/00000000-0000-0000-0000-000000000000",
            source_delete_status,
            "auth or fake-id response",
        )
    else:
        source_delete_response = {"ok": True}
    trim_patch_response: dict[str, Any] | Exception
    if trim_patch_status >= 400:
        trim_patch_response = audit.ReadinessHttpError(
            "http://fake.test/v1/engineering-config/trims/00000000-0000-0000-0000-000000000000",
            trim_patch_status,
            "auth or fake-id response",
        )
    else:
        trim_patch_response = {"ok": True}
    value_patch_response: dict[str, Any] | Exception
    if value_patch_status >= 400:
        value_patch_response = audit.ReadinessHttpError(
            "http://fake.test/v1/engineering-config/values/00000000-0000-0000-0000-000000000000",
            value_patch_status,
            "auth or fake-id response",
        )
    else:
        value_patch_response = {"ok": True}
    return FakeClient(
        {
            "/auth/me": {"username": f"{role}-user", "role": role},
            "/engineering-config/source/snapshots?limit=1": {"items": [{"sourceId": "s1"}]},
            "/engineering-config/trims?limit=1": {"items": [{"trimId": "t1"}]},
            "/engineering-config/compare?trim_ids=t1%2Ct2": {
                "trims": [{"trimId": "t1"}, {"trimId": "t2"}],
                "rows": [{"featureCode": "feature-a"}],
                "summary": {"totalFeatures": 1, "shownFeatures": 1, "differenceCount": 1},
            },
        },
        patch_responses={
            "/engineering-config/trims/00000000-0000-0000-0000-000000000000": trim_patch_response,
            "/engineering-config/values/00000000-0000-0000-0000-000000000000": value_patch_response,
        },
        delete_responses={
            "/engineering-config/source/snapshots/00000000-0000-0000-0000-000000000000": source_delete_response,
        },
    )


def test_build_readiness_report_includes_passing_auth_contract() -> None:
    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_auth_contract_smoke=True,
        compare_trim_ids=["t1", "t2"],
        auth_contract_clients={
            "viewer": _auth_role_client(role="viewer", source_delete_status=403, trim_patch_status=403),
            "editor": _auth_role_client(role="editor", source_delete_status=404, trim_patch_status=404),
        },
    )

    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 7, "degraded": 0, "failed": 0}
    assert report["readOnly"] is False
    assert report["includeAuthContractSmoke"] is True
    auth_check = next(check for check in report["checks"] if check["key"] == "auth_contract")
    assert auth_check["status"] == "passed"
    assert auth_check["details"]["roles"]["viewer"]["compareReadOk"] is True
    assert auth_check["details"]["writeBoundary"]["viewer"]["sourceDeleteForbidden"] is True
    assert auth_check["details"]["writeBoundary"]["editor"]["sourceDeleteHandlerReached"] is True
    assert auth_check["details"]["writeBoundary"]["editor"]["trimPatchHandlerReached"] is True
    assert auth_check["details"]["writeBoundary"]["editor"]["valuePatchHandlerReached"] is True


def test_build_readiness_report_degrades_when_auth_contract_tokens_are_missing() -> None:
    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_auth_contract_smoke=True,
    )

    assert report["status"] == "degraded"
    auth_check = next(check for check in report["checks"] if check["key"] == "auth_contract")
    assert auth_check["status"] == "degraded"
    assert auth_check["details"]["skipped"] == ["viewer token missing", "editor token missing"]


def test_build_readiness_report_fails_when_viewer_can_write() -> None:
    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_auth_contract_smoke=True,
        auth_contract_clients={
            "viewer": _auth_role_client(role="viewer", source_delete_status=200, trim_patch_status=200),
            "editor": _auth_role_client(role="editor", source_delete_status=404, trim_patch_status=404),
        },
    )

    assert report["status"] == "failed"
    auth_check = next(check for check in report["checks"] if check["key"] == "auth_contract")
    assert auth_check["status"] == "failed"
    assert "viewer write boundary was not enforced" in auth_check["details"]["failures"]


def test_build_readiness_report_fails_when_editor_value_save_is_blocked() -> None:
    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_auth_contract_smoke=True,
        auth_contract_clients={
            "viewer": _auth_role_client(role="viewer", source_delete_status=403, trim_patch_status=403),
            "editor": _auth_role_client(
                role="editor", source_delete_status=404, trim_patch_status=404, value_patch_status=403
            ),
        },
    )

    assert report["status"] == "failed"
    auth_check = next(check for check in report["checks"] if check["key"] == "auth_contract")
    assert auth_check["status"] == "failed"
    assert auth_check["details"]["writeBoundary"]["editor"]["valuePatchHandlerReached"] is False
    assert "editor write handlers were blocked by auth" in auth_check["details"]["failures"]


def test_build_readiness_report_can_target_compare_columns_by_query() -> None:
    responses = _ready_responses()
    responses["/engineering-config/trims?limit=8&q=T19C+MY+ICE&market=EU"] = {
        "items": [{"trimId": "basic"}, {"trimId": "comfort"}, {"trimId": "premium"}],
    }
    responses["/engineering-config/compare?trim_ids=basic%2Ccomfort%2Cpremium"] = {
        "trims": [{"trimId": "basic"}, {"trimId": "comfort"}, {"trimId": "premium"}],
        "rows": [{"featureCode": "parking-assist"}],
        "summary": {"totalFeatures": 227, "shownFeatures": 227, "differenceCount": 37},
    }
    client = FakeClient(responses)

    report = audit.build_readiness_report(
        client,
        compare_trim_query="T19C MY ICE",
        compare_trim_market="EU",
    )

    assert report["status"] == "passed"
    assert report["compareTrimQuery"] == "T19C MY ICE"
    assert report["compareTrimMarket"] == "EU"
    assert "/engineering-config/trims?limit=8&q=T19C+MY+ICE&market=EU" in client.calls
    assert "/engineering-config/compare?trim_ids=basic%2Ccomfort%2Cpremium" in client.calls
    compare_check = next(check for check in report["checks"] if check["key"] == "compare_api")
    assert compare_check["details"]["trimIds"] == ["basic", "comfort", "premium"]
    assert compare_check["details"]["totalFeatures"] == 227


def test_build_readiness_report_degrades_when_ocr_or_llm_are_missing() -> None:
    responses = _ready_responses()
    responses["/engineering-config/ocr/readiness"] = {
        "status": "not_configured",
        "defaultEngine": None,
        "imageOcrReady": False,
        "pdfOcrReady": False,
    }
    responses["/engineering-config/business-summary/readiness"] = {
        "ready": False,
        "status": "missing_key",
        "provider": "deepseek",
        "model": "deepseek-chat",
        "pipeline": "compare_runtime_compose",
        "persisted": False,
    }

    report = audit.build_readiness_report(FakeClient(responses))

    assert report["status"] == "degraded"
    assert report["summary"]["degraded"] == 2
    messages = {check["key"]: check["message"] for check in report["checks"]}
    assert "OCR not fully configured" in messages["ocr"]
    assert "LLM summary provider missing" in messages["ai_summary"]


def test_build_readiness_report_reports_ocr_engine_comparison_when_legacy_is_available() -> None:
    responses = _ready_responses()
    responses["/engineering-config/ocr/readiness"] = {
        "status": "ready",
        "defaultEngine": "paddleocr",
        "imageOcrReady": True,
        "pdfOcrReady": True,
        "paddleOcrReady": True,
        "legacyOcrReady": True,
    }

    report = audit.build_readiness_report(FakeClient(responses))

    assert report["status"] == "passed"
    ocr_check = next(check for check in report["checks"] if check["key"] == "ocr")
    assert "PaddleOCR and legacy/custom OCR are both available" in ocr_check["message"]
    assert ocr_check["details"]["ocrComparisonStatus"] == "ready"
    assert ocr_check["details"]["ocrComparisonReady"] is True
    assert "engineering_config_ocr_quality_audit.py" in " ".join(ocr_check["details"]["nextActions"])


def test_build_readiness_report_fails_when_required_library_endpoint_errors() -> None:
    responses = _ready_responses()
    responses["/engineering-config/source/snapshots?limit=1"] = audit.ReadinessHttpError(
        "http://fake.test/v1/engineering-config/source/snapshots",
        401,
        "unauthorized",
    )

    report = audit.build_readiness_report(FakeClient(responses))

    assert report["status"] == "failed"
    source_check = next(check for check in report["checks"] if check["key"] == "source_library")
    assert source_check["status"] == "failed"
    assert source_check["httpStatus"] == 401


def test_build_readiness_report_degrades_compare_smoke_when_fewer_than_two_columns() -> None:
    responses = _ready_responses()
    responses["/engineering-config/trims?limit=4"] = {"items": [{"trimId": "t1"}]}
    responses.pop("/engineering-config/compare?trim_ids=t1%2Ct2")

    report = audit.build_readiness_report(FakeClient(responses))

    assert report["status"] == "degraded"
    compare_check = next(check for check in report["checks"] if check["key"] == "compare_api")
    assert compare_check["status"] == "degraded"
    assert "fewer than 2 config columns" in compare_check["message"]


def test_build_readiness_report_fails_when_compare_endpoint_errors() -> None:
    responses = _ready_responses()
    responses["/engineering-config/compare?trim_ids=t1%2Ct2"] = audit.ReadinessHttpError(
        "http://fake.test/v1/engineering-config/compare",
        500,
        "compare failed",
    )

    report = audit.build_readiness_report(FakeClient(responses))

    assert report["status"] == "failed"
    compare_check = next(check for check in report["checks"] if check["key"] == "compare_api")
    assert compare_check["status"] == "failed"
    assert compare_check["httpStatus"] == 500


def test_build_readiness_report_can_include_export_smoke() -> None:
    client = FakeClient(
        _ready_responses(),
        post_responses={
            "/engineering-config/compare/export/xlsx": (
                b"PK\x03\x04fake-xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
            "/engineering-config/compare/export/pdf": (b"%PDF-1.4 fake-pdf", "application/pdf"),
        },
    )

    report = audit.build_readiness_report(client, include_export_smoke=True)

    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 8, "degraded": 0, "failed": 0}
    assert report["includeExportSmoke"] is True
    assert [path for path, _payload in client.post_calls] == [
        "/engineering-config/compare/export/xlsx",
        "/engineering-config/compare/export/pdf",
    ]
    export_xlsx = next(check for check in report["checks"] if check["key"] == "export_xlsx")
    export_pdf = next(check for check in report["checks"] if check["key"] == "export_pdf")
    assert export_xlsx["details"]["bytes"] > 0
    assert export_pdf["details"]["bytes"] > 0
    assert export_xlsx["details"]["payload"]["businessSummaryCount"] == 1
    assert export_xlsx["details"]["payload"]["hasBusinessSummaryUsage"] is True
    assert export_xlsx["details"]["payload"]["allSummaryItemsHaveEvidenceStatus"] is True
    assert client.post_calls[0][1]["rows"][0]["featureCode"] == "feature-a"
    assert client.post_calls[0][1]["businessSummary"][0]["evidenceStatus"]
    assert client.post_calls[0][1]["businessSummaryUsage"]["source"] == "readiness_export_payload"


def test_build_readiness_report_can_include_ai_summary_compose_smoke() -> None:
    responses = _ready_responses()
    responses["/engineering-config/compare?trim_ids=t1%2Ct2"] = {
        "trims": [
            {
                "trimId": "t1",
                "trimName": "Basic",
                "brand": "OMODA",
                "modelName": "T19C MY ICE",
                "market": "EU",
                "sourceFileName": "own.xlsx",
            },
            {
                "trimId": "t2",
                "trimName": "Premium",
                "brand": "OMODA",
                "modelName": "T19C MY ICE",
                "market": "EU",
                "sourceFileName": "own.xlsx",
            },
        ],
        "rows": [
            {
                "featureCode": "rear-camera",
                "featureName": "Rear Visual parking assist / 动态辅助线倒车影像",
                "category": "舒适便利",
                "comparisonType": "DIFFERENT_VALUE",
                "businessNote": "配置值不同，可用于版本差异说明。",
                "values": [
                    {"displayValue": "不配备", "availability": "NOT_AVAILABLE"},
                    {
                        "displayValue": "标配",
                        "availability": "EQUIPPED",
                        "source": {"sheetName": "T19C MY ICE", "cell": "F128"},
                    },
                ],
            }
        ],
        "summary": {
            "totalFeatures": 1,
            "shownFeatures": 1,
            "differenceCount": 1,
            "confirmedDifferenceCount": 1,
            "inferredDifferenceCount": 0,
            "missingOrUnknownCount": 0,
        },
    }
    client = FakeClient(
        responses,
        json_post_responses={
            "/engineering-config/business-summary/compose": {
                "summaries": [
                    {
                        "targetTrimId": "t2",
                        "targetLabel": "Premium",
                        "headline": "Premium 相比 Basic 主要增加泊车辅助配置。",
                        "mainUpgrades": ["泊车辅助：新增动态辅助线倒车影像。"],
                        "replacementsOrReductions": ["无明显减少。"],
                        "evidenceStatus": ["引用前点开 source evidence 核对。"],
                        "recommendedUse": "适合配置差异速读。",
                        "evidenceRefs": [],
                    }
                ],
                "usage": {
                    "provider": "deepseek",
                    "model": "deepseek-chat",
                    "status": "ok",
                    "totalTokens": 128,
                },
            }
        },
    )

    report = audit.build_readiness_report(client, include_ai_summary_smoke=True)

    assert report["status"] == "passed"
    assert report["includeAiSummarySmoke"] is True
    ai_compose = next(check for check in report["checks"] if check["key"] == "ai_summary_compose")
    assert ai_compose["status"] == "passed"
    assert ai_compose["details"]["summaryCount"] == 1
    assert ai_compose["details"]["targetCount"] == 1
    assert ai_compose["details"]["mainUpgradeSamples"] == ["泊车辅助：新增动态辅助线倒车影像。"]
    assert ai_compose["details"]["replacementSamples"] == ["无明显减少。"]
    assert ai_compose["details"]["sourceEvidenceBoundaryPresent"] is True
    assert ai_compose["details"]["requiredEvidenceBoundarySatisfied"] is True
    assert ai_compose["details"]["evidenceBoundaryCoverage"][0]["missingKinds"] == []
    assert ai_compose["details"]["summariesWithEvidenceStatus"] == 1
    assert ai_compose["details"]["sourceEvidenceSummarySamples"][0]["withSourceEvidenceCount"] == 1
    assert ai_compose["details"]["sourceEvidenceSummarySamples"][0]["missingSourceEvidenceCount"] == 0
    assert client.json_post_calls[0][0] == "/engineering-config/business-summary/compose"
    payload = client.json_post_calls[0][1]
    assert payload is not None
    assert payload["baseTrim"]["trimName"] == "Basic"
    assert payload["targets"][0]["targetLabel"] == "Premium"
    assert payload["targets"][0]["sourceEvidenceSummary"]["sourceSheetNames"] == ["T19C MY ICE"]
    assert payload["targets"][0]["differenceCounts"]["missingSourceEvidence"] == 0
    assert payload["context"]["compareScope"]["missingSourceEvidenceCount"] == 0
    assert payload["targets"][0]["evidenceFacts"][0]["featureCode"] == "rear-camera"
    assert payload["targets"][0]["evidenceFacts"][0]["targetValue"] == "标配"


def test_build_readiness_report_passes_ai_summary_timeout_to_compose_smoke(monkeypatch) -> None:
    responses = _ready_responses()
    responses["/engineering-config/compare?trim_ids=t1%2Ct2"] = {
        "trims": [
            {"trimId": "t1", "trimName": "Basic"},
            {"trimId": "t2", "trimName": "Premium"},
        ],
        "rows": [
            {
                "featureCode": "rear-camera",
                "featureName": "Rear camera",
                "category": "Comfort",
                "comparisonType": "DIFFERENT_VALUE",
                "values": [
                    {"displayValue": "不配备", "availability": "NOT_AVAILABLE"},
                    {"displayValue": "标配", "availability": "EQUIPPED"},
                ],
            }
        ],
        "summary": {"totalFeatures": 1, "shownFeatures": 1, "differenceCount": 1},
    }
    captured: dict[str, Any] = {}

    def fake_ai_summary_smoke_check(_client, _compare_payload, *, enabled: bool, timeout: float | None):
        captured["enabled"] = enabled
        captured["timeout"] = timeout
        return [
            {
                "key": "ai_summary_compose",
                "label": "Runtime AI summary compose smoke",
                "status": "passed",
                "path": "/engineering-config/business-summary/compose",
                "message": "runtime AI compose used the dedicated timeout",
            }
        ]

    monkeypatch.setattr(audit, "_ai_summary_smoke_check", fake_ai_summary_smoke_check)

    report = audit.build_readiness_report(
        FakeClient(responses),
        include_ai_summary_smoke=True,
        ai_summary_timeout=45.0,
    )

    assert captured == {"enabled": True, "timeout": 45.0}
    assert report["aiSummaryTimeout"] == 45.0
    assert next(check for check in report["checks"] if check["key"] == "ai_summary_compose")["status"] == "passed"


def test_ai_summary_compose_smoke_degrades_without_response_evidence_status() -> None:
    responses = _ready_responses()
    client = FakeClient(
        responses,
        json_post_responses={
            "/engineering-config/business-summary/compose": {
                "summaries": [
                    {
                        "targetTrimId": "t2",
                        "targetLabel": "t2",
                        "headline": "t2 相比 t1 有配置差异。",
                        "mainUpgrades": ["配置差异示例。"],
                        "replacementsOrReductions": ["无明显减少。"],
                    }
                ],
                "usage": {"provider": "deepseek", "model": "deepseek-chat", "status": "ok"},
            }
        },
    )

    report = audit.build_readiness_report(client, include_ai_summary_smoke=True)

    assert report["status"] == "degraded"
    ai_compose = next(check for check in report["checks"] if check["key"] == "ai_summary_compose")
    assert ai_compose["status"] == "degraded"
    assert "source evidence boundary" in ai_compose["message"]
    assert ai_compose["details"]["sourceEvidenceBoundaryPresent"] is True
    assert ai_compose["details"]["summariesWithEvidenceStatus"] == 0


def test_ai_summary_compose_smoke_degrades_when_required_evidence_boundary_is_missing() -> None:
    responses = _ready_responses()
    responses["/engineering-config/compare?trim_ids=t1%2Ct2"] = {
        "trims": [
            {"trimId": "t1", "trimName": "Basic", "modelName": "T19C"},
            {"trimId": "t2", "trimName": "Premium", "modelName": "T19C"},
        ],
        "rows": [
            {
                "featureCode": "parking-assist",
                "featureName": "Rear Visual parking assist",
                "category": "舒适便利",
                "comparisonType": "DIFFERENT_VALUE",
                "businessNote": "需核对规则推断值。",
                "values": [
                    {"displayValue": "标配", "availability": "EQUIPPED"},
                    {
                        "displayValue": "不配备*",
                        "availability": "NOT_AVAILABLE",
                        "inferred": True,
                    },
                ],
            },
            {
                "featureCode": "seat-count",
                "featureName": "Number of seats",
                "category": "基本参数",
                "comparisonType": "DIFFERENT_VALUE",
                "values": [
                    {"displayValue": "5", "availability": "EQUIPPED"},
                    {
                        "displayValue": "5",
                        "availability": "EQUIPPED",
                        "source": {
                            "sheetName": "Sheet A",
                            "cell": "E12",
                            "sourceCell": "D12",
                            "mergedRange": "D12:E12",
                        },
                    },
                ],
            },
            {
                "featureCode": "speaker",
                "featureName": "SONY 8 speakers",
                "category": "信息娱乐",
                "comparisonType": "DIFFERENT_VALUE",
                "values": [
                    {"displayValue": "不配备", "availability": "NOT_AVAILABLE"},
                    {
                        "displayValue": "标配",
                        "availability": "EQUIPPED",
                        "source": {"sheetName": "Sheet B", "cell": "F20"},
                    },
                ],
            },
        ],
        "summary": {
            "totalFeatures": 3,
            "shownFeatures": 3,
            "differenceCount": 3,
            "confirmedDifferenceCount": 3,
            "inferredDifferenceCount": 1,
            "missingOrUnknownCount": 0,
        },
    }
    client = FakeClient(
        responses,
        json_post_responses={
            "/engineering-config/business-summary/compose": {
                "summaries": [
                    {
                        "targetTrimId": "t2",
                        "targetLabel": "Premium",
                        "headline": "Premium 相比 Basic 主要升级泊车和音响。",
                        "mainUpgrades": ["泊车辅助和音响配置升级。"],
                        "replacementsOrReductions": [],
                        "evidenceStatus": ["引用前点开 source evidence 核对。"],
                    }
                ],
                "usage": {"provider": "deepseek", "model": "deepseek-chat", "status": "ok"},
            }
        },
    )

    report = audit.build_readiness_report(client, include_ai_summary_smoke=True)

    assert report["status"] == "degraded"
    ai_compose = next(check for check in report["checks"] if check["key"] == "ai_summary_compose")
    assert ai_compose["status"] == "degraded"
    assert "required evidence boundary warnings" in ai_compose["message"]
    coverage = ai_compose["details"]["evidenceBoundaryCoverage"][0]
    assert coverage["requiredKinds"] == ["review", "inferred", "missing_source", "merged", "multi_source"]
    assert coverage["missingKinds"] == ["review", "inferred", "missing_source", "merged", "multi_source"]
    assert ai_compose["details"]["requiredEvidenceBoundarySatisfied"] is False


def test_build_readiness_report_marks_export_signature_failure() -> None:
    client = FakeClient(
        _ready_responses(),
        post_responses={
            "/engineering-config/compare/export/xlsx": (
                b"not-xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
            "/engineering-config/compare/export/pdf": (b"%PDF-1.4 fake-pdf", "application/pdf"),
        },
    )

    report = audit.build_readiness_report(client, include_export_smoke=True)

    assert report["status"] == "failed"
    export_xlsx = next(check for check in report["checks"] if check["key"] == "export_xlsx")
    assert export_xlsx["status"] == "failed"
    assert "unexpected file signature" in export_xlsx["message"]


def test_build_readiness_report_can_include_competitor_recommendation_smoke() -> None:
    responses = _ready_responses()
    responses[COMPETITOR_RECOMMENDATION_PATH] = {
        "country": "Germany",
        "modelName": "T19C MY ICE",
        "powertrain": "ICE",
        "segment": "SUV C",
        "rows": 3,
        "items": [
            {
                "modelName": "BMW X7",
                "configAvailable": True,
                "sourceDigestAvailable": True,
                "sourceDigestSourceCount": 4,
                "sourceDigestGroupCount": 4,
                "sourceDigestTrimCount": 8,
                "nextAction": "select_config_trim",
            },
            {
                "modelName": "Audi Q7",
                "configAvailable": False,
                "sourceDigestAvailable": True,
                "sourceDigestSourceCount": 1,
                "sourceDigestGroupCount": 1,
                "sourceDigestTrimCount": 2,
                "nextAction": "create_from_source_digest",
            },
            {
                "modelName": "Urus",
                "configAvailable": False,
                "sourceDigestAvailable": False,
                "nextAction": "upload_source",
            },
        ],
        "message": "ok",
    }

    report = audit.build_readiness_report(
        FakeClient(responses),
        competitor_scope={
            "country": "Germany",
            "model": "T19C MY ICE",
            "powertrain": "ICE",
            "segment": "SUV C",
            "limit": 10,
        },
    )

    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 7, "degraded": 0, "failed": 0}
    competitor_check = next(check for check in report["checks"] if check["key"] == "competitor_recommendations")
    assert competitor_check["details"]["rows"] == 3
    assert competitor_check["details"]["configReadyCount"] == 1
    assert competitor_check["details"]["libraryReadyWithSourceEvidenceCount"] == 1
    assert competitor_check["details"]["sourceDigestCoverageCount"] == 2
    assert competitor_check["details"]["sourceDigestReadyCount"] == 1
    assert competitor_check["details"]["sourceDigestSourceCount"] == 5
    assert competitor_check["details"]["sourceDigestGroupCount"] == 5
    assert competitor_check["details"]["sourceDigestTrimCount"] == 10
    assert competitor_check["details"]["uploadNeededCount"] == 1
    assert "1 library-ready-with-source-evidence" in competitor_check["message"]


def test_build_readiness_report_can_include_competitor_entry_ui_smoke(monkeypatch) -> None:
    observed: dict[str, Any] = {}

    class Completed:
        returncode = 0
        stderr = ""
        stdout = audit.json.dumps(
            {
                "summaryPath": f"{COMPETITOR_ENTRY_ARTIFACT_DIR}/product_config_competitor_entry_smoke.json",
                "artifactDir": COMPETITOR_ENTRY_ARTIFACT_DIR,
                "targetUrl": "http://127.0.0.1:5177/product/compare/config?market=Germany&model=T19C%20MY%20ICE",
                "screenshotPath": f"{COMPETITOR_ENTRY_ARTIFACT_DIR}/product_config_competitor_entry_smoke.png",
                "recommendationState": {
                    "summary": "推荐范围Top 10/10库内可用1待生成0待上传9",
                    "queue": "补齐队列优先补上传缺口Urus 库内可用1待上传9上传 Urus 来源",
                },
                "sourceHandoffState": {
                    "sourceSearchValue": "LAMBORGHINI Urus Germany ICE SUV C",
                },
                "checks": {key: True for key in audit.COMPETITOR_ENTRY_UI_REQUIRED_CHECKS},
                "passed": True,
            },
            ensure_ascii=False,
        )

    def fake_run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return Completed()

    monkeypatch.setattr(audit.subprocess, "run", fake_run)
    responses = _ready_responses()
    responses[
        "/engineering-config/recommendations/competitors?"
        "country=Germany&model_name=T19C+MY+ICE&powertrain=ICE&segment=SUV+C&limit=10"
    ] = {
        "country": "Germany",
        "modelName": "T19C MY ICE",
        "rows": 1,
        "items": [
            {
                "modelName": "Urus",
                "brand": "LAMBORGHINI",
                "configAvailable": False,
                "sourceDigestAvailable": False,
                "nextAction": "upload_source",
            }
        ],
        "message": "ok",
    }

    report = audit.build_readiness_report(
        FakeClient(responses),
        include_competitor_entry_ui_smoke=True,
        frontend_base_url="http://127.0.0.1:5177",
        competitor_entry_ui_smoke_browser_channel="chrome",
        competitor_entry_ui_smoke_timeout_ms=123000,
        competitor_scope={
            "country": "Germany",
            "model": "T19C MY ICE",
            "powertrain": "ICE",
            "segment": "SUV C",
            "limit": 10,
        },
    )

    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 8, "degraded": 0, "failed": 0}
    assert report["readOnly"] is True
    assert report["includeCompetitorEntryUiSmoke"] is True
    assert report["competitorEntryUiSmokeBrowserChannel"] == "chrome"
    assert report["competitorEntryUiSmokeTimeoutMs"] == 123000
    assert "smoke:product-config-competitor-entry" in observed["command"]
    assert "--base-url=http://127.0.0.1:5177" in observed["command"]
    assert "--timeout-ms=123000" in observed["command"]
    assert "--channel=chrome" in observed["command"]
    assert "--country=Germany" in observed["command"]
    assert "--model=T19C MY ICE" in observed["command"]
    assert "--powertrain=ICE" in observed["command"]
    assert "--segment=SUV C" in observed["command"]
    assert "--write" not in observed["command"]
    assert not any(str(part).startswith("--api-base=") for part in observed["command"])
    assert observed["kwargs"]["cwd"] == audit.REPO_ROOT
    entry_check = next(check for check in report["checks"] if check["key"] == "competitor_entry_ui_smoke")
    assert entry_check["status"] == "passed"
    assert entry_check["details"]["recommendationQueue"].startswith("补齐队列")
    assert entry_check["details"]["sourceSearchValue"] == "LAMBORGHINI Urus Germany ICE SUV C"
    assert entry_check["details"]["requiredChecks"]["queueVisible"] is True
    assert entry_check["details"]["requiredChecks"]["queuePrimaryActionOk"] is True
    assert entry_check["details"]["failedRequiredChecks"] == []


def test_build_readiness_report_fails_when_competitor_entry_ui_smoke_lacks_queue(monkeypatch) -> None:
    checks = {key: True for key in audit.COMPETITOR_ENTRY_UI_REQUIRED_CHECKS}
    checks["queueVisible"] = False

    class Completed:
        returncode = 0
        stderr = ""
        stdout = audit.json.dumps(
            {
                "passed": True,
                "checks": checks,
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(audit.subprocess, "run", lambda *_args, **_kwargs: Completed())

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_competitor_entry_ui_smoke=True,
    )

    assert report["status"] == "failed"
    entry_check = next(check for check in report["checks"] if check["key"] == "competitor_entry_ui_smoke")
    assert entry_check["status"] == "failed"
    assert "completion queue" in entry_check["message"]
    assert entry_check["details"]["failedRequiredChecks"] == ["queueVisible"]


def test_build_readiness_report_can_include_competitor_workflow_smoke(monkeypatch) -> None:
    monkeypatch.setattr(audit, "_utc_stamp", lambda: "20260705T030000Z")
    responses = _ready_responses()
    recommendation_path = (
        "/engineering-config/recommendations/competitors?"
        "country=Germany&model_name=T19C+MY+ICE&powertrain=ICE&segment=SUV+C&limit=10"
    )
    upload_needed_payload = {
        "country": "Germany",
        "modelName": "T19C MY ICE",
        "rows": 1,
        "items": [
            {
                "modelName": "Workflow Rival",
                "brand": "WorkflowBrand",
                "profile": {"powertrain": "ICE", "segment": "SUV C"},
                "configAvailable": False,
                "sourceDigestAvailable": False,
                "nextAction": "upload_source",
            }
        ],
        "message": "ok",
    }
    after_draft_payload = {
        "country": "Germany",
        "modelName": "T19C MY ICE",
        "rows": 1,
        "items": [
            {
                "modelName": "Workflow Rival",
                "brand": "WorkflowBrand",
                "profile": {"powertrain": "ICE", "segment": "SUV C"},
                "configAvailable": True,
                "sourceDigestAvailable": True,
                "nextAction": "select_config_trim",
            }
        ],
        "message": "ok",
    }
    responses[recommendation_path] = [upload_needed_payload, upload_needed_payload, after_draft_payload]
    csv_bytes = audit._competitor_workflow_csv(
        brand="WorkflowBrand",
        model_name="Workflow Rival",
        country="Germany",
        powertrain="ICE",
        segment="SUV C",
        stamp="20260705t030000",
    )
    initiate_path = (
        "/engineering-config/source/upload/initiate?"
        "file_name=config-compare-workflow-workflow-rival-20260705t030000.csv"
        f"&total_size={len(csv_bytes)}&mime_type=text%2Fcsv"
    )
    compare_payload = {
        "trims": [{"trimId": "draft-basic"}, {"trimId": "draft-premium"}],
        "rows": [{"featureCode": "rear-camera"}],
        "summary": {"totalFeatures": 1, "shownFeatures": 1, "differenceCount": 1},
    }
    responses["/engineering-config/compare?trim_ids=draft-basic%2Cdraft-premium"] = compare_payload
    client = FakeClient(
        responses,
        json_post_responses={
            initiate_path: {"uploadId": "upload-1", "totalChunks": 1},
            "/engineering-config/source/upload/upload-1/complete": {
                "source_id": "source-workflow",
                "uploadStatus": "registered",
                "parseMode": "stored_source",
                "sourceDigest": {
                    "compareGroups": [
                        {
                            "groupId": "workflow-group",
                            "trimCount": 2,
                            "trims": [{"trimId": "basic"}, {"trimId": "premium"}],
                            "rows": [{"featureCode": "rear-camera"}],
                        }
                    ]
                },
            },
            "/engineering-config/source/snapshots/source-workflow/digest-groups/workflow-group/draft": {
                "sourceId": "source-workflow",
                "groupId": "workflow-group",
                "trimIds": ["draft-basic", "draft-premium"],
                "compareTrimIds": ["draft-basic", "draft-premium"],
                "createdTrimCount": 2,
                "reusedTrimCount": 0,
                "featureCount": 1,
                "valueRecordCount": 2,
            },
        },
        put_responses={
            "/engineering-config/source/upload/upload-1/parts/0": {
                "uploadId": "upload-1",
                "partNumber": 0,
                "receivedBytes": len(csv_bytes),
            }
        },
        post_responses={
            "/engineering-config/compare/export/xlsx": (
                b"PK\x03\x04workflow-xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
            "/engineering-config/compare/export/pdf": (b"%PDF-1.4 workflow-pdf", "application/pdf"),
        },
        patch_responses={
            "/engineering-config/trims/draft-basic": {"trimId": "draft-basic", "status": "trashed"},
            "/engineering-config/trims/draft-premium": {"trimId": "draft-premium", "status": "trashed"},
        },
        delete_responses={
            "/engineering-config/source/snapshots/source-workflow?country=Germany": {
                "sourceId": "source-workflow",
                "uploadStatus": "registered",
            },
            "/engineering-config/source/trash?country=Germany": {"cleared": 1},
            "/engineering-config/trims/trash?market=Germany": {"cleared": 2},
        },
    )

    report = audit.build_readiness_report(
        client,
        competitor_scope={
            "country": "Germany",
            "model": "T19C MY ICE",
            "powertrain": "ICE",
            "segment": "SUV C",
            "limit": 10,
        },
        include_competitor_workflow_smoke=True,
    )

    assert report["status"] == "passed"
    assert report["readOnly"] is False
    assert report["includeCompetitorWorkflowSmoke"] is True
    workflow_check = next(check for check in report["checks"] if check["key"] == "competitor_workflow")
    assert workflow_check["details"]["recommendation"]["beforeNextAction"] == "upload_source"
    assert workflow_check["details"]["recommendation"]["afterNextAction"] == "select_config_trim"
    assert workflow_check["details"]["upload"]["sourceId"] == "source-workflow"
    assert workflow_check["details"]["draft"]["trimIds"] == ["draft-basic", "draft-premium"]
    assert workflow_check["details"]["exports"]["competitor_workflow_export_xlsx"]["status"] == "passed"
    assert workflow_check["details"]["exports"]["competitor_workflow_export_pdf"]["status"] == "passed"
    assert workflow_check["details"]["cleanup"]["sourceTrashed"] is True
    assert workflow_check["details"]["cleanup"]["sourceTrashCleared"] == 1
    assert workflow_check["details"]["cleanup"]["trimTrashCleared"] == 2
    assert client.put_calls[0][1] == csv_bytes
    assert client.patch_calls == [
        ("/engineering-config/trims/draft-basic", {"status": "trashed"}),
        ("/engineering-config/trims/draft-premium", {"status": "trashed"}),
    ]
    assert client.delete_calls == [
        "/engineering-config/source/snapshots/source-workflow?country=Germany",
        "/engineering-config/source/trash?country=Germany",
        "/engineering-config/trims/trash?market=Germany",
    ]


def test_build_readiness_report_can_include_ui_edit_export_smoke(monkeypatch) -> None:
    observed: dict[str, Any] = {}

    class Completed:
        returncode = 0
        stderr = ""
        stdout = audit.json.dumps(
            {
                "summaryPath": f"{EDIT_EXPORT_ARTIFACT_DIR}/product_config_edit_export_smoke.json",
                "artifactDir": EDIT_EXPORT_ARTIFACT_DIR,
                "targetUrl": "http://127.0.0.1:5177/product/compare/config?trimIds=t1,t2",
                "sourceFormat": "pdf-text",
                "contentType": "application/pdf",
                "sourceId": "source-ui",
                "trimIds": ["t1", "t2"],
                "editResult": {
                    "scenario": "wireless_charging",
                    "savedAsExpected": True,
                    "savedAsOptional": True,
                    "saveStatus": 200,
                },
                "exports": {
                    "xlsx": {
                        "editedValueInPayload": True,
                        "signatureOk": True,
                    },
                    "pdf": {
                        "editedValueInPayload": True,
                        "signatureOk": True,
                    },
                },
                "cleanup": {
                    "errors": [],
                    "sourceTrashCleared": 1,
                    "trimTrashCleared": 2,
                },
                "passed": True,
            },
            ensure_ascii=False,
        )

    def fake_run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return Completed()

    monkeypatch.setattr(audit.subprocess, "run", fake_run)

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_ui_edit_export_smoke=True,
        frontend_base_url="http://127.0.0.1:5177",
        ui_edit_export_browser_channel="chrome",
        ui_edit_export_timeout_ms=123000,
        ui_edit_export_source_format="pdf-text",
    )

    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 7, "degraded": 0, "failed": 0}
    assert report["readOnly"] is False
    assert report["includeUiEditExportSmoke"] is True
    assert report["uiEditExportSourceFormat"] == "pdf-text"
    assert report["frontendBaseUrl"] == "http://127.0.0.1:5177"
    assert "--api-base=http://fake.test/v1" in observed["command"]
    assert "--base-url=http://127.0.0.1:5177" in observed["command"]
    assert "--channel=chrome" in observed["command"]
    assert "--source-format=pdf-text" in observed["command"]
    assert observed["kwargs"]["cwd"] == audit.REPO_ROOT
    ui_check = next(check for check in report["checks"] if check["key"] == "ui_edit_export_smoke")
    assert ui_check["status"] == "passed"
    assert ui_check["details"]["editSavedAsOptional"] is True
    assert ui_check["details"]["editSavedAsExpected"] is True
    assert ui_check["details"]["editScenario"] == "wireless_charging"
    assert ui_check["details"]["sourceFormat"] == "pdf-text"
    assert ui_check["details"]["smokeSourceFormat"] == "pdf-text"
    assert ui_check["details"]["smokeContentType"] == "application/pdf"
    assert ui_check["details"]["xlsxEditedValueInPayload"] is True
    assert ui_check["details"]["pdfEditedValueInPayload"] is True
    assert ui_check["details"]["sourceTrashCleared"] == 1
    assert ui_check["details"]["trimTrashCleared"] == 2


def test_build_readiness_report_fails_when_ui_edit_export_smoke_output_is_not_parseable(monkeypatch) -> None:
    class Completed:
        returncode = 0
        stdout = "smoke finished without json"
        stderr = ""

    monkeypatch.setattr(audit.subprocess, "run", lambda *_args, **_kwargs: Completed())

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_ui_edit_export_smoke=True,
    )

    assert report["status"] == "failed"
    ui_check = next(check for check in report["checks"] if check["key"] == "ui_edit_export_smoke")
    assert ui_check["status"] == "failed"
    assert "parseable summary" in ui_check["message"]


def test_build_readiness_report_can_include_price_list_ui_edit_export_smoke(monkeypatch) -> None:
    observed: dict[str, Any] = {}

    class Completed:
        returncode = 0
        stderr = ""
        stdout = audit.json.dumps(
            {
                "summaryPath": f"{EDIT_EXPORT_ARTIFACT_DIR}/product_config_edit_export_smoke.json",
                "artifactDir": EDIT_EXPORT_ARTIFACT_DIR,
                "targetUrl": "http://127.0.0.1:5177/product/compare/config?trimIds=t1,t2",
                "sourceFormat": "price-list-csv",
                "contentType": "text/csv",
                "sourceId": "source-price-ui",
                "trimIds": ["t1", "t2"],
                "editResult": {
                    "scenario": "msrp",
                    "savedAsExpected": True,
                    "savedAsOptional": False,
                    "saveStatus": 200,
                },
                "exports": {
                    "xlsx": {
                        "editedValueInPayload": True,
                        "signatureOk": True,
                    },
                    "pdf": {
                        "editedValueInPayload": True,
                        "signatureOk": True,
                    },
                },
                "cleanup": {
                    "errors": [],
                    "sourceTrashCleared": 1,
                    "trimTrashCleared": 2,
                },
                "passed": True,
            },
            ensure_ascii=False,
        )

    def fake_run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return Completed()

    monkeypatch.setattr(audit.subprocess, "run", fake_run)

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_ui_edit_export_smoke=True,
        ui_edit_export_source_format="price-list-csv",
    )

    assert report["status"] == "passed"
    assert report["uiEditExportSourceFormat"] == "price-list-csv"
    assert "--source-format=price-list-csv" in observed["command"]
    ui_check = next(check for check in report["checks"] if check["key"] == "ui_edit_export_smoke")
    assert ui_check["status"] == "passed"
    assert ui_check["details"]["sourceFormat"] == "price-list-csv"
    assert ui_check["details"]["smokeSourceFormat"] == "price-list-csv"
    assert ui_check["details"]["smokeContentType"] == "text/csv"
    assert ui_check["details"]["editScenario"] == "msrp"
    assert ui_check["details"]["editSavedAsExpected"] is True
    assert ui_check["details"]["editSavedAsOptional"] is False
    assert ui_check["details"]["xlsxEditedValueInPayload"] is True
    assert ui_check["details"]["pdfEditedValueInPayload"] is True


def test_build_readiness_report_can_include_floatingdeck_multisource_smokes(monkeypatch) -> None:
    observed: dict[str, Any] = {"commands": []}

    class Completed:
        returncode = 0
        stderr = ""

        def __init__(self, payload: dict[str, Any]) -> None:
            self.stdout = audit.json.dumps(payload, ensure_ascii=False)

    def fake_run(command, **kwargs):
        observed["commands"].append(command)
        observed["kwargs"] = kwargs
        if "smoke:product-config-multisource" in command:
            return Completed(
                {
                    "summaryPath": f"{MULTISOURCE_ARTIFACT_DIR}/product_config_multisource_same_model_smoke.json",
                    "artifactDir": MULTISOURCE_ARTIFACT_DIR,
                    "sourceIds": ["source-a", "source-b"],
                    "trimIds": ["a-basic", "a-premium", "b-basic", "b-premium"],
                    "compareApi": {
                        "trimCount": 4,
                        "duplicateBasicPremiumKept": True,
                    },
                    "floatingDeckSearch": {"passed": True},
                    "formalCompareUi": {"noHorizontalOverflow": True},
                    "cleanup": {"errors": [], "sourceTrashCleared": 2, "trimTrashCleared": 4},
                    "observed": {
                        "api": [{"step": "source_uploaded"}],
                        "ui": [{"step": "floating_deck_multisource_search"}],
                    },
                    "passed": True,
                }
            )
        if "smoke:product-config-cross-scope" in command:
            return Completed(
                {
                    "summaryPath": f"{CROSS_SCOPE_ARTIFACT_DIR}/product_config_cross_scope_direct_picker_smoke.json",
                    "artifactDir": CROSS_SCOPE_ARTIFACT_DIR,
                    "sourceIds": ["source-de", "source-fr"],
                    "trimIds": ["de-basic", "de-premium", "fr-basic", "fr-premium"],
                    "directPickerFlow": {
                        "countriesVisible": True,
                        "modelsVisible": True,
                        "sourcesVisible": True,
                        "selectedFourColumns": True,
                        "noModeText": True,
                        "noHorizontalOverflow": True,
                        "rowsStatus": "当前展示 12/12 配置行",
                    },
                    "cleanup": {"errors": [], "sourceTrashClearedByCountry": {"Germany": 1, "France": 1}},
                    "observed": {
                        "api": [{"step": "source_uploaded"}],
                        "ui": [{"step": "direct_picker_cross_scope_add"}],
                    },
                    "passed": True,
                }
            )
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(audit.subprocess, "run", fake_run)

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_floatingdeck_multisource_smoke=True,
        frontend_base_url="http://127.0.0.1:5177",
        floatingdeck_multisource_browser_channel="chrome",
        floatingdeck_multisource_timeout_ms=123000,
    )

    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 8, "degraded": 0, "failed": 0}
    assert report["readOnly"] is False
    assert report["includeFloatingDeckMultisourceSmoke"] is True
    assert report["floatingDeckMultisourceBrowserChannel"] == "chrome"
    assert report["floatingDeckMultisourceTimeoutMs"] == 123000
    assert len(observed["commands"]) == 2
    assert all("--write" in command for command in observed["commands"])
    assert all("--api-base=http://fake.test/v1" in command for command in observed["commands"])
    assert all("--base-url=http://127.0.0.1:5177" in command for command in observed["commands"])
    assert all("--channel=chrome" in command for command in observed["commands"])
    assert observed["kwargs"]["cwd"] == audit.REPO_ROOT
    same_model = next(check for check in report["checks"] if check["key"] == "floatingdeck_multisource_same_model")
    cross_scope = next(check for check in report["checks"] if check["key"] == "floatingdeck_cross_scope_direct_picker")
    assert same_model["status"] == "passed"
    assert same_model["details"]["compareTrimCount"] == 4
    assert same_model["details"]["duplicateBasicPremiumKept"] is True
    assert same_model["details"]["floatingDeckSearchPassed"] is True
    assert same_model["details"]["formalCompareNoHorizontalOverflow"] is True
    assert cross_scope["status"] == "passed"
    assert cross_scope["details"]["selectedFourColumns"] is True
    assert cross_scope["details"]["noOwnCompetitorModeText"] is True
    assert cross_scope["details"]["rowsStatus"] == "当前展示 12/12 配置行"


def test_build_readiness_report_fails_when_floatingdeck_multisource_smoke_output_is_not_parseable(monkeypatch) -> None:
    class Completed:
        returncode = 0
        stdout = "smoke finished without json"
        stderr = ""

    monkeypatch.setattr(audit.subprocess, "run", lambda *_args, **_kwargs: Completed())

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_floatingdeck_multisource_smoke=True,
    )

    assert report["status"] == "failed"
    checks = [check for check in report["checks"] if check["key"].startswith("floatingdeck_")]
    assert len(checks) == 2
    assert all(check["status"] == "failed" for check in checks)
    assert all("parseable summary" in check["message"] for check in checks)


def test_build_readiness_report_can_include_t19c_ai_ui_smoke(monkeypatch, tmp_path) -> None:
    observed: dict[str, Any] = {}
    summary_path = tmp_path / "product_config_t19c_ai_smoke.json"
    summary_path.write_text(
        audit.json.dumps(
            {
                "passed": True,
                "targetUrl": "http://127.0.0.1:5177/product/compare/config?trimIds=t1,t2,t3&baseTrimId=t1",
                "trimIds": ["t1", "t2", "t3"],
                "baseTrimId": "t1",
                "expectedRows": 229,
                "viewportMode": "desktop",
                "initialScreenshotPath": (
                    "06_AppPlatform/frontend/artifacts/product-config-t19c-ai-smoke/run/desktop_initial.png"
                ),
                "initialScreenshotPaths": {
                    "desktop": "06_AppPlatform/frontend/artifacts/product-config-t19c-ai-smoke/run/desktop_initial.png",
                },
                "screenshotPath": "06_AppPlatform/frontend/artifacts/product-config-t19c-ai-smoke/run/desktop.png",
                "screenshotPaths": {
                    "desktop": "06_AppPlatform/frontend/artifacts/product-config-t19c-ai-smoke/run/desktop.png",
                },
                "checks": {key: True for key in audit.T19C_AI_UI_REQUIRED_CHECKS},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    class Completed:
        returncode = 0
        stderr = ""
        stdout = f"Product config T19C AI smoke passed. Summary: {summary_path}\n"

    def fake_run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return Completed()

    monkeypatch.setattr(audit.subprocess, "run", fake_run)

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_t19c_ai_ui_smoke=True,
        frontend_base_url="http://127.0.0.1:5177",
        t19c_ai_ui_smoke_browser_channel="chrome",
        t19c_ai_ui_smoke_timeout_ms=123000,
        t19c_ai_ui_smoke_expected_rows=229,
        t19c_ai_ui_smoke_trim_ids=["t1", "t2", "t3"],
        t19c_ai_ui_smoke_base_trim_id="t1",
        t19c_ai_ui_smoke_viewport="desktop",
    )

    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 7, "degraded": 0, "failed": 0}
    assert report["readOnly"] is True
    assert report["includeT19cAiUiSmoke"] is True
    assert report["t19cAiUiSmokeBrowserChannel"] == "chrome"
    assert report["t19cAiUiSmokeExpectedRows"] == 229
    assert report["t19cAiUiSmokeTrimIds"] == ["t1", "t2", "t3"]
    assert report["t19cAiUiSmokeBaseTrimId"] == "t1"
    assert "smoke:product-config-ai" in observed["command"]
    assert "--base-url=http://127.0.0.1:5177" in observed["command"]
    assert "--channel=chrome" in observed["command"]
    assert "--timeout-ms=123000" in observed["command"]
    assert "--expected-rows=229" in observed["command"]
    assert "--trim-ids=t1,t2,t3" in observed["command"]
    assert "--base-trim-id=t1" in observed["command"]
    assert "--viewport=desktop" in observed["command"]
    assert "--write" not in observed["command"]
    assert not any(str(part).startswith("--api-base=") for part in observed["command"])
    assert observed["kwargs"]["cwd"] == audit.REPO_ROOT
    t19c_check = next(check for check in report["checks"] if check["key"] == "t19c_ai_ui_smoke")
    assert t19c_check["status"] == "passed"
    assert t19c_check["details"]["summaryTrimIds"] == ["t1", "t2", "t3"]
    assert t19c_check["details"]["summaryExpectedRows"] == 229
    assert t19c_check["details"]["initialScreenshotPath"].endswith("desktop_initial.png")
    assert t19c_check["details"]["initialScreenshotPaths"]["desktop"].endswith("desktop_initial.png")
    assert t19c_check["details"]["screenshotPaths"]["desktop"].endswith("desktop.png")
    assert t19c_check["details"]["failedRequiredChecks"] == []
    assert t19c_check["details"]["requiredChecks"]["allCompactAiCardsCollapsedByDefault"] is True
    assert t19c_check["details"]["requiredChecks"]["collapsedAiCardsStayCompact"] is True
    assert t19c_check["details"]["requiredChecks"]["compactEvidenceInlineBoundaryHidden"] is True
    assert t19c_check["details"]["requiredChecks"]["simpleTableNavigatorHiddenByDefault"] is True
    assert t19c_check["details"]["requiredChecks"]["floatingDeckEditGateOk"] is True


def test_build_readiness_report_fails_when_t19c_ai_ui_smoke_lacks_required_check(monkeypatch) -> None:
    checks = {key: True for key in audit.T19C_AI_UI_REQUIRED_CHECKS}
    checks["simpleTableNavigatorHiddenByDefault"] = False

    class Completed:
        returncode = 0
        stderr = ""
        stdout = audit.json.dumps(
            {
                "passed": True,
                "checks": checks,
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(audit.subprocess, "run", lambda *_args, **_kwargs: Completed())

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_t19c_ai_ui_smoke=True,
    )

    assert report["status"] == "failed"
    t19c_check = next(check for check in report["checks"] if check["key"] == "t19c_ai_ui_smoke")
    assert t19c_check["status"] == "failed"
    assert "scoped navigator" in t19c_check["message"]
    assert t19c_check["details"]["failedRequiredChecks"] == ["simpleTableNavigatorHiddenByDefault"]


def test_build_readiness_report_can_include_cross_user_source_library_smoke(monkeypatch) -> None:
    observed: dict[str, Any] = {}

    class Completed:
        returncode = 0
        stderr = ""
        stdout = audit.json.dumps(
            {
                "summaryPath": f"{CROSS_USER_ARTIFACT_DIR}/product_config_cross_user_source_library_smoke.json",
                "artifactDir": CROSS_USER_ARTIFACT_DIR,
                "targetUrl": "http://127.0.0.1:5177/product/compare/config?market=CrossUser",
                "screenshotPath": f"{CROSS_USER_ARTIFACT_DIR}/product_config_cross_user_source_library_smoke.png",
                "csvPath": f"{CROSS_USER_ARTIFACT_DIR}/cross-user-source.csv",
                "fileName": "cross-user-source.csv",
                "uploaderUserName": "product-config-uploader-smoke",
                "consumerUserName": "product-config-consumer-smoke",
                "sourceId": "source-cross-user",
                "trimIds": ["shared-basic", "shared-premium"],
                "consumerSourceList": {
                    "itemCount": 1,
                    "found": True,
                    "createdBy": "product-config-uploader-smoke",
                    "sourceId": "source-cross-user",
                },
                "uiResult": {
                    "rowsStatus": "当前展示 12/12 配置行",
                    "fileVisible": True,
                    "uploaderVisible": True,
                    "consumerVisible": True,
                    "successVisible": True,
                    "basicVisible": True,
                    "premiumVisible": True,
                    "noHorizontalOverflow": True,
                },
                "cleanup": {
                    "errors": [],
                    "sourceTrashCleared": 1,
                    "trimTrashCleared": 2,
                },
                "observed": {
                    "api": [
                        {"step": "source_uploaded_by_uploader"},
                        {"step": "consumer_can_list_uploaded_source"},
                        {"step": "consumer_created_draft_from_shared_source"},
                    ],
                    "ui": [{"step": "consumer_ui_created_formal_compare"}],
                },
                "passed": True,
            },
            ensure_ascii=False,
        )

    def fake_run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return Completed()

    monkeypatch.setattr(audit.subprocess, "run", fake_run)

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_cross_user_source_library_smoke=True,
        frontend_base_url="http://127.0.0.1:5177",
        cross_user_source_library_browser_channel="chrome",
        cross_user_source_library_timeout_ms=123000,
    )

    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 7, "degraded": 0, "failed": 0}
    assert report["readOnly"] is False
    assert report["includeCrossUserSourceLibrarySmoke"] is True
    assert report["crossUserSourceLibraryBrowserChannel"] == "chrome"
    assert report["crossUserSourceLibraryTimeoutMs"] == 123000
    assert "smoke:product-config-cross-user-source" in observed["command"]
    assert "--write" in observed["command"]
    assert "--api-base=http://fake.test/v1" in observed["command"]
    assert "--base-url=http://127.0.0.1:5177" in observed["command"]
    assert "--channel=chrome" in observed["command"]
    assert observed["kwargs"]["cwd"] == audit.REPO_ROOT
    cross_user_check = next(check for check in report["checks"] if check["key"] == "cross_user_source_library_smoke")
    assert cross_user_check["status"] == "passed"
    assert cross_user_check["details"]["uploaderUserName"] == "product-config-uploader-smoke"
    assert cross_user_check["details"]["consumerUserName"] == "product-config-consumer-smoke"
    assert cross_user_check["details"]["consumerSourceFound"] is True
    assert cross_user_check["details"]["consumerSourceCreatedBy"] == "product-config-uploader-smoke"
    assert cross_user_check["details"]["successVisible"] is True
    assert cross_user_check["details"]["uploaderVisible"] is True
    assert cross_user_check["details"]["consumerVisible"] is True
    assert cross_user_check["details"]["basicVisible"] is True
    assert cross_user_check["details"]["premiumVisible"] is True
    assert cross_user_check["details"]["noHorizontalOverflow"] is True
    assert cross_user_check["details"]["sourceTrashCleared"] == 1
    assert cross_user_check["details"]["trimTrashCleared"] == 2


def test_build_readiness_report_fails_when_cross_user_source_library_smoke_output_is_not_parseable(monkeypatch) -> None:
    class Completed:
        returncode = 0
        stdout = "cross-user source-library smoke finished without json"
        stderr = ""

    monkeypatch.setattr(audit.subprocess, "run", lambda *_args, **_kwargs: Completed())

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_cross_user_source_library_smoke=True,
    )

    assert report["status"] == "failed"
    cross_user_check = next(check for check in report["checks"] if check["key"] == "cross_user_source_library_smoke")
    assert cross_user_check["status"] == "failed"
    assert "parseable summary" in cross_user_check["message"]


def test_build_readiness_report_can_include_source_review_row_smoke(monkeypatch) -> None:
    observed: dict[str, Any] = {}

    class Completed:
        returncode = 0
        stderr = ""
        stdout = audit.json.dumps(
            {
                "summaryPath": f"{REVIEW_ROW_ARTIFACT_DIR}/product_config_review_row_smoke.json",
                "artifactDir": REVIEW_ROW_ARTIFACT_DIR,
                "targetUrl": "http://127.0.0.1:5177/product/compare/config?market=Review",
                "sourceId": "source-review-row",
                "trimIds": ["ocr-basic", "ocr-premium", "ocr-luxury"],
                "fileName": "review-row-source.jpg",
                "imageFormat": "JPEG",
                "mimeType": "image/jpeg",
                "selectedReviewFeature": "Roof rack",
                "reviewRowCount": 2,
                "cleanup": {
                    "errors": [],
                    "sourceTrashed": True,
                    "sourceTrashCleared": 1,
                    "trimTrashCleared": 3,
                },
                "observed": {
                    "api": [{"step": "source_uploaded"}],
                    "ui": [
                        {"step": "selected_review_row", "feature": "Roof rack"},
                        {"step": "formal_row_highlighted", "feature": "Roof rack"},
                        {
                            "step": "edited_selected_review_feature",
                            "feature": "Roof rack",
                            "savedAsOptional": True,
                            "saveStatus": 200,
                        },
                        {
                            "step": "export_xlsx_after_review_edit",
                            "feature": "Roof rack",
                            "status": 200,
                            "signatureOk": True,
                            "editedValueInPayload": True,
                        },
                        {
                            "step": "export_pdf_after_review_edit",
                            "feature": "Roof rack",
                            "status": 200,
                            "signatureOk": True,
                            "editedValueInPayload": True,
                        },
                    ],
                },
                "passed": True,
            },
            ensure_ascii=False,
        )

    def fake_run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return Completed()

    monkeypatch.setattr(audit.subprocess, "run", fake_run)

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_source_review_row_smoke=True,
        frontend_base_url="http://127.0.0.1:5177",
        source_review_row_browser_channel="chrome",
        source_review_row_timeout_ms=123000,
        source_review_row_image_format="jpeg",
    )

    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 7, "degraded": 0, "failed": 0}
    assert report["readOnly"] is False
    assert report["includeSourceReviewRowSmoke"] is True
    assert report["sourceReviewRowBrowserChannel"] == "chrome"
    assert report["sourceReviewRowTimeoutMs"] == 123000
    assert report["sourceReviewRowImageFormat"] == "jpeg"
    assert "smoke:product-config-review-row" in observed["command"]
    assert "--api-base=http://fake.test/v1" in observed["command"]
    assert "--base-url=http://127.0.0.1:5177" in observed["command"]
    assert "--channel=chrome" in observed["command"]
    assert "--image-format=jpeg" in observed["command"]
    assert observed["kwargs"]["cwd"] == audit.REPO_ROOT
    review_check = next(check for check in report["checks"] if check["key"] == "source_review_row_smoke")
    assert review_check["status"] == "passed"
    assert review_check["details"]["selectedReviewFeature"] == "Roof rack"
    assert review_check["details"]["reviewRowCount"] == 2
    assert review_check["details"]["selectedReviewRow"] is True
    assert review_check["details"]["formalRowHighlighted"] is True
    assert review_check["details"]["reviewEditSavedAsOptional"] is True
    assert review_check["details"]["reviewEditSaveStatus"] == 200
    assert review_check["details"]["xlsxEditedValueInPayload"] is True
    assert review_check["details"]["xlsxSignatureOk"] is True
    assert review_check["details"]["pdfEditedValueInPayload"] is True
    assert review_check["details"]["pdfSignatureOk"] is True
    assert review_check["details"]["sourceTrashed"] is True
    assert review_check["details"]["sourceTrashCleared"] == 1
    assert review_check["details"]["trimTrashCleared"] == 3


def test_build_readiness_report_fails_when_source_review_row_smoke_output_is_not_parseable(monkeypatch) -> None:
    class Completed:
        returncode = 0
        stdout = "review-row smoke finished without json"
        stderr = ""

    monkeypatch.setattr(audit.subprocess, "run", lambda *_args, **_kwargs: Completed())

    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        include_source_review_row_smoke=True,
    )

    assert report["status"] == "failed"
    review_check = next(check for check in report["checks"] if check["key"] == "source_review_row_smoke")
    assert review_check["status"] == "failed"
    assert "parseable summary" in review_check["message"]


def test_build_readiness_report_can_include_local_workbook_digest_smoke() -> None:
    responses = _ready_responses()
    responses["/engineering-config/source/local-workbook-digest?file_name=sample.xlsx"] = {
        "summary": {
            "sheetCount": 18,
            "featureCount": 227,
            "candidateTrimCount": 36,
            "comparableGroupCount": 13,
        },
        "compareGroups": [{"groupId": "g1", "trimCount": 3}],
    }

    report = audit.build_readiness_report(
        FakeClient(responses),
        include_local_workbook_smoke=True,
        local_workbook_file="sample.xlsx",
    )

    assert report["status"] == "passed"
    assert report["summary"] == {"passed": 7, "degraded": 0, "failed": 0}
    assert report["includeLocalWorkbookSmoke"] is True
    workbook_check = next(check for check in report["checks"] if check["key"] == "local_workbook_digest")
    assert workbook_check["details"]["comparableGroupCount"] == 13
    assert workbook_check["details"]["candidateTrimCount"] == 36
    assert workbook_check["details"]["timeoutSeconds"] == audit.DEFAULT_LOCAL_WORKBOOK_TIMEOUT


def test_local_workbook_digest_smoke_uses_dedicated_timeout(monkeypatch) -> None:
    observed_calls: list[tuple[str, float]] = []

    def fake_get_json(self: audit.ApiClient, path: str) -> dict[str, Any]:
        observed_calls.append((path, self.timeout))
        return {
            "summary": {
                "sheetCount": 18,
                "featureCount": 227,
                "candidateTrimCount": 36,
                "comparableGroupCount": 13,
            },
            "compareGroups": [{"groupId": "g1", "trimCount": 3}],
        }

    monkeypatch.setattr(audit.ApiClient, "get_json", fake_get_json)

    checks = audit._local_workbook_digest_check(
        audit.ApiClient("http://fake.test", timeout=2.0),
        enabled=True,
        file_name=None,
        timeout=45.0,
    )

    assert observed_calls == [("/engineering-config/source/local-workbook-digest", 45.0)]
    assert checks[0]["status"] == "passed"
    assert checks[0]["details"]["timeoutSeconds"] == 45.0


def test_build_readiness_report_degrades_when_local_workbook_has_no_compare_groups() -> None:
    responses = _ready_responses()
    responses["/engineering-config/source/local-workbook-digest"] = {
        "summary": {
            "sheetCount": 1,
            "featureCount": 0,
            "candidateTrimCount": 1,
            "comparableGroupCount": 0,
        },
        "compareGroups": [],
    }

    report = audit.build_readiness_report(
        FakeClient(responses),
        include_local_workbook_smoke=True,
    )

    assert report["status"] == "degraded"
    workbook_check = next(check for check in report["checks"] if check["key"] == "local_workbook_digest")
    assert workbook_check["status"] == "degraded"
    assert "no comparable config groups" in workbook_check["message"]


def test_build_readiness_report_degrades_when_competitor_recommendations_are_empty() -> None:
    responses = _ready_responses()
    responses[COMPETITOR_RECOMMENDATION_PATH] = {
        "country": "Germany",
        "modelName": "T19C MY ICE",
        "rows": 0,
        "items": [],
        "message": "advanced_analysis_unavailable",
    }

    report = audit.build_readiness_report(
        FakeClient(responses),
        competitor_scope={
            "country": "Germany",
            "model": "T19C MY ICE",
            "powertrain": "ICE",
            "segment": "SUV C",
            "limit": 10,
        },
    )

    assert report["status"] == "degraded"
    competitor_check = next(check for check in report["checks"] if check["key"] == "competitor_recommendations")
    assert competitor_check["status"] == "degraded"
    assert competitor_check["details"]["message"] == "advanced_analysis_unavailable"


def test_build_readiness_report_degrades_when_competitor_scope_is_incomplete() -> None:
    report = audit.build_readiness_report(
        FakeClient(_ready_responses()),
        competitor_scope={"country": "Germany", "model": "", "limit": 10},
    )

    assert report["status"] == "degraded"
    competitor_check = next(check for check in report["checks"] if check["key"] == "competitor_recommendations")
    assert competitor_check["status"] == "degraded"
    assert "country and model are required" in competitor_check["message"]


def test_render_markdown_and_write_outputs(tmp_path: Path) -> None:
    report = audit.build_readiness_report(FakeClient(_ready_responses()))

    markdown = audit.render_markdown(report)
    artifacts = audit.write_outputs(report, tmp_path)

    assert "Engineering Config Compare Readiness" in markdown
    assert "Only PaddleOCR is available" in markdown
    assert "| Runtime AI summary readiness | `passed` |" in markdown
    assert Path(artifacts["latestJson"]).exists()
    assert (
        Path(artifacts["latestMarkdown"])
        .read_text(encoding="utf-8")
        .startswith("# Engineering Config Compare Readiness")
    )
