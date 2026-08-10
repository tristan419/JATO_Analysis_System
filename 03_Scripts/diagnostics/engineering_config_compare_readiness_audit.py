#!/usr/bin/env python3
"""Readiness audit for Product Config Compare.

This script checks the live FastAPI surface that the Product Config Compare
page depends on. It does not upload files, create source snapshots, edit values,
or clear trash by default. Write-path validation is available only behind
explicit opt-in flags.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "engineering_config_compare_readiness_v1"
DEFAULT_API_BASE = "http://127.0.0.1:8004"
DEFAULT_FRONTEND_BASE = "http://127.0.0.1:5177"
DEFAULT_LOCAL_WORKBOOK_TIMEOUT = 30.0
DEFAULT_AI_SUMMARY_TIMEOUT = 45.0
REPO_ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DIR = REPO_ROOT / "06_AppPlatform" / "frontend"
DEFAULT_OCR_QUALITY_ARTIFACT_DIR = REPO_ROOT / "04_Processed_data" / "ops" / "engineering_config_ocr_quality_audits"
OCR_QUALITY_ARTIFACT_GLOB = "engineering_config_ocr_quality_audit_*.json"
PIPELINE_ID = "engineering_config_compare_readiness"
T19C_AI_UI_REQUIRED_CHECKS = (
    "hasAiBusinessPanel",
    "hasFullTableRows",
    "simpleAiOnlyPanelActive",
    "simpleAiSummaryVisible",
    "simpleDeterministicRuleBlocksHidden",
    "allCompactAiCardsCollapsedByDefault",
    "collapsedAiCardsStayCompact",
    "quickEvidenceButtonHorizontal",
    "compactEvidenceCardCollapsedByDefault",
    "compactEvidenceQuickActionVisible",
    "compactEvidenceInlineBoundaryHidden",
    "initialConfigLibraryDeferred",
    "initialSourceLibraryDeferred",
    "initialCompetitorRecommendationsDeferred",
    "initialSourceDigestControlsHidden",
    "initialAdvancedRecommendationsHidden",
    "simpleTableNavigatorDefaultFullRows",
    "simpleTableNavigatorHiddenByDefault",
    "simpleTableNavigatorDifferenceScopeReached",
    "simpleTableNavigatorVisibleInDifferenceScope",
    "simpleTableNavigatorCopyHiddenBeforeRowFocus",
    "simpleTableNavigatorCopyVisibleAfterRowFocus",
    "simpleTableNavigatorRestoresFullRows",
    "simpleTableNavigatorHiddenAfterRestore",
    "largeSourcePickerShowsSourceModelColumnPaths",
    "largeSourcePathPreviewAvailable",
    "multiSourceSameModelPickerHasManyOptions",
    "floatingDeckEditGateOk",
)
COMPETITOR_ENTRY_UI_REQUIRED_CHECKS = (
    "hasRecommendations",
    "topTenOk",
    "coverageCountsOk",
    "scopeOk",
    "queueVisible",
    "queueMissingPriorityOk",
    "queueCountsOk",
    "queuePrimaryActionOk",
    "bmwReadyOk",
    "urusMissingOk",
    "sourceSearchPrefilled",
    "missingContextOk",
    "uploadSurfaceVisible",
    "currentTargetPreserved",
    "noUploadWriteRequests",
)


class ReadinessHttpError(RuntimeError):
    def __init__(self, url: str, status: int | None, message: str) -> None:
        super().__init__(f"{url}: {status or 'connection'} {message}")
        self.url = url
        self.status = status
        self.message = message


@dataclass(frozen=True)
class ApiClient:
    api_base: str
    token: str | None = None
    user_name: str = "readiness-audit"
    timeout: float = 10.0

    def _headers(self, accept: str = "application/json", content_type: str | None = None) -> dict[str, str]:
        headers = {"Accept": accept, "X-User-Name": self.user_name}
        if content_type is not None:
            headers["Content-Type"] = content_type
        if self.token:
            headers["X-Auth-Token"] = self.token
        return headers

    def _url(self, path: str) -> str:
        base = self.api_base.rstrip("/")
        if path == "/healthz":
            return f"{base}{path}"
        return f"{base}/v1{path}"

    def get_json(self, path: str) -> dict[str, Any]:
        url = self._url(path)
        headers = self._headers()
        request = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout
            ) as response:  # noqa: S310 - local/provided API endpoint.
                body = response.read().decode("utf-8")
                if not body.strip():
                    return {"ok": True, "status": response.status}
                parsed = json.loads(body)
                return parsed if isinstance(parsed, dict) else {"items": parsed}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise ReadinessHttpError(url, exc.code, body[:300]) from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise ReadinessHttpError(url, None, str(exc)) from exc

    def post_json_bytes(self, path: str, payload: dict[str, Any]) -> tuple[bytes, str | None]:
        url = self._url(path)
        headers = self._headers(accept="*/*", content_type="application/json")
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout
            ) as response:  # noqa: S310 - local/provided API endpoint.
                return response.read(), response.headers.get("Content-Type")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise ReadinessHttpError(url, exc.code, body[:300]) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise ReadinessHttpError(url, None, str(exc)) from exc

    def post_json(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._json_request(path, method="POST", payload=payload)

    def patch_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._json_request(path, method="PATCH", payload=payload)

    def delete_json(self, path: str) -> dict[str, Any]:
        return self._json_request(path, method="DELETE")

    def put_bytes(self, path: str, body: bytes, content_type: str = "application/octet-stream") -> dict[str, Any]:
        url = self._url(path)
        headers = self._headers(content_type=content_type)
        request = urllib.request.Request(url, data=body, headers=headers, method="PUT")
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout
            ) as response:  # noqa: S310 - local/provided API endpoint.
                response_body = response.read().decode("utf-8")
                if not response_body.strip():
                    return {"ok": True, "status": response.status}
                parsed = json.loads(response_body)
                return parsed if isinstance(parsed, dict) else {"items": parsed}
        except urllib.error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            raise ReadinessHttpError(url, exc.code, response_body[:300]) from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise ReadinessHttpError(url, None, str(exc)) from exc

    def _json_request(self, path: str, *, method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = self._url(path)
        headers = self._headers()
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout
            ) as response:  # noqa: S310 - local/provided API endpoint.
                body = response.read().decode("utf-8")
                if not body.strip():
                    return {"ok": True, "status": response.status}
                parsed = json.loads(body)
                return parsed if isinstance(parsed, dict) else {"items": parsed}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise ReadinessHttpError(url, exc.code, body[:300]) from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise ReadinessHttpError(url, None, str(exc)) from exc


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve()))
    except ValueError:
        return str(path)


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _endpoint_result(
    key: str,
    label: str,
    path: str,
    client: ApiClient,
    evaluator,
) -> dict[str, Any]:
    try:
        payload = client.get_json(path)
    except ReadinessHttpError as exc:
        return {
            "key": key,
            "label": label,
            "status": "failed",
            "path": path,
            "httpStatus": exc.status,
            "message": exc.message,
        }
    status, message, details = evaluator(payload)
    return {
        "key": key,
        "label": label,
        "status": status,
        "path": path,
        "message": message,
        "details": details,
    }


def _health_evaluator(payload: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    ok = bool(payload.get("ok", True))
    return (
        "passed" if ok else "failed",
        "backend health endpoint is reachable" if ok else "backend health endpoint returned not-ok",
        {"payloadKeys": sorted(payload.keys())[:12]},
    )


def _ocr_comparison_status(payload: dict[str, Any], *, image_ready: bool) -> tuple[str, str, bool]:
    default_engine = str(payload.get("defaultEngine") or "").lower()
    paddle_ready = bool(payload.get("paddleOcrReady")) or (image_ready and default_engine == "paddleocr")
    legacy_ready = bool(payload.get("legacyOcrReady"))
    if paddle_ready and legacy_ready:
        return (
            "ready",
            "PaddleOCR and legacy/custom OCR are both available; OCR uploads can be compared across engines.",
            True,
        )
    if paddle_ready:
        return (
            "paddle_only",
            (
                "Only PaddleOCR is available; image/scanned PDF OCR can run, but PaddleOCR-vs-legacy/custom comparison"
                " is unavailable."
            ),
            False,
        )
    if legacy_ready:
        return (
            "legacy_only",
            "Only legacy/custom OCR is available; OCR can run, but PaddleOCR comparison is unavailable.",
            False,
        )
    if image_ready:
        return (
            "single_engine_unknown",
            (
                "OCR can run, but the readiness payload does not identify both PaddleOCR and legacy/custom engines for"
                " comparison."
            ),
            False,
        )
    return (
        "not_available",
        "OCR engine comparison is unavailable because no image OCR engine is ready.",
        False,
    )


def _ocr_runtime_next_actions(comparison_status: str) -> list[str]:
    if comparison_status == "ready":
        return [
            (
                "Run engineering_config_ocr_quality_audit.py on real scanned PDF/image config samples to choose the"
                " better OCR engine."
            ),
            (
                "Keep PaddleOCR/custom/tesseract candidates visible in Source Digest evidence before changing the"
                " runtime default."
            ),
        ]
    if comparison_status == "paddle_only":
        return [
            (
                "Install tesseract or set JATO_CONFIG_OCR_COMMAND to the previous PDF/image OCR command so the audit"
                " can compare a legacy/custom candidate against PaddleOCR."
            ),
            "Run engineering_config_ocr_quality_audit.py again after the second engine is available.",
            (
                "Use PaddleOCR as the current default only as a single-engine runtime choice until a two-engine quality"
                " artifact exists."
            ),
        ]
    if comparison_status == "legacy_only":
        return [
            "Install paddleocr and paddlepaddle so the legacy/custom OCR result can be compared against PaddleOCR.",
            (
                "Run engineering_config_ocr_quality_audit.py on the same real scanned PDF/image samples after PaddleOCR"
                " is available."
            ),
        ]
    if comparison_status == "single_engine_unknown":
        return [
            (
                "Check OCR readiness components and configure either PaddleOCR plus tesseract or"
                " JATO_CONFIG_OCR_COMMAND for a named two-engine comparison."
            ),
            (
                "Run engineering_config_ocr_quality_audit.py and verify ocrComparisonStatus includes"
                " selected_compared_with_alternates."
            ),
        ]
    return [
        (
            "Install paddleocr+paddlepaddle or configure JATO_CONFIG_OCR_COMMAND/tesseract before using scanned"
            " PDF/image Source Digest."
        ),
        "After OCR is ready, run engineering_config_ocr_quality_audit.py with real config-table samples.",
    ]


def _ocr_evaluator(payload: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    image_ready = bool(payload.get("imageOcrReady"))
    pdf_ready = bool(payload.get("pdfOcrReady"))
    status = str(payload.get("status") or "unknown")
    default_engine = payload.get("defaultEngine")
    paddle_ready = bool(payload.get("paddleOcrReady")) or (
        image_ready and str(default_engine or "").lower() == "paddleocr"
    )
    comparison_status, comparison_message, comparison_ready = _ocr_comparison_status(payload, image_ready=image_ready)
    if image_ready and pdf_ready:
        result = "passed"
        message = f"OCR ready for PDF/image via {default_engine or status}"
    elif image_ready or pdf_ready:
        result = "degraded"
        message = "OCR partially ready; one of PDF/image OCR paths is unavailable"
    else:
        result = "degraded"
        message = "OCR not fully configured; Excel/CSV/HTML/text-PDF compare remains usable"
    message = f"{message}; {comparison_message}"
    return (
        result,
        message,
        {
            "status": status,
            "defaultEngine": default_engine,
            "imageOcrReady": image_ready,
            "pdfOcrReady": pdf_ready,
            "paddleOcrReady": paddle_ready,
            "legacyOcrReady": bool(payload.get("legacyOcrReady")),
            "ocrComparisonStatus": comparison_status,
            "ocrComparisonReady": comparison_ready,
            "ocrComparisonMessage": comparison_message,
            "nextActions": _ocr_runtime_next_actions(comparison_status),
        },
    )


def _ocr_quality_artifact_runtime_issue(
    payload: dict[str, Any], ocr_runtime_details: dict[str, Any] | None
) -> str | None:
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    if not runtime:
        return None
    if runtime.get("sourceDigestImportOk") is False:
        return "artifact runtime could not import the Source Digest parser"
    modules = runtime.get("modules") if isinstance(runtime.get("modules"), dict) else {}
    if not isinstance(ocr_runtime_details, dict):
        return None
    backend_paddle_ready = bool(ocr_runtime_details.get("paddleOcrReady"))
    if not backend_paddle_ready:
        return None
    paddleocr_status = modules.get("paddleocr") if isinstance(modules.get("paddleocr"), dict) else {}
    paddle_status = modules.get("paddle") if isinstance(modules.get("paddle"), dict) else {}
    if paddleocr_status.get("available") is False:
        return "artifact runtime is missing paddleocr while backend OCR readiness reports PaddleOCR available"
    if paddle_status.get("available") is False:
        return "artifact runtime is missing paddlepaddle while backend OCR readiness reports PaddleOCR available"
    return None


def _read_ocr_quality_artifact_payload(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _latest_ocr_quality_artifact(artifact_dir: Path, ocr_runtime_details: dict[str, Any] | None = None) -> Path | None:
    if not artifact_dir.exists():
        return None
    candidates = [path for path in artifact_dir.glob(OCR_QUALITY_ARTIFACT_GLOB) if path.is_file()]
    if not candidates:
        return None
    latest_candidates = sorted(candidates, key=lambda path: (path.stat().st_mtime, path.name), reverse=True)
    for candidate in latest_candidates:
        payload = _read_ocr_quality_artifact_payload(candidate)
        if payload is None:
            return candidate
        if _ocr_quality_artifact_runtime_issue(payload, ocr_runtime_details) is None:
            return candidate
    return latest_candidates[0]


def _ocr_quality_artifact_check(
    artifact_path: Path | None, ocr_runtime_details: dict[str, Any] | None = None
) -> dict[str, Any]:
    if artifact_path is None:
        return {
            "key": "ocr_quality_recommendation",
            "label": "OCR quality recommendation",
            "status": "degraded",
            "path": None,
            "message": (
                "no OCR quality audit artifact found; run engineering_config_ocr_quality_audit.py with real PDF/image"
                " samples"
            ),
            "details": {
                "artifactFound": False,
                "nextActions": [
                    "Run engineering_config_ocr_quality_audit.py against real PDF/image configuration samples.",
                    (
                        "Include at least two OCR engines by installing tesseract or setting JATO_CONFIG_OCR_COMMAND"
                        " when comparing PaddleOCR with a previous OCR path."
                    ),
                ],
            },
        }
    if not artifact_path.exists():
        return {
            "key": "ocr_quality_recommendation",
            "label": "OCR quality recommendation",
            "status": "degraded",
            "path": _display_path(artifact_path),
            "message": "OCR quality audit artifact is missing; run a fresh real-sample OCR audit",
            "details": {
                "artifactFound": False,
                "artifactPath": _display_path(artifact_path),
                "nextActions": [
                    "Run engineering_config_ocr_quality_audit.py and write a fresh JSON artifact.",
                    (
                        "Configure a second OCR engine before the audit if the goal is to compare PaddleOCR with a"
                        " previous PDF/image OCR path."
                    ),
                ],
            },
        }
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "key": "ocr_quality_recommendation",
            "label": "OCR quality recommendation",
            "status": "failed",
            "path": _display_path(artifact_path),
            "message": f"OCR quality audit artifact could not be read: {exc}",
            "details": {"artifactFound": True, "artifactPath": _display_path(artifact_path)},
        }
    if not isinstance(payload, dict):
        return {
            "key": "ocr_quality_recommendation",
            "label": "OCR quality recommendation",
            "status": "failed",
            "path": _display_path(artifact_path),
            "message": "OCR quality audit artifact did not contain a JSON object",
            "details": {"artifactFound": True, "artifactPath": _display_path(artifact_path)},
        }
    runtime_issue = _ocr_quality_artifact_runtime_issue(payload, ocr_runtime_details)
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    if runtime_issue:
        return {
            "key": "ocr_quality_recommendation",
            "label": "OCR quality recommendation",
            "status": "degraded",
            "path": _display_path(artifact_path),
            "message": f"OCR quality audit artifact runtime does not match backend OCR readiness: {runtime_issue}",
            "details": {
                "artifactFound": True,
                "artifactPath": _display_path(artifact_path),
                "schemaVersion": payload.get("schemaVersion"),
                "generatedAtUtc": payload.get("generatedAtUtc"),
                "runtimeIssue": runtime_issue,
                "artifactRuntime": runtime,
                "backendOcrRuntime": ocr_runtime_details or {},
                "nextActions": [
                    (
                        "Re-run engineering_config_ocr_quality_audit.py with the same Python environment used by the"
                        " backend."
                    ),
                    (
                        "Prefer the project .venv/bin/python locally so PaddleOCR/PaddlePaddle availability matches"
                        " /engineering-config/ocr/readiness."
                    ),
                ],
            },
        }
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    recommendation = (
        summary.get("engineRecommendation") if isinstance(summary.get("engineRecommendation"), dict) else {}
    )
    decision = str(recommendation.get("decision") or "missing_recommendation")
    recommended_engine = recommendation.get("recommendedEngine")
    confidence = str(recommendation.get("confidence") or "unknown")
    candidate_engine_count = _safe_int(recommendation.get("candidateEngineCount"))
    comparable_candidate_count = _safe_int(recommendation.get("comparableCandidateCount"))
    ready_file_count = _safe_int(summary.get("readyComparableFileCount"))
    file_count = _safe_int(summary.get("fileCount"))
    next_actions: list[str]
    if decision == "use_recommended_engine" and recommended_engine:
        status = "passed"
        message = (
            f"OCR quality audit recommends {recommended_engine} with {confidence} confidence "
            f"from {candidate_engine_count} engines and {comparable_candidate_count} comparable candidates"
        )
        next_actions = [
            (
                f"Use {recommended_engine} as the preferred OCR engine for config-table digest while keeping per-file"
                " OCR evidence visible."
            ),
            "Re-run the OCR quality audit when new scanned PDF/image source formats are added.",
        ]
    elif decision == "single_engine_candidate" and recommended_engine:
        status = "degraded"
        message = (
            f"OCR quality audit recommends {recommended_engine} only as a single-engine candidate "
            f"({confidence}); legacy/custom OCR comparison proof is still missing"
        )
        next_actions = [
            "Install tesseract or set JATO_CONFIG_OCR_COMMAND to the previous PDF/image OCR command.",
            (
                "Re-run engineering_config_ocr_quality_audit.py on the same samples and require candidateEngineCount >="
                " 2 before calling PaddleOCR the winner."
            ),
            f"Keep {recommended_engine} as the current runtime fallback, not as a final quality winner.",
        ]
    elif decision == "tie_needs_more_samples":
        status = "degraded"
        message = (
            "OCR quality audit found tied engines; collect more scanned PDF/image samples before choosing a runtime"
            " engine"
        )
        next_actions = [
            "Add more real scanned PDF/image config samples from different source formats.",
            (
                "Re-run the OCR quality audit and choose an engine only after one candidate leads on comparable tables"
                " and non-empty config cells."
            ),
        ]
    elif decision == "insufficient_evidence":
        status = "degraded"
        message = (
            "OCR quality audit has insufficient comparable OCR evidence; run real scanned PDF/image samples before"
            " choosing a runtime engine"
        )
        next_actions = [
            "Use real PDF/image files that contain horizontal config matrices with at least two trims/options.",
            "Verify the audit reports readyComparableFileCount > 0 and comparableCandidateCount > 0.",
        ]
    else:
        status = "degraded"
        message = "OCR quality audit artifact does not contain a decisive engine recommendation"
        next_actions = [
            "Inspect the OCR quality artifact for missing engineRecommendation fields.",
            "Re-run engineering_config_ocr_quality_audit.py after confirming OCR dependencies and sample paths.",
        ]
    return {
        "key": "ocr_quality_recommendation",
        "label": "OCR quality recommendation",
        "status": status,
        "path": _display_path(artifact_path),
        "message": message,
        "details": {
            "artifactFound": True,
            "artifactPath": _display_path(artifact_path),
            "schemaVersion": payload.get("schemaVersion"),
            "generatedAtUtc": payload.get("generatedAtUtc"),
            "fileCount": file_count,
            "readyComparableFileCount": ready_file_count,
            "pendingOrFailedFileCount": _safe_int(summary.get("pendingOrFailedFileCount")),
            "decision": decision,
            "recommendedEngine": recommended_engine,
            "confidence": confidence,
            "reason": recommendation.get("reason"),
            "candidateEngineCount": candidate_engine_count,
            "candidateCount": _safe_int(recommendation.get("candidateCount")),
            "comparableCandidateCount": comparable_candidate_count,
            "runnerUpEngine": recommendation.get("runnerUpEngine"),
            "engineMetrics": (
                recommendation.get("engineMetrics") if isinstance(recommendation.get("engineMetrics"), list) else []
            ),
            "statusCounts": summary.get("statusCounts") if isinstance(summary.get("statusCounts"), dict) else {},
            "ocrComparisonStatusCounts": (
                summary.get("ocrComparisonStatusCounts")
                if isinstance(summary.get("ocrComparisonStatusCounts"), dict)
                else {}
            ),
            "nextActions": next_actions,
        },
    }


def _ai_evaluator(payload: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    ready = bool(payload.get("ready"))
    pipeline = str(payload.get("pipeline") or "")
    persisted = bool(payload.get("persisted"))
    provider = payload.get("provider")
    model = payload.get("model")
    if ready and pipeline == "compare_runtime_compose" and not persisted:
        result = "passed"
        message = f"runtime LLM summary ready via {provider}/{model}"
    elif ready:
        result = "degraded"
        message = "LLM provider is ready but the runtime/persisted boundary should be reviewed"
    else:
        result = "degraded"
        message = "LLM summary provider missing; compare table and evidence remain usable"
    return (
        result,
        message,
        {
            "ready": ready,
            "status": payload.get("status"),
            "provider": provider,
            "model": model,
            "pipeline": pipeline,
            "persisted": persisted,
            "cacheSize": payload.get("cacheSize"),
            "cacheLimit": payload.get("cacheLimit"),
        },
    )


def _list_evaluator(name: str):
    def evaluate(payload: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
        items = payload.get("items")
        if isinstance(items, list):
            count = len(items)
        else:
            count_value = payload.get("total") or payload.get("count") or payload.get("totalCount")
            count = int(count_value) if isinstance(count_value, int) else 0
        return (
            "passed",
            f"{name} list endpoint is reachable",
            {
                "returnedItems": count,
                "payloadKeys": sorted(payload.keys())[:12],
            },
        )

    return evaluate


def _request_result(
    client: Any,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        if method == "GET":
            response_payload = client.get_json(path)
        elif method == "PATCH":
            response_payload = client.patch_json(path, payload or {})
        elif method == "DELETE":
            response_payload = client.delete_json(path)
        elif method == "POST":
            response_payload = client.post_json(path, payload)
        else:
            raise ValueError(f"Unsupported method: {method}")
    except ReadinessHttpError as exc:
        return {
            "ok": False,
            "status": exc.status,
            "message": exc.message,
            "path": path,
            "method": method,
        }
    return {
        "ok": True,
        "status": 200,
        "payload": response_payload,
        "path": path,
        "method": method,
    }


def _resolved_role(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return ""
    role = payload.get("role")
    if role is None and isinstance(payload.get("user"), dict):
        role = payload["user"].get("role")
    return str(role or "").strip().lower()


def _forbidden(status: Any) -> bool:
    try:
        status_int = int(status)
    except (TypeError, ValueError):
        return False
    return status_int in {401, 403}


def _handler_reached(status: Any) -> bool:
    try:
        status_int = int(status)
    except (TypeError, ValueError):
        return False
    return status_int > 0 and status_int not in {401, 403}


def _auth_role_detail(
    client: Any,
    *,
    role_key: str,
    accepted_roles: set[str],
    compare_trim_ids: list[str],
) -> dict[str, Any]:
    me = _request_result(client, "GET", "/auth/me")
    resolved_role = _resolved_role(me.get("payload") if isinstance(me.get("payload"), dict) else None)
    source_read = _request_result(client, "GET", "/engineering-config/source/snapshots?limit=1")
    trim_read = _request_result(client, "GET", "/engineering-config/trims?limit=1")
    detail: dict[str, Any] = {
        "role": role_key,
        "resolvedRole": resolved_role,
        "meStatus": me.get("status"),
        "meOk": bool(me.get("ok")),
        "roleOk": bool(me.get("ok")) and resolved_role in accepted_roles,
        "sourceReadStatus": source_read.get("status"),
        "sourceReadOk": bool(source_read.get("ok")),
        "trimReadStatus": trim_read.get("status"),
        "trimReadOk": bool(trim_read.get("ok")),
    }
    if len(compare_trim_ids) >= 2:
        query = urllib.parse.urlencode({"trim_ids": ",".join(compare_trim_ids[:4])})
        compare = _request_result(client, "GET", f"/engineering-config/compare?{query}")
        detail.update(
            {
                "compareReadStatus": compare.get("status"),
                "compareReadOk": bool(compare.get("ok")),
            }
        )
    return detail


def _auth_writer_detail(client: Any, *, role_key: str) -> dict[str, Any]:
    fake_id = "00000000-0000-0000-0000-000000000000"
    source_delete = _request_result(client, "DELETE", f"/engineering-config/source/snapshots/{fake_id}")
    trim_patch = _request_result(client, "PATCH", f"/engineering-config/trims/{fake_id}", {"status": "trashed"})
    value_patch = _request_result(
        client,
        "PATCH",
        f"/engineering-config/values/{fake_id}",
        {
            "raw_value": "●",
            "expected_version": 1,
            "updated_by": f"{role_key}-auth-contract-smoke",
            "comment": "auth contract smoke fake-id write reachability",
        },
    )
    return {
        "role": role_key,
        "sourceDeleteStatus": source_delete.get("status"),
        "sourceDeleteHandlerReached": _handler_reached(source_delete.get("status")),
        "trimPatchStatus": trim_patch.get("status"),
        "trimPatchHandlerReached": _handler_reached(trim_patch.get("status")),
        "valuePatchStatus": value_patch.get("status"),
        "valuePatchHandlerReached": _handler_reached(value_patch.get("status")),
    }


def _auth_viewer_write_denial_detail(client: Any) -> dict[str, Any]:
    fake_id = "00000000-0000-0000-0000-000000000000"
    source_delete = _request_result(client, "DELETE", f"/engineering-config/source/snapshots/{fake_id}")
    trim_patch = _request_result(client, "PATCH", f"/engineering-config/trims/{fake_id}", {"status": "trashed"})
    value_patch = _request_result(
        client,
        "PATCH",
        f"/engineering-config/values/{fake_id}",
        {
            "raw_value": "●",
            "expected_version": 1,
            "updated_by": "viewer-auth-contract-smoke",
            "comment": "auth contract smoke fake-id write denial",
        },
    )
    return {
        "sourceDeleteStatus": source_delete.get("status"),
        "sourceDeleteForbidden": _forbidden(source_delete.get("status")),
        "trimPatchStatus": trim_patch.get("status"),
        "trimPatchForbidden": _forbidden(trim_patch.get("status")),
        "valuePatchStatus": value_patch.get("status"),
        "valuePatchForbidden": _forbidden(value_patch.get("status")),
    }


def _client_for_auth_role(base_client: ApiClient, *, token: str, user_name: str) -> ApiClient:
    return ApiClient(
        base_client.api_base,
        token=token,
        user_name=user_name,
        timeout=getattr(base_client, "timeout", 10.0),
    )


def _base64url_json(payload: dict[str, Any]) -> str:
    encoded = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")).decode("ascii")
    return encoded.rstrip("=")


def _mint_local_auth_token(
    *,
    username: str,
    role: str,
    jwt_secret: str,
    ttl_seconds: int,
) -> str:
    now = int(time.time())
    header = _base64url_json({"alg": "HS256", "typ": "JWT"})
    body = _base64url_json(
        {
            "username": username,
            "role": role,
            "exp": now + ttl_seconds,
            "iat": now,
        }
    )
    signature = (
        base64.urlsafe_b64encode(
            hmac.new(jwt_secret.encode("utf-8"), f"{header}.{body}".encode("utf-8"), hashlib.sha256).digest()
        )
        .decode("ascii")
        .rstrip("=")
    )
    return f"{header}.{body}.{signature}"


def _auth_contract_check(
    base_client: ApiClient,
    *,
    viewer_token: str | None = None,
    editor_token: str | None = None,
    admin_token: str | None = None,
    viewer_user: str = "config-viewer-smoke",
    editor_user: str = "config-editor-smoke",
    admin_user: str = "config-admin-smoke",
    compare_trim_ids: list[str] | None = None,
    role_clients: dict[str, Any] | None = None,
) -> dict[str, Any]:
    compare_ids = [trim_id for trim_id in compare_trim_ids or [] if trim_id]
    role_clients = role_clients or {}
    details: dict[str, Any] = {
        "compareTrimIds": compare_ids,
        "roles": {},
        "writeBoundary": {},
        "skipped": [],
    }

    viewer_client = role_clients.get("viewer")
    if viewer_client is None and viewer_token:
        viewer_client = _client_for_auth_role(base_client, token=viewer_token, user_name=viewer_user)
    editor_client = role_clients.get("editor")
    if editor_client is None and editor_token:
        editor_client = _client_for_auth_role(base_client, token=editor_token, user_name=editor_user)
    admin_client = role_clients.get("admin")
    if admin_client is None and admin_token:
        admin_client = _client_for_auth_role(base_client, token=admin_token, user_name=admin_user)

    failures: list[str] = []
    if viewer_client is not None:
        viewer_detail = _auth_role_detail(
            viewer_client,
            role_key="viewer",
            accepted_roles={"viewer", "order_filler"},
            compare_trim_ids=compare_ids,
        )
        viewer_denial = _auth_viewer_write_denial_detail(viewer_client)
        details["roles"]["viewer"] = viewer_detail
        details["writeBoundary"]["viewer"] = viewer_denial
        if not viewer_detail["roleOk"]:
            failures.append("viewer token did not resolve to viewer/order_filler")
        if not viewer_detail["sourceReadOk"] or not viewer_detail["trimReadOk"]:
            failures.append("viewer could not read source/config-column libraries")
        if len(compare_ids) >= 2 and not viewer_detail.get("compareReadOk"):
            failures.append("viewer could not read compare payload")
        if (
            not viewer_denial["sourceDeleteForbidden"]
            or not viewer_denial["trimPatchForbidden"]
            or not viewer_denial["valuePatchForbidden"]
        ):
            failures.append("viewer write boundary was not enforced")
    else:
        details["skipped"].append("viewer token missing")

    writer_clients: list[tuple[str, Any, set[str]]] = []
    if editor_client is not None:
        writer_clients.append(("editor", editor_client, {"editor", "admin", "developer"}))
    else:
        details["skipped"].append("editor token missing")
    if admin_client is not None:
        writer_clients.append(("admin", admin_client, {"admin", "developer"}))
    for role_key, role_client, accepted_roles in writer_clients:
        role_detail = _auth_role_detail(
            role_client,
            role_key=role_key,
            accepted_roles=accepted_roles,
            compare_trim_ids=compare_ids,
        )
        writer_detail = _auth_writer_detail(role_client, role_key=role_key)
        details["roles"][role_key] = role_detail
        details["writeBoundary"][role_key] = writer_detail
        if not role_detail["roleOk"]:
            failures.append(f"{role_key} token did not resolve to an accepted writer role")
        if not role_detail["sourceReadOk"] or not role_detail["trimReadOk"]:
            failures.append(f"{role_key} could not read source/config-column libraries")
        if len(compare_ids) >= 2 and not role_detail.get("compareReadOk"):
            failures.append(f"{role_key} could not read compare payload")
        if (
            not writer_detail["sourceDeleteHandlerReached"]
            or not writer_detail["trimPatchHandlerReached"]
            or not writer_detail["valuePatchHandlerReached"]
        ):
            failures.append(f"{role_key} write handlers were blocked by auth")

    if failures:
        return {
            "key": "auth_contract",
            "label": "Auth contract smoke",
            "status": "failed",
            "path": "/auth/me + engineering-config role-gated endpoints",
            "message": "; ".join(failures[:3]),
            "details": {**details, "failures": failures},
        }
    if details["skipped"]:
        return {
            "key": "auth_contract",
            "label": "Auth contract smoke",
            "status": "degraded",
            "path": "/auth/me + engineering-config role-gated endpoints",
            "message": f"auth contract smoke skipped: {', '.join(details['skipped'])}",
            "details": details,
        }
    return {
        "key": "auth_contract",
        "label": "Auth contract smoke",
        "status": "passed",
        "path": "/auth/me + engineering-config role-gated endpoints",
        "message": "viewer read/write-denial and editor/admin write reachability passed",
        "details": details,
    }


def _trim_id(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    raw = value.get("trimId") or value.get("trim_id") or value.get("id")
    text = str(raw or "").strip()
    return text or None


def _extract_trim_ids(payload: dict[str, Any]) -> list[str]:
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    trim_ids: list[str] = []
    seen: set[str] = set()
    for item in items:
        trim_id = _trim_id(item)
        if not trim_id or trim_id in seen:
            continue
        seen.add(trim_id)
        trim_ids.append(trim_id)
    return trim_ids


def _normalise_compare_trim_ids(value: str | None) -> list[str]:
    if value is None:
        return []
    return [part.strip() for part in re.split(r"[\s,]+", value) if part.strip()]


def _config_column_list_path(
    *,
    limit: int,
    compare_trim_query: str | None = None,
    compare_trim_market: str | None = None,
) -> str:
    params: dict[str, str] = {"limit": str(limit)}
    query = str(compare_trim_query or "").strip()
    market = str(compare_trim_market or "").strip()
    if query:
        params["q"] = query
    if market:
        params["market"] = market
    return f"/engineering-config/trims?{urllib.parse.urlencode(params)}"


def _compare_evaluator(payload: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    trims = payload.get("trims") if isinstance(payload.get("trims"), list) else []
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    total_features = summary.get("totalFeatures") or payload.get("totalFeatures") or len(rows)
    shown_features = summary.get("shownFeatures") or payload.get("shownFeatures") or len(rows)
    try:
        total_count = int(total_features)
    except (TypeError, ValueError):
        total_count = len(rows)
    try:
        shown_count = int(shown_features)
    except (TypeError, ValueError):
        shown_count = len(rows)
    if len(trims) < 2:
        return (
            "failed",
            "compare endpoint returned fewer than 2 trims",
            {"trimCount": len(trims), "rowCount": len(rows), "totalFeatures": total_count},
        )
    if total_count <= 0 and len(rows) == 0:
        return (
            "degraded",
            "compare endpoint is reachable but returned no config rows",
            {"trimCount": len(trims), "rowCount": len(rows), "totalFeatures": total_count},
        )
    return (
        "passed",
        f"compare endpoint returned {total_count} config rows for {len(trims)} trims",
        {
            "trimCount": len(trims),
            "rowCount": len(rows),
            "totalFeatures": total_count,
            "shownFeatures": shown_count,
            "differenceCount": summary.get("differenceCount"),
        },
    )


def _trim_label(trim: dict[str, Any]) -> str:
    for key in ("trimName", "fullTrimName", "trimId"):
        value = str(trim.get(key) or "").strip()
        if value:
            return value
    return "config-column"


def _compare_export_payload(compare_payload: dict[str, Any], file_name: str) -> dict[str, Any]:
    trims = compare_payload.get("trims") if isinstance(compare_payload.get("trims"), list) else []
    rows = compare_payload.get("rows") if isinstance(compare_payload.get("rows"), list) else []
    summary = compare_payload.get("summary") if isinstance(compare_payload.get("summary"), dict) else {}
    base_label = _trim_label(trims[0]) if trims and isinstance(trims[0], dict) else ""
    target_label = _trim_label(trims[1]) if len(trims) > 1 and isinstance(trims[1], dict) else ""
    business_summary = _export_business_summary(compare_payload)
    return {
        "fileName": file_name,
        "scope": {
            "rangeLabel": "readiness smoke",
            "baseLabel": base_label,
            "targetLabel": target_label,
        },
        "summary": (
            summary
            or {
                "totalFeatures": compare_payload.get("totalFeatures") or len(rows),
                "shownFeatures": compare_payload.get("shownFeatures") or len(rows),
            }
        ),
        "trims": trims,
        "rows": rows,
        "businessSummary": business_summary,
        "businessSummaryUsage": (
            {
                "provider": "readiness-smoke",
                "model": "deterministic-fixture",
                "status": "ok",
                "source": "readiness_export_payload",
            }
            if business_summary
            else None
        ),
    }


def _display_value(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    for key in ("displayValue", "rawValue", "normalizedValue"):
        text = str(value.get(key) or "").strip()
        if text:
            return text
    return None


def _row_value(row: dict[str, Any], index: int) -> dict[str, Any]:
    values = row.get("values") if isinstance(row.get("values"), list) else []
    if index < len(values) and isinstance(values[index], dict):
        return values[index]
    return {}


def _source_has_evidence(source: dict[str, Any]) -> bool:
    return any(str(source.get(key) or "").strip() for key in ("sheetName", "cell", "sourceCell", "mergedRange"))


def _source_is_merged_expansion(source: dict[str, Any]) -> bool:
    cell = str(source.get("cell") or "").strip()
    source_cell = str(source.get("sourceCell") or "").strip()
    merged_range = str(source.get("mergedRange") or "").strip()
    if merged_range:
        return True
    return bool(cell and source_cell and cell != source_cell)


def _availability_is_unknown(value: Any) -> bool:
    if not isinstance(value, dict):
        return True
    availability = str(value.get("availability") or value.get("valueState") or "").upper()
    if availability in {"UNKNOWN", "MISSING", "UNSET"}:
        return True
    if availability:
        return False
    return not bool(_display_value(value))


def _summary_trim_fact(trim: dict[str, Any]) -> dict[str, Any]:
    return {
        "trimId": _trim_id(trim),
        "targetLabel": _trim_label(trim),
        "trimName": trim.get("trimName") or trim.get("fullTrimName"),
        "fullTrimName": trim.get("fullTrimName") or trim.get("trimName"),
        "brand": trim.get("brand"),
        "modelName": trim.get("modelName"),
        "market": trim.get("market") or trim.get("country"),
        "country": trim.get("country") or trim.get("market"),
        "modelYear": trim.get("modelYear"),
        "powertrain": trim.get("energyType") or trim.get("powertrain") or trim.get("engine"),
        "sourceFileName": trim.get("sourceFileName"),
        "dataOrigin": trim.get("dataOrigin"),
        "materialNo": trim.get("materialNo"),
        "salesVersion": trim.get("salesVersion"),
    }


def _business_summary_evidence_fact(row: dict[str, Any], *, base_index: int, target_index: int) -> dict[str, Any]:
    base_value = _row_value(row, base_index)
    target_value = _row_value(row, target_index)
    feature_code = str(row.get("featureCode") or row.get("featureId") or row.get("featureName") or "feature").strip()
    business_note = str(row.get("businessNote") or "").strip()
    source = target_value.get("source") if isinstance(target_value.get("source"), dict) else {}
    requires_review = any(token in business_note for token in ("需核对", "待确认", "缺失", "缺少", "回看", "OCR"))
    return {
        "evidenceKey": f"{feature_code}:target-{target_index}",
        "featureCode": feature_code,
        "featureName": row.get("featureName") or feature_code,
        "category": row.get("category") or "未分类",
        "comparisonType": row.get("comparisonType"),
        "baseValue": _display_value(base_value),
        "targetValue": _display_value(target_value),
        "baseAvailability": base_value.get("availability"),
        "targetAvailability": target_value.get("availability"),
        "baseValueState": base_value.get("valueState"),
        "targetValueState": target_value.get("valueState"),
        "inferred": bool(base_value.get("inferred")) or bool(target_value.get("inferred")),
        "inferenceReason": target_value.get("inferenceReason") or base_value.get("inferenceReason"),
        "businessNote": business_note,
        "requiresReview": requires_review or bool(target_value.get("inferred")) or not _source_has_evidence(source),
        "source": {
            "sheetName": source.get("sheetName"),
            "cell": source.get("cell"),
            "sourceCell": source.get("sourceCell"),
            "mergedRange": source.get("mergedRange"),
        },
    }


def _source_evidence_summary(evidence_facts: list[dict[str, Any]]) -> dict[str, Any]:
    difference_count = len(evidence_facts)
    with_source_count = 0
    inferred_count = 0
    unknown_count = 0
    merged_count = 0
    sheet_names: set[str] = set()
    for fact in evidence_facts:
        source = fact.get("source") if isinstance(fact.get("source"), dict) else {}
        if _source_has_evidence(source):
            with_source_count += 1
        sheet_name = str(source.get("sheetName") or "").strip()
        if sheet_name:
            sheet_names.add(sheet_name)
        if bool(fact.get("inferred")):
            inferred_count += 1
        target_probe = {
            "availability": fact.get("targetAvailability"),
            "valueState": fact.get("targetValueState"),
            "displayValue": fact.get("targetValue"),
        }
        if _availability_is_unknown(target_probe):
            unknown_count += 1
        if _source_is_merged_expansion(source):
            merged_count += 1
    missing_count = max(0, difference_count - with_source_count)
    return {
        "differenceCount": difference_count,
        "withSourceEvidenceCount": with_source_count,
        "missingSourceEvidenceCount": missing_count,
        "inferredCount": inferred_count,
        "unknownCount": unknown_count,
        "mergedCellExpandedCount": merged_count,
        "sourceSheetNames": sorted(sheet_names),
        "sourceEvidencePolicy": (
            "source.sheetName/cell/sourceCell/mergedRange proves the row can be traced; "
            "inferred or source-missing facts must be reviewed in Source Evidence before external use."
        ),
    }


def _export_business_summary(compare_payload: dict[str, Any]) -> list[dict[str, Any]]:
    compose_payload = _business_summary_compose_payload(compare_payload)
    base_label = _trim_label(
        compose_payload.get("baseTrim") if isinstance(compose_payload.get("baseTrim"), dict) else {}
    )
    summaries: list[dict[str, Any]] = []
    targets = compose_payload.get("targets") if isinstance(compose_payload.get("targets"), list) else []
    for target in targets:
        if not isinstance(target, dict):
            continue
        target_label = str(target.get("targetLabel") or "目标配置列")
        source_summary = (
            target.get("sourceEvidenceSummary") if isinstance(target.get("sourceEvidenceSummary"), dict) else {}
        )
        evidence_facts = target.get("evidenceFacts") if isinstance(target.get("evidenceFacts"), list) else []
        main_upgrades = [
            f"{str(fact.get('category') or '配置')}："
            f"{str(fact.get('featureName') or fact.get('featureCode') or '配置项')} "
            f"{str(fact.get('baseValue') or '空')} -> {str(fact.get('targetValue') or '空')}"
            for fact in evidence_facts[:4]
            if isinstance(fact, dict)
        ]
        evidence_refs = [
            {
                "featureCode": fact.get("featureCode"),
                "featureName": fact.get("featureName"),
                "source": fact.get("source"),
            }
            for fact in evidence_facts[:4]
            if isinstance(fact, dict)
            and _source_has_evidence(fact.get("source") if isinstance(fact.get("source"), dict) else {})
        ]
        summaries.append(
            {
                "targetTrimId": target.get("targetTrimId"),
                "targetLabel": target_label,
                "headline": f"{target_label} 相比 {base_label} 的配置差异 readiness smoke 摘要。",
                "mainUpgrades": main_upgrades or ["暂无可归纳升级项，需查看完整配置表。"],
                "replacementsOrReductions": ["以配置表和 evidence drawer 为准。"],
                "evidenceStatus": [
                    (
                        f"{source_summary.get('inferredCount', 0)} 项规则推断，"
                        f"{source_summary.get('missingSourceEvidenceCount', 0)} 项缺少 source evidence，"
                        f"{source_summary.get('mergedCellExpandedCount', 0)} 项来自合并单元格展开。"
                    ),
                    "引用到卖点前请点开 source evidence 核对。",
                ],
                "recommendedUse": "readiness smoke export fixture，用于验证导出能携带 AI 摘要结构。",
                "evidenceRefs": evidence_refs,
            }
        )
    return summaries


def _business_summary_compose_payload(compare_payload: dict[str, Any]) -> dict[str, Any]:
    trims = [
        trim
        for trim in (compare_payload.get("trims") if isinstance(compare_payload.get("trims"), list) else [])
        if isinstance(trim, dict)
    ]
    rows = [
        row
        for row in (compare_payload.get("rows") if isinstance(compare_payload.get("rows"), list) else [])
        if isinstance(row, dict)
    ]
    summary = compare_payload.get("summary") if isinstance(compare_payload.get("summary"), dict) else {}
    base_trim = trims[0] if trims else {}
    difference_rows = [
        row for row in rows if str(row.get("comparisonType") or "").upper() not in {"SAME", "COMMON", "COMMON_SAME"}
    ]
    if not difference_rows:
        difference_rows = rows[:12]
    category_counts: dict[str, int] = {}
    for row in difference_rows:
        category = str(row.get("category") or "未分类")
        category_counts[category] = category_counts.get(category, 0) + 1
    category_facts = [
        {
            "category": category,
            "totalDifferenceCount": count,
            "changeSummary": f"差异 {count}",
            "sampleFeatures": " / ".join(
                str(row.get("featureName") or row.get("featureCode") or "")
                for row in difference_rows
                if str(row.get("category") or "未分类") == category
            )[:240],
        }
        for category, count in sorted(category_counts.items(), key=lambda item: item[1], reverse=True)[:8]
    ]
    targets = []
    for target_index, target_trim in enumerate(trims[1:4], start=1):
        evidence_facts = [
            _business_summary_evidence_fact(row, base_index=0, target_index=target_index)
            for row in difference_rows[:24]
        ]
        targets.append(
            {
                "targetTrimId": _trim_id(target_trim),
                "targetLabel": _trim_label(target_trim),
                "targetTrim": _summary_trim_fact(target_trim),
                "differenceCounts": {
                    "totalDifference": (
                        summary.get("differenceCount")
                        or summary.get("confirmedDifferenceCount")
                        or len(difference_rows)
                    ),
                    "inferred": summary.get("inferredDifferenceCount") or 0,
                    "unknown": summary.get("missingOrUnknownCount") or 0,
                    "missingSourceEvidence": _source_evidence_summary(evidence_facts).get("missingSourceEvidenceCount"),
                },
                "sourceEvidenceSummary": _source_evidence_summary(evidence_facts),
                "evidenceFacts": evidence_facts,
                "addedFeatures": [
                    str(row.get("featureName") or row.get("featureCode") or "") for row in difference_rows[:8]
                ],
                "removedFeatures": [],
                "changedFeatures": [
                    {
                        "feature": row.get("featureName") or row.get("featureCode"),
                        "baseValue": _display_value(_row_value(row, 0)),
                        "targetValue": _display_value(_row_value(row, target_index)),
                    }
                    for row in difference_rows[:8]
                ],
                "businessFocusGroups": [
                    {
                        "label": fact["category"],
                        "count": fact["totalDifferenceCount"],
                        "evidence": "readiness smoke compare facts",
                        "sampleFeatures": fact["sampleFeatures"],
                    }
                    for fact in category_facts[:5]
                ],
                "categoryFacts": category_facts,
                "evidence": {
                    "inferredCount": summary.get("inferredDifferenceCount") or 0,
                    "unknownCount": summary.get("missingOrUnknownCount") or 0,
                    "missingSourceEvidenceCount": (
                        _source_evidence_summary(evidence_facts).get("missingSourceEvidenceCount")
                    ),
                    "sourceSheetNames": _source_evidence_summary(evidence_facts).get("sourceSheetNames"),
                    "warning": "引用到卖点前仍需点开 source evidence 核对。",
                },
            }
        )
    source_evidence_summaries = [
        target.get("sourceEvidenceSummary")
        for target in targets
        if isinstance(target.get("sourceEvidenceSummary"), dict)
    ]
    return {
        "baseTrim": _summary_trim_fact(base_trim),
        "targets": targets,
        "context": {
            "deltaFilter": "ALL",
            "compareScope": {
                "marketScope": "readiness_smoke",
                "sourceScope": "runtime_compare_payload",
                "modelYearScope": "readiness_smoke",
                "identityScope": "direct_config_column_compare",
                "inferredCount": summary.get("inferredDifferenceCount") or 0,
                "missingSourceEvidenceCount": sum(
                    _safe_int(item.get("missingSourceEvidenceCount")) for item in source_evidence_summaries
                ),
                "mergedCellExpandedCount": sum(
                    _safe_int(item.get("mergedCellExpandedCount")) for item in source_evidence_summaries
                ),
            },
            "instruction": (
                "Use the LLM to write concise Chinese business summaries from these Product Config Compare facts. "
                "Do not invent features; mention evidence boundaries."
            ),
        },
    }


def _target_identity(value: dict[str, Any]) -> str:
    return str(value.get("targetTrimId") or value.get("trimId") or value.get("targetLabel") or "").strip()


def _summary_for_target(summaries: list[dict[str, Any]], target: dict[str, Any], index: int) -> dict[str, Any]:
    target_trim_id = str(target.get("targetTrimId") or "").strip()
    target_label = str(target.get("targetLabel") or "").strip()
    for summary in summaries:
        if target_trim_id and str(summary.get("targetTrimId") or "").strip() == target_trim_id:
            return summary
    for summary in summaries:
        if target_label and str(summary.get("targetLabel") or "").strip() == target_label:
            return summary
    if index < len(summaries):
        return summaries[index]
    return {}


def _summary_evidence_status_text(summary: dict[str, Any]) -> str:
    evidence_status = summary.get("evidenceStatus")
    if not isinstance(evidence_status, list):
        return ""
    return " ".join(str(item or "") for item in evidence_status).lower()


def _evidence_text_covers(kind: str, text: str) -> bool:
    if kind == "review":
        return any(token in text for token in ("需核对", "待核对", "待确认", "人工核对", "review", "ocr"))
    if kind == "inferred":
        return any(token in text for token in ("规则推断", "不是 excel 原文", "inferred", "not excel", "不配备*"))
    if kind == "missing_source":
        return any(token in text for token in ("缺 source", "缺少 source", "缺来源", "缺少来源", "missing source"))
    if kind == "unknown":
        return any(token in text for token in ("待确认", "unknown", "需补来源"))
    if kind == "merged":
        return any(token in text for token in ("合并格", "合并单元格", "merged"))
    if kind == "multi_source":
        return any(
            token in text
            for token in ("多个 sheet", "多 sheet", "多个 source", "多来源", "multi_source", "multi source")
        )
    return False


def _target_requires_review_notice(target: dict[str, Any]) -> bool:
    evidence_facts = target.get("evidenceFacts")
    if not isinstance(evidence_facts, list):
        return False
    for fact in evidence_facts:
        if not isinstance(fact, dict):
            continue
        if fact.get("requiresReview") is True:
            return True
        note = str(fact.get("businessNote") or "")
        if any(token in note for token in ("需核对", "待核对", "待确认", "缺失", "缺少", "回看", "人工核对", "OCR")):
            return True
    return False


def _required_evidence_boundary_kinds(target: dict[str, Any]) -> list[str]:
    source_summary = (
        target.get("sourceEvidenceSummary") if isinstance(target.get("sourceEvidenceSummary"), dict) else {}
    )
    kinds: list[str] = []
    if _target_requires_review_notice(target):
        kinds.append("review")
    if _safe_int(source_summary.get("inferredCount")) > 0:
        kinds.append("inferred")
    if _safe_int(source_summary.get("missingSourceEvidenceCount")) > 0:
        kinds.append("missing_source")
    if _safe_int(source_summary.get("unknownCount")) > 0:
        kinds.append("unknown")
    if _safe_int(source_summary.get("mergedCellExpandedCount")) > 0:
        kinds.append("merged")
    source_sheet_names = source_summary.get("sourceSheetNames")
    if (
        isinstance(source_sheet_names, list)
        and len([name for name in source_sheet_names if str(name or "").strip()]) > 1
    ):
        kinds.append("multi_source")
    return kinds


def _ai_summary_evidence_boundary_coverage(
    targets: list[dict[str, Any]], summaries: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    coverage: list[dict[str, Any]] = []
    for index, target in enumerate(targets):
        required_kinds = _required_evidence_boundary_kinds(target)
        summary = _summary_for_target(summaries, target, index)
        status_text = _summary_evidence_status_text(summary)
        missing_kinds = [kind for kind in required_kinds if not _evidence_text_covers(kind, status_text)]
        coverage.append(
            {
                "target": _target_identity(target),
                "requiredKinds": required_kinds,
                "missingKinds": missing_kinds,
                "satisfied": not missing_kinds,
            }
        )
    return coverage


def _client_with_timeout(client: Any, timeout: float) -> Any:
    if type(client) is not ApiClient:
        return client
    if client.timeout == timeout:
        return client
    return ApiClient(client.api_base, token=client.token, user_name=client.user_name, timeout=timeout)


def _ai_summary_smoke_check(
    client: ApiClient,
    compare_payload: dict[str, Any],
    *,
    enabled: bool,
    timeout: float | None = None,
) -> list[dict[str, Any]]:
    if not enabled:
        return []
    payload = _business_summary_compose_payload(compare_payload)
    target_count = len(payload.get("targets") if isinstance(payload.get("targets"), list) else [])
    if target_count <= 0:
        return [
            {
                "key": "ai_summary_compose",
                "label": "Runtime AI summary compose smoke",
                "status": "degraded",
                "path": "/engineering-config/business-summary/compose",
                "message": "AI summary compose smoke skipped because compare payload has no target config columns",
            }
        ]
    try:
        summary_client = _client_with_timeout(client, timeout) if timeout is not None else client
        response = summary_client.post_json("/engineering-config/business-summary/compose", payload)
    except ReadinessHttpError as exc:
        return [
            {
                "key": "ai_summary_compose",
                "label": "Runtime AI summary compose smoke",
                "status": "failed",
                "path": "/engineering-config/business-summary/compose",
                "httpStatus": exc.status,
                "message": exc.message,
                "details": {"timeoutSeconds": timeout if timeout is not None else getattr(client, "timeout", None)},
            }
        ]
    usage = response.get("usage") if isinstance(response.get("usage"), dict) else {}
    summaries = [
        summary
        for summary in (response.get("summaries") if isinstance(response.get("summaries"), list) else [])
        if isinstance(summary, dict)
    ]
    headline_samples = [
        str(summary.get("headline") or "").strip()
        for summary in summaries
        if isinstance(summary, dict) and str(summary.get("headline") or "").strip()
    ][:3]
    evidence_status_samples: list[str] = []
    main_upgrade_samples: list[str] = []
    replacement_samples: list[str] = []
    summaries_with_evidence_status = 0
    for summary in summaries:
        if not isinstance(summary, dict):
            continue
        main_upgrades = summary.get("mainUpgrades")
        if isinstance(main_upgrades, list):
            main_upgrade_samples.extend(str(item) for item in main_upgrades[:2] if str(item).strip())
        replacements = summary.get("replacementsOrReductions")
        if isinstance(replacements, list):
            replacement_samples.extend(str(item) for item in replacements[:2] if str(item).strip())
        evidence_status = summary.get("evidenceStatus")
        if isinstance(evidence_status, list):
            if evidence_status:
                summaries_with_evidence_status += 1
            evidence_status_samples.extend(str(item) for item in evidence_status[:2] if str(item).strip())
    targets = [
        target
        for target in (payload.get("targets") if isinstance(payload.get("targets"), list) else [])
        if isinstance(target, dict)
    ]
    source_evidence_summaries = [
        target.get("sourceEvidenceSummary")
        for target in targets
        if isinstance(target.get("sourceEvidenceSummary"), dict)
    ]
    source_evidence_boundary_present = len(source_evidence_summaries) == target_count and target_count > 0
    evidence_boundary_coverage = _ai_summary_evidence_boundary_coverage(targets, summaries)
    required_evidence_boundary_satisfied = all(item["satisfied"] for item in evidence_boundary_coverage)
    usage_status = str(usage.get("status") or "unknown")
    details = {
        "targetCount": target_count,
        "summaryCount": len(summaries),
        "usageStatus": usage_status,
        "provider": usage.get("provider"),
        "model": usage.get("model"),
        "totalTokens": usage.get("totalTokens"),
        "cacheHit": usage.get("cacheHit"),
        "headlineSamples": headline_samples,
        "mainUpgradeSamples": main_upgrade_samples[:6],
        "replacementSamples": replacement_samples[:4],
        "evidenceStatusSamples": evidence_status_samples[:4],
        "sourceEvidenceBoundaryPresent": source_evidence_boundary_present,
        "requiredEvidenceBoundarySatisfied": required_evidence_boundary_satisfied,
        "evidenceBoundaryCoverage": evidence_boundary_coverage,
        "summariesWithEvidenceStatus": summaries_with_evidence_status,
        "sourceEvidenceSummarySamples": source_evidence_summaries[:3],
    }
    fallback_headline = any("AI 摘要暂未返回" in headline for headline in headline_samples)
    if usage_status in {"missing_key", "failed"}:
        return [
            {
                "key": "ai_summary_compose",
                "label": "Runtime AI summary compose smoke",
                "status": "degraded",
                "path": "/engineering-config/business-summary/compose",
                "message": (
                    f"runtime AI compose returned provider status {usage_status}; compare table and evidence remain"
                    " usable"
                ),
                "details": details,
            }
        ]
    if len(summaries) < target_count or not headline_samples or fallback_headline:
        return [
            {
                "key": "ai_summary_compose",
                "label": "Runtime AI summary compose smoke",
                "status": "degraded",
                "path": "/engineering-config/business-summary/compose",
                "message": (
                    "runtime AI compose responded but did not return a complete business summary for every target"
                ),
                "details": details,
            }
        ]
    if not source_evidence_boundary_present or summaries_with_evidence_status < target_count:
        return [
            {
                "key": "ai_summary_compose",
                "label": "Runtime AI summary compose smoke",
                "status": "degraded",
                "path": "/engineering-config/business-summary/compose",
                "message": (
                    "runtime AI compose responded, but source evidence boundary was not fully present in"
                    " request/response"
                ),
                "details": details,
            }
        ]
    if not required_evidence_boundary_satisfied:
        return [
            {
                "key": "ai_summary_compose",
                "label": "Runtime AI summary compose smoke",
                "status": "degraded",
                "path": "/engineering-config/business-summary/compose",
                "message": (
                    "runtime AI compose responded, but required evidence boundary warnings were missing from"
                    " evidenceStatus"
                ),
                "details": details,
            }
        ]
    return [
        {
            "key": "ai_summary_compose",
            "label": "Runtime AI summary compose smoke",
            "status": "passed",
            "path": "/engineering-config/business-summary/compose",
            "message": (
                f"runtime AI compose returned {len(summaries)} business summaries via"
                f" {usage.get('provider')}/{usage.get('model')}"
            ),
            "details": details,
        }
    ]


def _export_file_check(
    key: str,
    label: str,
    path: str,
    client: ApiClient,
    payload: dict[str, Any],
    expected_prefix: bytes,
    expected_content_type: str,
) -> dict[str, Any]:
    try:
        body, content_type = client.post_json_bytes(path, payload)
    except ReadinessHttpError as exc:
        return {
            "key": key,
            "label": label,
            "status": "failed",
            "path": path,
            "httpStatus": exc.status,
            "message": exc.message,
        }
    content_type_text = str(content_type or "")
    payload_summary = _export_payload_summary(payload)
    if len(body) == 0:
        return {
            "key": key,
            "label": label,
            "status": "failed",
            "path": path,
            "message": "export endpoint returned an empty file",
            "details": {"contentType": content_type_text, "bytes": 0, "payload": payload_summary},
        }
    if not body.startswith(expected_prefix):
        return {
            "key": key,
            "label": label,
            "status": "failed",
            "path": path,
            "message": "export endpoint returned an unexpected file signature",
            "details": {"contentType": content_type_text, "bytes": len(body), "payload": payload_summary},
        }
    if expected_content_type not in content_type_text:
        return {
            "key": key,
            "label": label,
            "status": "degraded",
            "path": path,
            "message": "export file signature is valid but content type is unexpected",
            "details": {"contentType": content_type_text, "bytes": len(body), "payload": payload_summary},
        }
    if not payload_summary["hasBusinessSummaryUsage"] or payload_summary["businessSummaryCount"] <= 0:
        return {
            "key": key,
            "label": label,
            "status": "degraded",
            "path": path,
            "message": "export file is valid, but smoke payload did not include AI business summary content",
            "details": {"contentType": content_type_text, "bytes": len(body), "payload": payload_summary},
        }
    return {
        "key": key,
        "label": label,
        "status": "passed",
        "path": path,
        "message": f"export endpoint returned a valid {label.split()[-1]} file with business summary payload",
        "details": {"contentType": content_type_text, "bytes": len(body), "payload": payload_summary},
    }


def _export_payload_summary(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    trims = payload.get("trims") if isinstance(payload.get("trims"), list) else []
    business_summary = payload.get("businessSummary") if isinstance(payload.get("businessSummary"), list) else []
    summaries = [item for item in business_summary if isinstance(item, dict)]
    return {
        "rowCount": len(rows),
        "trimCount": len(trims),
        "businessSummaryCount": len(summaries),
        "hasBusinessSummaryUsage": isinstance(payload.get("businessSummaryUsage"), dict),
        "allSummaryItemsHaveEvidenceStatus": (
            bool(summaries)
            and all(
                isinstance(item.get("evidenceStatus"), list) and bool(item.get("evidenceStatus")) for item in summaries
            )
        ),
        "allSummaryItemsHaveRecommendedUse": (
            bool(summaries) and all(bool(str(item.get("recommendedUse") or "").strip()) for item in summaries)
        ),
        "hasEvidenceRefs": any(
            isinstance(item.get("evidenceRefs"), list) and bool(item.get("evidenceRefs")) for item in summaries
        ),
    }


def _export_smoke_checks(client: ApiClient, compare_payload: dict[str, Any]) -> list[dict[str, Any]]:
    xlsx_payload = _compare_export_payload(compare_payload, "config-compare-readiness-smoke.xlsx")
    pdf_payload = _compare_export_payload(compare_payload, "config-compare-readiness-smoke.pdf")
    return [
        _export_file_check(
            "export_xlsx",
            "XLSX export smoke",
            "/engineering-config/compare/export/xlsx",
            client,
            xlsx_payload,
            b"PK",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        _export_file_check(
            "export_pdf",
            "PDF export smoke",
            "/engineering-config/compare/export/pdf",
            client,
            pdf_payload,
            b"%PDF",
            "application/pdf",
        ),
    ]


def _competitor_recommendation_path(scope: dict[str, Any]) -> str:
    params: dict[str, str] = {
        "country": str(scope.get("country") or "").strip(),
        "model_name": str(scope.get("model") or scope.get("modelName") or "").strip(),
    }
    powertrain = str(scope.get("powertrain") or "").strip()
    segment = str(scope.get("segment") or "").strip()
    limit = scope.get("limit")
    if powertrain:
        params["powertrain"] = powertrain
    if segment:
        params["segment"] = segment
    if isinstance(limit, int) and limit > 0:
        params["limit"] = str(min(limit, 10))
    return f"/engineering-config/recommendations/competitors?{urllib.parse.urlencode(params)}"


def _competitor_recommendation_check(client: ApiClient, scope: dict[str, Any] | None) -> list[dict[str, Any]]:
    if scope is None:
        return []
    country = str(scope.get("country") or "").strip()
    model = str(scope.get("model") or scope.get("modelName") or "").strip()
    if not country or not model:
        return [
            {
                "key": "competitor_recommendations",
                "label": "Competitor recommendation smoke",
                "status": "degraded",
                "path": "/engineering-config/recommendations/competitors",
                "message": "competitor recommendation smoke skipped because country and model are required",
                "details": {"countryProvided": bool(country), "modelProvided": bool(model)},
            }
        ]
    path = _competitor_recommendation_path(scope)
    try:
        payload = client.get_json(path)
    except ReadinessHttpError as exc:
        return [
            {
                "key": "competitor_recommendations",
                "label": "Competitor recommendation smoke",
                "status": "failed",
                "path": path,
                "httpStatus": exc.status,
                "message": exc.message,
            }
        ]
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    rows = payload.get("rows")
    try:
        row_count = int(rows)
    except (TypeError, ValueError):
        row_count = len(items)
    config_ready_count = sum(1 for item in items if isinstance(item, dict) and bool(item.get("configAvailable")))
    library_with_source_evidence_count = sum(
        1
        for item in items
        if isinstance(item, dict) and bool(item.get("configAvailable")) and bool(item.get("sourceDigestAvailable"))
    )
    source_digest_coverage_count = sum(
        1 for item in items if isinstance(item, dict) and bool(item.get("sourceDigestAvailable"))
    )
    source_digest_ready_count = sum(
        1
        for item in items
        if isinstance(item, dict) and not bool(item.get("configAvailable")) and bool(item.get("sourceDigestAvailable"))
    )
    upload_needed_count = sum(
        1 for item in items if isinstance(item, dict) and item.get("nextAction") == "upload_source"
    )
    source_digest_source_count = sum(
        _safe_int(item.get("sourceDigestSourceCount")) for item in items if isinstance(item, dict)
    )
    source_digest_group_count = sum(
        _safe_int(item.get("sourceDigestGroupCount")) for item in items if isinstance(item, dict)
    )
    source_digest_trim_count = sum(
        _safe_int(item.get("sourceDigestTrimCount")) for item in items if isinstance(item, dict)
    )
    next_actions: dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        action = str(item.get("nextAction") or "unknown")
        next_actions[action] = next_actions.get(action, 0) + 1
    message = str(payload.get("message") or "")
    details = {
        "country": payload.get("country") or country,
        "modelName": payload.get("modelName") or model,
        "powertrain": payload.get("powertrain") or scope.get("powertrain"),
        "segment": payload.get("segment") or scope.get("segment"),
        "rows": row_count,
        "configReadyCount": config_ready_count,
        "libraryReadyWithSourceEvidenceCount": library_with_source_evidence_count,
        "sourceDigestCoverageCount": source_digest_coverage_count,
        "sourceDigestReadyCount": source_digest_ready_count,
        "sourceDigestSourceCount": source_digest_source_count,
        "sourceDigestGroupCount": source_digest_group_count,
        "sourceDigestTrimCount": source_digest_trim_count,
        "uploadNeededCount": upload_needed_count,
        "nextActions": next_actions,
        "message": message,
    }
    if row_count <= 0:
        return [
            {
                "key": "competitor_recommendations",
                "label": "Competitor recommendation smoke",
                "status": "degraded",
                "path": path,
                "message": message or "competitor recommendation endpoint returned no recommendations",
                "details": details,
            }
        ]
    return [
        {
            "key": "competitor_recommendations",
            "label": "Competitor recommendation smoke",
            "status": "passed",
            "path": path,
            "message": (
                f"competitor recommendation endpoint returned {row_count} recommendations "
                f"({config_ready_count} library-ready, "
                f"{library_with_source_evidence_count} library-ready-with-source-evidence, "
                f"{source_digest_ready_count} source-digest-ready, {upload_needed_count} upload-needed)"
            ),
            "details": details,
        }
    ]


def _recommendation_items(client: ApiClient, scope: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    path = _competitor_recommendation_path(scope)
    payload = client.get_json(path)
    raw_items = payload.get("items") if isinstance(payload.get("items"), list) else []
    items = [item for item in raw_items if isinstance(item, dict)]
    return path, items, payload


def _pick_upload_needed_recommendation(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    for item in items:
        if item.get("nextAction") == "upload_source":
            return item
    for item in items:
        if not bool(item.get("configAvailable")) and not bool(item.get("sourceDigestAvailable")):
            return item
    return None


def _find_recommendation_by_model(items: list[dict[str, Any]], model_name: str) -> dict[str, Any] | None:
    normalized_model = normalized_model_name = str(model_name or "").strip().lower()
    if not normalized_model_name:
        return None
    for item in items:
        if str(item.get("modelName") or "").strip().lower() == normalized_model:
            return item
    return None


def _recommendation_profile_text(recommendation: dict[str, Any], key: str) -> str | None:
    profile = recommendation.get("profile")
    if not isinstance(profile, dict):
        return None
    value = profile.get(key)
    text = str(value or "").strip()
    return text or None


def _csv_cell(value: Any) -> str:
    text = str(value or "")
    if any(char in text for char in [",", '"', "\n", "\r"]):
        return '"' + text.replace('"', '""') + '"'
    return text


def _safe_file_slug(value: str, fallback: str = "competitor") -> str:
    slug = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
    return slug or fallback


def _competitor_workflow_csv(
    *,
    brand: str,
    model_name: str,
    country: str,
    powertrain: str,
    segment: str,
    stamp: str,
) -> bytes:
    basic_trim = f"{model_name} Workflow Basic"
    premium_trim = f"{model_name} Workflow Premium"
    rows = [
        ["Feature", basic_trim, premium_trim],
        ["Brand / 品牌", brand, brand],
        ["Model / 车型", model_name, model_name],
        ["Country / 国家", country, country],
        ["Powertrain / 动力", powertrain, powertrain],
        ["Segment / 级别", segment, segment],
        ["Configuration version / 配置版型", "Workflow Basic", "Workflow Premium"],
        ["Material No. / 物料号", f"SMOKE-{stamp}-BASIC", f"SMOKE-{stamp}-PREM"],
        ["Rear Visual parking assist / 动态辅助线倒车影像", "-", "●"],
        ["360 round view camera / 360度高清全景影像", "-", "●"],
        ["Power sunroof / 电动天窗", "-", "O"],
        ["Wireless charging / 手机无线充电", "-", "●"],
    ]
    text = "\n".join(",".join(_csv_cell(cell) for cell in row) for row in rows) + "\n"
    return text.encode("utf-8")


def _competitor_entry_ui_smoke_check(
    *,
    enabled: bool,
    frontend_base_url: str,
    browser_channel: str | None,
    timeout_ms: int,
    scope: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not enabled:
        return []
    command = [
        "npm",
        "--prefix",
        str(FRONTEND_DIR),
        "run",
        "smoke:product-config-competitor-entry",
        "--",
        f"--base-url={frontend_base_url.rstrip('/')}",
        f"--timeout-ms={timeout_ms}",
    ]
    if browser_channel:
        command.append(f"--channel={browser_channel}")
    target_scope: dict[str, Any] = {}
    if scope:
        for scope_key, arg_name in (
            ("country", "country"),
            ("model", "model"),
            ("powertrain", "powertrain"),
            ("segment", "segment"),
        ):
            value = str(scope.get(scope_key) or "").strip()
            if value:
                target_scope[scope_key] = value
                command.append(f"--{arg_name}={value}")
    base_details: dict[str, Any] = {
        "command": command,
        "frontendBaseUrl": frontend_base_url.rstrip("/"),
        "timeoutMs": timeout_ms,
        "targetScope": target_scope,
    }
    try:
        completed = subprocess.run(  # noqa: S603 - fixed local npm smoke command.
            command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(60, int(timeout_ms / 1000) + 60),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return [
            {
                "key": "competitor_entry_ui_smoke",
                "label": "Competitor recommendation entry UI smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-competitor-entry",
                "message": f"Competitor entry UI smoke could not run: {exc}",
                "details": base_details,
            }
        ]
    summary = _parse_smoke_summary(completed.stdout)
    checks = summary.get("checks") if isinstance(summary, dict) and isinstance(summary.get("checks"), dict) else {}
    required_results = {key: bool(checks.get(key)) for key in COMPETITOR_ENTRY_UI_REQUIRED_CHECKS}
    failed_required_checks = [key for key, passed in required_results.items() if not passed]
    details = {
        **base_details,
        "returnCode": completed.returncode,
        "stdoutTail": completed.stdout[-1200:],
        "stderrTail": completed.stderr[-1200:],
        "requiredChecks": required_results,
        "failedRequiredChecks": failed_required_checks,
    }
    if summary:
        recommendation_state = (
            summary.get("recommendationState") if isinstance(summary.get("recommendationState"), dict) else {}
        )
        source_handoff_state = (
            summary.get("sourceHandoffState") if isinstance(summary.get("sourceHandoffState"), dict) else {}
        )
        details.update(
            {
                "summaryPath": summary.get("summaryPath"),
                "artifactDir": summary.get("artifactDir"),
                "targetUrl": summary.get("targetUrl"),
                "screenshotPath": summary.get("screenshotPath"),
                "recommendationSummary": recommendation_state.get("summary"),
                "recommendationQueue": recommendation_state.get("queue"),
                "sourceSearchValue": source_handoff_state.get("sourceSearchValue"),
                "passed": bool(summary.get("passed")),
            }
        )
    if completed.returncode != 0:
        return [
            {
                "key": "competitor_entry_ui_smoke",
                "label": "Competitor recommendation entry UI smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-competitor-entry",
                "message": "Competitor entry UI smoke exited non-zero",
                "details": details,
            }
        ]
    if not summary:
        return [
            {
                "key": "competitor_entry_ui_smoke",
                "label": "Competitor recommendation entry UI smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-competitor-entry",
                "message": "Competitor entry UI smoke passed process execution but did not emit a parseable summary",
                "details": details,
            }
        ]
    if not bool(summary.get("passed")) or failed_required_checks:
        return [
            {
                "key": "competitor_entry_ui_smoke",
                "label": "Competitor recommendation entry UI smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-competitor-entry",
                "message": (
                    "Competitor entry UI smoke summary did not prove recommendation coverage, completion queue, and"
                    " missing-source handoff"
                ),
                "details": details,
            }
        ]
    return [
        {
            "key": "competitor_entry_ui_smoke",
            "label": "Competitor recommendation entry UI smoke",
            "status": "passed",
            "path": "npm run smoke:product-config-competitor-entry",
            "message": (
                "real UI proved Advanced Analysis top-10 recommendation coverage, competitor completion queue, and"
                " upload-needed source handoff without writes"
            ),
            "details": details,
        }
    ]


def _first_importable_digest_group(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    digest = snapshot.get("sourceDigest") if isinstance(snapshot.get("sourceDigest"), dict) else {}
    groups = digest.get("compareGroups") if isinstance(digest.get("compareGroups"), list) else []
    for group in groups:
        if not isinstance(group, dict):
            continue
        trim_count = _safe_int(group.get("trimCount")) or len(
            group.get("trims") if isinstance(group.get("trims"), list) else []
        )
        row_count = len(group.get("rows") if isinstance(group.get("rows"), list) else [])
        group_id = str(group.get("groupId") or "").strip()
        if group_id and trim_count >= 2 and row_count > 0:
            return group
    return None


def _cleanup_competitor_workflow_artifacts(
    client: ApiClient,
    *,
    source_id: str | None,
    trim_ids: list[str],
    country: str,
) -> dict[str, Any]:
    cleanup: dict[str, Any] = {
        "trimTrashRequests": 0,
        "sourceTrashed": False,
        "sourceTrashCleared": None,
        "trimTrashCleared": None,
        "errors": [],
    }
    for trim_id in trim_ids:
        try:
            client.patch_json(f"/engineering-config/trims/{urllib.parse.quote(trim_id)}", {"status": "trashed"})
            cleanup["trimTrashRequests"] = _safe_int(cleanup.get("trimTrashRequests")) + 1
        except ReadinessHttpError as exc:
            cleanup["errors"].append(
                {"step": "trash_trim", "trimId": trim_id, "message": exc.message, "status": exc.status}
            )
    if source_id:
        try:
            query = urllib.parse.urlencode({"country": country})
            client.delete_json(f"/engineering-config/source/snapshots/{urllib.parse.quote(source_id)}?{query}")
            cleanup["sourceTrashed"] = True
        except ReadinessHttpError as exc:
            cleanup["errors"].append(
                {"step": "trash_source", "sourceId": source_id, "message": exc.message, "status": exc.status}
            )
        try:
            query = urllib.parse.urlencode({"country": country})
            result = client.delete_json(f"/engineering-config/source/trash?{query}")
            cleanup["sourceTrashCleared"] = result.get("cleared")
        except ReadinessHttpError as exc:
            cleanup["errors"].append({"step": "clear_source_trash", "message": exc.message, "status": exc.status})
    try:
        query = urllib.parse.urlencode({"market": country})
        result = client.delete_json(f"/engineering-config/trims/trash?{query}")
        cleanup["trimTrashCleared"] = result.get("cleared")
    except ReadinessHttpError as exc:
        cleanup["errors"].append({"step": "clear_trim_trash", "message": exc.message, "status": exc.status})
    return cleanup


def _competitor_workflow_smoke_check(
    client: ApiClient,
    scope: dict[str, Any] | None,
    *,
    enabled: bool,
) -> list[dict[str, Any]]:
    if not enabled:
        return []
    if scope is None:
        return [
            {
                "key": "competitor_workflow",
                "label": "Competitor upload/digest workflow smoke",
                "status": "degraded",
                "path": "/engineering-config/recommendations/competitors",
                "message": "competitor workflow smoke skipped because competitor scope is required",
            }
        ]
    country = str(scope.get("country") or "").strip()
    model = str(scope.get("model") or scope.get("modelName") or "").strip()
    if not country or not model:
        return [
            {
                "key": "competitor_workflow",
                "label": "Competitor upload/digest workflow smoke",
                "status": "degraded",
                "path": "/engineering-config/recommendations/competitors",
                "message": "competitor workflow smoke skipped because country and target model are required",
                "details": {"countryProvided": bool(country), "modelProvided": bool(model)},
            }
        ]
    source_id: str | None = None
    draft_trim_ids: list[str] = []
    cleanup: dict[str, Any] | None = None
    try:
        recommendation_path, before_items, before_payload = _recommendation_items(client, scope)
        recommendation = _pick_upload_needed_recommendation(before_items)
        if recommendation is None:
            return [
                {
                    "key": "competitor_workflow",
                    "label": "Competitor upload/digest workflow smoke",
                    "status": "degraded",
                    "path": recommendation_path,
                    "message": "competitor workflow smoke skipped because no upload-needed recommendation was found",
                    "details": {
                        "recommendationRows": before_payload.get("rows") or len(before_items),
                        "nextActions": {
                            str(item.get("nextAction") or "unknown"): sum(
                                1
                                for candidate in before_items
                                if str(candidate.get("nextAction") or "unknown")
                                == str(item.get("nextAction") or "unknown")
                            )
                            for item in before_items
                        },
                    },
                }
            ]
        competitor_model = str(recommendation.get("modelName") or "").strip()
        competitor_brand = str(recommendation.get("brand") or "WorkflowRival").strip() or "WorkflowRival"
        competitor_powertrain = (
            _recommendation_profile_text(recommendation, "powertrain")
            or str(scope.get("powertrain") or "").strip()
            or "Powertrain 待补"
        )
        competitor_segment = (
            _recommendation_profile_text(recommendation, "segment")
            or str(scope.get("segment") or "").strip()
            or "Segment 待补"
        )
        stamp = _utc_stamp().lower().replace("z", "")
        file_name = f"config-compare-workflow-{_safe_file_slug(competitor_model)}-{stamp}.csv"
        csv_bytes = _competitor_workflow_csv(
            brand=competitor_brand,
            model_name=competitor_model,
            country=country,
            powertrain=competitor_powertrain,
            segment=competitor_segment,
            stamp=stamp,
        )
        initiate_query = urllib.parse.urlencode(
            {
                "file_name": file_name,
                "total_size": str(len(csv_bytes)),
                "mime_type": "text/csv",
            }
        )
        initiate = client.post_json(f"/engineering-config/source/upload/initiate?{initiate_query}")
        upload_id = str(initiate.get("uploadId") or "").strip()
        if not upload_id:
            raise ReadinessHttpError(
                "/engineering-config/source/upload/initiate", None, "upload session did not return uploadId"
            )
        client.put_bytes(f"/engineering-config/source/upload/{urllib.parse.quote(upload_id)}/parts/0", csv_bytes)
        snapshot = client.post_json(
            f"/engineering-config/source/upload/{urllib.parse.quote(upload_id)}/complete",
            {
                "relatedContext": {
                    "brand": competitor_brand,
                    "model": competitor_model,
                    "market": country,
                    "country": country,
                    "powertrain": competitor_powertrain,
                    "segment": competitor_segment,
                    "contextType": "competitor_recommendation_upload",
                    "scenario": "readiness_competitor_workflow_smoke",
                    "identityAnchor": "brand_model_market",
                }
            },
        )
        source_id = (
            str(
                snapshot.get("sourceId")
                or snapshot.get("source_id")
                or snapshot.get("importBatchId")
                or snapshot.get("import_batch_id")
                or "",
            ).strip()
            or None
        )
        group = _first_importable_digest_group(snapshot)
        if group is None:
            raise ReadinessHttpError(
                "/engineering-config/source/upload/complete",
                None,
                "uploaded source did not produce an importable Source Digest group",
            )
        group_id = str(group.get("groupId") or "").strip()
        source_path_id = urllib.parse.quote(source_id or "")
        group_path_id = urllib.parse.quote(group_id)
        draft_path = (
            f"/engineering-config/source/snapshots/{source_path_id}"
            f"/digest-groups/{group_path_id}/draft"
        )
        draft = client.post_json(
            draft_path,
            {},
        )
        raw_trim_ids = draft.get("compareTrimIds") or draft.get("trimIds")
        draft_trim_ids = [str(trim_id) for trim_id in raw_trim_ids if trim_id] if isinstance(raw_trim_ids, list) else []
        if len(draft_trim_ids) < 2:
            raise ReadinessHttpError(
                "/engineering-config/source/draft", None, "draft creation returned fewer than 2 compare trim ids"
            )
        compare_ids = draft_trim_ids[:2]
        compare_query = urllib.parse.urlencode({"trim_ids": ",".join(compare_ids)})
        compare_payload = client.get_json(f"/engineering-config/compare?{compare_query}")
        compare_status, compare_message, compare_details = _compare_evaluator(compare_payload)
        export_checks = [
            _export_file_check(
                "competitor_workflow_export_xlsx",
                "Competitor workflow XLSX export smoke",
                "/engineering-config/compare/export/xlsx",
                client,
                _compare_export_payload(compare_payload, "config-compare-competitor-workflow-smoke.xlsx"),
                b"PK",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
            _export_file_check(
                "competitor_workflow_export_pdf",
                "Competitor workflow PDF export smoke",
                "/engineering-config/compare/export/pdf",
                client,
                _compare_export_payload(compare_payload, "config-compare-competitor-workflow-smoke.pdf"),
                b"%PDF",
                "application/pdf",
            ),
        ]
        _after_upload_path, after_items, _after_payload = _recommendation_items(client, scope)
        after_recommendation = _find_recommendation_by_model(after_items, competitor_model)
        cleanup = _cleanup_competitor_workflow_artifacts(
            client, source_id=source_id, trim_ids=draft_trim_ids, country=country
        )
        export_failed = [check for check in export_checks if check.get("status") == "failed"]
        cleanup_errors = cleanup.get("errors") if isinstance(cleanup.get("errors"), list) else []
        status = "passed"
        if compare_status == "failed" or export_failed:
            status = "failed"
        elif compare_status == "degraded" or cleanup_errors:
            status = "degraded"
        return [
            {
                "key": "competitor_workflow",
                "label": "Competitor upload/digest workflow smoke",
                "status": status,
                "path": recommendation_path,
                "message": (
                    f"uploaded {competitor_model} source, created {len(draft_trim_ids)} editable config columns, "
                    f"compared {compare_details.get('totalFeatures')} rows, exported XLSX/PDF"
                ),
                "details": {
                    "targetScope": {
                        "country": country,
                        "model": model,
                        "powertrain": scope.get("powertrain"),
                        "segment": scope.get("segment"),
                    },
                    "recommendation": {
                        "brand": competitor_brand,
                        "modelName": competitor_model,
                        "beforeNextAction": recommendation.get("nextAction"),
                        "afterNextAction": after_recommendation.get("nextAction") if after_recommendation else None,
                        "afterConfigAvailable": (
                            bool(after_recommendation.get("configAvailable")) if after_recommendation else None
                        ),
                        "afterSourceDigestAvailable": (
                            bool(after_recommendation.get("sourceDigestAvailable")) if after_recommendation else None
                        ),
                    },
                    "upload": {
                        "fileName": file_name,
                        "sourceId": source_id,
                        "uploadStatus": snapshot.get("uploadStatus"),
                        "parseMode": snapshot.get("parseMode"),
                    },
                    "draft": {
                        "groupId": group_id,
                        "trimIds": draft_trim_ids,
                        "createdTrimCount": draft.get("createdTrimCount"),
                        "reusedTrimCount": draft.get("reusedTrimCount"),
                        "featureCount": draft.get("featureCount"),
                        "valueRecordCount": draft.get("valueRecordCount"),
                    },
                    "compare": {**compare_details, "status": compare_status, "message": compare_message},
                    "exports": {
                        check["key"]: {
                            "status": check.get("status"),
                            "message": check.get("message"),
                            "details": check.get("details"),
                        }
                        for check in export_checks
                    },
                    "cleanup": cleanup,
                },
            }
        ]
    except ReadinessHttpError as exc:
        if cleanup is None and (source_id or draft_trim_ids):
            cleanup = _cleanup_competitor_workflow_artifacts(
                client, source_id=source_id, trim_ids=draft_trim_ids, country=country
            )
        return [
            {
                "key": "competitor_workflow",
                "label": "Competitor upload/digest workflow smoke",
                "status": "failed",
                "path": exc.url,
                "httpStatus": exc.status,
                "message": exc.message,
                "details": {"sourceId": source_id, "trimIds": draft_trim_ids, "cleanup": cleanup},
            }
        ]


def _local_workbook_digest_path(file_name: str | None) -> str:
    file_name_text = str(file_name or "").strip()
    if not file_name_text:
        return "/engineering-config/source/local-workbook-digest"
    return f"/engineering-config/source/local-workbook-digest?{urllib.parse.urlencode({'file_name': file_name_text})}"


def _int_field(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _local_workbook_digest_check(
    client: ApiClient,
    *,
    enabled: bool,
    file_name: str | None,
    timeout: float = DEFAULT_LOCAL_WORKBOOK_TIMEOUT,
) -> list[dict[str, Any]]:
    if not enabled:
        return []
    path = _local_workbook_digest_path(file_name)
    try:
        payload = _client_with_timeout(client, timeout).get_json(path)
    except ReadinessHttpError as exc:
        return [
            {
                "key": "local_workbook_digest",
                "label": "Local XLSX digest smoke",
                "status": "failed",
                "path": path,
                "httpStatus": exc.status,
                "message": exc.message,
                "details": {"timeoutSeconds": timeout},
            }
        ]
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    compare_groups = payload.get("compareGroups") if isinstance(payload.get("compareGroups"), list) else []
    comparable_group_count = _int_field(summary.get("comparableGroupCount"))
    candidate_trim_count = _int_field(summary.get("candidateTrimCount"))
    sheet_count = _int_field(summary.get("sheetCount"))
    feature_count = _int_field(summary.get("featureCount"))
    if comparable_group_count <= 0 or candidate_trim_count < 2:
        return [
            {
                "key": "local_workbook_digest",
                "label": "Local XLSX digest smoke",
                "status": "degraded",
                "path": path,
                "message": "local workbook digest returned no comparable config groups",
                "details": {
                    "sheetCount": sheet_count,
                    "featureCount": feature_count,
                    "candidateTrimCount": candidate_trim_count,
                    "comparableGroupCount": comparable_group_count,
                    "compareGroupCount": len(compare_groups),
                    "timeoutSeconds": timeout,
                },
            }
        ]
    return [
        {
            "key": "local_workbook_digest",
            "label": "Local XLSX digest smoke",
            "status": "passed",
            "path": path,
            "message": (
                f"local workbook digest returned {comparable_group_count} comparable groups "
                f"and {candidate_trim_count} candidate config columns"
            ),
            "details": {
                "sheetCount": sheet_count,
                "featureCount": feature_count,
                "candidateTrimCount": candidate_trim_count,
                "comparableGroupCount": comparable_group_count,
                "compareGroupCount": len(compare_groups),
                "fileName": file_name,
                "timeoutSeconds": timeout,
            },
        }
    ]


def _api_v1_base(api_base: str) -> str:
    base = api_base.rstrip("/")
    return base if base.endswith("/v1") else f"{base}/v1"


def _parse_smoke_summary(stdout: str) -> dict[str, Any] | None:
    start = stdout.find("{")
    end = stdout.rfind("}")
    if start >= 0 and end > start:
        try:
            payload = json.loads(stdout[start : end + 1])
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            return payload
    summary_match = re.search(r"Summary:\s*(?P<path>\S+)", stdout)
    if summary_match is None:
        return None
    summary_path = Path(summary_match.group("path"))
    if not summary_path.is_absolute():
        summary_path = REPO_ROOT / summary_path
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    payload.setdefault("summaryPath", _display_path(summary_path))
    payload.setdefault("artifactDir", _display_path(summary_path.parent))
    return payload


def _ui_edit_export_smoke_check(
    *,
    enabled: bool,
    frontend_base_url: str,
    api_base: str,
    browser_channel: str | None,
    timeout_ms: int,
    source_format: str,
) -> list[dict[str, Any]]:
    if not enabled:
        return []
    command = [
        "npm",
        "--prefix",
        str(FRONTEND_DIR),
        "run",
        "smoke:product-config-edit-export",
        "--",
        "--write",
        f"--base-url={frontend_base_url.rstrip('/')}",
        f"--api-base={_api_v1_base(api_base)}",
        f"--timeout-ms={timeout_ms}",
        f"--source-format={source_format}",
    ]
    if browser_channel:
        command.append(f"--channel={browser_channel}")
    try:
        completed = subprocess.run(  # noqa: S603 - fixed local npm smoke command.
            command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(60, int(timeout_ms / 1000) + 60),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return [
            {
                "key": "ui_edit_export_smoke",
                "label": "UI edit-after-digest export smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-edit-export",
                "message": f"UI edit-export smoke could not run: {exc}",
                "details": {
                    "command": command,
                    "frontendBaseUrl": frontend_base_url,
                    "apiBase": _api_v1_base(api_base),
                    "timeoutMs": timeout_ms,
                    "sourceFormat": source_format,
                },
            }
        ]
    summary = _parse_smoke_summary(completed.stdout)
    details: dict[str, Any] = {
        "command": command,
        "frontendBaseUrl": frontend_base_url,
        "apiBase": _api_v1_base(api_base),
        "timeoutMs": timeout_ms,
        "sourceFormat": source_format,
        "returnCode": completed.returncode,
        "stdoutTail": completed.stdout[-1200:],
        "stderrTail": completed.stderr[-1200:],
    }
    if summary:
        edit_result = summary.get("editResult") if isinstance(summary.get("editResult"), dict) else {}
        exports = summary.get("exports") if isinstance(summary.get("exports"), dict) else {}
        cleanup = summary.get("cleanup") if isinstance(summary.get("cleanup"), dict) else {}
        xlsx = exports.get("xlsx") if isinstance(exports.get("xlsx"), dict) else {}
        pdf = exports.get("pdf") if isinstance(exports.get("pdf"), dict) else {}
        details.update(
            {
                "summaryPath": summary.get("summaryPath"),
                "artifactDir": summary.get("artifactDir"),
                "targetUrl": summary.get("targetUrl"),
                "sourceId": summary.get("sourceId"),
                "trimIds": summary.get("trimIds"),
                "smokeSourceFormat": summary.get("sourceFormat"),
                "smokeContentType": summary.get("contentType"),
                "editScenario": edit_result.get("scenario"),
                "editSavedAsExpected": (
                    bool(edit_result.get("savedAsExpected")) or bool(edit_result.get("savedAsOptional"))
                ),
                "editSavedAsOptional": bool(edit_result.get("savedAsOptional")),
                "saveStatus": edit_result.get("saveStatus"),
                "xlsxEditedValueInPayload": bool(xlsx.get("editedValueInPayload")),
                "xlsxSignatureOk": bool(xlsx.get("signatureOk")),
                "pdfEditedValueInPayload": bool(pdf.get("editedValueInPayload")),
                "pdfSignatureOk": bool(pdf.get("signatureOk")),
                "cleanupErrors": cleanup.get("errors") if isinstance(cleanup.get("errors"), list) else [],
                "sourceTrashCleared": cleanup.get("sourceTrashCleared"),
                "sourceGloballyTrashed": cleanup.get("sourceGloballyTrashed"),
                "trimTrashCleared": cleanup.get("trimTrashCleared"),
            }
        )
    if completed.returncode != 0:
        return [
            {
                "key": "ui_edit_export_smoke",
                "label": "UI edit-after-digest export smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-edit-export",
                "message": "UI edit-export smoke exited non-zero",
                "details": details,
            }
        ]
    if not summary:
        return [
            {
                "key": "ui_edit_export_smoke",
                "label": "UI edit-after-digest export smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-edit-export",
                "message": "UI edit-export smoke passed process execution but did not emit a parseable summary",
                "details": details,
            }
        ]
    passed = bool(summary.get("passed"))
    edit_ok = bool(details.get("editSavedAsExpected"))
    xlsx_ok = bool(details.get("xlsxEditedValueInPayload")) and bool(details.get("xlsxSignatureOk"))
    pdf_ok = bool(details.get("pdfEditedValueInPayload")) and bool(details.get("pdfSignatureOk"))
    cleanup_errors = details.get("cleanupErrors") if isinstance(details.get("cleanupErrors"), list) else []
    if not (passed and edit_ok and xlsx_ok and pdf_ok):
        return [
            {
                "key": "ui_edit_export_smoke",
                "label": "UI edit-after-digest export smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-edit-export",
                "message": "UI edit-export smoke summary did not prove edit, XLSX export, and PDF export",
                "details": details,
            }
        ]
    if cleanup_errors:
        return [
            {
                "key": "ui_edit_export_smoke",
                "label": "UI edit-after-digest export smoke",
                "status": "degraded",
                "path": "npm run smoke:product-config-edit-export",
                "message": "UI edit-export smoke passed but cleanup reported errors",
                "details": details,
            }
        ]
    return [
        {
            "key": "ui_edit_export_smoke",
            "label": "UI edit-after-digest export smoke",
            "status": "passed",
            "path": "npm run smoke:product-config-edit-export",
            "message": (
                "real UI created editable config columns, edited a value through FloatingDeck, and exported XLSX/PDF"
                " with the edited value"
            ),
            "details": details,
        }
    ]


def _floatingdeck_smoke_command(
    npm_script: str,
    *,
    frontend_base_url: str,
    api_base: str,
    browser_channel: str | None,
    timeout_ms: int,
    extra_args: list[str] | None = None,
) -> list[str]:
    command = [
        "npm",
        "--prefix",
        str(FRONTEND_DIR),
        "run",
        npm_script,
        "--",
        "--write",
        f"--base-url={frontend_base_url.rstrip('/')}",
        f"--api-base={_api_v1_base(api_base)}",
        f"--timeout-ms={timeout_ms}",
    ]
    if browser_channel:
        command.append(f"--channel={browser_channel}")
    if extra_args:
        command.extend(extra_args)
    return command


def _run_floatingdeck_multisource_smoke(
    *,
    key: str,
    label: str,
    npm_script: str,
    frontend_base_url: str,
    api_base: str,
    browser_channel: str | None,
    timeout_ms: int,
) -> dict[str, Any]:
    command = _floatingdeck_smoke_command(
        npm_script,
        frontend_base_url=frontend_base_url,
        api_base=api_base,
        browser_channel=browser_channel,
        timeout_ms=timeout_ms,
    )
    path = f"npm run {npm_script}"
    base_details: dict[str, Any] = {
        "command": command,
        "frontendBaseUrl": frontend_base_url.rstrip("/"),
        "apiBase": _api_v1_base(api_base),
        "timeoutMs": timeout_ms,
    }
    try:
        completed = subprocess.run(  # noqa: S603 - fixed local npm smoke command.
            command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(60, int(timeout_ms / 1000) + 60),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "key": key,
            "label": label,
            "status": "failed",
            "path": path,
            "message": f"{label} could not run: {exc}",
            "details": base_details,
        }
    summary = _parse_smoke_summary(completed.stdout)
    details = {
        **base_details,
        "returnCode": completed.returncode,
        "stdoutTail": completed.stdout[-1200:],
        "stderrTail": completed.stderr[-1200:],
    }
    if summary:
        cleanup = summary.get("cleanup") if isinstance(summary.get("cleanup"), dict) else {}
        observed = summary.get("observed") if isinstance(summary.get("observed"), dict) else {}
        details.update(
            {
                "summaryPath": summary.get("summaryPath"),
                "artifactDir": summary.get("artifactDir"),
                "sourceIds": summary.get("sourceIds"),
                "trimIds": summary.get("trimIds"),
                "passed": bool(summary.get("passed")),
                "cleanupErrors": cleanup.get("errors") if isinstance(cleanup.get("errors"), list) else [],
                "observedApiSteps": len(observed.get("api", [])) if isinstance(observed.get("api"), list) else 0,
                "observedUiSteps": len(observed.get("ui", [])) if isinstance(observed.get("ui"), list) else 0,
            }
        )
        if key == "floatingdeck_multisource_same_model":
            compare_api = summary.get("compareApi") if isinstance(summary.get("compareApi"), dict) else {}
            floating_deck_search = (
                summary.get("floatingDeckSearch") if isinstance(summary.get("floatingDeckSearch"), dict) else {}
            )
            formal_compare_ui = (
                summary.get("formalCompareUi") if isinstance(summary.get("formalCompareUi"), dict) else {}
            )
            details.update(
                {
                    "compareTrimCount": compare_api.get("trimCount"),
                    "duplicateBasicPremiumKept": bool(compare_api.get("duplicateBasicPremiumKept")),
                    "floatingDeckSearchPassed": bool(floating_deck_search.get("passed")),
                    "formalCompareNoHorizontalOverflow": bool(formal_compare_ui.get("noHorizontalOverflow")),
                }
            )
        if key == "floatingdeck_cross_scope_direct_picker":
            direct_picker_flow = (
                summary.get("directPickerFlow") if isinstance(summary.get("directPickerFlow"), dict) else {}
            )
            details.update(
                {
                    "countriesVisible": bool(direct_picker_flow.get("countriesVisible")),
                    "modelsVisible": bool(direct_picker_flow.get("modelsVisible")),
                    "sourcesVisible": bool(direct_picker_flow.get("sourcesVisible")),
                    "selectedFourColumns": bool(direct_picker_flow.get("selectedFourColumns")),
                    "noOwnCompetitorModeText": bool(direct_picker_flow.get("noModeText")),
                    "noHorizontalOverflow": bool(direct_picker_flow.get("noHorizontalOverflow")),
                    "rowsStatus": direct_picker_flow.get("rowsStatus"),
                }
            )
    if completed.returncode != 0:
        return {
            "key": key,
            "label": label,
            "status": "failed",
            "path": path,
            "message": f"{label} exited non-zero",
            "details": details,
        }
    if not summary:
        return {
            "key": key,
            "label": label,
            "status": "failed",
            "path": path,
            "message": f"{label} passed process execution but did not emit a parseable summary",
            "details": details,
        }
    cleanup_errors = details.get("cleanupErrors") if isinstance(details.get("cleanupErrors"), list) else []
    if cleanup_errors:
        return {
            "key": key,
            "label": label,
            "status": "degraded",
            "path": path,
            "message": f"{label} passed but cleanup reported errors",
            "details": details,
        }
    if not bool(summary.get("passed")):
        return {
            "key": key,
            "label": label,
            "status": "failed",
            "path": path,
            "message": f"{label} summary did not prove the expected FloatingDeck flow",
            "details": details,
        }
    return {
        "key": key,
        "label": label,
        "status": "passed",
        "path": path,
        "message": f"{label} passed through the real FloatingDeck search flow",
        "details": details,
    }


def _floatingdeck_multisource_smoke_checks(
    *,
    enabled: bool,
    frontend_base_url: str,
    api_base: str,
    browser_channel: str | None,
    timeout_ms: int,
) -> list[dict[str, Any]]:
    if not enabled:
        return []
    return [
        _run_floatingdeck_multisource_smoke(
            key="floatingdeck_multisource_same_model",
            label="FloatingDeck same-model multi-source smoke",
            npm_script="smoke:product-config-multisource",
            frontend_base_url=frontend_base_url,
            api_base=api_base,
            browser_channel=browser_channel,
            timeout_ms=timeout_ms,
        ),
        _run_floatingdeck_multisource_smoke(
            key="floatingdeck_cross_scope_direct_picker",
            label="FloatingDeck cross-country/cross-model picker smoke",
            npm_script="smoke:product-config-cross-scope",
            frontend_base_url=frontend_base_url,
            api_base=api_base,
            browser_channel=browser_channel,
            timeout_ms=timeout_ms,
        ),
    ]


def _t19c_ai_ui_smoke_check(
    *,
    enabled: bool,
    frontend_base_url: str,
    browser_channel: str | None,
    timeout_ms: int,
    expected_rows: int,
    trim_ids: list[str] | None,
    base_trim_id: str | None,
    viewport: str,
) -> list[dict[str, Any]]:
    if not enabled:
        return []
    command = [
        "npm",
        "--prefix",
        str(FRONTEND_DIR),
        "run",
        "smoke:product-config-ai",
        "--",
        f"--base-url={frontend_base_url.rstrip('/')}",
        f"--timeout-ms={timeout_ms}",
        f"--expected-rows={expected_rows}",
        f"--viewport={viewport}",
    ]
    if browser_channel:
        command.append(f"--channel={browser_channel}")
    if trim_ids:
        command.append(f"--trim-ids={','.join(trim_ids)}")
    if base_trim_id:
        command.append(f"--base-trim-id={base_trim_id}")
    base_details: dict[str, Any] = {
        "command": command,
        "frontendBaseUrl": frontend_base_url.rstrip("/"),
        "timeoutMs": timeout_ms,
        "expectedRows": expected_rows,
        "viewport": viewport,
        "trimIds": trim_ids or [],
        "baseTrimId": base_trim_id,
    }
    try:
        completed = subprocess.run(  # noqa: S603 - fixed local npm smoke command.
            command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(60, int(timeout_ms / 1000) + 60),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return [
            {
                "key": "t19c_ai_ui_smoke",
                "label": "T19C simple-mode AI UI smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-ai",
                "message": f"T19C AI UI smoke could not run: {exc}",
                "details": base_details,
            }
        ]
    summary = _parse_smoke_summary(completed.stdout)
    checks = summary.get("checks") if isinstance(summary, dict) and isinstance(summary.get("checks"), dict) else {}
    required_results = {key: bool(checks.get(key)) for key in T19C_AI_UI_REQUIRED_CHECKS}
    failed_required_checks = [key for key, passed in required_results.items() if not passed]
    details = {
        **base_details,
        "returnCode": completed.returncode,
        "stdoutTail": completed.stdout[-1200:],
        "stderrTail": completed.stderr[-1200:],
        "requiredChecks": required_results,
        "failedRequiredChecks": failed_required_checks,
    }
    if summary:
        details.update(
            {
                "summaryPath": summary.get("summaryPath"),
                "artifactDir": summary.get("artifactDir"),
                "targetUrl": summary.get("targetUrl"),
                "summaryTrimIds": summary.get("trimIds"),
                "summaryBaseTrimId": summary.get("baseTrimId"),
                "summaryExpectedRows": summary.get("expectedRows"),
                "viewportMode": summary.get("viewportMode"),
                "initialScreenshotPath": summary.get("initialScreenshotPath"),
                "initialScreenshotPaths": summary.get("initialScreenshotPaths"),
                "screenshotPath": summary.get("screenshotPath"),
                "screenshotPaths": summary.get("screenshotPaths"),
                "passed": bool(summary.get("passed")),
            }
        )
    if completed.returncode != 0:
        return [
            {
                "key": "t19c_ai_ui_smoke",
                "label": "T19C simple-mode AI UI smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-ai",
                "message": "T19C AI UI smoke exited non-zero",
                "details": details,
            }
        ]
    if not summary:
        return [
            {
                "key": "t19c_ai_ui_smoke",
                "label": "T19C simple-mode AI UI smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-ai",
                "message": "T19C AI UI smoke passed process execution but did not emit a parseable summary",
                "details": details,
            }
        ]
    if not bool(summary.get("passed")) or failed_required_checks:
        return [
            {
                "key": "t19c_ai_ui_smoke",
                "label": "T19C simple-mode AI UI smoke",
                "status": "failed",
                "path": "npm run smoke:product-config-ai",
                "message": (
                    "T19C AI UI smoke summary did not prove simple-mode full-row table, scoped navigator, source"
                    " picker, and FloatingDeck edit gate"
                ),
                "details": details,
            }
        ]
    return [
        {
            "key": "t19c_ai_ui_smoke",
            "label": "T19C simple-mode AI UI smoke",
            "status": "passed",
            "path": "npm run smoke:product-config-ai",
            "message": (
                "T19C UI proved AI-first simple mode, full-row default table, scoped difference navigator, source"
                " picker, and FloatingDeck edit gate"
            ),
            "details": details,
        }
    ]


def _cross_user_source_library_smoke_check(
    *,
    enabled: bool,
    frontend_base_url: str,
    api_base: str,
    browser_channel: str | None,
    timeout_ms: int,
) -> list[dict[str, Any]]:
    if not enabled:
        return []
    command = _floatingdeck_smoke_command(
        "smoke:product-config-cross-user-source",
        frontend_base_url=frontend_base_url,
        api_base=api_base,
        browser_channel=browser_channel,
        timeout_ms=timeout_ms,
    )
    path = "npm run smoke:product-config-cross-user-source"
    base_details: dict[str, Any] = {
        "command": command,
        "frontendBaseUrl": frontend_base_url.rstrip("/"),
        "apiBase": _api_v1_base(api_base),
        "timeoutMs": timeout_ms,
    }
    try:
        completed = subprocess.run(  # noqa: S603 - fixed local npm smoke command.
            command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(60, int(timeout_ms / 1000) + 60),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return [
            {
                "key": "cross_user_source_library_smoke",
                "label": "Cross-user source-library smoke",
                "status": "failed",
                "path": path,
                "message": f"Cross-user source-library smoke could not run: {exc}",
                "details": base_details,
            }
        ]
    summary = _parse_smoke_summary(completed.stdout)
    details = {
        **base_details,
        "returnCode": completed.returncode,
        "stdoutTail": completed.stdout[-1200:],
        "stderrTail": completed.stderr[-1200:],
    }
    if summary:
        cleanup = summary.get("cleanup") if isinstance(summary.get("cleanup"), dict) else {}
        consumer_source_list = (
            summary.get("consumerSourceList") if isinstance(summary.get("consumerSourceList"), dict) else {}
        )
        ui_result = summary.get("uiResult") if isinstance(summary.get("uiResult"), dict) else {}
        observed = summary.get("observed") if isinstance(summary.get("observed"), dict) else {}
        details.update(
            {
                "summaryPath": summary.get("summaryPath"),
                "artifactDir": summary.get("artifactDir"),
                "targetUrl": summary.get("targetUrl"),
                "screenshotPath": summary.get("screenshotPath"),
                "csvPath": summary.get("csvPath"),
                "fileName": summary.get("fileName"),
                "uploaderUserName": summary.get("uploaderUserName"),
                "consumerUserName": summary.get("consumerUserName"),
                "sourceId": summary.get("sourceId"),
                "trimIds": summary.get("trimIds"),
                "consumerSourceFound": bool(consumer_source_list.get("found")),
                "consumerSourceCreatedBy": consumer_source_list.get("createdBy"),
                "consumerSourceItemCount": consumer_source_list.get("itemCount"),
                "rowsStatus": ui_result.get("rowsStatus"),
                "fileVisible": bool(ui_result.get("fileVisible")),
                "uploaderVisible": bool(ui_result.get("uploaderVisible")),
                "consumerVisible": bool(ui_result.get("consumerVisible")),
                "successVisible": bool(ui_result.get("successVisible")),
                "basicVisible": bool(ui_result.get("basicVisible")),
                "premiumVisible": bool(ui_result.get("premiumVisible")),
                "noHorizontalOverflow": bool(ui_result.get("noHorizontalOverflow")),
                "cleanupErrors": cleanup.get("errors") if isinstance(cleanup.get("errors"), list) else [],
                "sourceTrashCleared": cleanup.get("sourceTrashCleared"),
                "trimTrashCleared": cleanup.get("trimTrashCleared"),
                "observedApiSteps": len(observed.get("api", [])) if isinstance(observed.get("api"), list) else 0,
                "observedUiSteps": len(observed.get("ui", [])) if isinstance(observed.get("ui"), list) else 0,
                "passed": bool(summary.get("passed")),
            }
        )
    if completed.returncode != 0:
        return [
            {
                "key": "cross_user_source_library_smoke",
                "label": "Cross-user source-library smoke",
                "status": "failed",
                "path": path,
                "message": "Cross-user source-library smoke exited non-zero",
                "details": details,
            }
        ]
    if not summary:
        return [
            {
                "key": "cross_user_source_library_smoke",
                "label": "Cross-user source-library smoke",
                "status": "failed",
                "path": path,
                "message": (
                    "Cross-user source-library smoke passed process execution but did not emit a parseable summary"
                ),
                "details": details,
            }
        ]
    cleanup_errors = details.get("cleanupErrors") if isinstance(details.get("cleanupErrors"), list) else []
    if cleanup_errors:
        return [
            {
                "key": "cross_user_source_library_smoke",
                "label": "Cross-user source-library smoke",
                "status": "degraded",
                "path": path,
                "message": "Cross-user source-library smoke passed but cleanup reported errors",
                "details": details,
            }
        ]
    uploaded_by_expected_user = (
        not details.get("consumerSourceCreatedBy")
        or not details.get("uploaderUserName")
        or details.get("consumerSourceCreatedBy") == details.get("uploaderUserName")
    )
    cross_user_reuse_ok = (
        bool(summary.get("passed"))
        and bool(details.get("consumerSourceFound"))
        and uploaded_by_expected_user
        and bool(details.get("successVisible"))
        and bool(details.get("fileVisible"))
        and bool(details.get("uploaderVisible"))
        and bool(details.get("consumerVisible"))
        and bool(details.get("basicVisible"))
        and bool(details.get("premiumVisible"))
        and bool(details.get("noHorizontalOverflow"))
    )
    if not cross_user_reuse_ok:
        return [
            {
                "key": "cross_user_source_library_smoke",
                "label": "Cross-user source-library smoke",
                "status": "failed",
                "path": path,
                "message": (
                    "Cross-user source-library smoke summary did not prove upload-by-user-A, search-by-user-B, and"
                    " FloatingDeck reuse"
                ),
                "details": details,
            }
        ]
    return [
        {
            "key": "cross_user_source_library_smoke",
            "label": "Cross-user source-library smoke",
            "status": "passed",
            "path": path,
            "message": (
                "temporary source uploaded by one user was listed and reused by another user through FloatingDeck"
            ),
            "details": details,
        }
    ]


def _source_review_row_smoke_check(
    *,
    enabled: bool,
    frontend_base_url: str,
    api_base: str,
    browser_channel: str | None,
    timeout_ms: int,
    image_format: str,
) -> list[dict[str, Any]]:
    if not enabled:
        return []
    command = _floatingdeck_smoke_command(
        "smoke:product-config-review-row",
        frontend_base_url=frontend_base_url,
        api_base=api_base,
        browser_channel=browser_channel,
        timeout_ms=timeout_ms,
        extra_args=[f"--image-format={image_format}"],
    )
    path = "npm run smoke:product-config-review-row"
    base_details: dict[str, Any] = {
        "command": command,
        "frontendBaseUrl": frontend_base_url.rstrip("/"),
        "apiBase": _api_v1_base(api_base),
        "timeoutMs": timeout_ms,
        "imageFormat": image_format,
    }
    try:
        completed = subprocess.run(  # noqa: S603 - fixed local npm smoke command.
            command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(60, int(timeout_ms / 1000) + 60),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return [
            {
                "key": "source_review_row_smoke",
                "label": "Source Digest review-row smoke",
                "status": "failed",
                "path": path,
                "message": f"Source Digest review-row smoke could not run: {exc}",
                "details": base_details,
            }
        ]
    summary = _parse_smoke_summary(completed.stdout)
    details = {
        **base_details,
        "returnCode": completed.returncode,
        "stdoutTail": completed.stdout[-1200:],
        "stderrTail": completed.stderr[-1200:],
    }
    if summary:
        cleanup = summary.get("cleanup") if isinstance(summary.get("cleanup"), dict) else {}
        observed = summary.get("observed") if isinstance(summary.get("observed"), dict) else {}
        ui_steps = observed.get("ui") if isinstance(observed.get("ui"), list) else []
        edit_steps = [
            item for item in ui_steps if isinstance(item, dict) and item.get("step") == "edited_selected_review_feature"
        ]
        export_xlsx_steps = [
            item for item in ui_steps if isinstance(item, dict) and item.get("step") == "export_xlsx_after_review_edit"
        ]
        export_pdf_steps = [
            item for item in ui_steps if isinstance(item, dict) and item.get("step") == "export_pdf_after_review_edit"
        ]
        edit_step = edit_steps[0] if edit_steps else {}
        export_xlsx = export_xlsx_steps[0] if export_xlsx_steps else {}
        export_pdf = export_pdf_steps[0] if export_pdf_steps else {}
        details.update(
            {
                "summaryPath": summary.get("summaryPath"),
                "artifactDir": summary.get("artifactDir"),
                "targetUrl": summary.get("targetUrl"),
                "sourceId": summary.get("sourceId"),
                "trimIds": summary.get("trimIds"),
                "fileName": summary.get("fileName"),
                "imageFormat": summary.get("imageFormat") or image_format,
                "mimeType": summary.get("mimeType"),
                "selectedReviewFeature": summary.get("selectedReviewFeature"),
                "reviewRowCount": summary.get("reviewRowCount"),
                "selectedReviewRow": any(
                    item.get("step") == "selected_review_row" for item in ui_steps if isinstance(item, dict)
                ),
                "formalRowHighlighted": any(
                    item.get("step") == "formal_row_highlighted" for item in ui_steps if isinstance(item, dict)
                ),
                "reviewEditSavedAsOptional": bool(edit_step.get("savedAsOptional")),
                "reviewEditSaveStatus": edit_step.get("saveStatus"),
                "xlsxEditedValueInPayload": bool(export_xlsx.get("editedValueInPayload")),
                "xlsxSignatureOk": bool(export_xlsx.get("signatureOk")),
                "pdfEditedValueInPayload": bool(export_pdf.get("editedValueInPayload")),
                "pdfSignatureOk": bool(export_pdf.get("signatureOk")),
                "cleanupErrors": cleanup.get("errors") if isinstance(cleanup.get("errors"), list) else [],
                "sourceTrashed": bool(cleanup.get("sourceTrashed")),
                "sourceTrashCleared": cleanup.get("sourceTrashCleared"),
                "trimTrashCleared": cleanup.get("trimTrashCleared"),
                "passed": bool(summary.get("passed")),
            }
        )
    if completed.returncode != 0:
        return [
            {
                "key": "source_review_row_smoke",
                "label": "Source Digest review-row smoke",
                "status": "failed",
                "path": path,
                "message": "Source Digest review-row smoke exited non-zero",
                "details": details,
            }
        ]
    if not summary:
        return [
            {
                "key": "source_review_row_smoke",
                "label": "Source Digest review-row smoke",
                "status": "failed",
                "path": path,
                "message": (
                    "Source Digest review-row smoke passed process execution but did not emit a parseable summary"
                ),
                "details": details,
            }
        ]
    cleanup_errors = details.get("cleanupErrors") if isinstance(details.get("cleanupErrors"), list) else []
    if cleanup_errors:
        return [
            {
                "key": "source_review_row_smoke",
                "label": "Source Digest review-row smoke",
                "status": "degraded",
                "path": path,
                "message": "Source Digest review-row smoke passed but cleanup reported errors",
                "details": details,
            }
        ]
    edit_export_ok = (
        bool(details.get("reviewEditSavedAsOptional"))
        and bool(details.get("xlsxEditedValueInPayload"))
        and bool(details.get("xlsxSignatureOk"))
        and bool(details.get("pdfEditedValueInPayload"))
        and bool(details.get("pdfSignatureOk"))
    )
    if not (
        bool(summary.get("passed"))
        and details.get("selectedReviewRow")
        and details.get("formalRowHighlighted")
        and edit_export_ok
    ):
        return [
            {
                "key": "source_review_row_smoke",
                "label": "Source Digest review-row smoke",
                "status": "failed",
                "path": path,
                "message": (
                    "Source Digest review-row smoke summary did not prove review-row selection, formal-table focus,"
                    " edit, and XLSX/PDF export"
                ),
                "details": details,
            }
        ]
    return [
        {
            "key": "source_review_row_smoke",
            "label": "Source Digest review-row smoke",
            "status": "passed",
            "path": path,
            "message": (
                "real Source Digest OCR review-row flow selected a risky row, created editable config columns, edited"
                " the formal row, and exported XLSX/PDF"
            ),
            "details": details,
        }
    ]


def _skipped_export_checks(reason: str) -> list[dict[str, Any]]:
    return [
        {
            "key": "export_xlsx",
            "label": "XLSX export smoke",
            "status": "degraded",
            "path": "/engineering-config/compare/export/xlsx",
            "message": reason,
        },
        {
            "key": "export_pdf",
            "label": "PDF export smoke",
            "status": "degraded",
            "path": "/engineering-config/compare/export/pdf",
            "message": reason,
        },
    ]


def _skipped_ai_summary_compose_checks(reason: str) -> list[dict[str, Any]]:
    return [
        {
            "key": "ai_summary_compose",
            "label": "Runtime AI summary compose smoke",
            "status": "degraded",
            "path": "/engineering-config/business-summary/compose",
            "message": reason,
        }
    ]


def _config_columns_and_compare_checks(
    client: ApiClient,
    *,
    include_export_smoke: bool = False,
    include_ai_summary_smoke: bool = False,
    compare_trim_ids: list[str] | None = None,
    compare_trim_query: str | None = None,
    compare_trim_market: str | None = None,
    ai_summary_timeout: float | None = None,
) -> list[dict[str, Any]]:
    direct_compare_trim_ids = [trim_id for trim_id in compare_trim_ids or [] if trim_id]
    list_limit = 8 if (compare_trim_query or compare_trim_market) else 4
    trim_list_path = _config_column_list_path(
        limit=list_limit,
        compare_trim_query=compare_trim_query,
        compare_trim_market=compare_trim_market,
    )
    try:
        trim_payload = client.get_json(trim_list_path)
    except ReadinessHttpError as exc:
        return [
            {
                "key": "config_columns",
                "label": "Config column library",
                "status": "failed",
                "path": trim_list_path,
                "httpStatus": exc.status,
                "message": exc.message,
            },
            {
                "key": "compare_api",
                "label": "Compare API smoke",
                "status": "failed",
                "path": "/engineering-config/compare",
                "message": "compare smoke skipped because config column library failed",
            },
            *(
                _skipped_export_checks("export smoke skipped because config column library failed")
                if include_export_smoke
                else []
            ),
            *(
                _skipped_ai_summary_compose_checks("AI compose smoke skipped because config column library failed")
                if include_ai_summary_smoke
                else []
            ),
        ]
    list_status, list_message, list_details = _list_evaluator("config column")(trim_payload)
    config_check = {
        "key": "config_columns",
        "label": "Config column library",
        "status": list_status,
        "path": trim_list_path,
        "message": list_message,
        "details": {
            **list_details,
            "compareTrimQuery": compare_trim_query,
            "compareTrimMarket": compare_trim_market,
            "directCompareTrimIds": direct_compare_trim_ids,
        },
    }
    trim_ids = direct_compare_trim_ids or _extract_trim_ids(trim_payload)
    if len(trim_ids) < 2:
        return [
            config_check,
            {
                "key": "compare_api",
                "label": "Compare API smoke",
                "status": "degraded",
                "path": "/engineering-config/compare",
                "message": "compare smoke skipped because fewer than 2 config columns are available",
                "details": {
                    "availableTrimIds": len(trim_ids),
                    "compareTrimQuery": compare_trim_query,
                    "compareTrimMarket": compare_trim_market,
                    "directCompareTrimIds": direct_compare_trim_ids,
                },
            },
            *(
                _skipped_export_checks("export smoke skipped because fewer than 2 config columns are available")
                if include_export_smoke
                else []
            ),
            *(
                _skipped_ai_summary_compose_checks(
                    "AI compose smoke skipped because fewer than 2 config columns are available"
                )
                if include_ai_summary_smoke
                else []
            ),
        ]
    compare_ids = trim_ids[:4]
    query = urllib.parse.urlencode({"trim_ids": ",".join(compare_ids)})
    compare_path = f"/engineering-config/compare?{query}"
    try:
        compare_payload = client.get_json(compare_path)
    except ReadinessHttpError as exc:
        return [
            config_check,
            {
                "key": "compare_api",
                "label": "Compare API smoke",
                "status": "failed",
                "path": compare_path,
                "httpStatus": exc.status,
                "message": exc.message,
                "details": {"trimIds": compare_ids},
            },
            *(
                _skipped_export_checks("export smoke skipped because compare API failed")
                if include_export_smoke
                else []
            ),
            *(
                _skipped_ai_summary_compose_checks("AI compose smoke skipped because compare API failed")
                if include_ai_summary_smoke
                else []
            ),
        ]
    compare_status, compare_message, compare_details = _compare_evaluator(compare_payload)
    compare_details = {**compare_details, "trimIds": compare_ids}
    checks = [
        config_check,
        {
            "key": "compare_api",
            "label": "Compare API smoke",
            "status": compare_status,
            "path": compare_path,
            "message": compare_message,
            "details": compare_details,
        },
    ]
    if include_export_smoke:
        checks.extend(
            _export_smoke_checks(client, compare_payload)
            if compare_status == "passed"
            else _skipped_export_checks("export smoke skipped because compare API did not return config rows")
        )
    if include_ai_summary_smoke:
        checks.extend(
            _ai_summary_smoke_check(client, compare_payload, enabled=True, timeout=ai_summary_timeout)
            if compare_status == "passed"
            else _skipped_ai_summary_compose_checks(
                "AI compose smoke skipped because compare API did not return config rows"
            )
        )
    return checks


def _overall_status(checks: list[dict[str, Any]]) -> str:
    statuses = {str(check.get("status")) for check in checks}
    if "failed" in statuses:
        return "failed"
    if "degraded" in statuses:
        return "degraded"
    return "passed"


def _checks_by_key(checks: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for check in checks:
        key = str(check.get("key") or "")
        if not key:
            continue
        grouped.setdefault(key, []).append(check)
    return grouped


def _check_passed(grouped: dict[str, list[dict[str, Any]]], key: str) -> bool:
    return any(check.get("status") == "passed" for check in grouped.get(key, []))


def _check_failed(grouped: dict[str, list[dict[str, Any]]], key: str) -> bool:
    return any(check.get("status") == "failed" for check in grouped.get(key, []))


def _check_labels(grouped: dict[str, list[dict[str, Any]]], keys: list[str]) -> list[str]:
    labels: list[str] = []
    for key in keys:
        for check in grouped.get(key, []):
            label = str(check.get("label") or key).strip()
            status = str(check.get("status") or "unknown").strip()
            labels.append(f"{label}: {status}")
    return labels


def _coverage_status(
    grouped: dict[str, list[dict[str, Any]]],
    *,
    proved_by: list[str],
    partial_by: list[str] | None = None,
) -> str:
    partial_keys = partial_by or []
    if any(_check_failed(grouped, key) for key in [*proved_by, *partial_keys]):
        return "risk"
    if proved_by and all(_check_passed(grouped, key) for key in proved_by):
        return "proved"
    if any(_check_passed(grouped, key) for key in partial_keys):
        return "partial"
    return "unverified"


def _goal_coverage_item(
    grouped: dict[str, list[dict[str, Any]]],
    *,
    key: str,
    label: str,
    requirement: str,
    proved_by: list[str],
    partial_by: list[str] | None,
    next_evidence: str,
) -> dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "requirement": requirement,
        "status": _coverage_status(grouped, proved_by=proved_by, partial_by=partial_by),
        "provedBy": proved_by,
        "partialBy": partial_by or [],
        "currentEvidence": _check_labels(grouped, [*(partial_by or []), *proved_by]),
        "nextEvidence": next_evidence,
    }


def _goal_coverage(checks: list[dict[str, Any]]) -> dict[str, Any]:
    grouped = _checks_by_key(checks)
    items = [
        _goal_coverage_item(
            grouped,
            key="simple_excel_mode",
            label="Simple Excel-like compare UI",
            requirement=(
                "Default users should see AI conclusions and a full Excel-like config table before expert diagnostics."
            ),
            proved_by=["t19c_ai_ui_smoke"],
            partial_by=["compare_api", "ai_summary"],
            next_evidence="Run --include-t19c-ai-ui-smoke on the target environment.",
        ),
        _goal_coverage_item(
            grouped,
            key="multi_format_digest_to_editable_export",
            label="Multi-format digest -> editable table -> export",
            requirement=(
                "XLSX, PDF/price-list, and image/JPG sources should digest into editable config columns and export to"
                " XLSX/PDF after edits."
            ),
            proved_by=["ui_edit_export_smoke", "source_review_row_smoke"],
            partial_by=["local_workbook_digest", "ui_edit_export_smoke", "export_xlsx", "export_pdf"],
            next_evidence=(
                "Run --include-ui-edit-export-smoke with xlsx/pdf-text/price-list-csv and"
                " --include-source-review-row-smoke --source-review-row-image-format=jpeg."
            ),
        ),
        _goal_coverage_item(
            grouped,
            key="floatingdeck_multisource_direct_picker",
            label="FloatingDeck multi-source direct picker",
            requirement=(
                "Users should add any brand/model/config column from library or Source Digest without own-vs-competitor"
                " mode switches."
            ),
            proved_by=["floatingdeck_multisource_same_model", "floatingdeck_cross_scope_direct_picker"],
            partial_by=["config_columns", "t19c_ai_ui_smoke"],
            next_evidence=(
                "Run --include-floatingdeck-multisource-smoke with temporary write data, then repeat on staging/prod"
                " data."
            ),
        ),
        _goal_coverage_item(
            grouped,
            key="competitor_recommendation_and_upload_gap",
            label="Advanced Analysis competitor recommendation entry",
            requirement=(
                "Same-country/powertrain/segment top competitors should show library-ready, Source-Digest-ready, and"
                " upload-needed states with a clear next action."
            ),
            proved_by=["competitor_entry_ui_smoke", "competitor_workflow"],
            partial_by=["competitor_recommendations"],
            next_evidence=(
                "Run --include-competitor-entry-ui-smoke for read-only entry and --include-competitor-workflow-smoke"
                " for the upload-needed write path."
            ),
        ),
        _goal_coverage_item(
            grouped,
            key="cross_user_country_source_library_trash",
            label="Shared country-scoped source library and trash",
            requirement=(
                "Uploaded sources should be shared across users, scoped by country, and support trash/restore/clear"
                " without leaking across countries."
            ),
            proved_by=["cross_user_source_library_smoke"],
            partial_by=["source_library", "source_review_row_smoke", "ui_edit_export_smoke"],
            next_evidence="Run --include-cross-user-source-library-smoke and production auth/data-retention checks.",
        ),
        _goal_coverage_item(
            grouped,
            key="admin_editor_edit_governance",
            label="Editor/admin gated online editing",
            requirement=(
                "Online config editing should be guarded by editor/admin/developer permissions and opened from"
                " FloatingDeck only."
            ),
            proved_by=["auth_contract", "ui_edit_export_smoke"],
            partial_by=["auth_contract", "t19c_ai_ui_smoke", "ui_edit_export_smoke"],
            next_evidence=(
                "Run --include-auth-contract-smoke with viewer/editor/admin tokens plus --include-ui-edit-export-smoke."
            ),
        ),
        _goal_coverage_item(
            grouped,
            key="paddle_vs_legacy_ocr_decision",
            label="PaddleOCR vs legacy/custom OCR decision",
            requirement=(
                "OCR source parsing should use the best engine after real-sample PaddleOCR-vs-legacy/custom quality"
                " comparison."
            ),
            proved_by=["ocr_quality_recommendation"],
            partial_by=["ocr"],
            next_evidence=(
                "Run engineering_config_ocr_quality_audit.py in the target runtime and feed the JSON through"
                " --ocr-quality-artifact."
            ),
        ),
        _goal_coverage_item(
            grouped,
            key="runtime_ai_business_summary",
            label="Runtime AI business summary",
            requirement=(
                "AI summaries should be generated from current compare facts, include evidence warnings, and not"
                " masquerade as persisted digest artifacts."
            ),
            proved_by=["ai_summary", "ai_summary_compose"],
            partial_by=["ai_summary"],
            next_evidence=(
                "Run --include-ai-summary-smoke against real compare columns and inspect evidenceStatus/mainUpgrades"
                " output."
            ),
        ),
    ]
    counts = {
        "proved": sum(1 for item in items if item["status"] == "proved"),
        "partial": sum(1 for item in items if item["status"] == "partial"),
        "unverified": sum(1 for item in items if item["status"] == "unverified"),
        "risk": sum(1 for item in items if item["status"] == "risk"),
    }
    return {
        "schemaVersion": "engineering_config_compare_goal_coverage_v1",
        "summary": counts,
        "items": items,
    }


def build_readiness_report(
    client: ApiClient,
    *,
    include_ocr_quality_audit: bool = False,
    ocr_quality_artifact: Path | None = None,
    ocr_quality_artifact_dir: Path = DEFAULT_OCR_QUALITY_ARTIFACT_DIR,
    include_auth_contract_smoke: bool = False,
    auth_viewer_token: str | None = None,
    auth_editor_token: str | None = None,
    auth_admin_token: str | None = None,
    auth_viewer_user: str = "config-viewer-smoke",
    auth_editor_user: str = "config-editor-smoke",
    auth_admin_user: str = "config-admin-smoke",
    auth_contract_clients: dict[str, Any] | None = None,
    include_export_smoke: bool = False,
    include_ai_summary_smoke: bool = False,
    include_local_workbook_smoke: bool = False,
    include_competitor_entry_ui_smoke: bool = False,
    include_competitor_workflow_smoke: bool = False,
    include_ui_edit_export_smoke: bool = False,
    include_floatingdeck_multisource_smoke: bool = False,
    include_t19c_ai_ui_smoke: bool = False,
    include_cross_user_source_library_smoke: bool = False,
    include_source_review_row_smoke: bool = False,
    compare_trim_ids: list[str] | None = None,
    compare_trim_query: str | None = None,
    compare_trim_market: str | None = None,
    ai_summary_timeout: float | None = None,
    frontend_base_url: str = DEFAULT_FRONTEND_BASE,
    ui_edit_export_browser_channel: str | None = None,
    ui_edit_export_timeout_ms: int = 180000,
    ui_edit_export_source_format: str = "csv",
    floatingdeck_multisource_browser_channel: str | None = None,
    floatingdeck_multisource_timeout_ms: int = 180000,
    competitor_entry_ui_smoke_browser_channel: str | None = None,
    competitor_entry_ui_smoke_timeout_ms: int = 180000,
    t19c_ai_ui_smoke_browser_channel: str | None = None,
    t19c_ai_ui_smoke_timeout_ms: int = 120000,
    t19c_ai_ui_smoke_expected_rows: int = 227,
    t19c_ai_ui_smoke_trim_ids: list[str] | None = None,
    t19c_ai_ui_smoke_base_trim_id: str | None = None,
    t19c_ai_ui_smoke_viewport: str = "desktop",
    cross_user_source_library_browser_channel: str | None = None,
    cross_user_source_library_timeout_ms: int = 180000,
    source_review_row_browser_channel: str | None = None,
    source_review_row_timeout_ms: int = 180000,
    source_review_row_image_format: str = "png",
    local_workbook_file: str | None = None,
    local_workbook_timeout: float = DEFAULT_LOCAL_WORKBOOK_TIMEOUT,
    competitor_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ocr_readiness_check = _endpoint_result(
        "ocr", "OCR readiness", "/engineering-config/ocr/readiness", client, _ocr_evaluator
    )
    checks = [
        _endpoint_result("health", "Backend health", "/healthz", client, _health_evaluator),
        ocr_readiness_check,
        _endpoint_result(
            "ai_summary",
            "Runtime AI summary readiness",
            "/engineering-config/business-summary/readiness",
            client,
            _ai_evaluator,
        ),
        _endpoint_result(
            "source_library",
            "Source snapshot library",
            "/engineering-config/source/snapshots?limit=1",
            client,
            _list_evaluator("source snapshot"),
        ),
    ]
    ocr_runtime_details = (
        ocr_readiness_check.get("details") if isinstance(ocr_readiness_check.get("details"), dict) else None
    )
    resolved_ocr_quality_artifact = ocr_quality_artifact
    if include_ocr_quality_audit and resolved_ocr_quality_artifact is None:
        resolved_ocr_quality_artifact = _latest_ocr_quality_artifact(ocr_quality_artifact_dir, ocr_runtime_details)
    if include_ocr_quality_audit:
        checks.append(_ocr_quality_artifact_check(resolved_ocr_quality_artifact, ocr_runtime_details))
    if include_auth_contract_smoke:
        checks.append(
            _auth_contract_check(
                client,
                viewer_token=auth_viewer_token,
                editor_token=auth_editor_token,
                admin_token=auth_admin_token,
                viewer_user=auth_viewer_user,
                editor_user=auth_editor_user,
                admin_user=auth_admin_user,
                compare_trim_ids=compare_trim_ids or [],
                role_clients=auth_contract_clients,
            )
        )
    checks.extend(
        _config_columns_and_compare_checks(
            client,
            include_export_smoke=include_export_smoke,
            include_ai_summary_smoke=include_ai_summary_smoke,
            compare_trim_ids=compare_trim_ids,
            compare_trim_query=compare_trim_query,
            compare_trim_market=compare_trim_market,
            ai_summary_timeout=ai_summary_timeout,
        )
    )
    checks.extend(
        _local_workbook_digest_check(
            client,
            enabled=include_local_workbook_smoke,
            file_name=local_workbook_file,
            timeout=local_workbook_timeout,
        )
    )
    checks.extend(_competitor_recommendation_check(client, competitor_scope))
    checks.extend(
        _competitor_entry_ui_smoke_check(
            enabled=include_competitor_entry_ui_smoke,
            frontend_base_url=frontend_base_url,
            browser_channel=competitor_entry_ui_smoke_browser_channel,
            timeout_ms=competitor_entry_ui_smoke_timeout_ms,
            scope=competitor_scope,
        )
    )
    checks.extend(
        _competitor_workflow_smoke_check(
            client,
            competitor_scope,
            enabled=include_competitor_workflow_smoke,
        )
    )
    checks.extend(
        _ui_edit_export_smoke_check(
            enabled=include_ui_edit_export_smoke,
            frontend_base_url=frontend_base_url,
            api_base=client.api_base,
            browser_channel=ui_edit_export_browser_channel,
            timeout_ms=ui_edit_export_timeout_ms,
            source_format=ui_edit_export_source_format,
        )
    )
    checks.extend(
        _floatingdeck_multisource_smoke_checks(
            enabled=include_floatingdeck_multisource_smoke,
            frontend_base_url=frontend_base_url,
            api_base=client.api_base,
            browser_channel=floatingdeck_multisource_browser_channel,
            timeout_ms=floatingdeck_multisource_timeout_ms,
        )
    )
    checks.extend(
        _t19c_ai_ui_smoke_check(
            enabled=include_t19c_ai_ui_smoke,
            frontend_base_url=frontend_base_url,
            browser_channel=t19c_ai_ui_smoke_browser_channel,
            timeout_ms=t19c_ai_ui_smoke_timeout_ms,
            expected_rows=t19c_ai_ui_smoke_expected_rows,
            trim_ids=t19c_ai_ui_smoke_trim_ids,
            base_trim_id=t19c_ai_ui_smoke_base_trim_id,
            viewport=t19c_ai_ui_smoke_viewport,
        )
    )
    checks.extend(
        _cross_user_source_library_smoke_check(
            enabled=include_cross_user_source_library_smoke,
            frontend_base_url=frontend_base_url,
            api_base=client.api_base,
            browser_channel=cross_user_source_library_browser_channel,
            timeout_ms=cross_user_source_library_timeout_ms,
        )
    )
    checks.extend(
        _source_review_row_smoke_check(
            enabled=include_source_review_row_smoke,
            frontend_base_url=frontend_base_url,
            api_base=client.api_base,
            browser_channel=source_review_row_browser_channel,
            timeout_ms=source_review_row_timeout_ms,
            image_format=source_review_row_image_format,
        )
    )
    status = _overall_status(checks)
    goal_coverage = _goal_coverage(checks)
    return {
        "schemaVersion": SCHEMA_VERSION,
        "pipelineId": PIPELINE_ID,
        "status": status,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "apiBase": client.api_base.rstrip("/"),
        "frontendBaseUrl": frontend_base_url.rstrip("/"),
        "readOnly": (
            not include_competitor_workflow_smoke
            and not include_auth_contract_smoke
            and not include_ui_edit_export_smoke
            and not include_floatingdeck_multisource_smoke
            and not include_cross_user_source_library_smoke
            and not include_source_review_row_smoke
        ),
        "includeOcrQualityAudit": include_ocr_quality_audit,
        "ocrQualityArtifact": _display_path(resolved_ocr_quality_artifact) if resolved_ocr_quality_artifact else None,
        "includeAuthContractSmoke": include_auth_contract_smoke,
        "includeExportSmoke": include_export_smoke,
        "includeAiSummarySmoke": include_ai_summary_smoke,
        "aiSummaryTimeout": ai_summary_timeout,
        "compareTrimIds": compare_trim_ids or [],
        "compareTrimQuery": compare_trim_query,
        "compareTrimMarket": compare_trim_market,
        "includeLocalWorkbookSmoke": include_local_workbook_smoke,
        "includeCompetitorEntryUiSmoke": include_competitor_entry_ui_smoke,
        "competitorEntryUiSmokeBrowserChannel": competitor_entry_ui_smoke_browser_channel,
        "competitorEntryUiSmokeTimeoutMs": competitor_entry_ui_smoke_timeout_ms,
        "includeCompetitorWorkflowSmoke": include_competitor_workflow_smoke,
        "includeUiEditExportSmoke": include_ui_edit_export_smoke,
        "uiEditExportBrowserChannel": ui_edit_export_browser_channel,
        "uiEditExportTimeoutMs": ui_edit_export_timeout_ms,
        "uiEditExportSourceFormat": ui_edit_export_source_format,
        "includeFloatingDeckMultisourceSmoke": include_floatingdeck_multisource_smoke,
        "floatingDeckMultisourceBrowserChannel": floatingdeck_multisource_browser_channel,
        "floatingDeckMultisourceTimeoutMs": floatingdeck_multisource_timeout_ms,
        "includeT19cAiUiSmoke": include_t19c_ai_ui_smoke,
        "t19cAiUiSmokeBrowserChannel": t19c_ai_ui_smoke_browser_channel,
        "t19cAiUiSmokeTimeoutMs": t19c_ai_ui_smoke_timeout_ms,
        "t19cAiUiSmokeExpectedRows": t19c_ai_ui_smoke_expected_rows,
        "t19cAiUiSmokeTrimIds": t19c_ai_ui_smoke_trim_ids or [],
        "t19cAiUiSmokeBaseTrimId": t19c_ai_ui_smoke_base_trim_id,
        "t19cAiUiSmokeViewport": t19c_ai_ui_smoke_viewport,
        "includeCrossUserSourceLibrarySmoke": include_cross_user_source_library_smoke,
        "crossUserSourceLibraryBrowserChannel": cross_user_source_library_browser_channel,
        "crossUserSourceLibraryTimeoutMs": cross_user_source_library_timeout_ms,
        "includeSourceReviewRowSmoke": include_source_review_row_smoke,
        "sourceReviewRowBrowserChannel": source_review_row_browser_channel,
        "sourceReviewRowTimeoutMs": source_review_row_timeout_ms,
        "sourceReviewRowImageFormat": source_review_row_image_format,
        "localWorkbookFile": local_workbook_file,
        "localWorkbookTimeout": local_workbook_timeout,
        "competitorScope": competitor_scope,
        "summary": {
            "passed": sum(1 for check in checks if check.get("status") == "passed"),
            "degraded": sum(1 for check in checks if check.get("status") == "degraded"),
            "failed": sum(1 for check in checks if check.get("status") == "failed"),
        },
        "goalCoverage": goal_coverage,
        "checks": checks,
    }


def render_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    goal_coverage = report.get("goalCoverage") if isinstance(report.get("goalCoverage"), dict) else {}
    coverage_summary = goal_coverage.get("summary") if isinstance(goal_coverage.get("summary"), dict) else {}
    lines = [
        "# Engineering Config Compare Readiness",
        "",
        f"- Schema: `{report.get('schemaVersion')}`",
        f"- Status: **{report.get('status')}**",
        f"- API base: `{report.get('apiBase')}`",
        f"- Generated: `{report.get('generatedAt')}`",
        f"- Read-only: `{report.get('readOnly')}`",
        (
            f"- Checks: passed {summary.get('passed', 0)}, degraded {summary.get('degraded', 0)}, failed"
            f" {summary.get('failed', 0)}"
        ),
        "",
        "| Check | Status | Message |",
        "| --- | --- | --- |",
    ]
    checks = report.get("checks") if isinstance(report.get("checks"), list) else []
    for check in checks:
        lines.append(
            f"| {check.get('label')} | `{check.get('status')}` | {str(check.get('message') or '').replace('|', '/')} |"
        )
    coverage_items = goal_coverage.get("items") if isinstance(goal_coverage.get("items"), list) else []
    if coverage_items:
        lines.extend(
            [
                "",
                "## Goal Coverage",
                "",
                (
                    f"- Proved {coverage_summary.get('proved', 0)}, partial {coverage_summary.get('partial', 0)}, "
                    f"unverified {coverage_summary.get('unverified', 0)}, risk {coverage_summary.get('risk', 0)}"
                ),
                "",
                "| Goal | Coverage | Next evidence |",
                "| --- | --- | --- |",
            ]
        )
        for item in coverage_items:
            lines.append(
                "| "
                f"{str(item.get('label') or '').replace('|', '/')} | "
                f"`{item.get('status')}` | "
                f"{str(item.get('nextEvidence') or '').replace('|', '/')} |"
            )
    return "\n".join(lines).rstrip() + "\n"


def write_outputs(report: dict[str, Any], output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = _utc_stamp()
    latest_json = output_dir / "engineering_config_compare_readiness.json"
    latest_md = output_dir / "engineering_config_compare_readiness.md"
    history_json = output_dir / f"engineering_config_compare_readiness_{suffix}.json"
    history_md = output_dir / f"engineering_config_compare_readiness_{suffix}.md"
    json_text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    md_text = render_markdown(report)
    for path in (latest_json, history_json):
        path.write_text(json_text, encoding="utf-8")
    for path in (latest_md, history_md):
        path.write_text(md_text, encoding="utf-8")
    return {
        "latestJson": str(latest_json),
        "latestMarkdown": str(latest_md),
        "historyJson": str(history_json),
        "historyMarkdown": str(history_md),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit Product Config Compare readiness through read-only API calls.")
    parser.add_argument("--api-base", default=os.getenv("JATO_CONFIG_COMPARE_API_BASE", DEFAULT_API_BASE))
    parser.add_argument(
        "--frontend-base", default=os.getenv("JATO_CONFIG_COMPARE_FRONTEND_BASE", DEFAULT_FRONTEND_BASE)
    )
    parser.add_argument("--token", default=os.getenv("JATO_API_TOKEN") or os.getenv("JATO_AUTH_TOKEN"))
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--skip-ocr-quality-audit",
        action="store_true",
        help=(
            "Skip the real-sample OCR quality recommendation check. By default the latest local OCR quality artifact is"
            " included."
        ),
    )
    parser.add_argument(
        "--ocr-quality-artifact",
        type=Path,
        help="Optional engineering_config_ocr_quality_audit_*.json to include in the readiness gate.",
    )
    parser.add_argument(
        "--ocr-quality-artifact-dir",
        type=Path,
        default=DEFAULT_OCR_QUALITY_ARTIFACT_DIR,
        help=(
            "Directory used to discover the latest OCR quality audit JSON when --ocr-quality-artifact is not provided."
        ),
    )
    parser.add_argument(
        "--include-auth-contract-smoke",
        action="store_true",
        help=(
            "Also verify live auth role boundaries for Product Config Compare. This sends read requests plus fake-id"
            " DELETE/PATCH requests to prove viewer denial and writer handler reachability."
        ),
    )
    parser.add_argument("--auth-viewer-token", default=os.getenv("JATO_CONFIG_COMPARE_VIEWER_TOKEN"))
    parser.add_argument("--auth-editor-token", default=os.getenv("JATO_CONFIG_COMPARE_EDITOR_TOKEN"))
    parser.add_argument("--auth-admin-token", default=os.getenv("JATO_CONFIG_COMPARE_ADMIN_TOKEN"))
    parser.add_argument(
        "--auth-viewer-user", default=os.getenv("JATO_CONFIG_COMPARE_VIEWER_USER", "config-viewer-smoke")
    )
    parser.add_argument(
        "--auth-editor-user", default=os.getenv("JATO_CONFIG_COMPARE_EDITOR_USER", "config-editor-smoke")
    )
    parser.add_argument("--auth-admin-user", default=os.getenv("JATO_CONFIG_COMPARE_ADMIN_USER", "config-admin-smoke"))
    parser.add_argument(
        "--auth-mint-local-tokens",
        action="store_true",
        help=(
            "Mint local viewer/editor/admin HS256 JWTs for dev auth-contract checks. "
            "Use explicit role tokens for staging/production."
        ),
    )
    parser.add_argument(
        "--auth-jwt-secret",
        default=os.getenv("APP_JWT_SECRET", "change-me-jwt-secret"),
        help="JWT secret used only with --auth-mint-local-tokens.",
    )
    parser.add_argument(
        "--auth-jwt-ttl-seconds",
        type=int,
        default=24 * 3600,
        help="Lifetime for local minted auth-contract JWTs.",
    )
    parser.add_argument(
        "--include-export-smoke",
        action="store_true",
        help="Also POST the compare payload to XLSX/PDF export endpoints. This does not persist data.",
    )
    parser.add_argument(
        "--include-ai-summary-smoke",
        action="store_true",
        help=(
            "Also POST the compare payload facts to the runtime LLM business-summary composer. This does not persist"
            " source/config data."
        ),
    )
    parser.add_argument(
        "--ai-summary-timeout",
        type=float,
        default=float(os.getenv("JATO_CONFIG_COMPARE_AI_SUMMARY_TIMEOUT", DEFAULT_AI_SUMMARY_TIMEOUT)),
        help=(
            "Timeout for the optional runtime LLM business-summary compose smoke. "
            "Set this higher than --timeout because live model calls can exceed ordinary API health-check latency."
        ),
    )
    parser.add_argument(
        "--compare-trim-ids",
        help=(
            "Optional comma- or space-separated config column ids to use for compare/export/AI smoke. Uses the first"
            " 2-4 ids."
        ),
    )
    parser.add_argument(
        "--compare-trim-query", help="Optional config-column library search text for compare/export/AI smoke."
    )
    parser.add_argument(
        "--compare-trim-market",
        help="Optional market filter for config-column library search used by compare/export/AI smoke.",
    )
    parser.add_argument(
        "--include-local-workbook-smoke",
        action="store_true",
        help="Also GET the local workbook digest endpoint to verify xlsx parser facts. This does not persist data.",
    )
    parser.add_argument(
        "--local-workbook-timeout",
        type=float,
        default=float(os.getenv("JATO_CONFIG_COMPARE_LOCAL_WORKBOOK_TIMEOUT", DEFAULT_LOCAL_WORKBOOK_TIMEOUT)),
        help=(
            "Timeout for the optional local workbook digest smoke. This endpoint parses and returns a large xlsx"
            " digest, so it intentionally has a wider default than ordinary API health checks."
        ),
    )
    parser.add_argument(
        "--include-competitor-entry-ui-smoke",
        action="store_true",
        help=(
            "Also run the read-only real browser competitor recommendation entry smoke through npm. This verifies"
            " Advanced Analysis top-10 coverage, the competitor completion queue, and upload-needed source handoff."
        ),
    )
    parser.add_argument(
        "--include-competitor-workflow-smoke",
        action="store_true",
        help=(
            "Also run a write-path competitor workflow smoke: choose an upload-needed recommendation, upload a"
            " temporary CSV source, create editable config columns, compare/export, then clean temporary artifacts."
        ),
    )
    parser.add_argument(
        "--include-ui-edit-export-smoke",
        action="store_true",
        help=(
            "Also run the real browser edit-after-digest export smoke through npm. This writes a temporary"
            " source/config columns, edits one value from FloatingDeck, exports XLSX/PDF, then cleans up."
        ),
    )
    parser.add_argument(
        "--include-floatingdeck-multisource-smoke",
        action="store_true",
        help=(
            "Also run real browser FloatingDeck multi-source smokes through npm. This writes temporary sources/config"
            " columns for same-model multi-source and cross-country/cross-model selection, then cleans up."
        ),
    )
    parser.add_argument(
        "--include-t19c-ai-ui-smoke",
        action="store_true",
        help=(
            "Also run the read-only real browser T19C simple-mode AI UI smoke through npm. This verifies full-row"
            " default display, scoped difference navigation, source/model/trim picker labels, and FloatingDeck edit"
            " gating."
        ),
    )
    parser.add_argument(
        "--include-cross-user-source-library-smoke",
        action="store_true",
        help=(
            "Also run the real browser cross-user source-library smoke through npm. This uploads a temporary source as"
            " one user, searches/reuses it as another user from FloatingDeck, then cleans up."
        ),
    )
    parser.add_argument(
        "--include-source-review-row-smoke",
        action="store_true",
        help=(
            "Also run the real browser Source Digest review-row smoke through npm. This writes a temporary OCR image"
            " source, selects a review row, creates editable columns, verifies row focus, then cleans up."
        ),
    )
    parser.add_argument(
        "--ui-edit-export-channel",
        default=os.getenv("JATO_CONFIG_COMPARE_BROWSER_CHANNEL"),
        help="Optional Playwright browser channel for --include-ui-edit-export-smoke, e.g. chrome.",
    )
    parser.add_argument(
        "--ui-edit-export-timeout-ms",
        type=int,
        default=int(os.getenv("JATO_CONFIG_COMPARE_UI_EDIT_EXPORT_TIMEOUT_MS", "180000")),
        help="Timeout passed to the real browser edit-export smoke.",
    )
    parser.add_argument(
        "--ui-edit-export-source-format",
        default=os.getenv("JATO_CONFIG_COMPARE_UI_EDIT_EXPORT_SOURCE_FORMAT", "csv"),
        help="Temporary source format for --include-ui-edit-export-smoke. Use csv, xlsx, pdf-text, or price-list-csv.",
    )
    parser.add_argument(
        "--floatingdeck-multisource-channel",
        default=os.getenv("JATO_CONFIG_COMPARE_BROWSER_CHANNEL"),
        help="Optional Playwright browser channel for --include-floatingdeck-multisource-smoke, e.g. chrome.",
    )
    parser.add_argument(
        "--floatingdeck-multisource-timeout-ms",
        type=int,
        default=int(os.getenv("JATO_CONFIG_COMPARE_FLOATINGDECK_MULTISOURCE_TIMEOUT_MS", "180000")),
        help="Timeout passed to each FloatingDeck multi-source browser smoke.",
    )
    parser.add_argument(
        "--competitor-entry-ui-smoke-channel",
        default=os.getenv("JATO_CONFIG_COMPARE_COMPETITOR_ENTRY_UI_SMOKE_CHANNEL")
        or os.getenv("JATO_CONFIG_COMPARE_BROWSER_CHANNEL"),
        help="Optional Playwright browser channel for --include-competitor-entry-ui-smoke, e.g. chrome.",
    )
    parser.add_argument(
        "--competitor-entry-ui-smoke-timeout-ms",
        type=int,
        default=int(os.getenv("JATO_CONFIG_COMPARE_COMPETITOR_ENTRY_UI_SMOKE_TIMEOUT_MS", "180000")),
        help="Timeout passed to the competitor recommendation entry UI browser smoke.",
    )
    parser.add_argument(
        "--t19c-ai-ui-smoke-channel",
        default=os.getenv("JATO_CONFIG_COMPARE_T19C_AI_UI_SMOKE_CHANNEL")
        or os.getenv("JATO_CONFIG_COMPARE_BROWSER_CHANNEL"),
        help="Optional Playwright browser channel for --include-t19c-ai-ui-smoke, e.g. chrome.",
    )
    parser.add_argument(
        "--t19c-ai-ui-smoke-timeout-ms",
        type=int,
        default=int(os.getenv("JATO_CONFIG_COMPARE_T19C_AI_UI_SMOKE_TIMEOUT_MS", "120000")),
        help="Timeout passed to the T19C simple-mode AI UI browser smoke.",
    )
    parser.add_argument(
        "--t19c-ai-ui-smoke-expected-rows",
        type=int,
        default=int(os.getenv("JATO_CONFIG_COMPARE_T19C_AI_UI_SMOKE_EXPECTED_ROWS", "227")),
        help="Expected full-table config row count for the T19C simple-mode AI UI smoke.",
    )
    parser.add_argument(
        "--t19c-ai-ui-smoke-trim-ids",
        default=os.getenv("JATO_CONFIG_COMPARE_T19C_AI_UI_SMOKE_TRIM_IDS"),
        help="Optional comma- or space-separated config-column ids for the T19C simple-mode AI UI smoke.",
    )
    parser.add_argument(
        "--t19c-ai-ui-smoke-base-trim-id",
        default=os.getenv("JATO_CONFIG_COMPARE_T19C_AI_UI_SMOKE_BASE_TRIM_ID"),
        help="Optional base config-column id for the T19C simple-mode AI UI smoke.",
    )
    parser.add_argument(
        "--t19c-ai-ui-smoke-viewport",
        default=os.getenv("JATO_CONFIG_COMPARE_T19C_AI_UI_SMOKE_VIEWPORT", "desktop"),
        help="Viewport mode for the T19C simple-mode AI UI smoke: desktop, mobile, or all.",
    )
    parser.add_argument(
        "--cross-user-source-library-channel",
        default=os.getenv("JATO_CONFIG_COMPARE_BROWSER_CHANNEL"),
        help="Optional Playwright browser channel for --include-cross-user-source-library-smoke, e.g. chrome.",
    )
    parser.add_argument(
        "--cross-user-source-library-timeout-ms",
        type=int,
        default=int(os.getenv("JATO_CONFIG_COMPARE_CROSS_USER_SOURCE_LIBRARY_TIMEOUT_MS", "180000")),
        help="Timeout passed to the cross-user source-library browser smoke.",
    )
    parser.add_argument(
        "--source-review-row-channel",
        default=os.getenv("JATO_CONFIG_COMPARE_BROWSER_CHANNEL"),
        help="Optional Playwright browser channel for --include-source-review-row-smoke, e.g. chrome.",
    )
    parser.add_argument(
        "--source-review-row-timeout-ms",
        type=int,
        default=int(os.getenv("JATO_CONFIG_COMPARE_SOURCE_REVIEW_ROW_TIMEOUT_MS", "180000")),
        help="Timeout passed to the Source Digest review-row browser smoke.",
    )
    parser.add_argument(
        "--source-review-row-image-format",
        default=os.getenv("JATO_CONFIG_COMPARE_SOURCE_REVIEW_ROW_IMAGE_FORMAT", "png"),
        help="Image format for --include-source-review-row-smoke. Use png, jpg, or jpeg.",
    )
    parser.add_argument(
        "--local-workbook-file", help="Optional file name under 02_Config_MetaData for local workbook digest smoke."
    )
    parser.add_argument("--competitor-country", help="Optional country/market for competitor recommendation smoke.")
    parser.add_argument("--competitor-model", help="Optional target model for competitor recommendation smoke.")
    parser.add_argument(
        "--competitor-powertrain", help="Optional powertrain filter for competitor recommendation smoke."
    )
    parser.add_argument("--competitor-segment", help="Optional segment filter for competitor recommendation smoke.")
    parser.add_argument("--competitor-limit", type=int, default=10, help="Competitor recommendation limit, max 10.")
    parser.add_argument("--markdown", action="store_true", help="Print Markdown instead of JSON.")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero unless all checks pass.")
    args = parser.parse_args(argv)

    competitor_scope = None
    if any([args.competitor_country, args.competitor_model, args.competitor_powertrain, args.competitor_segment]):
        competitor_scope = {
            "country": args.competitor_country,
            "model": args.competitor_model,
            "powertrain": args.competitor_powertrain,
            "segment": args.competitor_segment,
            "limit": args.competitor_limit,
        }
    auth_viewer_token = args.auth_viewer_token
    auth_editor_token = args.auth_editor_token
    auth_admin_token = args.auth_admin_token
    if args.auth_mint_local_tokens:
        ttl_seconds = max(60, args.auth_jwt_ttl_seconds)
        auth_viewer_token = auth_viewer_token or _mint_local_auth_token(
            username=args.auth_viewer_user,
            role="viewer",
            jwt_secret=args.auth_jwt_secret,
            ttl_seconds=ttl_seconds,
        )
        auth_editor_token = auth_editor_token or _mint_local_auth_token(
            username=args.auth_editor_user,
            role="editor",
            jwt_secret=args.auth_jwt_secret,
            ttl_seconds=ttl_seconds,
        )
        auth_admin_token = auth_admin_token or _mint_local_auth_token(
            username=args.auth_admin_user,
            role="admin",
            jwt_secret=args.auth_jwt_secret,
            ttl_seconds=ttl_seconds,
        )

    report = build_readiness_report(
        ApiClient(args.api_base, token=args.token, timeout=args.timeout),
        include_ocr_quality_audit=not args.skip_ocr_quality_audit,
        ocr_quality_artifact=args.ocr_quality_artifact,
        ocr_quality_artifact_dir=args.ocr_quality_artifact_dir,
        include_auth_contract_smoke=args.include_auth_contract_smoke,
        auth_viewer_token=auth_viewer_token,
        auth_editor_token=auth_editor_token,
        auth_admin_token=auth_admin_token,
        auth_viewer_user=args.auth_viewer_user,
        auth_editor_user=args.auth_editor_user,
        auth_admin_user=args.auth_admin_user,
        include_export_smoke=args.include_export_smoke,
        include_ai_summary_smoke=args.include_ai_summary_smoke,
        ai_summary_timeout=args.ai_summary_timeout,
        compare_trim_ids=_normalise_compare_trim_ids(args.compare_trim_ids),
        compare_trim_query=args.compare_trim_query,
        compare_trim_market=args.compare_trim_market,
        frontend_base_url=args.frontend_base,
        include_ui_edit_export_smoke=args.include_ui_edit_export_smoke,
        ui_edit_export_browser_channel=args.ui_edit_export_channel,
        ui_edit_export_timeout_ms=args.ui_edit_export_timeout_ms,
        ui_edit_export_source_format=args.ui_edit_export_source_format,
        include_floatingdeck_multisource_smoke=args.include_floatingdeck_multisource_smoke,
        floatingdeck_multisource_browser_channel=args.floatingdeck_multisource_channel,
        floatingdeck_multisource_timeout_ms=args.floatingdeck_multisource_timeout_ms,
        include_t19c_ai_ui_smoke=args.include_t19c_ai_ui_smoke,
        t19c_ai_ui_smoke_browser_channel=args.t19c_ai_ui_smoke_channel,
        t19c_ai_ui_smoke_timeout_ms=args.t19c_ai_ui_smoke_timeout_ms,
        t19c_ai_ui_smoke_expected_rows=args.t19c_ai_ui_smoke_expected_rows,
        t19c_ai_ui_smoke_trim_ids=_normalise_compare_trim_ids(args.t19c_ai_ui_smoke_trim_ids),
        t19c_ai_ui_smoke_base_trim_id=args.t19c_ai_ui_smoke_base_trim_id,
        t19c_ai_ui_smoke_viewport=args.t19c_ai_ui_smoke_viewport,
        include_cross_user_source_library_smoke=args.include_cross_user_source_library_smoke,
        cross_user_source_library_browser_channel=args.cross_user_source_library_channel,
        cross_user_source_library_timeout_ms=args.cross_user_source_library_timeout_ms,
        include_source_review_row_smoke=args.include_source_review_row_smoke,
        source_review_row_browser_channel=args.source_review_row_channel,
        source_review_row_timeout_ms=args.source_review_row_timeout_ms,
        source_review_row_image_format=args.source_review_row_image_format,
        include_local_workbook_smoke=args.include_local_workbook_smoke,
        include_competitor_entry_ui_smoke=args.include_competitor_entry_ui_smoke,
        competitor_entry_ui_smoke_browser_channel=args.competitor_entry_ui_smoke_channel,
        competitor_entry_ui_smoke_timeout_ms=args.competitor_entry_ui_smoke_timeout_ms,
        include_competitor_workflow_smoke=args.include_competitor_workflow_smoke,
        local_workbook_file=args.local_workbook_file,
        local_workbook_timeout=args.local_workbook_timeout,
        competitor_scope=competitor_scope,
    )
    if args.output_dir:
        report["artifacts"] = write_outputs(report, args.output_dir)
    if args.markdown:
        print(render_markdown(report), end="")
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))

    if report["status"] == "failed":
        return 1
    if args.strict and report["status"] != "passed":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
