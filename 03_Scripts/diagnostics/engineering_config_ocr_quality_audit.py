#!/usr/bin/env python3
"""Audit OCR quality for engineering config source files.

This script is intentionally read-only. It reuses the Source Digest parser so
the reported PaddleOCR / legacy OCR choice matches the product runtime path.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import sys
from typing import Any, Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_DIR = REPO_ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

try:
    from app.services.engineering_config_source_digest import build_source_digest
except Exception as exc:  # pragma: no cover - only used when backend deps are missing.
    build_source_digest = None  # type: ignore[assignment]
    BUILD_SOURCE_DIGEST_IMPORT_ERROR = str(exc)
else:
    BUILD_SOURCE_DIGEST_IMPORT_ERROR = ""


SCHEMA_VERSION = "engineering_config_ocr_quality_audit_v2"
SUPPORTED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".webp"}
DEFAULT_MARKDOWN_LIMIT = 20


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve()))
    except ValueError:
        return str(path)


def _int_value(payload: dict[str, Any] | None, key: str) -> int:
    if not isinstance(payload, dict):
        return 0
    value = payload.get(key)
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def _safe_text(value: object, max_length: int = 220) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_length:
        return text
    return f"{text[:max_length].rstrip()}..."


def _safe_token(value: str | None, fallback: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    safe = "".join(ch if ch.isalnum() else "-" for ch in text)
    return "-".join(part for part in safe.split("-") if part) or fallback


def _module_status(import_name: str, distribution_name: str | None = None) -> dict[str, Any]:
    available = importlib.util.find_spec(import_name) is not None
    version: str | None = None
    if available:
        try:
            version = importlib.metadata.version(distribution_name or import_name)
        except importlib.metadata.PackageNotFoundError:
            version = None
    return {
        "available": available,
        "version": version,
    }


def _runtime_environment() -> dict[str, Any]:
    return {
        "pythonExecutable": sys.executable,
        "pythonVersion": sys.version.split()[0],
        "backendDir": _display_path(BACKEND_DIR),
        "sourceDigestImportOk": build_source_digest is not None,
        "sourceDigestImportError": BUILD_SOURCE_DIGEST_IMPORT_ERROR or None,
        "modules": {
            "paddleocr": _module_status("paddleocr"),
            "paddle": _module_status("paddle", "paddlepaddle"),
            "pypdfium2": _module_status("pypdfium2"),
        },
    }


def _history_suffix(report: dict[str, Any]) -> str:
    generated = str(report.get("generatedAtUtc") or _utc_now_iso())
    stamp = (
        generated.replace(":", "")
        .replace("-", "")
        .replace("+", "z")
        .replace(".", "-")
    )
    return _safe_token(stamp, "unknown-time")


def _iter_source_files(paths: Sequence[Path], *, recursive: bool) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        if path.is_dir():
            iterator: Iterable[Path] = path.rglob("*") if recursive else path.iterdir()
            candidates = [item for item in iterator if item.is_file()]
        elif path.is_file():
            candidates = [path]
        else:
            candidates = []
        for candidate in candidates:
            if candidate.suffix.lower() not in SUPPORTED_EXTENSIONS:
                continue
            resolved = candidate.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            files.append(candidate)
    return sorted(files, key=lambda item: _display_path(item).lower())


def _candidate_payload(candidate: dict[str, Any]) -> dict[str, Any]:
    score = candidate.get("score") if isinstance(candidate.get("score"), dict) else {}
    return {
        "engine": str(candidate.get("engine") or "unknown"),
        "selected": bool(candidate.get("selected")),
        "comparableTableDetected": bool(candidate.get("comparableTableDetected")),
        "featureCount": _int_value(score, "featureCount"),
        "candidateTrimCount": _int_value(score, "candidateTrimCount"),
        "differenceCount": _int_value(score, "differenceCount"),
        "rowCount": _int_value(score, "rowCount"),
        "columnCount": _int_value(score, "columnCount"),
        "nonEmptyCount": _int_value(score, "nonEmptyCount"),
        "lineCount": _int_value(candidate, "lineCount"),
        "message": _safe_text(candidate.get("message")),
        "textPreview": _safe_text(candidate.get("textPreview")),
    }


def _selected_vs_alternates(
    candidates: list[dict[str, Any]],
    selected_engine: str | None,
) -> list[dict[str, Any]]:
    if len(candidates) < 2:
        return []
    selected = next((item for item in candidates if item.get("selected")), None)
    if selected is None and selected_engine:
        selected = next((item for item in candidates if item.get("engine") == selected_engine), None)
    if selected is None:
        return []
    result: list[dict[str, Any]] = []
    for candidate in candidates:
        if candidate is selected:
            continue
        result.append({
            "engine": candidate.get("engine") or "unknown",
            "featureDelta": _int_value(selected, "featureCount") - _int_value(candidate, "featureCount"),
            "candidateTrimDelta": _int_value(selected, "candidateTrimCount") - _int_value(candidate, "candidateTrimCount"),
            "differenceDelta": _int_value(selected, "differenceCount") - _int_value(candidate, "differenceCount"),
            "nonEmptyDelta": _int_value(selected, "nonEmptyCount") - _int_value(candidate, "nonEmptyCount"),
            "selectedComparable": bool(selected.get("comparableTableDetected")),
            "alternateComparable": bool(candidate.get("comparableTableDetected")),
            "alternateMessage": _safe_text(candidate.get("message"), 140),
        })
    return result


def _selected_vs_alternates_text(item: dict[str, Any]) -> str:
    comparisons = item.get("selectedVsAlternates")
    if not isinstance(comparisons, list) or not comparisons:
        return ""
    parts: list[str] = []
    for comparison in comparisons[:2]:
        if not isinstance(comparison, dict):
            continue
        engine = str(comparison.get("engine") or "unknown")
        deltas = [
            ("features", _int_value(comparison, "featureDelta")),
            ("trims", _int_value(comparison, "candidateTrimDelta")),
            ("diffs", _int_value(comparison, "differenceDelta")),
            ("non-empty", _int_value(comparison, "nonEmptyDelta")),
        ]
        delta_text = ", ".join(
            f"{value:+d} {label}"
            for label, value in deltas
            if value != 0
        )
        parts.append(f"vs {engine}: {delta_text or 'same score'}")
    if len(comparisons) > 2:
        parts.append(f"+{len(comparisons) - 2} alternates")
    return "; ".join(parts)


def _ocr_comparison_status(
    *,
    candidates: list[dict[str, Any]],
    selected_engine: str | None,
    digest_type: str,
    source_format: str,
) -> str:
    if selected_engine:
        if len(candidates) > 1:
            return "selected_compared_with_alternates"
        return "selected_without_alternate_candidate"
    if candidates:
        return "ocr_candidates_without_selected_engine"
    format_text = f"{digest_type} {source_format}".lower()
    if "ocr" in format_text or "image" in format_text:
        return "ocr_not_configured_or_no_candidates"
    return "not_ocr_path"


def _ocr_comparison_status_text(item: dict[str, Any]) -> str:
    status = str(item.get("ocrComparisonStatus") or "")
    if status == "selected_compared_with_alternates":
        return "compared"
    if status == "selected_without_alternate_candidate":
        return "no alternate OCR candidate"
    if status == "ocr_candidates_without_selected_engine":
        return "OCR candidates, none selected"
    if status == "ocr_not_configured_or_no_candidates":
        return "OCR not configured / no candidates"
    if status == "not_ocr_path":
        return "non-OCR/text path"
    return status


def _group_summary(digest: dict[str, Any]) -> list[dict[str, Any]]:
    groups = digest.get("compareGroups")
    if not isinstance(groups, list):
        return []
    result: list[dict[str, Any]] = []
    for group in groups[:5]:
        if not isinstance(group, dict):
            continue
        result.append({
            "groupId": str(group.get("groupId") or ""),
            "modelName": str(group.get("modelName") or group.get("title") or ""),
            "trimCount": _int_value(group, "trimCount"),
            "featureCount": _int_value(group, "featureCount"),
            "differenceCount": _int_value(group, "differenceCount"),
            "sourceKind": str(group.get("sourceKind") or ""),
            "identityStatus": str(group.get("identityStatus") or ""),
            "reviewRowCount": sum(
                1
                for row in group.get("rows") or []
                if isinstance(row, dict) and row.get("reviewNotes")
            ),
        })
    return result


def _engine_candidate_metrics(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_engine: dict[str, dict[str, Any]] = {}
    for item in items:
        file_name = str(item.get("fileName") or item.get("path") or "")
        candidates = item.get("candidates") if isinstance(item.get("candidates"), list) else []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            engine = str(candidate.get("engine") or "unknown").strip() or "unknown"
            metrics = by_engine.setdefault(engine, {
                "engine": engine,
                "candidateCount": 0,
                "comparableCandidateCount": 0,
                "selectedCount": 0,
                "failedCandidateCount": 0,
                "featureCount": 0,
                "candidateTrimCount": 0,
                "differenceCount": 0,
                "rowCount": 0,
                "columnCount": 0,
                "nonEmptyCount": 0,
                "files": [],
            })
            metrics["candidateCount"] += 1
            if candidate.get("comparableTableDetected"):
                metrics["comparableCandidateCount"] += 1
            if candidate.get("selected"):
                metrics["selectedCount"] += 1
            if not candidate.get("comparableTableDetected") and str(candidate.get("message") or "").strip():
                metrics["failedCandidateCount"] += 1
            metrics["featureCount"] += _int_value(candidate, "featureCount")
            metrics["candidateTrimCount"] += _int_value(candidate, "candidateTrimCount")
            metrics["differenceCount"] += _int_value(candidate, "differenceCount")
            metrics["rowCount"] += _int_value(candidate, "rowCount")
            metrics["columnCount"] += _int_value(candidate, "columnCount")
            metrics["nonEmptyCount"] += _int_value(candidate, "nonEmptyCount")
            if file_name and file_name not in metrics["files"]:
                metrics["files"].append(file_name)

    return sorted(
        by_engine.values(),
        key=lambda metrics: (
            _int_value(metrics, "comparableCandidateCount"),
            _int_value(metrics, "selectedCount"),
            _int_value(metrics, "featureCount"),
            _int_value(metrics, "candidateTrimCount"),
            _int_value(metrics, "differenceCount"),
            _int_value(metrics, "nonEmptyCount"),
            -_int_value(metrics, "failedCandidateCount"),
            str(metrics.get("engine") or ""),
        ),
        reverse=True,
    )


def _engine_metric_score(metrics: dict[str, Any]) -> tuple[int, int, int, int, int, int, int]:
    return (
        _int_value(metrics, "comparableCandidateCount"),
        _int_value(metrics, "selectedCount"),
        _int_value(metrics, "featureCount"),
        _int_value(metrics, "candidateTrimCount"),
        _int_value(metrics, "differenceCount"),
        _int_value(metrics, "nonEmptyCount"),
        -_int_value(metrics, "failedCandidateCount"),
    )


def _engine_recommendation(items: list[dict[str, Any]]) -> dict[str, Any]:
    engine_metrics = _engine_candidate_metrics(items)
    total_candidates = sum(_int_value(metrics, "candidateCount") for metrics in engine_metrics)
    comparable_candidates = sum(_int_value(metrics, "comparableCandidateCount") for metrics in engine_metrics)
    if not engine_metrics or comparable_candidates == 0:
        return {
            "decision": "insufficient_evidence",
            "recommendedEngine": None,
            "confidence": "low",
            "reason": "No OCR engine produced a comparable configuration table in this audit sample.",
            "candidateEngineCount": len(engine_metrics),
            "candidateCount": total_candidates,
            "comparableCandidateCount": comparable_candidates,
            "engineMetrics": engine_metrics,
        }

    winner = engine_metrics[0]
    runner_up = engine_metrics[1] if len(engine_metrics) > 1 else None
    winner_score = _engine_metric_score(winner)
    runner_score = _engine_metric_score(runner_up) if runner_up else None
    recommended_engine = str(winner.get("engine") or "unknown")

    if runner_up is not None and winner_score == runner_score:
        decision = "tie_needs_more_samples"
        confidence = "low"
        reason = (
            f"{recommended_engine} ties with {runner_up.get('engine') or 'runner-up'} on comparable table quality; "
            "collect more scanned PDF/image samples before changing runtime preference."
        )
    elif runner_up is None:
        decision = "single_engine_candidate"
        confidence = "medium" if _int_value(winner, "selectedCount") > 0 else "low"
        reason = (
            f"{recommended_engine} is the only OCR engine with comparable candidates in this audit sample; "
            "keep it, but compare against another engine before treating it as a final benchmark."
        )
    else:
        decision = "use_recommended_engine"
        confidence = "high" if (
            _int_value(winner, "comparableCandidateCount") > _int_value(runner_up, "comparableCandidateCount")
            and _int_value(winner, "selectedCount") >= _int_value(runner_up, "selectedCount")
        ) else "medium"
        reason = (
            f"{recommended_engine} leads {runner_up.get('engine') or 'runner-up'} by "
            f"{_int_value(winner, 'comparableCandidateCount') - _int_value(runner_up, 'comparableCandidateCount'):+d} comparable candidates, "
            f"{_int_value(winner, 'featureCount') - _int_value(runner_up, 'featureCount'):+d} features, "
            f"{_int_value(winner, 'candidateTrimCount') - _int_value(runner_up, 'candidateTrimCount'):+d} trims, "
            f"{_int_value(winner, 'nonEmptyCount') - _int_value(runner_up, 'nonEmptyCount'):+d} non-empty cells."
        )

    return {
        "decision": decision,
        "recommendedEngine": recommended_engine if decision != "tie_needs_more_samples" else None,
        "confidence": confidence,
        "reason": reason,
        "candidateEngineCount": len(engine_metrics),
        "candidateCount": total_candidates,
        "comparableCandidateCount": comparable_candidates,
        "engineMetrics": engine_metrics,
        "runnerUpEngine": runner_up.get("engine") if runner_up else None,
    }


def _recommended_action(
    digest: dict[str, Any] | None,
    candidates: list[dict[str, Any]],
    selected_engine: str | None,
    ground_truth_qualification: dict[str, Any],
) -> str:
    if digest is None:
        return "digest_failed"
    groups = digest.get("compareGroups") if isinstance(digest.get("compareGroups"), list) else []
    has_temporary_identity = any(
        isinstance(group, dict)
        and (
            group.get("sourceKind") == "ocr_headerless"
            or group.get("identityStatus") == "temporary_ocr_column"
        )
        for group in groups
    )
    if selected_engine and digest.get("status") == "ready" and groups and not has_temporary_identity:
        if ground_truth_qualification.get("manualReviewRequired"):
            return "manual_review_ocr_ground_truth"
        return "use_selected_engine"
    if selected_engine and groups:
        return "manual_review_identity_or_rows"
    if digest.get("status") == "ready" and groups:
        return "use_text_or_structured_extraction"
    if candidates:
        return "manual_review_ocr_candidates"
    return "install_or_configure_ocr"


def _ground_truth_qualification(
    digest: dict[str, Any],
    selected_engine: str | None,
) -> dict[str, Any]:
    """Keep OCR parser scoring separate from factual source verification.

    Parser scores only compare candidate shape and cannot establish that an
    arbitrary screenshot is a configuration table or that every value matches
    its source. OCR uploads therefore require a reviewed labelled sample before
    an audit can be treated as a runtime selection benchmark.
    """
    digest_type = str(digest.get("digestType") or "").lower()
    source_format = str(digest.get("sourceFormat") or "").lower()
    is_ocr = bool(selected_engine) or "ocr" in digest_type or "image" in source_format
    if not is_ocr:
        return {
            "status": "not_required",
            "manualReviewRequired": False,
            "reason": "Structured/text extraction is outside this OCR ground-truth gate.",
        }
    return {
        "status": "unverified",
        "manualReviewRequired": True,
        "reason": (
            "OCR engine scoring measures parseability only. Verify the original source type, "
            "trim headers, and sampled values against a labelled configuration-table source before use."
        ),
    }


def audit_source_file(path: Path) -> dict[str, Any]:
    if build_source_digest is None:
        return {
            "path": _display_path(path),
            "fileName": path.name,
            "status": "failed",
            "error": f"Cannot import Source Digest parser: {BUILD_SOURCE_DIGEST_IMPORT_ERROR}",
            "recommendedAction": "fix_runtime_import",
            "candidates": [],
            "groups": [],
        }

    try:
        digest = build_source_digest(path, path.name)
    except Exception as exc:  # pragma: no cover - defensive runtime report.
        return {
            "path": _display_path(path),
            "fileName": path.name,
            "status": "failed",
            "error": str(exc),
            "recommendedAction": "digest_failed",
            "candidates": [],
            "groups": [],
        }

    if not isinstance(digest, dict):
        return {
            "path": _display_path(path),
            "fileName": path.name,
            "status": "failed",
            "error": "Source Digest returned no payload.",
            "recommendedAction": "digest_failed",
            "candidates": [],
            "groups": [],
        }

    evaluation = digest.get("ocrEvaluation") if isinstance(digest.get("ocrEvaluation"), dict) else {}
    summary = digest.get("summary") if isinstance(digest.get("summary"), dict) else {}
    raw_candidates = digest.get("ocrEngineCandidates") if isinstance(digest.get("ocrEngineCandidates"), list) else []
    candidates = [_candidate_payload(candidate) for candidate in raw_candidates if isinstance(candidate, dict)]
    selected_engine = evaluation.get("selectedEngine") or digest.get("ocrEngine")
    selected_engine_text = str(selected_engine) if selected_engine else None
    digest_type = str(digest.get("digestType") or "")
    source_format = str(digest.get("sourceFormat") or "")
    selected_vs_alternates = _selected_vs_alternates(candidates, selected_engine_text)
    ground_truth_qualification = _ground_truth_qualification(digest, selected_engine_text)
    return {
        "path": _display_path(path),
        "fileName": path.name,
        "status": str(digest.get("status") or "unknown"),
        "digestType": digest_type,
        "sourceFormat": source_format,
        "selectedEngine": selected_engine_text,
        "candidateCount": _int_value(evaluation, "candidateCount") or len(candidates),
        "comparableCandidateCount": _int_value(evaluation, "comparableCandidateCount"),
        "selectedReasonDetails": [
            _safe_text(item)
            for item in evaluation.get("selectedReasonDetails") or []
            if str(item).strip()
        ],
        "summary": {
            "comparableGroupCount": _int_value(summary, "comparableGroupCount"),
            "candidateTrimCount": _int_value(summary, "candidateTrimCount"),
            "featureCount": _int_value(summary, "featureCount"),
            "differenceCount": _int_value(summary, "differenceCount"),
        },
        "message": _safe_text(digest.get("message") or digest.get("errorMessage")),
        "recommendedAction": _recommended_action(
            digest,
            candidates,
            selected_engine_text,
            ground_truth_qualification,
        ),
        "groundTruthQualification": ground_truth_qualification,
        "candidates": candidates,
        "ocrComparisonStatus": _ocr_comparison_status(
            candidates=candidates,
            selected_engine=selected_engine_text,
            digest_type=digest_type,
            source_format=source_format,
        ),
        "selectedVsAlternates": selected_vs_alternates,
        "groups": _group_summary(digest),
    }


def _summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts = Counter(str(item.get("status") or "unknown") for item in items)
    action_counts = Counter(str(item.get("recommendedAction") or "unknown") for item in items)
    selected_engine_counts = Counter(
        str(item.get("selectedEngine"))
        for item in items
        if item.get("selectedEngine")
    )
    comparison_status_counts = Counter(
        str(item.get("ocrComparisonStatus") or "unknown")
        for item in items
    )
    ready_items = [
        item for item in items
        if item.get("status") == "ready"
        and _int_value(item.get("summary") if isinstance(item.get("summary"), dict) else {}, "comparableGroupCount") > 0
    ]
    ground_truth_review_items = [
        item for item in items
        if isinstance(item.get("groundTruthQualification"), dict)
        and item["groundTruthQualification"].get("manualReviewRequired")
    ]
    return {
        "fileCount": len(items),
        "readyComparableFileCount": len(ready_items),
        "groundTruthQualifiedFileCount": 0,
        "groundTruthReviewRequiredFileCount": len(ground_truth_review_items),
        "pendingOrFailedFileCount": sum(1 for item in items if item.get("status") != "ready"),
        "statusCounts": dict(status_counts),
        "recommendedActionCounts": dict(action_counts),
        "selectedEngineCounts": dict(selected_engine_counts),
        "ocrComparisonStatusCounts": dict(comparison_status_counts),
        "candidateCount": sum(_int_value(item, "candidateCount") for item in items),
        "comparableCandidateCount": sum(_int_value(item, "comparableCandidateCount") for item in items),
        "engineRecommendation": _engine_recommendation(items),
    }


def build_report(paths: Sequence[Path], *, recursive: bool = False) -> dict[str, Any]:
    files = _iter_source_files(paths, recursive=recursive)
    items = [audit_source_file(path) for path in files]
    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAtUtc": _utc_now_iso(),
        "runtime": _runtime_environment(),
        "inputPaths": [_display_path(path) for path in paths],
        "recursive": recursive,
        "supportedExtensions": sorted(SUPPORTED_EXTENSIONS),
        "summary": _summary(items),
        "items": items,
    }


def _markdown_cell(value: Any) -> str:
    return str(value if value not in (None, "") else "-").replace("\n", " ").replace("|", "\\|")


def render_markdown(report: dict[str, Any], *, limit: int = DEFAULT_MARKDOWN_LIMIT) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
    modules = runtime.get("modules") if isinstance(runtime.get("modules"), dict) else {}
    recommendation = (
        summary.get("engineRecommendation")
        if isinstance(summary.get("engineRecommendation"), dict)
        else {}
    )
    items = [item for item in report.get("items") or [] if isinstance(item, dict)]
    lines = [
        "# Engineering Config OCR Quality Audit",
        "",
        f"**Generated:** {report.get('generatedAtUtc', '-')}",
        f"**Files:** {summary.get('fileCount', 0)}",
        f"**Ready comparable:** {summary.get('readyComparableFileCount', 0)}",
        f"**OCR ground-truth qualified:** {summary.get('groundTruthQualifiedFileCount', 0)}",
        f"**OCR ground-truth review required:** {summary.get('groundTruthReviewRequiredFileCount', 0)}",
        f"**Pending / failed:** {summary.get('pendingOrFailedFileCount', 0)}",
        "",
        "## Runtime",
        "",
        f"- **Python:** {_markdown_cell(runtime.get('pythonExecutable'))} ({_markdown_cell(runtime.get('pythonVersion'))})",
        f"- **Source Digest import:** {_markdown_cell('ok' if runtime.get('sourceDigestImportOk') else runtime.get('sourceDigestImportError') or 'failed')}",
    ]
    for module_name in ("paddleocr", "paddle", "pypdfium2"):
        module_status = modules.get(module_name) if isinstance(modules.get(module_name), dict) else {}
        availability = "available" if module_status.get("available") else "missing"
        version = module_status.get("version")
        lines.append(f"- **{module_name}:** {availability}{f' {version}' if version else ''}")
    lines.extend([
        "",
        "## Engine Recommendation",
        "",
        f"- **Decision:** {_markdown_cell(recommendation.get('decision'))}",
        f"- **Recommended engine:** {_markdown_cell(recommendation.get('recommendedEngine'))}",
        f"- **Confidence:** {_markdown_cell(recommendation.get('confidence'))}",
        f"- **Reason:** {_markdown_cell(recommendation.get('reason'))}",
        "",
    ])
    lines.extend([
        "| Engine | Candidates | Comparable | Selected | Features | Trims | Non-empty | Failures |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ])
    engine_metrics = recommendation.get("engineMetrics") if isinstance(recommendation.get("engineMetrics"), list) else []
    if engine_metrics:
        for metrics in engine_metrics:
            if not isinstance(metrics, dict):
                continue
            lines.append(
                "| "
                f"{_markdown_cell(metrics.get('engine'))} | "
                f"{_markdown_cell(metrics.get('candidateCount'))} | "
                f"{_markdown_cell(metrics.get('comparableCandidateCount'))} | "
                f"{_markdown_cell(metrics.get('selectedCount'))} | "
                f"{_markdown_cell(metrics.get('featureCount'))} | "
                f"{_markdown_cell(metrics.get('candidateTrimCount'))} | "
                f"{_markdown_cell(metrics.get('nonEmptyCount'))} | "
                f"{_markdown_cell(metrics.get('failedCandidateCount'))} |"
            )
    else:
        lines.append("| - | 0 | 0 | 0 | 0 | 0 | 0 | 0 |")
    lines.extend([
        "",
        "## Engine Selection",
        "",
        "| Engine | Count |",
        "|---|---:|",
    ])
    selected_counts = summary.get("selectedEngineCounts") if isinstance(summary.get("selectedEngineCounts"), dict) else {}
    if selected_counts:
        for engine, count in sorted(selected_counts.items()):
            lines.append(f"| {_markdown_cell(engine)} | {_markdown_cell(count)} |")
    else:
        lines.append("| - | 0 |")
    lines.extend([
        "",
        "## Files",
        "",
        "| File | Status | Selected OCR | Groups | Features | Action | Qualification | OCR Compare | OCR Delta | Reason |",
        "|---|---|---|---:|---:|---|---|---|---|---|",
    ])
    for item in items[:limit]:
        item_summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
        reason = "; ".join(item.get("selectedReasonDetails") or []) or item.get("message") or ""
        qualification = item.get("groundTruthQualification") if isinstance(item.get("groundTruthQualification"), dict) else {}
        delta = _selected_vs_alternates_text(item)
        comparison_status = _ocr_comparison_status_text(item)
        lines.append(
            "| "
            f"{_markdown_cell(item.get('path'))} | "
            f"{_markdown_cell(item.get('status'))} | "
            f"{_markdown_cell(item.get('selectedEngine'))} | "
            f"{_markdown_cell(item_summary.get('comparableGroupCount'))} | "
            f"{_markdown_cell(item_summary.get('featureCount'))} | "
            f"{_markdown_cell(item.get('recommendedAction'))} | "
            f"{_markdown_cell(qualification.get('status'))} | "
            f"{_markdown_cell(comparison_status)} | "
            f"{_markdown_cell(delta)} | "
            f"{_markdown_cell(reason)} |"
        )
    if len(items) > limit:
        lines.append(f"| ... | ... | ... | ... | ... | ... | ... | ... | ... | {len(items) - limit} more files omitted |")
    return "\n".join(lines) + "\n"


def write_report_artifacts(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = _history_suffix(report)
    json_path = output_dir / f"engineering_config_ocr_quality_audit_{suffix}.json"
    markdown_path = output_dir / f"engineering_config_ocr_quality_audit_{suffix}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, markdown_path


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path, help="PDF/image file or directory paths to audit.")
    parser.add_argument("--recursive", action="store_true", help="Recursively scan directories.")
    parser.add_argument("--json-output", type=Path, help="Write the full JSON report to this path.")
    parser.add_argument("--markdown-output", type=Path, help="Write a Markdown summary to this path.")
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        help="Write timestamped JSON and Markdown artifacts into this directory.",
    )
    parser.add_argument("--markdown", action="store_true", help="Print Markdown instead of JSON.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_report(args.paths, recursive=bool(args.recursive))
    if args.artifact_dir:
        json_path, markdown_path = write_report_artifacts(report, args.artifact_dir)
        report["artifactRefs"] = [_display_path(json_path), _display_path(markdown_path)]
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.markdown_output:
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(render_markdown(report), encoding="utf-8")
    if args.markdown:
        print(render_markdown(report), end="")
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
