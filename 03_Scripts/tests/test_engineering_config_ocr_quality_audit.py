from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "diagnostics"
    / "engineering_config_ocr_quality_audit.py"
)


def load_module():
    module_name = "engineering_config_ocr_quality_audit_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


audit_module = load_module()


def test_build_report_summarizes_selected_ocr_engine(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "config.png"
    source_path.write_bytes(b"fake image")
    (tmp_path / "ignored.txt").write_text("not an OCR source", encoding="utf-8")

    def fake_digest(_path: Path, _name: str) -> dict:
        return {
            "status": "ready",
            "digestType": "image_ocr",
            "sourceFormat": "image_ocr",
            "ocrEngine": "paddleocr",
            "ocrEvaluation": {
                "candidateCount": 2,
                "comparableCandidateCount": 1,
                "selectedEngine": "paddleocr",
                "selectedReasonDetails": [
                    "paddleocr 识别到可比配置表；tesseract 未形成可比配置表。",
                ],
            },
            "ocrEngineCandidates": [
                {
                    "engine": "paddleocr",
                    "selected": True,
                    "comparableTableDetected": True,
                    "score": {
                        "featureCount": 12,
                        "candidateTrimCount": 3,
                        "differenceCount": 4,
                        "rowCount": 14,
                        "columnCount": 4,
                        "nonEmptyCount": 42,
                    },
                    "lineCount": 14,
                    "textPreview": "Feature | Basic | Premium",
                },
                {
                    "engine": "tesseract",
                    "selected": False,
                    "comparableTableDetected": False,
                    "score": {
                        "featureCount": 0,
                        "candidateTrimCount": 0,
                        "differenceCount": 0,
                        "nonEmptyCount": 3,
                    },
                    "message": "tesseract OCR text did not contain comparable table rows.",
                },
            ],
            "summary": {
                "comparableGroupCount": 1,
                "candidateTrimCount": 3,
                "featureCount": 12,
                "differenceCount": 4,
            },
            "compareGroups": [
                {
                    "groupId": "group-1",
                    "modelName": "Config Model",
                    "trimCount": 3,
                    "featureCount": 12,
                    "differenceCount": 4,
                    "rows": [],
                },
            ],
        }

    monkeypatch.setattr(audit_module, "build_source_digest", fake_digest)

    report = audit_module.build_report([tmp_path])

    assert report["schemaVersion"] == audit_module.SCHEMA_VERSION
    assert report["runtime"]["pythonExecutable"] == sys.executable
    assert report["runtime"]["sourceDigestImportOk"] is True
    assert "paddleocr" in report["runtime"]["modules"]
    assert "paddle" in report["runtime"]["modules"]
    assert report["summary"]["fileCount"] == 1
    assert report["summary"]["readyComparableFileCount"] == 1
    assert report["summary"]["selectedEngineCounts"] == {"paddleocr": 1}
    assert report["summary"]["ocrComparisonStatusCounts"] == {"selected_compared_with_alternates": 1}
    assert report["summary"]["candidateCount"] == 2
    assert report["summary"]["comparableCandidateCount"] == 1
    recommendation = report["summary"]["engineRecommendation"]
    assert recommendation["decision"] == "use_recommended_engine"
    assert recommendation["recommendedEngine"] == "paddleocr"
    assert recommendation["confidence"] == "high"
    assert recommendation["engineMetrics"][0]["engine"] == "paddleocr"
    item = report["items"][0]
    assert item["selectedEngine"] == "paddleocr"
    assert item["ocrComparisonStatus"] == "selected_compared_with_alternates"
    assert item["recommendedAction"] == "manual_review_ocr_ground_truth"
    assert item["groundTruthQualification"] == {
        "status": "unverified",
        "manualReviewRequired": True,
        "reason": "OCR engine scoring measures parseability only. Verify the original source type, trim headers, and sampled values against a labelled configuration-table source before use.",
    }
    assert report["summary"]["groundTruthQualifiedFileCount"] == 0
    assert report["summary"]["groundTruthReviewRequiredFileCount"] == 1
    assert item["candidates"][0]["featureCount"] == 12
    assert item["candidates"][0]["nonEmptyCount"] == 42
    assert item["candidates"][1]["message"] == "tesseract OCR text did not contain comparable table rows."
    assert item["selectedVsAlternates"] == [
        {
            "engine": "tesseract",
            "featureDelta": 12,
            "candidateTrimDelta": 3,
            "differenceDelta": 4,
            "nonEmptyDelta": 39,
            "selectedComparable": True,
            "alternateComparable": False,
            "alternateMessage": "tesseract OCR text did not contain comparable table rows.",
        },
    ]
    markdown = audit_module.render_markdown(report)
    assert "## Runtime" in markdown
    assert f"**Python:** {sys.executable}" in markdown
    assert "## Engine Recommendation" in markdown
    assert "**Recommended engine:** paddleocr" in markdown
    assert "**OCR ground-truth qualified:** 0" in markdown
    assert "compared" in markdown
    assert "vs tesseract: +12 features, +3 trims, +4 diffs, +39 non-empty" in markdown


def test_engine_recommendation_can_choose_legacy_when_it_beats_paddle(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "legacy-wins.png"
    source_path.write_bytes(b"fake image")

    monkeypatch.setattr(audit_module, "build_source_digest", lambda _path, _name: {
        "status": "ready",
        "digestType": "image_ocr",
        "sourceFormat": "image_ocr",
        "ocrEngine": "legacy_image_ocr",
        "ocrEvaluation": {
            "candidateCount": 2,
            "comparableCandidateCount": 1,
            "selectedEngine": "legacy_image_ocr",
        },
        "ocrEngineCandidates": [
            {
                "engine": "paddleocr",
                "selected": False,
                "comparableTableDetected": False,
                "score": {"featureCount": 1, "candidateTrimCount": 1, "differenceCount": 0, "nonEmptyCount": 8},
                "message": "paddleocr OCR text did not contain comparable table rows.",
            },
            {
                "engine": "legacy_image_ocr",
                "selected": True,
                "comparableTableDetected": True,
                "score": {
                    "featureCount": 8,
                    "candidateTrimCount": 3,
                    "differenceCount": 5,
                    "rowCount": 9,
                    "columnCount": 4,
                    "nonEmptyCount": 36,
                },
            },
        ],
        "summary": {"comparableGroupCount": 1, "candidateTrimCount": 3, "featureCount": 8, "differenceCount": 5},
        "compareGroups": [
            {
                "groupId": "legacy",
                "modelName": "Legacy Better",
                "trimCount": 3,
                "featureCount": 8,
                "differenceCount": 5,
                "rows": [],
            },
        ],
    })

    report = audit_module.build_report([source_path])

    recommendation = report["summary"]["engineRecommendation"]
    assert recommendation["decision"] == "use_recommended_engine"
    assert recommendation["recommendedEngine"] == "legacy_image_ocr"
    assert recommendation["confidence"] == "high"
    assert recommendation["runnerUpEngine"] == "paddleocr"
    assert recommendation["engineMetrics"][0]["engine"] == "legacy_image_ocr"
    assert "legacy_image_ocr" in audit_module.render_markdown(report)


def test_build_report_marks_selected_ocr_without_alternate_candidate(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "config-card.png"
    source_path.write_bytes(b"fake image")

    monkeypatch.setattr(audit_module, "build_source_digest", lambda _path, _name: {
        "status": "ready",
        "digestType": "image_ocr",
        "sourceFormat": "image_ocr",
        "ocrEngine": "paddleocr",
        "ocrEvaluation": {
            "candidateCount": 1,
            "comparableCandidateCount": 1,
            "selectedEngine": "paddleocr",
        },
        "ocrEngineCandidates": [
            {
                "engine": "paddleocr",
                "selected": True,
                "comparableTableDetected": True,
                "score": {"featureCount": 6, "candidateTrimCount": 3, "differenceCount": 4},
            },
        ],
        "summary": {"comparableGroupCount": 1, "candidateTrimCount": 3, "featureCount": 6, "differenceCount": 4},
        "compareGroups": [
            {
                "groupId": "image-ocr",
                "modelName": "OCR Model",
                "trimCount": 3,
                "featureCount": 6,
                "differenceCount": 4,
                "rows": [],
            },
        ],
    })

    report = audit_module.build_report([source_path])

    item = report["items"][0]
    assert item["selectedEngine"] == "paddleocr"
    assert item["ocrComparisonStatus"] == "selected_without_alternate_candidate"
    assert item["selectedVsAlternates"] == []
    assert report["summary"]["ocrComparisonStatusCounts"] == {"selected_without_alternate_candidate": 1}
    markdown = audit_module.render_markdown(report)
    assert "no alternate OCR candidate" in markdown


def test_build_report_marks_temporary_ocr_identity_for_review(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "scan.pdf"
    source_path.write_bytes(b"%PDF fake")

    monkeypatch.setattr(audit_module, "build_source_digest", lambda _path, _name: {
        "status": "ready",
        "digestType": "pdf_ocr",
        "sourceFormat": "pdf_ocr",
        "ocrEngine": "paddleocr",
        "ocrEvaluation": {
            "candidateCount": 1,
            "comparableCandidateCount": 1,
            "selectedEngine": "paddleocr",
        },
        "ocrEngineCandidates": [
            {
                "engine": "paddleocr",
                "selected": True,
                "comparableTableDetected": True,
                "score": {"featureCount": 5, "candidateTrimCount": 2, "differenceCount": 2},
            },
        ],
        "summary": {"comparableGroupCount": 1, "candidateTrimCount": 2, "featureCount": 5, "differenceCount": 2},
        "compareGroups": [
            {
                "groupId": "ocr-headerless",
                "modelName": "OCR Image 1",
                "sourceKind": "ocr_headerless",
                "identityStatus": "temporary_ocr_column",
                "trimCount": 2,
                "featureCount": 5,
                "differenceCount": 2,
                "rows": [{"reviewNotes": ["long mixed value"]}],
            },
        ],
    })

    report = audit_module.build_report([source_path])

    item = report["items"][0]
    assert item["recommendedAction"] == "manual_review_identity_or_rows"
    assert item["groups"][0]["identityStatus"] == "temporary_ocr_column"
    assert item["groups"][0]["reviewRowCount"] == 1


def test_build_report_treats_ready_text_pdf_as_non_ocr_success(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "text-config.pdf"
    source_path.write_bytes(b"%PDF text")

    monkeypatch.setattr(audit_module, "build_source_digest", lambda _path, _name: {
        "status": "ready",
        "digestType": "pdf_text",
        "sourceFormat": "pdf_text",
        "summary": {"comparableGroupCount": 1, "candidateTrimCount": 2, "featureCount": 2, "differenceCount": 2},
        "compareGroups": [
            {
                "groupId": "pdf-text",
                "modelName": "PDF Text Model",
                "trimCount": 2,
                "featureCount": 2,
                "differenceCount": 2,
                "rows": [],
            },
        ],
    })

    report = audit_module.build_report([source_path])

    item = report["items"][0]
    assert item["selectedEngine"] is None
    assert item["ocrComparisonStatus"] == "not_ocr_path"
    assert item["recommendedAction"] == "use_text_or_structured_extraction"
    assert report["summary"]["readyComparableFileCount"] == 1


def test_main_writes_json_and_markdown_outputs(tmp_path, monkeypatch, capsys) -> None:
    source_path = tmp_path / "pending.jpg"
    source_path.write_bytes(b"fake image")
    json_path = tmp_path / "audit.json"
    markdown_path = tmp_path / "audit.md"

    monkeypatch.setattr(audit_module, "build_source_digest", lambda _path, _name: {
        "status": "pending",
        "digestType": "image_ocr",
        "sourceFormat": "image_ocr",
        "message": "OCR engine is not configured.",
        "summary": {"comparableGroupCount": 0, "candidateTrimCount": 0, "featureCount": 0, "differenceCount": 0},
        "compareGroups": [],
    })

    exit_code = audit_module.main([
        str(source_path),
        "--json-output",
        str(json_path),
        "--markdown-output",
        str(markdown_path),
        "--markdown",
    ])

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "Engineering Config OCR Quality Audit" in captured.out
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["summary"]["pendingOrFailedFileCount"] == 1
    assert payload["summary"]["engineRecommendation"]["decision"] == "insufficient_evidence"
    assert payload["summary"]["engineRecommendation"]["recommendedEngine"] is None
    assert payload["items"][0]["recommendedAction"] == "install_or_configure_ocr"
    assert payload["items"][0]["ocrComparisonStatus"] == "ocr_not_configured_or_no_candidates"
    assert "OCR engine is not configured." in markdown_path.read_text(encoding="utf-8")
