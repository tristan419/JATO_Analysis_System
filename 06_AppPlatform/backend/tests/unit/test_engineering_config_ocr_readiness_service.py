from __future__ import annotations

from typing import Any

from app.api.routes import engineering_config
from app.services import engineering_config_ocr_readiness_service as readiness


def _module_probe(available_modules: set[str]):
    def probe(module_name: str) -> bool:
        return module_name in available_modules

    return probe


def test_ocr_readiness_reports_not_configured_when_engines_are_missing(monkeypatch) -> None:
    monkeypatch.delenv("JATO_CONFIG_OCR_COMMAND", raising=False)
    monkeypatch.delenv("JATO_TESSERACT_COMMAND", raising=False)
    monkeypatch.setattr(readiness, "_module_available", _module_probe(set()))
    monkeypatch.setattr(readiness, "_which", lambda _executable: None)

    payload = readiness.get_engineering_config_ocr_readiness()

    assert payload["status"] == "not_configured"
    assert payload["ready"] is False
    assert payload["defaultEngine"] is None
    assert payload["imageOcrReady"] is False
    assert payload["pdfOcrReady"] is False
    assert payload["paddleOcrReady"] is False
    assert payload["legacyOcrReady"] is False
    assert "No OCR engine is available" in " ".join(payload["warnings"])


def test_ocr_readiness_reports_paddle_stack_ready(monkeypatch) -> None:
    monkeypatch.delenv("JATO_CONFIG_OCR_COMMAND", raising=False)
    monkeypatch.setenv("JATO_PADDLEOCR_LANG", "en")
    monkeypatch.setattr(
        readiness,
        "_module_available",
        _module_probe({"pypdfium2", "paddleocr", "paddle"}),
    )
    monkeypatch.setattr(readiness, "_which", lambda _executable: None)

    payload = readiness.get_engineering_config_ocr_readiness()

    assert payload["status"] == "ready"
    assert payload["ready"] is True
    assert payload["defaultEngine"] == "paddleocr"
    assert payload["imageOcrReady"] is True
    assert payload["pdfOcrReady"] is True
    assert payload["pdfRenderReady"] is True
    assert payload["paddleOcrReady"] is True
    assert payload["legacyOcrReady"] is False
    assert payload["configuredLanguage"] == "en"
    assert payload["warnings"] == []


def test_ocr_readiness_reports_degraded_image_only_tesseract(monkeypatch) -> None:
    monkeypatch.delenv("JATO_CONFIG_OCR_COMMAND", raising=False)
    monkeypatch.setenv("JATO_TESSERACT_COMMAND", "tesseract")
    monkeypatch.setattr(readiness, "_module_available", _module_probe(set()))
    monkeypatch.setattr(readiness, "_which", lambda executable: "/usr/bin/tesseract" if executable == "tesseract" else None)
    monkeypatch.setattr(readiness, "_command_version", lambda path: "tesseract 5.5.2" if path == "/usr/bin/tesseract" else None)

    payload = readiness.get_engineering_config_ocr_readiness()

    assert payload["status"] == "degraded"
    assert payload["ready"] is False
    assert payload["defaultEngine"] == "tesseract"
    assert payload["imageOcrReady"] is True
    assert payload["pdfOcrReady"] is False
    assert payload["legacyOcrReady"] is True
    assert "pypdfium2 is missing" in " ".join(payload["warnings"])
    tesseract_component = next(component for component in payload["components"] if component["name"] == "tesseract")
    assert tesseract_component["path"] == "/usr/bin/tesseract"
    assert tesseract_component["version"] == "tesseract 5.5.2"


def test_ocr_readiness_reports_custom_command_identity(monkeypatch) -> None:
    monkeypatch.setenv("JATO_CONFIG_OCR_COMMAND", "/opt/jato/bin/legacy-ocr --table {input}")
    monkeypatch.delenv("JATO_TESSERACT_COMMAND", raising=False)
    monkeypatch.setattr(readiness, "_module_available", _module_probe({"pypdfium2"}))
    monkeypatch.setattr(readiness.Path, "exists", lambda self: str(self) == "/opt/jato/bin/legacy-ocr")
    monkeypatch.setattr(readiness, "_which", lambda _executable: None)

    payload = readiness.get_engineering_config_ocr_readiness()

    assert payload["status"] == "ready"
    assert payload["defaultEngine"] == "legacy-ocr"
    assert payload["imageOcrReady"] is True
    assert payload["pdfOcrReady"] is True
    assert payload["legacyOcrReady"] is True
    custom_component = next(component for component in payload["components"] if component["name"] == "legacy-ocr")
    assert custom_component["available"] is True
    assert custom_component["path"] == "/opt/jato/bin/legacy-ocr"
    assert custom_component["command"] == "/opt/jato/bin/legacy-ocr --table {input}"


def test_engineering_config_ocr_readiness_route_uses_service(monkeypatch) -> None:
    expected: dict[str, Any] = {"status": "ready", "ready": True}
    monkeypatch.setattr(engineering_config, "get_engineering_config_ocr_readiness", lambda: expected)

    assert engineering_config.get_ocr_readiness() == expected
