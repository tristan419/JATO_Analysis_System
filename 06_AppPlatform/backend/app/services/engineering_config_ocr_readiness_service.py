"""Runtime readiness report for engineering config OCR dependencies."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import shlex
import shutil
import subprocess
from typing import Any


def _module_available(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def _which(executable: str) -> str | None:
    return shutil.which(executable)


def _custom_ocr_command_name(raw_command: str) -> str | None:
    try:
        parts = shlex.split(raw_command)
    except ValueError:
        return None
    if not parts:
        return None
    return Path(parts[0]).name or "custom_ocr"


def _command_parts(raw_command: str) -> list[str]:
    try:
        parts = shlex.split(raw_command)
    except ValueError:
        return []
    return parts


def _resolve_executable_path(executable: str) -> str | None:
    executable_path = Path(executable)
    if executable_path.is_absolute() or "/" in executable:
        return str(executable_path) if executable_path.exists() else None
    return _which(executable)


def _custom_ocr_executable_path(raw_command: str) -> str | None:
    parts = _command_parts(raw_command)
    if not parts:
        return None
    return _resolve_executable_path(parts[0])


def _custom_ocr_executable_available(raw_command: str) -> bool:
    return _custom_ocr_executable_path(raw_command) is not None


def _command_version(executable_path: str | None) -> str | None:
    if not executable_path:
        return None
    try:
        completed = subprocess.run(
            [executable_path, "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except Exception:
        return None
    if completed.returncode != 0:
        return None
    lines = [line.strip() for line in (completed.stdout or "").splitlines() if line.strip()]
    return lines[0] if lines else None


def _component(
    name: str,
    available: bool,
    detail: str,
    *,
    executable_path: str | None = None,
    version: str | None = None,
    command: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": name,
        "available": available,
        "detail": detail,
    }
    if executable_path:
        payload["path"] = executable_path
    if version:
        payload["version"] = version
    if command:
        payload["command"] = command
    return payload


def get_engineering_config_ocr_readiness() -> dict[str, Any]:
    """Return a lightweight OCR readiness report without initializing models."""
    pypdfium2_available = _module_available("pypdfium2")
    paddleocr_available = _module_available("paddleocr")
    paddlepaddle_available = _module_available("paddle")
    raw_custom_command = os.environ.get("JATO_CONFIG_OCR_COMMAND", "").strip()
    custom_command_name = _custom_ocr_command_name(raw_custom_command) if raw_custom_command else None
    custom_command_path = _custom_ocr_executable_path(raw_custom_command) if raw_custom_command else None
    custom_command_available = (
        custom_command_path is not None
        if raw_custom_command
        else False
    )
    tesseract_command = os.environ.get("JATO_TESSERACT_COMMAND", "tesseract").strip() or "tesseract"
    tesseract_path = _which(tesseract_command)
    tesseract_available = tesseract_path is not None

    paddle_ready = paddleocr_available and paddlepaddle_available
    ocr_engine_ready = custom_command_available or paddle_ready or tesseract_available
    pdf_ocr_ready = pypdfium2_available and ocr_engine_ready
    default_engine = (
        custom_command_name
        if custom_command_available and custom_command_name
        else "paddleocr"
        if paddle_ready
        else "tesseract"
        if tesseract_available
        else None
    )

    warnings: list[str] = []
    if paddleocr_available and not paddlepaddle_available:
        warnings.append("paddleocr is installed but paddlepaddle runtime is missing.")
    if not pypdfium2_available:
        warnings.append("pypdfium2 is missing; scanned PDF pages cannot be rendered for OCR.")
    if raw_custom_command and not custom_command_available:
        warnings.append("JATO_CONFIG_OCR_COMMAND is set but its executable is not available.")
    if not ocr_engine_ready:
        warnings.append("No OCR engine is available for scanned PDF/image source digest.")

    image_ocr_ready = ocr_engine_ready
    status = "ready" if image_ocr_ready else "not_configured"
    if ocr_engine_ready and not pdf_ocr_ready:
        status = "degraded"

    return {
        "status": status,
        "ready": status == "ready",
        "defaultEngine": default_engine,
        "imageOcrReady": image_ocr_ready,
        "pdfOcrReady": pdf_ocr_ready,
        "pdfRenderReady": pypdfium2_available,
        "paddleOcrReady": paddle_ready,
        "legacyOcrReady": tesseract_available or custom_command_available,
        "configuredLanguage": os.environ.get("JATO_PADDLEOCR_LANG", "ch"),
        "components": [
            _component("pypdfium2", pypdfium2_available, "Scanned PDF page rendering"),
            _component("paddleocr", paddleocr_available, "PaddleOCR wrapper package"),
            _component("paddlepaddle", paddlepaddle_available, "PaddlePaddle inference runtime"),
            _component(
                custom_command_name or "custom_ocr",
                custom_command_available,
                "JATO_CONFIG_OCR_COMMAND" if raw_custom_command else "Custom OCR command not configured",
                executable_path=custom_command_path,
                command=raw_custom_command or None,
            ),
            _component(
                "tesseract",
                tesseract_available,
                f"Command: {tesseract_command}",
                executable_path=tesseract_path,
                version=_command_version(tesseract_path),
            ),
        ],
        "warnings": warnings,
        "notes": [
            "This readiness check does not initialize PaddleOCR or download models.",
            "Use the OCR quality audit script with real scanned PDF/image samples to verify extraction quality.",
        ],
    }
