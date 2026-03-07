import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SENSITIVE_VALUE_PATTERN = re.compile(
    r"(?i)(password|secret|token|api[_-]?key)\s*[:=]\s*([^\s,;]+)"
)


class SanitizedTextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.now(timezone.utc).isoformat()
        message = sanitize_log_text(record.getMessage())
        job_id = str(getattr(record, "jobId", "")).strip()
        if job_id:
            message = f"jobId={job_id} {message}"
        return (
            f"{timestamp} | {record.levelname} | "
            f"{record.name} | {message}"
        )


class SanitizedJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "message": sanitize_log_text(record.getMessage()),
        }
        job_id = str(getattr(record, "jobId", "")).strip()
        if job_id:
            payload["jobId"] = job_id
        return json.dumps(payload, ensure_ascii=False)


def env_flag_enabled(name: str) -> bool:
    value = str(os.getenv(name, "")).strip().lower()
    return value in {"1", "true", "yes", "on"}


def sanitize_log_text(message: str) -> str:
    sanitized = str(message)
    sanitized = sanitized.replace(str(PROJECT_ROOT), "<project_root>")
    sanitized = SENSITIVE_VALUE_PATTERN.sub(r"\1=<redacted>", sanitized)
    return sanitized


def build_job_id(prefix: str = "job") -> str:
    suffix = uuid4().hex[:8]
    return f"{prefix}-{suffix}"


def get_logger(name: str, job_id: str | None = None) -> logging.LoggerAdapter:
    logger = logging.getLogger(name)
    if not logger.handlers:
        level_name = os.getenv("JATO_LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
        logger.setLevel(level)

        handler = logging.StreamHandler()
        if env_flag_enabled("JATO_LOG_JSON"):
            handler.setFormatter(SanitizedJsonFormatter())
        else:
            handler.setFormatter(SanitizedTextFormatter())

        logger.addHandler(handler)
        logger.propagate = False

    return logging.LoggerAdapter(logger, {"jobId": job_id or ""})
