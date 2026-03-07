import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SENSITIVE_VALUE_PATTERN = re.compile(
    r"(?i)(password|secret|token|api[_-]?key)\s*[:=]\s*([^\s,;]+)"
)


def sanitize_log_text(message: str) -> str:
    sanitized = str(message)
    sanitized = sanitized.replace(str(PROJECT_ROOT), "<project_root>")
    sanitized = SENSITIVE_VALUE_PATTERN.sub(r"\1=<redacted>", sanitized)
    return sanitized


def env_flag_enabled(name: str) -> bool:
    value = str(os.getenv(name, "")).strip().lower()
    return value in {"1", "true", "yes", "on"}


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "message": sanitize_log_text(record.getMessage()),
        }
        return json.dumps(payload, ensure_ascii=False)


class TextLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.now(timezone.utc).isoformat()
        message = sanitize_log_text(record.getMessage())
        return (
            f"{timestamp} | {record.levelname} | "
            f"{record.name} | {message}"
        )


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level_name = os.getenv("JATO_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logger.setLevel(level)
    handler = logging.StreamHandler()
    use_json = env_flag_enabled("JATO_LOG_JSON")
    if use_json:
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(TextLogFormatter())

    logger.addHandler(handler)
    logger.propagate = False
    return logger
