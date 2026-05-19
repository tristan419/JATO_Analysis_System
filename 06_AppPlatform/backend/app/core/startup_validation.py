"""Startup environment validation for production-sensitive settings."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Mapping

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StartupValidationIssue:
    code: str
    severity: str
    message: str


class StartupValidationError(RuntimeError):
    def __init__(self, issues: list[StartupValidationIssue]) -> None:
        self.issues = issues
        joined = "; ".join(f"{issue.code}: {issue.message}" for issue in issues)
        super().__init__(f"Startup environment validation failed: {joined}")


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _validation_mode(environ: Mapping[str, str]) -> str:
    raw_mode = environ.get("APP_STARTUP_VALIDATION_MODE", "").strip().lower()
    if raw_mode in {"off", "warn", "strict"}:
        return raw_mode
    app_env = environ.get("APP_ENV", "").strip().lower()
    return "strict" if app_env in {"prod", "production"} else "warn"


def validate_startup_environment(
    environ: Mapping[str, str] | None = None,
) -> list[StartupValidationIssue]:
    """Return startup configuration issues without exposing secret values."""
    env = environ or os.environ
    mode = _validation_mode(env)
    if mode == "off":
        return []

    issues: list[StartupValidationIssue] = []
    database_url = env.get("APP_DATABASE_URL", "").strip()
    database_enabled = _parse_bool(
        env.get("APP_DATABASE_ENABLED"),
        bool(database_url),
    )
    if database_enabled and not database_url:
        issues.append(StartupValidationIssue(
            code="env.database_url_missing",
            severity="error",
            message="APP_DATABASE_ENABLED=true requires APP_DATABASE_URL to be set.",
        ))
    if database_url and not database_enabled:
        issues.append(StartupValidationIssue(
            code="env.database_disabled_with_url",
            severity="warning",
            message="APP_DATABASE_URL is set but APP_DATABASE_ENABLED=false; DB writes are disabled.",
        ))

    country_copilot_enabled = _parse_bool(env.get("APP_COUNTRY_COPILOT_ENABLED"), True)
    require_llm_key = mode == "strict" or _parse_bool(
        env.get("APP_COUNTRY_COPILOT_REQUIRE_LLM_KEY"),
        False,
    )
    if country_copilot_enabled and require_llm_key and not env.get("DEEPSEEK_API_KEY", "").strip():
        issues.append(StartupValidationIssue(
            code="env.deepseek_api_key_missing",
            severity="error" if mode == "strict" else "warning",
            message="Country Copilot is enabled and requires DEEPSEEK_API_KEY.",
        ))

    redis_enabled = _parse_bool(env.get("APP_REDIS_ENABLED"), True)
    if redis_enabled and not env.get("APP_REDIS_URL", "redis://localhost:6379/0").strip():
        issues.append(StartupValidationIssue(
            code="env.redis_url_missing",
            severity="error",
            message="APP_REDIS_ENABLED=true requires APP_REDIS_URL to be set.",
        ))

    return issues


def run_startup_validation(environ: Mapping[str, str] | None = None) -> None:
    issues = validate_startup_environment(environ)
    errors = [issue for issue in issues if issue.severity == "error"]
    warnings = [issue for issue in issues if issue.severity == "warning"]
    for issue in warnings:
        logger.warning("Startup env warning [%s]: %s", issue.code, issue.message)
    if errors:
        raise StartupValidationError(errors)
