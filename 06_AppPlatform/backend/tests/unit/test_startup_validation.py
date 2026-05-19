import pytest

from app.core.startup_validation import (
    StartupValidationError,
    run_startup_validation,
    validate_startup_environment,
)


def test_database_enabled_requires_database_url():
    issues = validate_startup_environment({
        "APP_STARTUP_VALIDATION_MODE": "warn",
        "APP_DATABASE_ENABLED": "true",
        "APP_DATABASE_URL": "",
    })

    assert [issue.code for issue in issues] == ["env.database_url_missing"]
    assert issues[0].severity == "error"


def test_run_startup_validation_raises_for_error():
    with pytest.raises(StartupValidationError) as exc_info:
        run_startup_validation({
            "APP_STARTUP_VALIDATION_MODE": "warn",
            "APP_DATABASE_ENABLED": "true",
            "APP_DATABASE_URL": "",
        })

    assert "APP_DATABASE_ENABLED=true requires APP_DATABASE_URL" in str(exc_info.value)


def test_database_url_with_disabled_database_is_warning_only():
    issues = validate_startup_environment({
        "APP_STARTUP_VALIDATION_MODE": "warn",
        "APP_DATABASE_ENABLED": "false",
        "APP_DATABASE_URL": "postgresql+asyncpg://example",
    })

    assert [issue.code for issue in issues] == ["env.database_disabled_with_url"]
    assert issues[0].severity == "warning"


def test_strict_country_copilot_requires_deepseek_key():
    issues = validate_startup_environment({
        "APP_STARTUP_VALIDATION_MODE": "strict",
        "APP_COUNTRY_COPILOT_ENABLED": "true",
        "APP_DATABASE_ENABLED": "false",
    })

    assert [issue.code for issue in issues] == ["env.deepseek_api_key_missing"]
    assert issues[0].severity == "error"


def test_warn_mode_does_not_require_deepseek_by_default():
    assert validate_startup_environment({
        "APP_STARTUP_VALIDATION_MODE": "warn",
        "APP_COUNTRY_COPILOT_ENABLED": "true",
        "APP_DATABASE_ENABLED": "false",
    }) == []
