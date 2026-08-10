from app.core.startup_validation import validate_startup_environment


def test_database_environment_rejects_disabled_auth_without_explicit_strict_mode() -> None:
    issues = validate_startup_environment({
        "APP_DATABASE_ENABLED": "true",
        "APP_DATABASE_URL": "postgresql+psycopg://config-release",
        "APP_AUTH_ENABLED": "false",
        "APP_COUNTRY_COPILOT_ENABLED": "false",
    })

    assert [issue.code for issue in issues] == ["env.auth_disabled_in_production"]
    assert issues[0].severity == "error"


def test_database_environment_accepts_enabled_auth_without_static_token() -> None:
    issues = validate_startup_environment({
        "APP_DATABASE_ENABLED": "true",
        "APP_DATABASE_URL": "postgresql+psycopg://config-release",
        "APP_AUTH_ENABLED": "true",
        "APP_AUTH_TOKEN": "",
        "APP_COUNTRY_COPILOT_ENABLED": "false",
    })

    assert issues == []
