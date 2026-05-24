from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.api.routes import auth
from app.db.models import User


def test_normalize_secondary_countries_deduplicates_and_excludes_primary() -> None:
    assert auth._normalize_secondary_countries(
        [" se ", "cz", "SE", "", "pl"],
        "SE",
    ) == ["CZ", "PL"]


def test_normalize_country_code_rejects_invalid_long_value() -> None:
    with pytest.raises(HTTPException):
        auth._normalize_country_code("SWEDEN-LONG")


def test_user_payload_includes_country_profile_fields() -> None:
    user = User(
        id=uuid4(),
        username="tristan",
        password_hash="hash",
        role="editor",
        is_active=True,
        primary_country_code="SE",
        secondary_country_codes=["CZ", "PL"],
        preferred_landing_page="/product/order-genius",
    )

    payload = auth._user_payload(user)

    assert payload["username"] == "tristan"
    assert payload["role"] == "editor"
    assert payload["primaryCountry"] == "SE"
    assert payload["secondaryCountries"] == ["CZ", "PL"]
    assert payload["preferredLandingPage"] == "/product/order-genius"
    assert payload["profileComplete"] is True
