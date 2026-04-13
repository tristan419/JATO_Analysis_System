from app.services.country_service import (
    country_filter_aliases,
    to_display_country,
)


def test_to_display_country_normalizes_known_aliases() -> None:
    assert to_display_country("瑞典") == "Sweden"
    assert to_display_country("SE") == "Sweden"
    assert to_display_country("Germany") == "Germany"


def test_country_filter_aliases_expands_known_market_aliases() -> None:
    aliases = country_filter_aliases("se")

    assert {"sweden", "se", "瑞典"}.issubset(aliases)
    assert "finland" not in aliases


def test_country_filter_aliases_falls_back_to_query_when_unknown() -> None:
    assert country_filter_aliases("unknown-land") == {"unknown-land"}
