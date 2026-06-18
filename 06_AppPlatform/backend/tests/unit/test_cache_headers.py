from fastapi import Response

from app.api.cache_headers import set_strong_json_cache_headers


def test_set_strong_json_cache_headers_sets_browser_cache_headers() -> None:
    response = Response()

    set_strong_json_cache_headers(
        response,
        {"items": ["Country", "Make"]},
        namespace="metadata-columns",
    )

    assert "max-age=" in response.headers["Cache-Control"]
    assert "stale-while-revalidate=86400" in response.headers["Cache-Control"]
    assert response.headers["ETag"].startswith('W/"metadata-columns-')
    assert response.headers["Vary"] == "Origin"
