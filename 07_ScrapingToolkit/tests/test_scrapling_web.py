from jato_scraper.base import ExtractorConfig
from jato_scraper.extractors.scrapling_web import (
    ScraplingExtractor,
    ScraplingProfile,
)


def build_extractor() -> ScraplingExtractor:
    return ScraplingExtractor(
        ExtractorConfig(
            source_code="test_source",
            country="SE",
            brand="Volkswagen",
            source_url="https://example.com",
        ),
        ScraplingProfile(url="https://example.com"),
    )


def test_resolve_json_path_walks_graph_lists() -> None:
    extractor = build_extractor()

    payload = {
        "@graph": [
            {
                "@type": "BreadcrumbList",
                "itemListElement": [],
            },
            {
                "@type": "Car",
                "offers": [
                    {"name": "ID.4 Pro", "price": 499000},
                    {"name": "ID.4 GTX", "price": 579000},
                ],
            },
        ]
    }

    resolved = extractor._resolve_json_path(payload, "offers")

    assert resolved == [
        {"name": "ID.4 Pro", "price": 499000},
        {"name": "ID.4 GTX", "price": 579000},
    ]


def test_resolve_json_path_inherits_parent_fields() -> None:
    extractor = build_extractor()

    payload = {
        "vehicle": {
            "name": "Volkswagen ID.4",
            "model": "ID.4",
            "description": "Battery electric SUV",
            "offers": [
                {"price": 499000, "priceCurrency": "SEK"},
                {
                    "price": 579000,
                    "priceCurrency": "SEK",
                    "name": "Volkswagen ID.4 GTX",
                },
            ],
        }
    }

    resolved = extractor._resolve_json_path(payload, "vehicle.offers")

    assert resolved == [
        {
            "name": "Volkswagen ID.4",
            "model": "ID.4",
            "description": "Battery electric SUV",
            "price": 499000,
            "priceCurrency": "SEK",
        },
        {
            "name": "Volkswagen ID.4 GTX",
            "model": "ID.4",
            "description": "Battery electric SUV",
            "price": 579000,
            "priceCurrency": "SEK",
        },
    ]
