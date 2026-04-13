from app.services import review_workbench_service
from app.services.review_workbench_service import (
    build_backlog_items,
    build_country_scope_items,
)


def test_get_review_workbench_disables_backlog_when_schema_drift_occurs(
    monkeypatch,
) -> None:
    report = {
        "generated_at_utc": "2026-04-12T00:00:00+00:00",
        "top_n": 20,
        "country_count": 1,
        "candidate_count": 3,
        "coverage_summary": {
            "model_source": 1,
            "brand_source": 1,
            "missing_source": 1,
        },
        "country_summaries": [],
    }

    monkeypatch.setattr(
        review_workbench_service,
        "load_candidate_scope_report",
        lambda: report,
    )
    monkeypatch.setattr(
        review_workbench_service,
        "build_country_scope_items",
        lambda *args, **kwargs: [],
    )

    def _raise_schema_drift(*args, **kwargs):
        raise AttributeError("priority_rank")

    monkeypatch.setattr(
        review_workbench_service,
        "build_backlog_items",
        _raise_schema_drift,
    )

    payload = review_workbench_service.get_review_workbench()

    assert payload["candidateScopeAvailable"] is True
    assert payload["backlogAvailable"] is False
    assert payload["backlog"] == []


def test_build_country_scope_items_summarizes_missing_scope():
    report = {
        "country_summaries": [
            {
                "country": "德国",
                "latest_month": "2026 Jan",
                "window_start_month": "2025 Feb",
                "window_end_month": "2026 Jan",
                "candidates": [
                    {
                        "brand": "VOLKSWAGEN",
                        "model": "GOLF",
                        "coverage_status": "missing_source",
                    },
                    {
                        "brand": "VOLKSWAGEN",
                        "model": "T-ROC",
                        "coverage_status": "missing_source",
                    },
                    {
                        "brand": "BMW",
                        "model": "SERIES 3",
                        "coverage_status": "brand_source",
                    },
                ],
            }
        ]
    }

    items = build_country_scope_items(report)

    assert items == [
        {
            "country": "Germany",
            "latestMonth": "2026 Jan",
            "windowStartMonth": "2025 Feb",
            "windowEndMonth": "2026 Jan",
            "candidateCount": 3,
            "missingCount": 2,
            "topMissingBrands": ["VOLKSWAGEN (2)"],
            "topMissingModels": ["GOLF", "T-ROC"],
        }
    ]


def test_build_country_scope_items_filters_by_country_and_brand():
    report = {
        "country_summaries": [
            {
                "country": "德国",
                "latest_month": "2026 Jan",
                "window_start_month": "2025 Feb",
                "window_end_month": "2026 Jan",
                "candidates": [
                    {
                        "brand": "VOLKSWAGEN",
                        "model": "GOLF",
                        "coverage_status": "missing_source",
                    },
                    {
                        "brand": "BMW",
                        "model": "SERIES 3",
                        "coverage_status": "brand_source",
                    },
                ],
            },
            {
                "country": "瑞典",
                "latest_month": "2026 Jan",
                "window_start_month": "2025 Feb",
                "window_end_month": "2026 Jan",
                "candidates": [
                    {
                        "brand": "VOLKSWAGEN",
                        "model": "ID.7",
                        "coverage_status": "missing_source",
                    }
                ],
            },
        ]
    }

    items = build_country_scope_items(
        report,
        country_filter="Germany",
        brand_filter="Volks",
    )

    assert len(items) == 1
    assert items[0]["country"] == "Germany"
    assert items[0]["candidateCount"] == 1
    assert items[0]["topMissingModels"] == ["GOLF"]


def test_build_backlog_items_maps_ranked_opportunities_for_frontend():
    report = {
        "top_n": 20,
        "country_summaries": [
            {
                "country": "德国",
                "candidates": [
                    {
                        "brand": "VOLKSWAGEN",
                        "model": "GOLF",
                        "sales_12m": 83915,
                        "coverage_status": "missing_source",
                    },
                    {
                        "brand": "VOLKSWAGEN",
                        "model": "T-ROC",
                        "sales_12m": 75681,
                        "coverage_status": "missing_source",
                    },
                ],
            },
            {
                "country": "瑞典",
                "candidates": [
                    {
                        "brand": "VOLKSWAGEN",
                        "model": "ID.7",
                        "sales_12m": 27756,
                        "coverage_status": "missing_source",
                    }
                ],
            },
        ],
    }

    items = build_backlog_items(report, country_filter="Germany")

    assert items == [
        {
            "priorityRank": 1,
            "country": "Germany",
            "countryCode": "de",
            "brand": "VOLKSWAGEN",
            "brandSlug": "volkswagen",
            "candidateModelCount": 2,
            "sales12mSum": 159596.0,
            "topModels": ["GOLF", "T-ROC"],
            "sourceCode": "volkswagen_de_draft_scrapling",
            "fileName": "01_volkswagen_de.yaml",
            "relativePath": "de/01_volkswagen_de.yaml",
        }
    ]


def test_build_country_scope_items_does_not_cap_at_twenty_by_default():
    report = {
        "country_summaries": [
            {
                "country": "德国",
                "latest_month": f"2026 {index:02d}",
                "window_start_month": "2025 01",
                "window_end_month": f"2026 {index:02d}",
                "candidates": [
                    {
                        "brand": f"BRAND {index:02d}",
                        "model": f"MODEL {index:02d}",
                        "coverage_status": "missing_source",
                    }
                ],
            }
            for index in range(21)
        ]
    }

    items = build_country_scope_items(report)

    assert len(items) == 21


def test_build_backlog_items_does_not_cap_at_twenty_by_default():
    report = {
        "top_n": 20,
        "country_summaries": [
            {
                "country": "德国",
                "candidates": [
                    {
                        "brand": f"BRAND {index:02d}",
                        "model": f"MODEL {index:02d}",
                        "sales_12m": 1000 - index,
                        "coverage_status": "missing_source",
                    }
                    for index in range(21)
                ],
            }
        ],
    }

    items = build_backlog_items(report)

    assert len(items) == 21
    assert {item["brand"] for item in items} == {
        f"BRAND {index:02d}" for index in range(21)
    }
