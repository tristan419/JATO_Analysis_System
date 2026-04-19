from __future__ import annotations

from jato_scraper.source_bootstrap import (
    rank_source_draft_opportunities,
    render_draft_batch_markdown,
)


def test_rank_source_draft_opportunities_groups_country_brand_backlog(
) -> None:
    report = {
        "country_summaries": [
            {
                "country": "瑞典",
                "candidates": [
                    {
                        "coverage_status": "missing_source",
                        "brand": "Volkswagen",
                        "model": "Tayron",
                        "sales_12m": 80,
                        "rank": 1,
                    },
                    {
                        "coverage_status": "missing_source",
                        "brand": "Volkswagen",
                        "model": "Tiguan",
                        "sales_12m": 100,
                        "rank": 2,
                    },
                    {
                        "coverage_status": "missing_source",
                        "brand": "Volvo",
                        "model": "XC60",
                        "sales_12m": 120,
                        "rank": 3,
                    },
                ],
            }
        ]
    }

    opportunities = rank_source_draft_opportunities(
        report,
        jato_powertrains_by_key={
            ("瑞典", "volkswagen", "tayron"): ("PHEV",),
            ("瑞典", "volkswagen", "tiguan"): ("ICE", "PHEV"),
        },
    )

    assert len(opportunities) == 2

    volkswagen = opportunities[0]
    assert volkswagen.brand == "Volkswagen"
    assert volkswagen.country_priority_rank == 1
    assert volkswagen.country_model_rank == 1
    assert volkswagen.model == "Tayron"
    assert volkswagen.top_models == ("Tayron", "Tiguan")
    assert volkswagen.candidate_model_count == 2
    assert volkswagen.sales_12m == 180.0
    assert volkswagen.source_code == "volkswagen_se_draft_scrapling"
    assert volkswagen.file_name == "01_volkswagen_se.yaml"
    assert volkswagen.jato_powertrains == ("PHEV", "ICE")


def test_render_draft_batch_markdown_describes_brand_cluster_scope() -> None:
    report = {
        "dataset_path": "demo/report.json",
        "top_n": 30,
        "filters": {"vehicle_category": "SUV"},
    }
    opportunities = rank_source_draft_opportunities(
        {
            "country_summaries": [
                {
                    "country": "瑞典",
                    "candidates": [
                        {
                            "coverage_status": "missing_source",
                            "brand": "Volkswagen",
                            "model": "Tayron",
                            "sales_12m": 80,
                            "rank": 1,
                        },
                        {
                            "coverage_status": "missing_source",
                            "brand": "Volkswagen",
                            "model": "Tiguan",
                            "sales_12m": 100,
                            "rank": 2,
                        },
                    ],
                }
            ]
        }
    )

    markdown = render_draft_batch_markdown(opportunities, report)

    assert (
        "Selection unit: country×brand backlog groups from the report."
        in markdown
    )
    assert "brand_cluster scope" in markdown
    assert "single_model scope" not in markdown
