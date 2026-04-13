from __future__ import annotations

import yaml

from app.scraper.source_bootstrap import (
    rank_source_draft_opportunities,
    render_source_yaml_draft,
)


def test_rank_source_draft_opportunities_prefers_brand_clusters():
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
                    {
                        "brand": "BMW",
                        "model": "SERIES 3",
                        "sales_12m": 31976,
                        "coverage_status": "brand_source",
                    },
                ],
            },
            {
                "country": "瑞典",
                "candidates": [
                    {
                        "brand": "TOYOTA",
                        "model": "RAV4",
                        "sales_12m": 4860,
                        "coverage_status": "missing_source",
                    },
                ],
            },
        ],
    }

    opportunities = rank_source_draft_opportunities(report, batch_size=2)

    assert len(opportunities) == 2
    assert opportunities[0].country == "德国"
    assert opportunities[0].brand == "VOLKSWAGEN"
    assert opportunities[0].candidate_model_count == 2
    assert opportunities[0].country_priority_rank == 1
    assert opportunities[0].source_code == "volkswagen_de_draft_scrapling"
    assert opportunities[0].file_name == "01_volkswagen_de.yaml"
    assert opportunities[0].relative_path == "de/01_volkswagen_de.yaml"
    assert opportunities[1].country == "瑞典"
    assert opportunities[1].brand == "TOYOTA"
    assert opportunities[1].country_priority_rank == 1
    assert opportunities[1].file_name == "01_toyota_se.yaml"
    assert opportunities[1].relative_path == "se/01_toyota_se.yaml"


def test_render_source_yaml_draft_contains_manual_only_schedule():
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
            }
        ],
    }

    opportunity = rank_source_draft_opportunities(report, batch_size=1)[0]
    yaml_text = render_source_yaml_draft(opportunity)

    assert "source_code: volkswagen_de_draft_scrapling" in yaml_text
    assert "frequency: manual_only" in yaml_text
    assert "# Country queue rank: 1" in yaml_text
    assert "# Draft path: de/01_volkswagen_de.yaml" in yaml_text
    assert "# Candidate models: GOLF, T-ROC" in yaml_text

    payload = yaml.safe_load(
        "\n".join(
            line
            for line in yaml_text.splitlines()
            if not line.startswith("#")
        )
    )

    assert payload["bootstrap_meta"]["candidate_model_count"] == 2
    assert payload["bootstrap_meta"]["top_models"] == ["GOLF", "T-ROC"]
