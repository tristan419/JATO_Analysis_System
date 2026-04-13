from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.scraper.candidate_scope import (
    SourceScope,
    build_candidate_scope_report_from_frame,
    build_country_windows,
    load_source_scopes,
    render_candidate_scope_markdown,
    resolve_scope_columns,
)


def test_resolve_scope_columns_and_month_sorting():
    columns = [
        "Make",
        "2025 Feb",
        "国家",
        "Model",
        "2024 Dec",
        "2025 Jan",
    ]

    resolved = resolve_scope_columns(columns)

    assert resolved.country == "国家"
    assert resolved.brand == "Make"
    assert resolved.model == "Model"
    assert resolved.month_columns == ("2024 Dec", "2025 Jan", "2025 Feb")


def test_build_country_windows_uses_latest_non_zero_month():
    frame = pd.DataFrame(
        {
            "国家": ["瑞典", "瑞典", "德国", "德国"],
            "Make": ["Volvo", "Tesla", "BMW", "BMW"],
            "Model": ["XC60", "Model Y", "X1", "X3"],
            "2025 Jan": [100, 80, 10, 20],
            "2025 Feb": [120, 90, 0, 0],
            "2025 Mar": [0, 0, 0, 0],
            "2025 Apr": [0, 0, 5, 7],
        }
    )
    columns = resolve_scope_columns(list(frame.columns))
    windows = build_country_windows(frame, columns)

    assert windows["瑞典"].latest_month == "2025 Feb"
    assert windows["瑞典"].window_columns == ("2025 Jan", "2025 Feb")
    assert windows["德国"].latest_month == "2025 Apr"
    assert windows["德国"].window_columns == (
        "2025 Jan",
        "2025 Feb",
        "2025 Mar",
        "2025 Apr",
    )


def test_load_source_scopes_detects_brand_and_model_scopes(tmp_path: Path):
    (tmp_path / "bmw_de.yaml").write_text(
        "\n".join(
            [
                "source_code: bmw_de_scrapling",
                "country: 德国",
                "brand: BMW",
                "source_url: https://bmw.example.com",
                "extractor_type: scrapling",
                "profile:",
                "  url: https://bmw.example.com",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "volvo_se_xc60.yaml").write_text(
        "\n".join(
            [
                "source_code: volvo_se_xc60_build_scrapling",
                "country: 瑞典",
                "brand: Volvo",
                "source_url: https://volvo.example.com",
                "extractor_type: scrapling",
                "profile:",
                "  url: https://volvo.example.com",
                "  fixed_jato_model: XC60",
            ]
        ),
        encoding="utf-8",
    )

    scopes = load_source_scopes(tmp_path)

    actual_scopes = [
        (scope.source_file, scope.scope_kind, scope.jato_model)
        for scope in scopes
    ]

    assert actual_scopes == [
        ("bmw_de.yaml", "brand", None),
        ("volvo_se_xc60.yaml", "model", "XC60"),
    ]


def test_load_source_scopes_expands_model_rules_into_model_scopes(
    tmp_path: Path,
):
    (tmp_path / "renault_fr.yaml").write_text(
        "\n".join(
            [
                "source_code: renault_fr_scrapling",
                "country: 法国",
                "brand: Renault",
                "source_url: https://renault.example.com",
                "extractor_type: scrapling",
                "profile:",
                "  url: https://renault.example.com/gamme",
                "  model_rules:",
                "    - key: model_clio",
                "      jato_model: Clio",
                "      keywords: ['clio']",
                "    - key: model_captur",
                "      jato_model: Captur",
                "      keywords: ['captur']",
            ]
        ),
        encoding="utf-8",
    )

    scopes = load_source_scopes(tmp_path)

    actual_scopes = [
        (scope.source_file, scope.scope_kind, scope.jato_model)
        for scope in scopes
    ]

    assert actual_scopes == [
        ("renault_fr.yaml", "model", "Clio"),
        ("renault_fr.yaml", "model", "Captur"),
    ]


def test_build_candidate_scope_report_marks_source_coverage():
    frame = pd.DataFrame(
        {
            "国家": ["瑞典", "瑞典", "德国", "德国", "德国"],
            "Make": ["Volvo", "Tesla", "BMW", "BMW", "Audi"],
            "Model": ["XC60", "Model Y", "X3", "X1", "Q5"],
            "2025 Jan": [100, 90, 30, 20, 5],
            "2025 Feb": [110, 95, 35, 21, 5],
            "2025 Mar": [120, 100, 32, 18, 5],
        }
    )
    columns = resolve_scope_columns(list(frame.columns))
    source_scopes = [
        SourceScope(
            source_code="volvo_se_xc60_build_scrapling",
            source_file="volvo_se_xc60.yaml",
            country="瑞典",
            brand="Volvo",
            scope_kind="model",
            jato_model="XC60",
        ),
        SourceScope(
            source_code="bmw_de_scrapling",
            source_file="bmw_de.yaml",
            country="德国",
            brand="BMW",
            scope_kind="brand",
            jato_model=None,
        ),
    ]

    report = build_candidate_scope_report_from_frame(
        frame=frame,
        scope_columns=columns,
        source_scopes=source_scopes,
        top_n=2,
        dataset_path="/tmp/test.parquet",
    )

    assert report["coverage_summary"]["model_source"] == 1
    assert report["coverage_summary"]["brand_source"] == 2
    assert report["coverage_summary"]["missing_source"] == 1

    sweden_summary = next(
        summary
        for summary in report["country_summaries"]
        if summary["country"] == "瑞典"
    )
    germany_summary = next(
        summary
        for summary in report["country_summaries"]
        if summary["country"] == "德国"
    )
    sweden_rows = sweden_summary["candidates"]
    germany_rows = germany_summary["candidates"]

    assert sweden_rows[0]["model"] == "XC60"
    assert sweden_rows[0]["coverage_status"] == "model_source"
    assert sweden_rows[1]["model"] == "Model Y"
    assert sweden_rows[1]["coverage_status"] == "missing_source"
    assert germany_rows[0]["coverage_status"] == "brand_source"
    assert germany_rows[1]["coverage_status"] == "brand_source"

    markdown = render_candidate_scope_markdown(report)
    assert "## 瑞典" in markdown
    assert "brand-scoped" in markdown
