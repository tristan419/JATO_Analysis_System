from __future__ import annotations

from app.services.jato_visual_artifact_service import build_visual_artifacts, format_artifact_value


def _evidence_package(intent: str = "market_overview") -> dict:
    return {
        "evidenceId": "evpkg_visual",
        "intent": intent,
        "country": "Sweden",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "rowCount": 3,
                "sourceType": "jato_parquet",
                "summary": "Powertrain snapshot",
                "keyFindings": ["BEV: 25235 units", "PHEV: 15028 units"],
                "evidenceRefs": [
                    {"refId": "ev_1", "label": "BEV", "value": 25235, "unit": "units", "source": "jato"},
                    {"refId": "ev_2", "label": "PHEV", "value": 15028, "unit": "units", "source": "jato"},
                    {"refId": "ev_3", "label": "ICE", "value": 8129, "unit": "units", "source": "jato"},
                ],
            }
        ],
        "missingEvidence": [],
    }


def test_format_artifact_value_formats_numeric_strings_without_rewriting_ranges() -> None:
    assert format_artifact_value("3000", "EUR") == "3,000 EUR"
    assert format_artifact_value("39121.74", "EUR") == "39,121.7 EUR"
    assert format_artifact_value("30,000-40,000 EUR", "EUR") == "30,000-40,000 EUR"


def test_market_overview_returns_metric_cards_and_chart_fallback() -> None:
    package = _evidence_package("market_overview")
    artifacts = build_visual_artifacts(
        question="Show Sweden BEV sales trend with a chart",
        answer={"title": "Sweden market", "direct": "BEV leads the snapshot."},
        evidence_package=package,
        charts=[
            {
                "chartId": "top_ranking",
                "chartType": "bar",
                "title": "Top Models",
                "data": [{"x": [2945], "y": ["EX40"], "type": "bar"}],
                "layout": {},
            },
            {
                "chartId": "company_car_channel_mix",
                "chartType": "bar",
                "title": "Company car channel mix",
                "data": [{"x": [60.3, 64.8], "y": ["BEV Business", "PHEV Business"], "type": "bar"}],
                "layout": {},
            },
        ],
    )

    assert artifacts[0]["type"] == "chart"
    metric_cards = next(item for item in artifacts if item["type"] == "metric_cards")
    assert metric_cards["sourceEvidenceRefs"] == ["ev_1", "ev_2", "ev_3"]
    assert [row["label"] for row in metric_cards["data"]["rows"]] == ["BEV", "PHEV", "ICE"]
    assert metric_cards["data"]["intentAnalysis"]["template"] == "market_overview"
    assert metric_cards["data"]["intentAnalysis"]["powertrainMix"]
    fallback = next(item for item in artifacts if item["id"] == "artifact_snapshot_fallback_chart")
    assert fallback["type"] == "chart"
    assert fallback["fallbackReason"] == "monthly trend series missing"
    assert fallback["spec"]["chartType"] == "bar"
    assert fallback["spec"]["note"] == "Trend series unavailable; showing current snapshot instead."
    assert fallback["sourceEvidenceRefs"] == ["ev_1", "ev_2", "ev_3"]
    assert package["missingEvidence"][0]["name"] == "monthly_trend_series"
    assert package["confidence"] == "medium"


def test_market_overview_returns_decision_table_from_snapshot_refs() -> None:
    package = {
        "evidenceId": "evpkg_market_table",
        "intent": "market_overview",
        "country": "Hungary",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {"refId": "ev_total", "label": "totalRows", "value": 33327, "unit": "units", "source": "jato_country_snapshot"},
                    {"refId": "ev_avg", "label": "avgMsrp", "value": 57954, "unit": "currency", "source": "jato_country_snapshot"},
                    {"refId": "ev_bev", "label": "BEV", "value": 4200, "unit": "units", "source": "jato_country_snapshot"},
                    {"refId": "ev_hev", "label": "HEV", "value": 6100, "unit": "units", "source": "jato_country_snapshot"},
                    {"refId": "ev_seg", "label": "segmentByFuel.SUV A.HEV.share", "value": 38.2, "unit": "%", "source": "jato_country_snapshot"},
                    {"refId": "ev_model", "label": "topModels.RAV4.sales", "value": 930, "unit": "units", "source": "jato_country_snapshot"},
                ],
            }
        ],
        "missingEvidence": [],
    }
    artifacts = build_visual_artifacts(
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
        answer={
            "title": "Hungary market",
            "direct": "先用市场结构判断动力路线。",
            "recommendedActions": [{"action": "用市场结构表判断 HEV/PHEV 进入顺序"}],
        },
        evidence_package=package,
        charts=[
            {
                "chartId": "top_ranking",
                "chartType": "bar",
                "title": "Top Models",
                "data": [{"x": [2945], "y": ["EX40"], "type": "bar"}],
                "layout": {},
            },
            {
                "chartId": "company_car_channel_mix",
                "chartType": "bar",
                "title": "Company car channel mix",
                "data": [{"x": [60.3, 64.8], "y": ["BEV Business", "PHEV Business"], "type": "bar"}],
                "layout": {},
            },
        ],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_market_overview_table")
    rows = table["data"]["rows"]

    assert table["title"] == "市场决策表"
    assert table["spec"]["columns"] == [
        "dimension",
        "signal",
        "evidence",
        "businessImplication",
        "recommendedAction",
        "confidence",
    ]
    assert rows[0]["dimension"] == "Powertrain mix"
    assert rows[0]["signal"] == "BEV"
    assert rows[0]["evidence"] == "4,200 units"
    assert "4,200 units" in rows[0]["businessImplication"]
    assert "验证 BEV" in rows[0]["businessImplication"]
    assert rows[0]["recommendedAction"] == "用市场结构表判断 HEV/PHEV 进入顺序"
    assert table["sourceEvidenceRefs"] == ["ev_bev", "ev_hev", "ev_seg", "ev_model"]
    assert "avgMsrp" not in str(rows)
    assert "totalRows" not in str(rows)
    metric_cards = next(item for item in artifacts if item["id"] == "artifact_metric_cards")
    metric_labels = [row["label"] for row in metric_cards["data"]["rows"]]
    assert "平均 MSRP" not in metric_labels
    assert "totalRows" not in str(metric_cards["data"]["rows"])


def test_market_overview_metric_cards_prioritize_business_structure_over_big_kpis() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利市场情况怎么样？请先看动力结构。",
        answer={
            "title": "Hungary market",
            "direct": "直接结论：先看动力结构和市场体量。",
            "recommendedActions": [{"action": "拆分动力结构和车型机会"}],
        },
        evidence_package={
            "evidenceId": "evpkg_market_metric_order",
            "intent": "market_overview",
            "country": "Hungary",
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {
                            "refId": "ev_total",
                            "label": "marketSnapshot.kpis.cumulativeSales",
                            "value": 1200000,
                            "unit": "units",
                            "source": "jato_country_snapshot",
                        },
                        {
                            "refId": "ev_avg",
                            "label": "results.kpis.avgMsrp",
                            "value": 57954.1,
                            "unit": "currency",
                            "source": "jato_country_snapshot",
                        },
                        {
                            "refId": "ev_bev",
                            "label": "marketSnapshot.powertrainMix.BEV.sales",
                            "value": 4200,
                            "unit": "units",
                            "source": "jato_country_snapshot",
                        },
                        {
                            "refId": "ev_hev",
                            "label": "marketSnapshot.powertrainMix.HEV.sales",
                            "value": 1200,
                            "unit": "units",
                            "source": "jato_country_snapshot",
                        },
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    metric_cards = next(item for item in artifacts if item["id"] == "artifact_metric_cards")

    assert metric_cards["sourceEvidenceRefs"][:2] == ["ev_bev", "ev_hev"]
    assert "ev_avg" not in metric_cards["sourceEvidenceRefs"]
    assert "平均 MSRP" not in str(metric_cards["data"]["rows"])


def test_market_overview_report_block_uses_market_refs_not_generic_evidence_text() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利 J7 HEV 市场情况怎么样？请给数据支撑和图表。",
        answer={
            "title": "匈牙利 · 市场机会判断",
            "direct": "直接结论：匈牙利市场 J7 HEV 的机会初筛必须先看已查数据。",
            "reportReadyBullets": ["Evidence：当前结论需要同时保留内部结构化数据和外部研究边界。"],
            "recommendedActions": [{"action": "拆到车型/品牌"}],
        },
        evidence_package={
            "evidenceId": "evpkg_hu_market_report",
            "intent": "market_overview",
            "country": "Hungary",
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_total", "label": "totalRows", "value": 33327, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "ev_hev", "label": "HEV", "value": 2687, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_hev_crosstab", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales", "value": 2687, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suva0", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales", "value": 7303, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suva", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 3535, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct", "value": 89.5, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["id"] == "artifact_report_block")
    evidence = report["data"]["evidence"]

    assert "HEV = 2,687 units" in evidence
    assert "SUV A0 = 7,303 units" in evidence
    assert "SUV A = 3,535 units" in evidence
    assert "2WD 占比 = 89.5 %" in evidence
    assert not any("当前结论需要同时保留" in line for line in evidence)
    assert "ev_hev" in report["sourceEvidenceRefs"]
    assert "ev_total" not in report["sourceEvidenceRefs"]


def test_market_overview_adds_powertrain_mix_chart_before_generic_tool_chart() -> None:
    package = {
        "evidenceId": "evpkg_powertrain_chart",
        "intent": "market_overview",
        "country": "Sweden",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "build_market_chart",
                "success": True,
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {"refId": "bev_sales", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                    {"refId": "phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 15028, "unit": "units", "source": "jato_country_chart_deck"},
                    {"refId": "hev_sales", "label": "contextSnapshot.powertrainMix.HEV.sales", "value": 5051, "unit": "units", "source": "jato_country_chart_deck"},
                ],
            }
        ],
        "missingEvidence": [],
    }
    artifacts = build_visual_artifacts(
        question="北欧 BEV 增长是否会压缩 HEV 空间？",
        answer={"title": "Powertrain pressure", "direct": "BEV is pressuring HEV but not fully replacing it."},
        evidence_package=package,
        charts=[
            {
                "chartId": "top_ranking",
                "chartType": "bar",
                "title": "Top Models",
                "data": [{"x": [2945, 2893], "y": ["EX40", "XC60"], "type": "bar"}],
                "layout": {},
            }
        ],
    )

    chart_ids = [item["id"] for item in artifacts if item["type"] == "chart"]
    assert chart_ids[:2] == ["artifact_market_powertrain_mix_chart", "top_ranking"]
    chart = next(item for item in artifacts if item["id"] == "artifact_market_powertrain_mix_chart")
    assert chart["title"] == "Powertrain mix chart"
    assert [row["label"] for row in chart["data"]] == ["BEV", "PHEV", "HEV"]
    assert chart["sourceEvidenceRefs"] == ["bev_sales", "phev_sales", "hev_sales"]


def test_market_overview_drive_choice_uses_retrieved_drive_mix_chart() -> None:
    package = {
        "evidenceId": "evpkg_drive_mix_chart",
        "intent": "market_overview",
        "country": "Hungary",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "query_country_snapshot",
                "success": True,
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {"refId": "hev_2wd", "label": "crossTabs.driveByFuel.HEV.2WD_pct", "value": 89.5, "unit": "%", "source": "jato_country_snapshot"},
                    {"refId": "hev_4wd", "label": "crossTabs.driveByFuel.HEV.4WD_pct", "value": 9.9, "unit": "%", "source": "jato_country_snapshot"},
                    {"refId": "bev_2wd", "label": "crossTabs.driveByFuel.BEV.2WD_pct", "value": 82.4, "unit": "%", "source": "jato_country_snapshot"},
                    {"refId": "bev_4wd", "label": "crossTabs.driveByFuel.BEV.4WD_pct", "value": 15.1, "unit": "%", "source": "jato_country_snapshot"},
                ],
            }
        ],
        "missingEvidence": [],
    }

    artifacts = build_visual_artifacts(
        question="匈牙利 J7 HEV 应先把 2WD 还是 4WD 作为主销，并提供市场结构图。",
        answer={"title": "Hungary drivetrain", "direct": "2WD should be prioritised."},
        evidence_package=package,
        charts=[],
    )

    chart = next(item for item in artifacts if item["id"] == "artifact_market_drive_mix_chart")
    assert chart["title"] == "HEV 2WD/4WD 市场结构"
    assert chart["data"] == [
        {"label": "2WD", "value": 89.5, "unit": "%", "series": "HEV"},
        {"label": "4WD", "value": 9.9, "unit": "%", "series": "HEV"},
    ]
    assert chart["sourceEvidenceRefs"] == ["hev_2wd", "hev_4wd"]


def test_market_overview_table_uses_cross_tab_refs_when_topline_snapshot_is_empty() -> None:
    package = {
        "evidenceId": "evpkg_hungary_cross_tabs",
        "intent": "market_overview",
        "country": "Hungary",
        "confidence": "medium",
        "toolResults": [
            {
                "toolName": "build_market_chart",
                "success": True,
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {
                        "refId": "ev_hev_sales",
                        "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales",
                        "value": 2687,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                    {
                        "refId": "ev_hev_2wd",
                        "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct",
                        "value": 89.5,
                        "unit": "%",
                        "source": "jato_country_chart_deck",
                    },
                    {
                        "refId": "ev_suv_a0",
                        "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales",
                        "value": 7303,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                    {
                        "refId": "ev_suv_a",
                        "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales",
                        "value": 3535,
                        "unit": "units",
                        "source": "jato_country_chart_deck",
                    },
                ],
            }
        ],
        "missingEvidence": [],
    }
    artifacts = build_visual_artifacts(
        question="匈牙利 HEV 市场机会？",
        answer={
            "title": "Hungary HEV opportunity",
            "direct": "先用 cross-tab 市场结构判断。",
            "recommendedActions": [{"action": "验证 HEV SUV A0/A 的车型级竞品池"}],
        },
        evidence_package=package,
        charts=[
            {
                "chartId": "top_ranking",
                "chartType": "bar",
                "title": "Top Models",
                "data": [{"x": [2945], "y": ["EX40"], "type": "bar"}],
                "layout": {},
            },
            {
                "chartId": "company_car_channel_mix",
                "chartType": "bar",
                "title": "Company car channel mix",
                "data": [{"x": [60.3, 64.8], "y": ["BEV Business", "PHEV Business"], "type": "bar"}],
                "layout": {},
            },
        ],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_market_overview_table")
    rows = table["data"]["rows"]
    signals = {row["signal"] for row in rows}
    dimensions = {row["dimension"] for row in rows}

    assert "HEV" in signals
    assert "SUV A0" in signals
    assert "Powertrain mix" in dimensions
    assert "Segment structure" in dimensions
    hev_row = next(row for row in rows if row["signal"] == "HEV")
    assert "2,687 units" in hev_row["businessImplication"]
    assert "验证 HEV" in hev_row["businessImplication"]
    assert set(table["sourceEvidenceRefs"]) == {"ev_hev_sales", "ev_hev_2wd", "ev_suv_a0", "ev_suv_a"}

    chart = next(item for item in artifacts if item["id"] == "artifact_market_structure_chart")
    assert chart["type"] == "chart"
    assert chart["spec"]["chartType"] == "bar"
    assert chart["spec"]["seriesField"] == "series"
    assert chart["spec"]["note"] == "交叉表柱状图按维度展示有证据支撑的市场信号，不代表可相加的市场总量。"
    assert [(row["label"], row["value"], row["series"]) for row in chart["data"]] == [
        ("HEV", 2687.0, "动力销量"),
        ("SUV A0", 7303.0, "级别销量"),
        ("SUV A", 3535.0, "级别销量"),
    ]
    assert chart["sourceEvidenceRefs"] == ["ev_hev_sales", "ev_suv_a0", "ev_suv_a"]


def test_market_overview_powertrain_route_outputs_comparison_table() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利市场现在适合推 PHEV 还是 HEV？请基于数据给结论，并展示图表和对比表。",
        answer={
            "title": "Hungary HEV/PHEV route",
            "direct": "HEV 做低风险主线，PHEV 做公司车/TCO 验证线。",
            "recommendedActions": [{"action": "建立 HEV vs PHEV 场景决策表"}],
        },
        evidence_package={
            "intent": "market_overview",
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_segment_breakdown",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_hev_sales", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales", "value": 2687, "unit": "units", "source": "jato_segment_breakdown"},
                        {"refId": "ev_hev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct", "value": 89.5, "unit": "%", "source": "jato_segment_breakdown"},
                        {"refId": "ev_hev_4wd", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.4WD_pct", "value": 9.9, "unit": "%", "source": "jato_segment_breakdown"},
                        {"refId": "ev_hev_other", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.OTHER_pct", "value": 0.6, "unit": "%", "source": "jato_segment_breakdown"},
                        {"refId": "ev_phev_sales", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.sales", "value": 969, "unit": "units", "source": "jato_segment_breakdown"},
                        {"refId": "ev_phev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.2WD_pct", "value": 52.0, "unit": "%", "source": "jato_segment_breakdown"},
                        {"refId": "ev_phev_4wd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.4WD_pct", "value": 46.9, "unit": "%", "source": "jato_segment_breakdown"},
                        {"refId": "ev_phev_other", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.OTHER_pct", "value": 1.1, "unit": "%", "source": "jato_segment_breakdown"},
                    ],
                }
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    assert ids[0] == "artifact_market_powertrain_mix_chart"
    assert ids.index("artifact_powertrain_route_table") < ids.index("artifact_market_overview_table")
    chart = next(item for item in artifacts if item["id"] == "artifact_market_powertrain_mix_chart")
    assert chart["title"] == "Powertrain mix chart"
    assert [(row["label"], row["value"]) for row in chart["data"]] == [("PHEV", 969.0), ("HEV", 2687.0)]
    assert chart["sourceEvidenceRefs"] == ["ev_phev_sales", "ev_hev_sales"]
    table = next(item for item in artifacts if item["id"] == "artifact_powertrain_route_table")
    assert table["spec"]["columns"] == [
        "powertrain",
        "sales",
        "share",
        "twoWheelDrive",
        "fourWheelDrive",
        "routeRole",
        "productAction",
    ]
    rows = table["data"]["rows"]
    assert rows[0]["powertrain"] == "PHEV"
    assert rows[0]["sales"] == "969 units"
    assert rows[0]["share"] == "待补"
    assert rows[0]["twoWheelDrive"] == "52%"
    assert rows[0]["fourWheelDrive"] == "46.9%"
    assert rows[0]["routeRole"] == "公司车/TCO 验证线"
    assert rows[1]["powertrain"] == "HEV"
    assert rows[1]["sales"] == "2,687 units"
    assert rows[1]["share"] == "待补"
    assert rows[1]["twoWheelDrive"] == "89.5%"
    assert rows[1]["routeRole"] == "低风险主线"
    assert table["sourceEvidenceRefs"] == [
        "ev_phev_sales",
        "ev_phev_2wd",
        "ev_phev_4wd",
        "ev_hev_sales",
        "ev_hev_2wd",
        "ev_hev_4wd",
    ]


def test_market_metric_cards_clean_cross_tab_and_top_model_labels() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利 HEV 市场为什么适合 T7 HEV？请拆 SUV A 级别和 PHEV 对比。",
        answer={
            "title": "Hungary market",
            "direct": "HEV 做低风险主线，PHEV 做公司车/TCO 验证线。",
            "recommendedActions": [{"action": "建立 HEV vs PHEV 场景决策表"}],
        },
        evidence_package={
            "intent": "market_overview",
            "country": "Hungary",
            "entities": {"models": ["T7 HEV"], "powertrains": ["HEV", "PHEV"], "segments": ["SUV A"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "hev_sales", "label": "contextSnapshot.powertrainMix.HEV.sales", "value": 8200, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 3100, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "suv_a_hev_sales", "label": "crossTabs.segmentByFuel.SUV A.HEV.sales", "value": 2450, "unit": "units", "source": "jato_cross_tab"},
                        {"refId": "suv_a_phev_sales", "label": "crossTabs.segmentByFuel.SUV A.PHEV.sales", "value": 780, "unit": "units", "source": "jato_cross_tab"},
                        {"refId": "corolla_sales", "label": "topModels.Corolla Cross.sales", "value": 1250, "unit": "units", "source": "jato_country_snapshot"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    metric_cards = next(item for item in artifacts if item["id"] == "artifact_metric_cards")
    labels = {row["label"] for row in metric_cards["data"]["rows"]}
    assert "SUV A HEV 销量" in labels
    assert "SUV A PHEV 销量" in labels
    assert "Corolla Cross 销量" in labels
    assert not any("crossTabs" in label or "topModels" in label for label in labels)


def test_market_overview_artifacts_include_size_and_readable_intent_analysis_labels() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利市场情况怎么样？",
        answer={
            "title": "Hungary market",
            "direct": "看市场总量、动力结构、细分结构和 Top models。",
            "recommendedActions": [{"action": "补齐价格、配置和渠道证据"}],
        },
        evidence_package={
            "intent": "market_overview",
            "country": "Hungary",
            "entities": {"models": ["T7 HEV"], "powertrains": ["HEV"], "segments": ["SUV A"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_total", "label": "marketSnapshot.kpis.cumulativeSales", "value": 12000, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "ev_bev", "label": "marketSnapshot.powertrainMix.BEV.sales", "value": 4200, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "ev_hev", "label": "marketSnapshot.powertrainMix.HEV.sales", "value": 1200, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "ev_suv_a", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 3535, "unit": "units", "source": "jato_cross_tab"},
                        {"refId": "ev_corolla", "label": "topModels.Corolla Cross.sales", "value": 1250, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "ev_tucson", "label": "topModels.Tucson.sales", "value": 980, "unit": "units", "source": "jato_country_snapshot"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_market_overview_table")
    table_rows = table["data"]["rows"]
    metric_cards = next(item for item in artifacts if item["id"] == "artifact_metric_cards")
    analysis = metric_cards["data"]["intentAnalysis"]
    analysis_labels = {
        row["label"]
        for section in ("keyMetrics", "powertrainMix", "topModels")
        for row in analysis[section]
    }

    assert table_rows[0]["dimension"] == "Market size"
    assert table_rows[0]["signal"] == "累计销量"
    assert table_rows[0]["evidence"] == "12,000 units"
    assert "ev_total" in table["sourceEvidenceRefs"]
    assert "市场累计销量" in analysis_labels
    assert "BEV 动力销量" in analysis_labels
    assert "SUV A 细分销量" in analysis_labels
    assert "Corolla Cross 销量" in analysis_labels
    assert not any("marketSnapshot" in label or "contextSnapshot" in label or "topModels." in label for label in analysis_labels)


def test_market_overview_artifacts_prioritize_cross_tabs_and_filter_method_material() -> None:
    package = {
        "evidenceId": "evpkg_sweden_j7_hev",
        "intent": "market_overview",
        "country": "Sweden",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "query_segment_breakdown",
                "success": True,
                "sourceType": "jato_parquet",
                "evidenceRefs": [
                    {
                        "refId": "ev_hev_sales",
                        "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales",
                        "value": 1946,
                        "unit": "units",
                        "source": "jato_segment_breakdown",
                    },
                    {
                        "refId": "ev_suv_a0",
                        "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales",
                        "value": 5416,
                        "unit": "units",
                        "source": "jato_segment_breakdown",
                    },
                    {
                        "refId": "ev_suv_a",
                        "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales",
                        "value": 7544,
                        "unit": "units",
                        "source": "jato_segment_breakdown",
                    },
                ],
            },
            {
                "toolName": "business_method_material",
                "success": True,
                "sourceType": "generated",
                "evidenceRefs": [
                    {
                        "refId": "ev_method_warranty",
                        "label": "J7 HEV visible feature value.7年/15万公里质保",
                        "value": "15万公里质保",
                        "source": "J7_HEV_method_fallback.txt",
                    },
                    {
                        "refId": "ev_method_pva",
                        "label": "J7 HEV user material PVA coverage",
                        "value": 118,
                        "unit": "%",
                        "source": "J7_HEV_method_fallback.txt",
                    },
                ],
            },
        ],
        "missingEvidence": [],
    }

    artifacts = build_visual_artifacts(
        question="瑞典 HEV 市场为什么适合 J7？请给出数据支撑和图表。",
        answer={
            "title": "Sweden J7 HEV opportunity",
            "direct": "用 cross-tab 判断 HEV + SUV A0/A 机会。",
            "recommendedActions": [{"action": "补齐 J7 HEV 车型级价格/配置矩阵"}],
        },
        evidence_package=package,
        charts=[],
    )

    assert artifacts[0]["id"] == "artifact_market_structure_chart"
    table = next(item for item in artifacts if item["id"] == "artifact_market_overview_table")
    serialized_rows = str(table["data"]["rows"])

    assert set(table["sourceEvidenceRefs"]) == {"ev_hev_sales", "ev_suv_a0", "ev_suv_a"}
    assert "15万公里质保" not in serialized_rows
    assert "PVA" not in serialized_rows
    assert "SUV A0" in serialized_rows
    assert "SUV A" in serialized_rows


def test_market_overview_framework_table_requires_business_structure() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利市场机会是什么？",
        answer={
            "title": "Hungary market",
            "direct": "当前没有 evidenceRef，但可以先给验证框架。",
            "businessFrame": {"action": "调用 query_country_snapshot"},
            "recommendedActions": [{"action": "调用 query_country_snapshot 并生成市场结构图表"}],
        },
        evidence_package={
            "intent": "market_overview",
            "country": "Hungary",
            "confidence": "low",
            "toolResults": [],
            "missingEvidence": [{"name": "market_snapshot", "reason": "No country snapshot evidence."}],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_market_overview_framework_table")

    assert table["fallbackReason"] == "evidence_refs_missing"
    assert table["spec"]["evidenceMode"] == "missing_refs_framework"
    assert table["data"]["rows"][0]["dimension"] == "Market size"
    assert table["sourceEvidenceRefs"] == []


def test_penetration_change_report_uses_snapshot_fallback_chart() -> None:
    package = _evidence_package("report_generation")
    artifacts = build_visual_artifacts(
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        answer={
            "title": "BEV penetration report",
            "direct": "BEV penetration should shape product definition.",
            "reportReadyBullets": ["Key message", "Evidence", "Implication", "Next action"],
            "recommendedActions": [{"action": "Build product definition page"}],
        },
        evidence_package=package,
        charts=[],
    )

    fallback = next(item for item in artifacts if item["id"] == "artifact_snapshot_fallback_chart")
    assert fallback["type"] == "chart"
    assert fallback["sourceEvidenceRefs"] == ["ev_1", "ev_2", "ev_3"]
    assert package["missingEvidence"][0]["name"] == "monthly_trend_series"


def test_visual_artifacts_require_evidence_refs() -> None:
    artifacts = build_visual_artifacts(
        question="Show a chart",
        answer={"title": "No evidence", "direct": "No refs."},
        evidence_package={"intent": "market_overview", "toolResults": [], "missingEvidence": []},
        charts=[],
    )

    assert artifacts == []


def test_metric_cards_ignore_dates_sources_and_row_counts() -> None:
    artifacts = build_visual_artifacts(
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        answer={"title": "O9 pricing", "direct": "Need current price refs."},
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "search_market_news",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {"refId": "ev_1", "label": "row_count", "value": 3, "source": "web"},
                        {"refId": "ev_2", "label": "O9 source", "value": "https://example.test/o9", "source": "web"},
                        {"refId": "ev_3", "label": "O9 price article.date", "value": "2026-06-01", "source": "web"},
                        {"refId": "ev_4", "label": "O9 MSRP", "value": 54000, "unit": "EUR", "source": "official"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    metric_cards = next(item for item in artifacts if item["type"] == "metric_cards")

    assert metric_cards["sourceEvidenceRefs"] == ["ev_4"]
    assert metric_cards["data"]["rows"] == [
        {
            "label": "O9 MSRP",
            "value": 54000.0,
            "unit": "EUR",
            "source": "official",
            "sourceEvidenceRef": "ev_4",
        }
    ]


def test_metric_cards_render_business_labels_and_display_currency_units() -> None:
    artifacts = build_visual_artifacts(
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        answer={"title": "O9 pricing", "direct": "Use price corridor and O9 material evidence."},
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_price_positioning",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato_msrp_postgres"},
                        {"refId": "ev_o9", "label": "O9 user material main trim MSRP", "value": 54000, "unit": "EUR", "source": "O9_user_material.pptx"},
                        {"refId": "ev_pva", "label": "O9 user material PVA coverage", "value": 105, "unit": "%", "source": "O9_user_material.pptx"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    metric_cards = next(item for item in artifacts if item["id"] == "artifact_metric_cards")
    rows = metric_cards["data"]["rows"]

    assert any(row["label"] == "参考样本上沿" and row["unit"] == "EUR" and row["source"] == "JATO MSRP 数据" for row in rows)
    assert any(row["label"] == "O9 用户材料主销高配价格" and row["unit"] == "EUR" for row in rows)
    assert any(row["label"] == "O9 用户材料高配 PVA 覆盖率" and row["unit"] == "%" for row in rows)
    assert set(metric_cards["sourceEvidenceRefs"]) == {"ev_max", "ev_o9", "ev_pva"}


def test_pricing_metric_cards_suppress_reference_sample_stats_when_current_prices_are_missing() -> None:
    artifacts = build_visual_artifacts(
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        answer={"title": "J7 pricing", "direct": "需要补当前官方 MSRP。"},
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV", "Sportage HEV"], "competitors": ["Sportage HEV"]},
            "confidence": "low",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_msrp_postgres"},
                        {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato_msrp_postgres"},
                        {"refId": "ev_j7", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                    ],
                }
            ],
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "No J7 HEV / Sportage HEV current price rows.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    metric_cards = next(item for item in artifacts if item["id"] == "artifact_metric_cards")
    labels = [row["label"] for row in metric_cards["data"]["rows"]]

    assert "J7 HEV 用户材料主销高配价格" in labels
    assert "J7 HEV 用户材料高配 PVA 覆盖率" in labels
    assert "参考样本下沿" not in labels
    assert "参考样本上沿" not in labels
    assert "ev_min" not in metric_cards["sourceEvidenceRefs"]
    assert "ev_max" not in metric_cards["sourceEvidenceRefs"]


def test_metric_cards_ignore_snapshot_technical_counts_and_external_source_rank_refs() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利市场现在适合推 PHEV 还是 HEV？",
        answer={"title": "Hungary market", "direct": "Use external evidence only as background."},
        evidence_package={
            "intent": "market_overview",
            "country": "Hungary",
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_1", "label": "totalRows", "value": 0, "source": "jato_country_snapshot"},
                        {"refId": "ev_2", "label": "countryCount", "value": 0, "source": "jato_country_snapshot"},
                        {"refId": "ev_3", "label": "brandCount", "value": 0, "source": "jato_country_snapshot"},
                        {"refId": "ev_4", "label": "modelCount", "value": 0, "source": "jato_country_snapshot"},
                    ],
                },
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {"refId": "ev_5", "label": "Hungary EV policy.source", "value": "https://example.test", "source": "jato_external_research_web"},
                        {"refId": "ev_6", "label": "Hungary EV policy.claim", "value": "BEV incentive budget is 79.1 million EUR.", "source": "jato_external_research_web"},
                        {"refId": "ev_7", "label": "Hungary EV policy.rank", "value": 1, "source": "jato_external_research_web"},
                        {"refId": "ev_8", "label": "Hungary EV policy.rankSeed", "value": 2, "source": "jato_external_research_web"},
                    ],
                },
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    assert artifacts == []


def test_business_table_falls_back_when_only_weak_refs_exist() -> None:
    artifacts = build_visual_artifacts(
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        answer={
            "title": "O9 pricing",
            "direct": "当前只能给验证框架。",
            "businessFrame": {
                "verdict": "证据不足",
                "action": "补齐 MSRP 和竞品价格走廊",
            },
            "recommendedActions": [{"action": "补齐 MSRP 和竞品价格走廊"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["O9"]},
            "confidence": "low",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_1", "label": "row_count", "value": 1, "source": "jato"},
                        {"refId": "ev_2", "label": "O9 source", "value": "https://example.test/o9", "source": "web"},
                        {"refId": "ev_3", "label": "O9 pricing article.sourc", "value": "https://example.test/o9-truncated", "source": "web"},
                    ],
                }
            ],
            "missingEvidence": [{"name": "current_msrp", "reason": "No MSRP evidence."}],
        },
        charts=[],
    )

    assert not any(item["type"] == "metric_cards" for item in artifacts)
    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_framework_table")
    report = next(item for item in artifacts if item["type"] == "report_block")
    assert table["fallbackReason"] == "evidence_refs_missing"
    assert table["sourceEvidenceRefs"] == []
    assert report["sourceEvidenceRefs"] == []


def test_business_framework_returns_artifacts_without_evidence_refs() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典 J7 HEV 应该怎么定价？",
        answer={
            "title": "J7 HEV pricing",
            "direct": "先按核心竞争带中段 + 高配主推推进。",
            "businessFrame": {
                "verdict": "待补证据后定价",
                "why": "没有可引用 evidenceRef。",
                "soWhat": "低配锚点，高配主推。",
                "action": "补齐竞品 MSRP / TP / 月供价格矩阵",
                "risk": "不能给确定数字。",
            },
            "reportReadyBullets": ["低配做价格锚点，高配做主推版本。"],
            "recommendedActions": [{"action": "补齐竞品 MSRP / TP / 月供价格矩阵"}],
            "businessImplications": ["低配应作为价格锚点，高配作为主推版本。"],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"]},
            "toolResults": [],
            "missingEvidence": [{"name": "own_model_price", "reason": "No MSRP evidence."}],
            "confidence": "low",
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_framework_table")
    report = next(item for item in artifacts if item["type"] == "report_block")
    assert table["fallbackReason"] == "evidence_refs_missing"
    assert table["sourceEvidenceRefs"] == []
    assert table["spec"]["evidenceMode"] == "missing_refs_framework"
    assert table["data"]["rows"][0]["model"] == "J7 HEV"
    assert "待补" in table["data"]["rows"][0]["msrp"]
    assert report["fallbackReason"] == "evidence_refs_missing"
    assert report["sourceEvidenceRefs"] == []


def test_inventory_table_uses_readable_display_columns_and_keeps_raw_columns() -> None:
    artifacts = build_visual_artifacts(
        question="OMODA9 一个版型多个物料号应该怎么解释？",
        answer={
            "title": "BOM logic",
            "direct": "先按实体关系解释。",
            "businessFrame": {"verdict": "需要区分版本和物料生命周期。"},
            "recommendedActions": [{"action": "画实体关系并定义生命周期"}],
        },
        evidence_package={
            "intent": "inventory_analysis",
            "country": "Sweden",
            "entities": {"models": ["OMODA9"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_bom", "label": "BOM material lifecycle risk", "value": "duplicate material", "source": "jato"}
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    assert artifacts[0]["id"] == "artifact_bom_entity_validation_table"
    table = next(item for item in artifacts if item["id"] == "artifact_inventory_analysis_table")
    assert table["spec"]["columns"] == ["market", "model", "version", "colorSpec", "materialCode", "availableUnits", "risk"]
    assert table["spec"]["rawColumns"] == ["market", "model", "version", "exterior", "interior", "materialCode", "availableUnits", "risk"]
    assert len(table["spec"]["columns"]) == 7
    assert "colorSpec" in table["data"]["rows"][0]
    assert "interior" not in table["data"]["rows"][0]
    assert table["spec"]["columnPolicy"].startswith("Main table is capped")


def test_existing_line_chart_prevents_trend_fallback() -> None:
    package = _evidence_package("market_overview")
    artifacts = build_visual_artifacts(
        question="Show trend chart",
        answer={"title": "Trend", "direct": "Trend returned."},
        evidence_package=package,
        charts=[
            {
                "chartId": "trend",
                "chartType": "line",
                "title": "Monthly trend",
                "data": [{"x": ["Jan"], "y": [10], "type": "scatter", "mode": "lines"}],
                "layout": {"title": "Monthly trend"},
            }
        ],
    )

    assert any(item["id"] == "trend" for item in artifacts)
    chart = next(item for item in artifacts if item["id"] == "trend")
    assert chart["spec"]["chartType"] == "line"
    assert chart["sourceEvidenceRefs"] == ["ev_1", "ev_2", "ev_3"]
    assert not any(item.get("fallbackReason") == "monthly trend series missing" for item in artifacts)
    assert package["missingEvidence"] == []


def test_trend_evidence_refs_build_line_chart_without_missing_evidence() -> None:
    package = {
        "evidenceId": "evpkg_visual_trend",
        "intent": "report_generation",
        "country": "Sweden",
        "confidence": "high",
        "toolResults": [
            {
                "toolName": "build_market_chart",
                "success": True,
                "sourceType": "generated",
                "evidenceRefs": [
                    {
                        "refId": "ev_t1",
                        "label": "contextSnapshot.yearSeries.2024.bevShare",
                        "value": 0.31,
                        "unit": "%",
                        "source": "jato",
                    },
                    {
                        "refId": "ev_t2",
                        "label": "contextSnapshot.yearSeries.2025.bevShare",
                        "value": 0.36,
                        "unit": "%",
                        "source": "jato",
                    },
                    {
                        "refId": "ev_t3",
                        "label": "contextSnapshot.monthSeries.2026-01.sales",
                        "value": 1020,
                        "unit": "units",
                        "source": "jato",
                    },
                ],
            }
        ],
        "missingEvidence": [],
    }
    artifacts = build_visual_artifacts(
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        answer={"title": "Trend", "direct": "Trend evidence returned."},
        evidence_package=package,
        charts=[],
    )

    trend = next(item for item in artifacts if item["id"] == "artifact_trend_series_chart")
    assert trend["spec"]["chartType"] == "line"
    assert trend["spec"]["seriesField"] == "series"
    assert trend["sourceEvidenceRefs"] == ["ev_t1", "ev_t2", "ev_t3"]
    assert not any(item.get("fallbackReason") == "monthly trend series missing" for item in artifacts)
    assert package["missingEvidence"] == []


def test_pricing_analysis_returns_table_artifact() -> None:
    package = _evidence_package("pricing_analysis")
    artifacts = build_visual_artifacts(
        question="J7 HEV pricing",
        answer={"title": "Pricing", "direct": "Use core corridor."},
        evidence_package=package,
        charts=[],
    )

    assert artifacts[0]["id"] == "artifact_pricing_analysis_framework_table"
    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_framework_table")
    assert table["title"] == "价格验证框架表"
    assert table["sourceEvidenceRefs"] == []
    assert table["spec"]["columns"] == ["model", "powertrain", "evidenceStatus", "msrp", "monthlyPayment", "rv", "pricePosition", "action"]
    assert table["spec"]["maxRows"] == 2
    assert len(table["data"]["rows"]) <= 10
    assert set(table["spec"]["columns"]).issubset(table["data"]["rows"][0])
    assert table["data"]["intentAnalysis"]["template"] == "pricing_analysis"
    assert table["data"]["intentAnalysis"]["evidenceMode"] == "missing_refs_framework"
    assert "evidenceRef" not in table["data"]["rows"][0]
    assert "source" not in table["data"]["rows"][0]


def test_pricing_table_prefers_decision_rows_for_target_price_range() -> None:
    artifacts = build_visual_artifacts(
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        answer={
            "title": "O9 pricing",
            "direct": "目标价落在样本走廊内。",
            "recommendedActions": [{"action": "补官方 MSRP 后定案"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["O9"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "query_price_positioning",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_min", "label": "priceStats.min", "value": 38600, "unit": "currency", "source": "jato"},
                        {"refId": "ev_max", "label": "priceStats.max", "value": 91304, "unit": "currency", "source": "jato"},
                        {"refId": "ev_avg", "label": "priceStats.avg", "value": 58300, "unit": "currency", "source": "jato"},
                        {"refId": "ev_median", "label": "priceStats.median", "value": 53165, "unit": "currency", "source": "jato"},
                    ],
                },
                {
                    "toolName": "user_supplied_target_price",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_target_min", "label": "User supplied own-model target price min", "value": 53000, "unit": "EUR", "source": "user_question"},
                        {"refId": "ev_target_max", "label": "User supplied own-model target price max", "value": 55000, "unit": "EUR", "source": "user_question"},
                        {"refId": "ev_target_mid", "label": "User supplied own-model target price midpoint", "value": 54000, "unit": "EUR", "source": "user_question"},
                    ],
                },
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    assert artifacts[0]["id"] == "artifact_pricing_corridor_chart"
    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_table")
    chart = next(item for item in artifacts if item["id"] == "artifact_pricing_corridor_chart")
    ids = [item["id"] for item in artifacts]
    rows = table["data"]["rows"]
    chart_rows = chart["data"]
    assert ids[:2] == ["artifact_pricing_corridor_chart", "artifact_pricing_analysis_table"]
    assert rows[0]["model"] == "O9"
    assert rows[0]["pricePosition"] == "走廊中段，低于样本均值"
    assert rows[1]["model"] == "Reference sample range"
    assert rows[2]["model"] == "Reference sample center"
    assert len(rows) <= 10
    assert all(set(row) <= set(table["spec"]["columns"]) for row in rows)
    assert chart["title"] == "价格走廊图"
    assert chart["spec"]["chartType"] == "bar"
    assert [row["label"] for row in chart_rows[:4]] == ["O9 target midpoint", "参考样本下沿", "参考样本上沿", "参考样本均值"]
    assert any(row["label"] == "O9 target midpoint" and row["value"] == 54000 for row in chart_rows)
    assert "ev_target_mid" in chart["sourceEvidenceRefs"]


def test_plain_pricing_question_does_not_render_tco_validation_from_generic_answer_text() -> None:
    artifacts = build_visual_artifacts(
        question="O9 在瑞典 53k-55k 欧元是否合理？",
        answer={
            "title": "O9 pricing",
            "direct": "目标价需要补官方 MSRP、月供/RV 和竞品走廊后定案。",
            "recommendedActions": [{"action": "补官方 MSRP、月供/RV 后定案"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["O9"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_price_positioning",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato"},
                        {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato"},
                        {"refId": "ev_target_min", "label": "User supplied own-model target price min", "value": 53000, "unit": "EUR", "source": "user_question"},
                        {"refId": "ev_target_max", "label": "User supplied own-model target price max", "value": 55000, "unit": "EUR", "source": "user_question"},
                    ],
                },
            ],
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Requested model has no current price row.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]

    assert "artifact_tco_validation_table" not in ids
    assert ids[:2] == ["artifact_pricing_corridor_chart", "artifact_pricing_analysis_table"]


def test_pricing_table_labels_target_range_partial_low_overlap() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利 T7 HEV 如果定在 32,000-35,000 EUR，是否能打 Corolla Cross 和 Tucson？",
        answer={
            "title": "T7 pricing",
            "direct": "目标价 32,000-35,000 EUR 是低位切入场景。",
            "recommendedActions": [{"action": "补齐本车型 MSRP、月供/RV 和配置差异"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Hungary",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "target_min", "label": "User supplied own-model target price min", "value": 32000, "unit": "EUR", "source": "user_material"},
                        {"refId": "target_max", "label": "User supplied own-model target price max", "value": 35000, "unit": "EUR", "source": "user_material"},
                        {"refId": "target_mid", "label": "User supplied own-model target price midpoint", "value": 33500, "unit": "EUR", "source": "user_material"},
                        {"refId": "cor_price", "label": "Corolla Cross.msrp", "value": 34500, "unit": "EUR", "source": "current_price"},
                        {"refId": "tuc_price", "label": "Tucson.msrp", "value": 36800, "unit": "EUR", "source": "current_price"},
                        {"refId": "price_min", "label": "priceStats.min", "value": 34500, "unit": "EUR", "source": "current_price"},
                        {"refId": "price_max", "label": "priceStats.max", "value": 36800, "unit": "EUR", "source": "current_price"},
                        {"refId": "price_median", "label": "priceStats.median", "value": 35650, "unit": "EUR", "source": "current_price"},
                    ],
                },
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_table")
    rows = table["data"]["rows"]
    assert rows[0]["model"] == "T7 HEV"
    assert rows[0]["pricePosition"] == "低位切入，部分进入样本走廊"
    assert rows[1]["model"] == "Corolla Cross"
    assert rows[1]["msrp"] == "34,500 EUR"
    assert rows[2]["model"] == "Tucson"
    assert rows[2]["msrp"] == "36,800 EUR"

    chart = next(item for item in artifacts if item["id"] == "artifact_pricing_corridor_chart")
    labels = [row["label"] for row in chart["data"]]
    assert "T7 HEV target midpoint" in labels
    assert "Corolla Cross" in labels
    assert "Tucson" in labels


def test_pricing_chart_requires_requested_model_or_target_price_anchor() -> None:
    artifacts = build_visual_artifacts(
        question="O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        answer={
            "title": "O5 BEV pricing",
            "direct": "3k 价差只是场景假设，必须补 O5/EV3 官方 MSRP 后定案。",
            "recommendedActions": [{"action": "先验证 O5 和 EV3 的官方价格来源"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV", "O5", "EV3"], "competitors": ["O5", "EV3"]},
            "confidence": "low",
            "toolResults": [
                {
                    "toolName": "query_price_positioning",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_price_positioning"},
                        {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato_price_positioning"},
                        {"refId": "ev_avg", "label": "priceStats.avg", "value": 48467.4, "unit": "currency", "source": "jato_price_positioning"},
                        {"refId": "ev_median", "label": "priceStats.median", "value": 52130.4, "unit": "currency", "source": "jato_price_positioning"},
                    ],
                },
                {
                    "toolName": "user_supplied_price_delta",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_delta", "label": "User supplied relative price delta", "value": -3000, "unit": "EUR", "source": "user_question"},
                    ],
                },
            ],
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Requested O5 BEV and EV3 current prices are not available.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_table")
    row_models = [row["model"] for row in table["data"]["rows"]]
    metric_cards = next((item for item in artifacts if item["id"] == "artifact_metric_cards"), None)
    metric_labels = [
        row["label"]
        for row in ((metric_cards or {}).get("data") or {}).get("rows", [])
    ]

    assert "artifact_pricing_corridor_chart" not in ids
    assert ids[0] == "artifact_pricing_analysis_table"
    assert "Relative price delta" in row_models
    assert "Reference sample range" in row_models
    assert "Reference sample center" in row_models
    assert "竞品价格走廊" not in row_models
    sample_row = next(row for row in table["data"]["rows"] if row["model"] == "Reference sample range")
    assert sample_row["pricePosition"] == "非本题核心竞品走廊；仅作已物化 MSRP 背景样本"
    assert "用户给定价差" in metric_labels
    assert "价格走廊上沿" not in metric_labels
    assert "价格走廊下沿" not in metric_labels


def test_pricing_table_uses_gap_pva_monthly_and_rv_refs() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典 J7 HEV 应该怎么定价？",
        answer={
            "title": "J7 HEV pricing",
            "direct": "核心竞争带中段 + 高配主推。",
            "recommendedActions": [{"action": "用价格走廊、PVA 和月供/RV 生成主销版本建议"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_msrp", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_gap", "label": "J7 HEV user material price gap", "value": 3230, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_monthly", "label": "J7 HEV monthlyPayment", "value": 499, "unit": "EUR/month", "source": "leasing_fixture"},
                        {"refId": "ev_rv", "label": "J7 HEV residualValue", "value": 45, "unit": "%", "source": "rv_fixture"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_table")
    rows = table["data"]["rows"]

    assert rows[0]["model"] == "J7 HEV"
    assert rows[0]["evidenceStatus"] == "用户材料假设；非当前官方 MSRP"
    assert rows[0]["msrp"] == "34,720 EUR"
    assert rows[0]["monthlyPayment"] == "499 EUR/month"
    assert rows[0]["rv"] == "45 %"
    assert "用户材料价格锚点；不是当前官方 MSRP" in rows[0]["pricePosition"]
    assert "PVA 118 %" in rows[0]["pricePosition"]
    assert rows[0]["action"] == "先把用户材料价格作为定位假设，补当前官方 MSRP、竞品官方价格、月供/RV 和配置差异后再定案。"
    assert rows[1]["model"] == "高配价值证明"
    assert rows[1]["msrp"] == "非 MSRP：高低配价差 3,230 EUR"
    assert rows[1]["pricePosition"] == "高配价值覆盖：PVA 118 % 覆盖价差 3,230 EUR"
    assert rows[1]["action"] == "用 PVA 覆盖率证明高配价差可被用户感知价值覆盖，支撑高配主推。"
    assert table["sourceEvidenceRefs"] == ["ev_msrp", "ev_pva"]


def test_pricing_table_separates_user_material_from_source_draft_candidates() -> None:
    artifacts = build_visual_artifacts(
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？请给出数据支撑、结论和图表。",
        answer={
            "title": "J7 HEV vs Sportage HEV 定价判断",
            "direct": "用户材料价格只能作为假设，Sportage 价格源仍待审核。",
            "recommendedActions": [{"action": "补 J7 和 Sportage 官方 MSRP 后再判断价差"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"], "competitors": ["Sportage HEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_j7_price", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                    ],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "sourceRepairCandidates": {
                            "dataStatus": "competitor_current_price_available_own_model_missing",
                            "candidateCount": 1,
                            "competitorCorridor": [
                                {
                                    "brand": "Kia",
                                    "model": "Sportage HEV",
                                    "country": "Sweden",
                                    "candidateSourceType": "source_draft",
                                    "draftStatus": "source_draft_available",
                                    "sourceDraftPath": "source_drafts/se/kia_sportage_hev.yaml",
                                    "reviewPendingStatus": "review_pending_not_current_price",
                                    "reviewPendingRows": 2,
                                }
                            ],
                        }
                    },
                },
            ],
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "J7 HEV and Sportage HEV official current prices are not materialized.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_table")
    rows = table["data"]["rows"]
    j7_row = next(row for row in rows if row["model"] == "J7 HEV")
    sportage_row = next(row for row in rows if row["model"] == "Sportage HEV")

    assert j7_row["evidenceStatus"] == "用户材料假设；非当前官方 MSRP"
    assert sportage_row["msrp"] == "待补官方 MSRP"
    assert sportage_row["evidenceStatus"] == "待审核价格候选；非当前价格证据"
    assert "source_draft_available" not in str(rows)
    assert "review_pending_not_current_price" not in str(rows)


def test_pricing_table_does_not_turn_market_context_refs_into_price_rows() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利 J7 HEV 是否应该比 Kia Sportage HEV 便宜？请给出数据支撑、结论和图表，不要回答瑞典。",
        answer={
            "title": "Hungary J7 HEV pricing",
            "direct": "市场结构能支撑方向判断，但价格矩阵缺失。",
            "recommendedActions": [{"action": "补 J7 HEV 和 Sportage HEV 官方 MSRP 后再判断价差"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Hungary",
            "entities": {"models": ["J7 HEV"], "competitors": ["Sportage HEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "hev_sales", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales", "value": 2687, "unit": "units", "source": "dashboardContext"},
                        {"refId": "suv_a0_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales", "value": 7303, "unit": "units", "source": "dashboardContext"},
                        {"refId": "suv_a_hev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.HEV_pct", "value": 16.1, "unit": "%", "source": "dashboardContext"},
                    ],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "sourceRepairCandidates": {
                            "dataStatus": "no_current_prices_for_requested_models",
                            "candidateCount": 2,
                            "ownModel": [
                                {
                                    "brand": "JAECOO",
                                    "model": "J7 HEV",
                                    "country": "Hungary",
                                    "candidateSourceType": "generic_official_price_search",
                                    "draftStatus": "candidate_search_query",
                                }
                            ],
                            "competitorCorridor": [
                                {
                                    "brand": "Kia",
                                    "model": "Sportage HEV",
                                    "country": "Hungary",
                                    "candidateSourceType": "generic_official_price_search",
                                    "draftStatus": "candidate_search_query",
                                }
                            ],
                        }
                    },
                },
            ],
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "Official current prices are not materialized.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    pricing_table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_table")
    market_table = next(item for item in artifacts if item["id"] == "artifact_pricing_market_structure_table")
    pricing_rows = pricing_table["data"]["rows"]
    pricing_text = str(pricing_rows)

    assert [row["model"] for row in pricing_rows[:2]] == ["J7 HEV", "Sportage HEV"]
    assert all(row["msrp"] == "待补官方 MSRP" for row in pricing_rows[:2])
    assert all("官方价格源候选" in row["evidenceStatus"] for row in pricing_rows[:2])
    assert "contextSnapshot" not in pricing_text
    assert "driveByFuel" not in pricing_text
    assert market_table["data"]["rows"]


def test_pricing_metric_cards_localize_direct_material_labels() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典 J7 HEV 应该怎么定价？",
        answer={
            "title": "J7 HEV pricing",
            "direct": "核心竞争带中段 + 高配主推。",
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "j7_main", "label": "J7 HEV main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "j7_corridor", "label": "J7 HEV competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "j7_gap", "label": "J7 HEV high-low trim price gap", "value": 3230, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "j7_pva", "label": "J7 HEV PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    metric_cards = next(item for item in artifacts if item["id"] == "artifact_metric_cards")
    labels = [row["label"] for row in metric_cards["data"]["rows"]]

    assert "J7 HEV 竞品价格带上沿" in labels
    assert "J7 HEV 竞品价格带下沿" in labels
    assert "J7 HEV 主销高配 MSRP 假设" in labels
    assert "J7 HEV PVA 覆盖率" in labels
    assert "J7 HEV competitor corridor上沿" not in labels
    assert "J7 HEV main trim MSRP" not in labels


def test_pricing_corridor_chart_is_kept_when_generic_tool_charts_exist() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典 J7 HEV 应该怎么定价？请给出竞品价格走廊、数据支撑和图表。",
        answer={
            "title": "J7 HEV pricing",
            "direct": "材料价格只能作为定位假设，不能当作当前官方 MSRP。",
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"], "competitors": ["Corolla Cross", "RAV4", "C-HR", "Qashqai"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_j7_price", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_method_fallback.txt"},
                        {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_method_fallback.txt"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "reason": "Official prices missing.", "impact": "weakens_answer"}
            ],
        },
        charts=[
            {
                "chartId": "top_ranking",
                "chartType": "bar",
                "title": "Top Models",
                "data": [{"x": [2945], "y": ["EX40"], "type": "bar"}],
                "layout": {},
            }
        ],
    )

    ids = [item["id"] for item in artifacts]
    chart = next(item for item in artifacts if item["id"] == "artifact_pricing_corridor_chart")

    assert "top_ranking" not in ids
    assert "artifact_metric_cards" in ids
    assert [row["label"] for row in chart["data"][:3]] == [
        "J7 HEV 用户材料主销价假设",
        "J7 HEV 用户材料竞品价格带下沿",
        "J7 HEV 用户材料竞品价格带上沿",
    ]
    assert chart["data"][0]["value"] == 34720
    assert chart["title"] == "定价假设走廊图"
    assert chart["spec"]["note"] == "仅为用户材料假设，不是当前官方 MSRP 或已验证竞品价。"


def test_pricing_corridor_chart_marks_mixed_verified_price_and_material_hypothesis() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典 J7 HEV 应该怎么定价？请给出官方价格、竞品价格走廊和图表。",
        answer={
            "title": "J7 HEV pricing",
            "direct": "官方价格和用户材料假设需要分开呈现。",
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"], "competitors": ["RAV4"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_j7_official", "label": "pricing.records.J7 HEV.msrp", "value": 34900, "unit": "EUR", "source": "current_price"},
                        {"refId": "ev_rav4_official", "label": "pricing.records.RAV4.msrp", "value": 40200, "unit": "EUR", "source": "current_price"},
                    ],
                },
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_j7_material", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    ],
                },
            ],
            "missingEvidence": [
                {"name": "monthly_payment", "reason": "No leasing rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No config rows.", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    chart = next(item for item in artifacts if item["id"] == "artifact_pricing_corridor_chart")
    labels = [row["label"] for row in chart["data"]]

    assert chart["title"] == "价格证据与假设图"
    assert "已验证价格锚点" in chart["subtitle"]
    assert "混合证据" in chart["spec"]["note"]
    assert "J7 HEV" in labels
    assert "RAV4" in labels
    assert "J7 HEV 用户材料主销价假设" in labels
    assert "J7 HEV 用户材料竞品价格带下沿" in labels
    assert chart["sourceEvidenceRefs"][:3] == ["ev_j7_official", "ev_rav4_official", "ev_j7_material"]


def test_pricing_artifacts_add_market_structure_table_and_drop_unrelated_top_models() -> None:
    artifacts = build_visual_artifacts(
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？请给出数据支撑、结论和图表。",
        answer={
            "title": "J7 HEV vs Sportage HEV pricing",
            "direct": "J7 HEV 应比 Sportage HEV 保持更强价格吸引力，但价差要靠市场结构、MSRP 和月供验证。",
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"], "competitors": ["Kia Sportage HEV"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_hev_sales", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.sales", "value": 1946, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_hev_2wd", "label": "contextSnapshot.crossTabs.driveByFuel.HEV.2WD_pct", "value": 85.9, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_hev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.HEV.Business_pct", "value": 54.0, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_hev_private", "label": "contextSnapshot.crossTabs.registrationByFuel.HEV.Private_pct", "value": 46.0, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_a0_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales", "value": 5416, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_a_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 7544, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_a0_hev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A0.HEV_pct", "value": 13.9, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_a_hev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.HEV_pct", "value": 5.4, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                },
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_j7_price", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                    ],
                },
            ],
            "missingEvidence": [],
        },
        charts=[
            {
                "chartId": "top_ranking",
                "chartType": "bar",
                "title": "Top Models",
                "data": [{"x": [2945, 2893], "y": ["EX40", "XC60"], "type": "bar"}],
                "layout": {},
            }
        ],
    )

    ids = [item["id"] for item in artifacts]
    table = next(item for item in artifacts if item["id"] == "artifact_pricing_market_structure_table")
    serialized_rows = str(table["data"]["rows"])

    assert "top_ranking" not in ids
    assert ids.index("artifact_pricing_analysis_table") < ids.index("artifact_pricing_market_structure_table")
    assert table["title"] == "定价相关市场结构证据表"
    assert "HEV需求池" in serialized_rows
    assert "1,946 units" in serialized_rows
    assert "2WD 85.9 %" in serialized_rows
    assert "Business 54 %" in serialized_rows
    assert "SUV A0" in serialized_rows
    assert "SUV A" in serialized_rows
    assert "定价问题背后有可量化需求池" in serialized_rows
    assert "官方 MSRP" in serialized_rows
    assert "ev_hev_sales" in table["sourceEvidenceRefs"]


def test_pricing_table_adds_user_relative_price_delta_row() -> None:
    artifacts = build_visual_artifacts(
        question="O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        answer={
            "title": "O5 vs EV3 pricing",
            "direct": "3k 价差只能作为验证假设。",
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_price_positioning",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "EUR", "source": "jato_msrp_postgres"},
                        {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "EUR", "source": "jato_msrp_postgres"},
                    ],
                },
                {
                    "toolName": "user_price_delta",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_delta", "label": "User supplied relative price delta", "value": 3000, "unit": "EUR", "source": "user_question"},
                    ],
                },
            ],
            "missingEvidence": [
                {"name": "current_msrp", "reason": "O5/EV3 official MSRP missing.", "impact": "weakens_answer"}
            ],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_table")
    rows = table["data"]["rows"]
    delta_row = next(row for row in rows if row["model"] == "Relative price delta")
    o5_row = next(row for row in rows if row["model"] == "O5 BEV")
    ev3_row = next(row for row in rows if row["model"] == "EV3")

    assert o5_row["msrp"] == "待补官方 MSRP"
    assert o5_row["pricePosition"] == "目标车型价格缺口"
    assert "EV3" in o5_row["action"]
    assert "月供/RV" in o5_row["action"]
    assert ev3_row["msrp"] == "待补官方 MSRP"
    assert ev3_row["pricePosition"] == "竞品价格缺口"
    assert "电池/续航/配置" in ev3_row["action"]
    assert delta_row["msrp"] == "3,000 EUR"
    assert delta_row["pricePosition"] == "用户给定价差假设；不是官方 MSRP"
    assert "电池/续航" in delta_row["action"]
    assert "company car" in delta_row["action"]
    assert "ev_delta" in table["sourceEvidenceRefs"]


def test_pricing_report_block_refreshes_stale_o5_ev3_sample_wording() -> None:
    artifacts = build_visual_artifacts(
        question="O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        answer={
            "title": "O5 EV3 pricing",
            "direct": (
                "价差判断：O5 BEV 比 EV3 小电池便宜 3k 暂时只能作为验证假设。"
                "当前价格样本显示：样本走廊 39,121.7-53,165.2，中位数 52,130.4。"
                "当前仍缺 O5/EV3 官方 MSRP 或版本价差证据。"
            ),
            "reportReadyBullets": [
                (
                    "Key message：价差判断：O5 BEV 比 EV3 小电池便宜 3k 暂时只能作为验证假设。"
                    "当前价格样本显示：样本走廊 39,121.7-53,165.2，中位数 52,130.4。"
                    "当前仍缺 O5/EV3 官方 MSRP 或版本价差证据。"
                ),
                "Evidence：本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证",
                "Next action：补齐本车型与竞品 MSRP / TP / 月供价格矩阵",
            ],
            "recommendedActions": [{"action": "补齐本车型与竞品 MSRP / TP / 月供价格矩阵"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "user_price_delta",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_delta", "label": "User supplied relative price delta", "value": 3000, "unit": "EUR", "source": "user_question"},
                    ],
                }
            ],
            "missingEvidence": [{"name": "current_msrp", "reason": "O5/EV3 official MSRP missing.", "impact": "weakens_answer"}],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["id"] == "artifact_report_block")
    key_message = report["data"]["keyMessage"]

    assert "当前价格样本显示" not in key_message
    assert "非本题核心车型的已物化价格背景" in key_message
    assert "不能当作 O5/EV3 官方 MSRP 或竞品价格走廊" in key_message


def test_pricing_table_groups_pricing_records_by_model() -> None:
    artifacts = build_visual_artifacts(
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        answer={
            "title": "J7 pricing corridor",
            "direct": "用竞品价格记录做锚点。",
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_rav_msrp", "label": "pricing.records.RAV4.msrp", "value": 40200, "unit": "EUR", "source": "pricing"},
                        {"refId": "ev_rav_power", "label": "pricing.records.RAV4.powertrain", "value": "HEV", "source": "pricing"},
                        {"refId": "ev_sport_min", "label": "pricing.records.Kia Sportage.minPrice", "value": 35800, "unit": "EUR", "source": "pricing"},
                        {"refId": "ev_sport_max", "label": "pricing.records.Kia Sportage.maxPrice", "value": 41400, "unit": "EUR", "source": "pricing"},
                        {"refId": "ev_sport_monthly", "label": "pricing.records.Kia Sportage.monthlyPayment", "value": 515, "unit": "EUR/month", "source": "pricing"},
                        {"refId": "ev_sport_rv", "label": "pricing.records.Kia Sportage.residualValue", "value": 47, "unit": "%", "source": "pricing"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_table")
    chart = next(item for item in artifacts if item["id"] == "artifact_pricing_corridor_chart")
    rows = table["data"]["rows"]
    chart_rows = chart["data"]

    assert rows[0]["model"] == "RAV4"
    assert rows[0]["powertrain"] == "HEV"
    assert rows[0]["msrp"] == "40,200 EUR"
    assert rows[0]["pricePosition"] == "竞品价格锚点"
    assert rows[1]["model"] == "Kia Sportage"
    assert rows[1]["msrp"] == "35,800 EUR-41,400 EUR"
    assert rows[1]["monthlyPayment"] == "515 EUR/month"
    assert rows[1]["rv"] == "47 %"
    assert table["sourceEvidenceRefs"] == ["ev_rav_msrp", "ev_sport_min"]
    assert [row["label"] for row in chart_rows] == ["RAV4", "Kia Sportage min", "Kia Sportage max"]
    assert chart_rows[0]["value"] == 40200
    assert chart["sourceEvidenceRefs"] == ["ev_rav_msrp", "ev_sport_min", "ev_sport_max"]


def test_pricing_table_turns_target_and_competitor_prices_into_row_level_actions() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利 T7 HEV 应该怎么定价？请给出数据表和价格走廊图。",
        answer={
            "title": "T7 HEV pricing",
            "direct": "T7 HEV 当前价格低于已查竞品价格带。",
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Hungary",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_t7_price", "label": "pricing.records.T7 HEV.msrp", "value": 33000, "unit": "EUR", "source": "current_price"},
                        {"refId": "ev_corolla_price", "label": "pricing.records.Corolla Cross.msrp", "value": 34500, "unit": "EUR", "source": "current_price"},
                        {"refId": "ev_tucson_price", "label": "pricing.records.Tucson.msrp", "value": 36800, "unit": "EUR", "source": "current_price"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "monthly_payment", "reason": "No leasing rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No config rows.", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_table")
    rows = table["data"]["rows"]
    t7_row = next(row for row in rows if row["model"] == "T7 HEV")
    corolla_row = next(row for row in rows if row["model"] == "Corolla Cross")

    assert t7_row["pricePosition"] == "低于已查竞品价格下沿 34,500，可做低位切入/价格锚点"
    assert "低位切入不是单纯低价" in t7_row["action"]
    assert "配置差异" in t7_row["action"]
    assert "月供/RV" in t7_row["action"]
    assert corolla_row["pricePosition"] == "高于本车型 1,500，作为竞品价格走廊上方锚点"
    assert "判断本车型低价是否有价值感" in corolla_row["action"]
    assert table["sourceEvidenceRefs"] == ["ev_t7_price", "ev_corolla_price", "ev_tucson_price"]
    assert "瑞典" not in str(table)


def test_pricing_artifacts_filter_unrelated_price_positioning_models_when_question_names_targets() -> None:
    artifacts = build_visual_artifacts(
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        answer={
            "title": "J7 HEV pricing",
            "direct": "J7 HEV 应用 Sportage HEV 做相对定价校验。",
            "recommendedActions": [{"action": "先补 J7 HEV / Sportage HEV 官方 MSRP"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV", "Sportage HEV"], "competitors": ["Sportage HEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_price_positioning"},
                        {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato_price_positioning"},
                        {"refId": "ev_avg", "label": "priceStats.avg", "value": 48467.39, "unit": "currency", "source": "jato_price_positioning"},
                        {"refId": "ev_enyaq", "label": "ENYAQ.msrp", "value": 52130.43, "unit": "currency", "source": "jato_price_positioning"},
                        {"refId": "ev_tayron", "label": "TAYRON.msrp", "value": 53165.22, "unit": "currency", "source": "jato_price_positioning"},
                        {"refId": "ev_j7", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_sales", "label": "cumulativeSales", "value": 1182452, "unit": "units", "source": "jato_country_snapshot"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_table")
    chart = next(item for item in artifacts if item["id"] == "artifact_pricing_corridor_chart")
    metric_cards = next(item for item in artifacts if item["id"] == "artifact_metric_cards")
    table_models = [row["model"] for row in table["data"]["rows"]]
    chart_labels = [row["label"] for row in chart["data"]]
    metric_labels = [row["label"] for row in metric_cards["data"]["rows"]]

    assert "ENYAQ" not in table_models
    assert "TAYRON" not in table_models
    assert "ENYAQ" not in chart_labels
    assert "TAYRON" not in chart_labels
    assert "cumulativeSales" not in metric_labels
    assert "J7 HEV" in table_models
    assert "Reference sample range" in table_models
    assert "竞品价格走廊" in table_models
    assert "J7 HEV 用户材料主销价假设" in chart_labels
    assert "参考样本下沿" in chart_labels
    assert "参考样本上沿" in chart_labels
    assert "J7 HEV 用户材料竞品价格带下沿" in chart_labels
    assert "J7 HEV 用户材料竞品价格带上沿" in chart_labels
    corridor_cards = [row for row in metric_cards["data"]["rows"] if "竞品价格带" in row["label"]]
    assert {row["label"] for row in corridor_cards} >= {"J7 HEV 用户材料竞品价格带下沿", "J7 HEV 用户材料竞品价格带上沿"}
    assert all(row["unit"] == "EUR" for row in corridor_cards)
    assert {row["value"] for row in corridor_cards} >= {30000.0, 40000.0}
    assert chart["title"] == "定价假设走廊图"


def test_leasing_tco_question_without_models_prioritizes_tco_validation_over_generic_pricing() -> None:
    artifacts = build_visual_artifacts(
        question="大客户 leasing 场景下，PHEV 还有没有理由？",
        answer={
            "title": "PHEV fleet leasing",
            "direct": "PHEV 是否成立要看 TCO、月供、残值、公司车税和充电条件。",
            "recommendedActions": [{"action": "建立 PHEV fleet leasing TCO 表"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": [], "competitors": []},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_price_positioning",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_price_positioning"},
                        {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato_price_positioning"},
                        {"refId": "ev_enyaq", "label": "ENYAQ.msrp", "value": 52130.43, "unit": "currency", "source": "jato_msrp_postgres"},
                        {"refId": "ev_tayron", "label": "TAYRON.msrp", "value": 53165.22, "unit": "currency", "source": "jato_msrp_postgres"},
                    ],
                },
            ],
            "missingEvidence": [
                {
                    "name": "leasing_tco_or_company_car_evidence",
                    "reason": "Question requires leasing, TCO, residual value, fleet, or company-car benefit evidence.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    repair_table = next(item for item in artifacts if item["id"] == "artifact_external_source_repair_table")
    tco_table = next(item for item in artifacts if item["id"] == "artifact_tco_validation_table")
    repair_rows = repair_table["data"]["rows"]

    assert ids[:2] == ["artifact_tco_validation_table", "artifact_external_source_repair_table"]
    assert "artifact_pricing_corridor_chart" not in ids
    assert "artifact_pricing_analysis_table" not in ids
    assert "artifact_pricing_analysis_framework_table" not in ids
    assert "artifact_metric_cards" not in ids
    assert "ENYAQ" not in str(artifacts)
    assert "TAYRON" not in str(artifacts)
    assert repair_table["title"] == "External source validation matrix"
    assert repair_rows[0]["sourceNeed"] == "Leasing/TCO/company-car source"
    assert "monthly payment/RV/tax formula" in repair_rows[0]["requiredFields"]
    assert "residual value" in repair_rows[0]["evidenceUse"]
    assert "VOC" not in str(repair_rows)
    assert "forum" not in str(repair_rows).lower()
    assert tco_table["data"]["rows"][0]["scenario"] == "Channel / fleet exposure"
    assert tco_table["data"]["rows"][1]["scenario"] == "Monthly payment / lease quote"


def test_tco_validation_table_does_not_treat_mining_lease_claim_as_monthly_payment() -> None:
    artifacts = build_visual_artifacts(
        question="大客户 leasing 场景下，PHEV 还有没有理由？",
        answer={
            "title": "PHEV fleet leasing",
            "direct": "PHEV fleet leasing 需要先看公司车暴露，再补月供、残值和税务 benefit。",
            "recommendedActions": [{"action": "建立 PHEV fleet leasing TCO 表"}],
        },
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "entities": {"models": ["PHEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {
                            "refId": "mining_claim",
                            "label": "Sweden mining lease.claim",
                            "value": "The Swedish government granted a 25-year mining lease to a rare-earth deposit.",
                            "source": "external_research",
                        }
                    ],
                },
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "phev_sales", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.sales", "value": 6498, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "bev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.Business_pct", "value": 60.3, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [
                {
                    "name": "leasing_tco_or_company_car_evidence",
                    "reason": "Question requires leasing, TCO, residual value, fleet, or company-car benefit evidence.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    tco_table = next(item for item in artifacts if item["id"] == "artifact_tco_validation_table")
    rows = tco_table["data"]["rows"]
    channel = next(row for row in rows if row["scenario"] == "Channel / fleet exposure")
    monthly = next(row for row in rows if row["scenario"] == "Monthly payment / lease quote")

    assert "PHEV 公司车注册占比" in channel["currentStatus"]
    assert "BEV 公司车注册占比" not in channel["currentStatus"]
    assert monthly["currentStatus"] == "待补可引用证据"
    assert "mining_claim" not in str(rows)


def test_pricing_report_block_prefers_business_conclusion_over_method_line() -> None:
    artifacts = build_visual_artifacts(
        question="大客户 leasing 场景下，PHEV 还有没有理由？",
        answer={
            "title": "PHEV fleet leasing",
            "direct": "直接结论：PHEV 是否成立要看 TCO、月供、残值、公司车税和充电条件。",
            "businessImplications": [
                "若 PHEV 不能在公司车成本或使用风险上比 BEV/HEV 更稳，就不应作为主推动力路线。"
            ],
            "evidenceDigest": [
                "PHEV 公司车注册占比：64.8%，但缺少 leasing/TCO 证据。"
            ],
            "reportReadyBullets": [
                "Key message：瑞典 定价逻辑应先验证目标车型的价格走廊、竞品池、配置价值和用户购买场景。",
                "Evidence：市场窗口、竞品走廊和配置差异要一起验证；若 MSRP、竞品价格、leasing/RV 或配置估值缺失，不能直接给确定价格。",
            ],
            "recommendedActions": [
                {
                    "action": "先在 External source repair table 中验证 leasing/TCO/company-car 补源入口，并生成当前价格记录。"
                }
            ],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": [], "competitors": []},
            "confidence": "medium",
            "toolResults": [],
            "missingEvidence": [
                {
                    "name": "leasing_tco_or_company_car_evidence",
                    "reason": "Question requires leasing, TCO, residual value, fleet, or company-car benefit evidence.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    report_block = next(item for item in artifacts if item["id"] == "artifact_report_block")
    data = report_block["data"]

    assert data["keyMessage"] == "PHEV 是否成立要看 TCO、月供、残值、公司车税和充电条件。"
    assert data["evidence"] == ["PHEV 公司车注册占比：64.8%，但缺少 leasing/TCO 证据。"]
    assert "定价逻辑应先验证" not in str(data)
    assert "External source repair table" not in str(data)
    assert "current_price" not in str(data)
    assert data["nextAction"] == "补齐 leasing/TCO/company-car 的月供、残值/RV、税务 benefit 和充电条件后，再判断 PHEV 大客户场景是否成立。"
    assert "外部来源验证矩阵" not in data["nextAction"]
    assert "当前价格记录" not in data["nextAction"]


def test_msrp_source_repair_candidates_render_as_pricing_table_artifact() -> None:
    artifacts = build_visual_artifacts(
        question="O5 BEV 如果比 EV3 小电池便宜 3k，逻辑是否成立？",
        answer={
            "title": "O5 vs EV3 pricing",
            "direct": "当前缺 O5/EV3 官方 MSRP，只能先作为验证假设。",
            "recommendedActions": [{"action": "按官方价格搜索候选补齐当前价格记录"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV", "EV3"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "diagnosis": "no_current_prices_for_requested_models",
                        "sourceRepairCandidates": {
                            "dataStatus": "own_model_current_price_source_candidates",
                            "missingOwnModelSource": True,
                            "ownModel": [
                                {
                                    "brand": "",
                                    "model": "O5 BEV",
                                    "sourceCode": "msrp-source-sweden-o5-bev-1",
                                    "draftStatus": "candidate_search_query",
                                    "sourceUrl": "https://www.google.com/search?q=Sweden+O5+BEV+official+price+MSRP",
                                    "candidateSourceType": "generic_official_price_search",
                                    "sourceSearchQuery": "Sweden O5 BEV pris price MSRP official",
                                },
                                {
                                    "brand": "KIA",
                                    "model": "EV3",
                                    "sourceCode": "kia_ev3_se_draft_scrapling",
                                    "draftStatus": "source_draft_available",
                                    "currentPriceRows": 0,
                                    "candidateSourceType": "source_draft",
                                    "candidateDomain": "kia.com",
                                    "sourceDraftPath": "se/04_kia_ev3_se.yaml",
                                    "relativePath": "se/04_kia_ev3_se.yaml",
                                    "sourceUrl": "https://www.kia.com/se/nya-bilar/ev3/upptack/",
                                    "materializationStatus": "ready_for_extraction",
                                    "materializationReviewStatus": "selector_review_required",
                                    "materializationRiskFlags": [
                                        "price_selector_too_broad",
                                        "vehicle_container_too_broad",
                                    ],
                                    "materializationReadinessScore": 1.0,
                                    "materializationRequiredFields": [
                                        "source_id/source_code",
                                        "country",
                                        "brand",
                                        "jato_model",
                                        "jato_trim",
                                        "msrp_value",
                                    ],
                                    "materializationWorkflow": [
                                        "msrp_workflow_service.create_scrape_batch_ingest",
                                        "msrp_workflow_service.materialize_current_price_from_observation",
                                    ],
                                    "materializationGate": "dry_run_review_required",
                                    "safeToAutoMaterialize": False,
                                    "priceSanityRules": {
                                        "currency": "SEK",
                                        "powertrain": "BEV",
                                        "expectedLocalPriceMin": 250000,
                                        "expectedLocalPriceMax": 1300000,
                                    },
                                    "dryRunCommand": "cd 07_ScrapingToolkit && python run.py --sources source_drafts/suv_only_country_model_top30/se/04_kia_ev3_se.yaml --dry-run -v",
                                    "submitCommand": "cd 07_ScrapingToolkit && python run.py --sources source_drafts/suv_only_country_model_top30/se/04_kia_ev3_se.yaml --api-base http://127.0.0.1:8001/v1 --trigger manual -v",
                                    "ingestApiPath": "POST /v1/msrp/batches",
                                    "reviewChecklist": [
                                        "dry_run_status_is_dry_run",
                                        "price_is_full_vehicle_msrp_not_monthly_payment_or_offer",
                                    ],
                                    "materializationNextStep": "Run the dry-run command, review observed trim/currency/date and price sanity, then ingest via create_scrape_batch_ingest only after approval.",
                                },
                            ],
                            "competitorCorridor": [],
                            "candidateCount": 2,
                            "materializedCandidateCount": 0,
                        },
                    },
                }
            ],
            "missingEvidence": [
                {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    repair_table = next(item for item in artifacts if item["id"] == "artifact_msrp_source_repair_table")
    rows = repair_table["data"]["rows"]

    assert repair_table["title"] == "MSRP 来源验证表"
    assert repair_table["sourceEvidenceRefs"] == []
    assert repair_table["spec"]["evidenceMode"] == "source_repair_candidates_not_price_evidence"
    assert repair_table["spec"]["columns"] == [
        "candidateRole",
        "model",
        "sourceType",
        "sourceStatus",
        "reviewPendingRows",
        "readiness",
        "reviewStatus",
        "requiredFields",
        "draftPath",
        "searchQuery",
        "nextStep",
    ]
    assert rows[0]["candidateRole"] == "请求车型"
    assert rows[0]["model"] == "O5 BEV"
    assert rows[0]["sourceType"] == "官方价格搜索"
    assert rows[0]["sourceStatus"] == "官方价格搜索候选"
    assert rows[0]["readiness"] == ""
    assert rows[0]["requiredFields"] == ""
    assert rows[0]["draftPath"] == ""
    assert rows[0]["searchQuery"] == "Sweden O5 BEV pris price MSRP official"
    assert "创建当前价格记录" in rows[0]["nextStep"]
    assert rows[1]["model"] == "KIA EV3"
    assert rows[1]["sourceType"] == "来源草稿"
    assert rows[1]["sourceStatus"] == "来源草稿待审核"
    assert rows[1]["readiness"] == "可进入抽取前审核 · 1.0"
    assert rows[1]["reviewStatus"] == "选择器需审核"
    assert "价格选择器过宽" in rows[1]["requiredFields"]
    assert rows[1]["draftPath"] == "se/04_kia_ev3_se.yaml"
    assert rows[1]["searchQuery"] == "https://www.kia.com/se/nya-bilar/ev3/upptack/"
    assert "dry-run" not in rows[1]["nextStep"]
    assert "current price" not in rows[1]["nextStep"].lower()
    assert rows[1]["nextStep"] == "先做抽取前审核，确认版本/配置、币种、日期和价格合理性；通过后再写入当前价格记录。"
    commands = repair_table["data"]["intentAnalysis"]["materializationCommands"]
    assert commands[0]["model"] == "KIA EV3"
    assert commands[0]["materializationGate"] == "dry_run_review_required"
    assert commands[0]["materializationReviewStatus"] == "selector_review_required"
    assert "vehicle_container_too_broad" in commands[0]["materializationRiskFlags"]
    assert commands[0]["priceSanityRules"]["expectedLocalPriceMin"] == 250000
    assert commands[0]["safeToAutoMaterialize"] is False
    assert "--dry-run" in commands[0]["dryRunCommand"]
    assert commands[0]["ingestApiPath"] == "POST /v1/msrp/batches"
    assert "price_is_full_vehicle_msrp_not_monthly_payment_or_offer" in commands[0]["reviewChecklist"]
    assert "current_price" not in str(repair_table)


def test_answer_level_msrp_source_repair_candidates_render_as_table_artifact() -> None:
    artifacts = build_visual_artifacts(
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        answer={
            "title": "J7 vs Sportage pricing",
            "direct": "先补 J7 / Sportage 官方 MSRP，再判断是否应该便宜。",
            "sourceRepairCandidates": {
                "dataStatus": "competitor_current_price_available_own_model_missing",
                "missingOwnModelSource": True,
                "ownModel": [
                    {
                        "brand": "",
                        "model": "J7 HEV",
                        "sourceCode": "msrp-source-sweden-j7-hev-1",
                        "draftStatus": "candidate_search_query",
                        "sourceUrl": "https://www.google.com/search?q=Sweden+J7+HEV+official+price+MSRP",
                    }
                ],
                "competitorCorridor": [
                    {
                        "brand": "KIA",
                        "model": "Sportage HEV",
                        "sourceCode": "msrp-source-sweden-sportage-hev-1",
                        "draftStatus": "candidate_search_query",
                        "candidateSourceType": "brand_official_search",
                        "candidateDomain": "kia.com/se",
                        "sourceSearchQuery": "site:kia.com/se Sweden KIA Sportage HEV pris price MSRP official",
                    }
                ],
                "candidateCount": 2,
            },
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "confidence": "medium",
            "toolResults": [],
            "missingEvidence": [
                {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    repair_table = next(item for item in artifacts if item["id"] == "artifact_msrp_source_repair_table")
    rows = repair_table["data"]["rows"]

    assert [row["model"] for row in rows] == ["J7 HEV", "KIA Sportage HEV"]
    assert rows[0]["candidateRole"] == "请求车型"
    assert rows[0]["sourceType"] == "官方价格搜索"
    assert rows[0]["searchQuery"] == "Sweden J7 HEV official price MSRP"
    assert rows[1]["candidateRole"] == "竞品/价格走廊"
    assert rows[1]["sourceType"] == "品牌官网搜索"


def test_msrp_source_repair_table_marks_review_pending_observations() -> None:
    artifacts = build_visual_artifacts(
        question="EX30 和 EV3 怎么做价格对比？",
        answer={
            "sourceRepairCandidates": {
                "dataStatus": "source_draft_candidate_not_price_evidence",
                "missingOwnModelSource": True,
                "ownModel": [
                    {
                        "brand": "VOLVO",
                        "model": "EX30",
                        "candidateSourceType": "source_draft",
                        "draftStatus": "source_draft_available",
                        "sourceDraftPath": "se/05_volvo_ex30_se.yaml",
                        "reviewPendingRows": 3,
                        "reviewPendingStatus": "review_pending_not_current_price",
                        "reviewPendingObservations": [
                            {
                                "trim": "Core",
                                "sourceMsrpValue": 429000,
                                "sourceCurrency": "SEK",
                                "evidenceStatus": "review_pending_not_current_price",
                            }
                        ],
                    }
                ],
                "competitorCorridor": [],
                "candidateCount": 1,
                "reviewPendingObservationCount": 3,
            }
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "confidence": "medium",
            "toolResults": [],
            "missingEvidence": [
                {"name": "current_msrp", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    repair_table = next(item for item in artifacts if item["id"] == "artifact_msrp_source_repair_table")
    row = repair_table["data"]["rows"][0]

    assert row["sourceType"] == "待审核价格观察"
    assert row["sourceStatus"] == "待审核价格观察：3（非正式当前价）"
    assert row["reviewPendingRows"] == 3
    assert row["reviewStatus"] == "价格观察待审核"
    assert row["nextStep"] == "审核版本/配置、币种、发布日期和来源；人工确认后再生成当前价格记录。"
    assert "3 pending review observations" in repair_table["data"]["intentAnalysis"]["coverage"]


def test_pending_msrp_review_observations_render_as_price_table_and_chart() -> None:
    artifacts = build_visual_artifacts(
        question="EX30 和 EV3 怎么做价格对比？",
        answer={
            "sourceRepairCandidates": {
                "dataStatus": "source_draft_candidate_not_price_evidence",
                "missingOwnModelSource": True,
                "ownModel": [
                    {
                        "brand": "VOLVO",
                        "model": "EX30",
                        "candidateSourceType": "source_draft",
                        "draftStatus": "source_draft_available",
                        "reviewPendingRows": 3,
                        "reviewPendingStatus": "review_pending_not_current_price",
                        "reviewPendingObservations": [
                            {
                                "brand": "VOLVO",
                                "model": "EX30",
                                "trim": "Ultra",
                                "sourceMsrpValue": 559000,
                                "sourceCurrency": "SEK",
                                "msrpValue": 48608.7,
                                "currency": "EUR",
                                "sourceUrl": "https://www.volvocars.com/se/build/ex30-electric/",
                                "reviewStatus": "open",
                                "matchStatus": "review_required",
                                "evidenceStatus": "review_pending_not_current_price",
                            },
                            {
                                "brand": "VOLVO",
                                "model": "EX30",
                                "trim": "Plus",
                                "sourceMsrpValue": 457000,
                                "sourceCurrency": "SEK",
                                "msrpValue": 39739.13,
                                "currency": "EUR",
                                "sourceUrl": "https://www.volvocars.com/se/build/ex30-electric/",
                                "reviewStatus": "open",
                                "matchStatus": "review_required",
                                "evidenceStatus": "review_pending_not_current_price",
                            },
                        ],
                    }
                ],
                "competitorCorridor": [],
                "candidateCount": 1,
                "reviewPendingObservationCount": 3,
            }
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["EX30"]},
            "confidence": "medium",
            "toolResults": [],
            "missingEvidence": [
                {"name": "current_msrp", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )
    artifact_ids = [item["id"] for item in artifacts]
    assert artifact_ids[:3] == [
        "artifact_pending_msrp_review_chart",
        "artifact_pending_msrp_review_table",
        "artifact_msrp_source_repair_table",
    ]

    table = next(item for item in artifacts if item["id"] == "artifact_pending_msrp_review_table")
    assert table["spec"]["evidenceMode"] == "review_pending_not_current_price"
    rows = table["data"]["rows"]
    assert rows[0]["model"] == "VOLVO EX30"
    assert rows[0]["trim"] == "Ultra"
    assert rows[0]["localMsrp"] == "559,000 SEK"
    assert rows[0]["eurMsrp"] == "48,608.7 EUR"
    assert rows[0]["source"] == "volvocars.com"
    assert rows[0]["decisionUse"] == "待审核：可用于 review/价格阶梯骨架，不能当确定 MSRP"
    assert "not accepted current price" in table["data"]["intentAnalysis"]["coverage"]

    chart = next(item for item in artifacts if item["id"] == "artifact_pending_msrp_review_chart")
    assert chart["spec"]["evidenceMode"] == "review_pending_not_current_price"
    assert chart["spec"]["chartType"] == "bar"
    assert chart["data"][0] == {
        "label": "VOLVO EX30 Ultra",
        "value": 559000.0,
        "unit": "SEK",
        "series": "review pending MSRP",
    }


def test_msrp_source_repair_table_filters_unrelated_candidates_for_display() -> None:
    artifacts = build_visual_artifacts(
        question="J7 HEV 是否应该比 Kia Sportage HEV 便宜？",
        answer={
            "title": "J7 vs Sportage pricing",
            "direct": "先补 J7 / Sportage 官方 MSRP，再判断是否应该便宜。",
            "sourceRepairCandidates": {
                "dataStatus": "competitor_current_price_available_own_model_missing",
                "missingOwnModelSource": True,
                "ownModel": [
                    {
                        "brand": "",
                        "model": "J7 HEV",
                        "sourceCode": "msrp-source-sweden-j7-hev-1",
                        "draftStatus": "candidate_search_query",
                        "sourceUrl": "https://www.google.com/search?q=Sweden+J7+HEV+official+price+MSRP",
                    },
                    {
                        "brand": "SKODA",
                        "model": "ENYAQ",
                        "draftStatus": "current_price_materialized",
                        "currentPriceRows": 1,
                        "candidateSourceType": "source_draft",
                        "sourceUrl": "https://www.skoda.se/modeller/enyaq/enyaq",
                    },
                ],
                "competitorCorridor": [
                    {
                        "brand": "KIA",
                        "model": "SPORTAGE",
                        "draftStatus": "source_draft_available",
                        "candidateSourceType": "source_draft",
                        "sourceDraftPath": "se/13_kia_sportage_se.yaml",
                        "relativePath": "se/13_kia_sportage_se.yaml",
                        "sourceUrl": "https://www.kia.com/se/nya-bilar/sportage/upptack/",
                    },
                    {
                        "brand": "VOLKSWAGEN",
                        "model": "TAYRON",
                        "draftStatus": "current_price_materialized",
                        "currentPriceRows": 1,
                        "candidateSourceType": "source_draft",
                        "sourceUrl": "https://www.volkswagen.se/",
                    },
                ],
                "candidateCount": 4,
                "materializedCandidateCount": 2,
            },
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {
                "models": ["J7 HEV"],
                "competitors": ["Kia Sportage HEV", "Sportage"],
            },
            "confidence": "medium",
            "toolResults": [],
            "missingEvidence": [
                {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    repair_table = next(item for item in artifacts if item["id"] == "artifact_msrp_source_repair_table")
    rows = repair_table["data"]["rows"]
    models = [row["model"] for row in rows]

    assert models == ["J7 HEV", "KIA SPORTAGE"]
    roles = {row["model"]: row["candidateRole"] for row in rows}
    assert roles["J7 HEV"] == "请求车型"
    assert roles["KIA SPORTAGE"] == "竞品/价格走廊"
    assert "SKODA ENYAQ" not in models
    assert "VOLKSWAGEN TAYRON" not in models
    coverage = repair_table["data"]["intentAnalysis"]["coverage"]
    assert coverage.startswith("0/2 candidates materialized")
    assert "requested model source missing" in coverage


def test_msrp_source_repair_table_marks_pricing_set_competitors_even_when_returned_as_own_model_candidates() -> None:
    artifacts = build_visual_artifacts(
        question="基于瑞典市场、竞品格局和配置差异，J7 HEV 应该怎么定价？",
        answer={
            "title": "J7 HEV pricing",
            "direct": "J7 HEV 可以先按核心竞争带中段 + 高配主推。",
            "recommendedActions": [{"action": "补齐 J7 HEV 与核心竞品官方 MSRP"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {
                "models": ["J7 HEV"],
                "competitors": ["Corolla Cross", "RAV4", "C-HR", "Qashqai"],
            },
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "diagnosis": "no_current_prices_for_requested_models",
                        "sourceRepairCandidates": {
                            "dataStatus": "own_model_current_price_source_candidates",
                            "ownModel": [
                                {"model": "J7 HEV", "draftStatus": "candidate_search_query", "candidateSourceType": "generic_official_price_search"},
                                {"brand": "TOYOTA", "model": "Corolla Cross", "draftStatus": "source_draft_available", "candidateSourceType": "source_draft"},
                                {"brand": "TOYOTA", "model": "RAV4", "draftStatus": "source_draft_available", "candidateSourceType": "source_draft"},
                                {"brand": "TOYOTA", "model": "C-HR", "draftStatus": "source_draft_available", "candidateSourceType": "source_draft"},
                            ],
                            "competitorCorridor": [],
                            "candidateCount": 4,
                            "materializedCandidateCount": 0,
                        },
                    },
                }
            ],
            "missingEvidence": [
                {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    repair_table = next(item for item in artifacts if item["id"] == "artifact_msrp_source_repair_table")
    roles = {row["model"]: row["candidateRole"] for row in repair_table["data"]["rows"]}
    assert roles["J7 HEV"] == "请求车型"
    assert roles["TOYOTA Corolla Cross"] == "竞品/价格走廊"
    assert roles["TOYOTA RAV4"] == "竞品/价格走廊"
    assert roles["TOYOTA C-HR"] == "竞品/价格走廊"


def test_policy_source_candidates_do_not_render_as_msrp_repair_artifact() -> None:
    artifacts = build_visual_artifacts(
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        answer={"title": "Policy", "direct": "先补官方政策来源。"},
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "confidence": "low",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "web",
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "sourceRepairCandidates": {
                            "dataStatus": "external_policy_source_candidates",
                            "missingOwnModelSource": False,
                            "ownModel": [],
                            "competitorCorridor": [
                                {
                                    "brand": "official",
                                    "model": "Sweden government policy source",
                                    "sourceCode": "policy-official-sweden-1",
                                },
                            ],
                            "candidateCount": 1,
                            "materializedCandidateCount": 0,
                        },
                    },
                }
            ],
            "missingEvidence": [
                {"name": "minimum_external_sources", "impact": "blocking"},
            ],
        },
        charts=[],
    )

    assert all(item["id"] != "artifact_msrp_source_repair_table" for item in artifacts)
    repair_table = next(item for item in artifacts if item["id"] == "artifact_external_source_repair_table")
    rows = repair_table["data"]["rows"]
    assert repair_table["title"] == "External source validation matrix"
    assert rows[0]["sourceNeed"] == "Official policy/news source"
    assert rows[0]["queryOrSource"] == "site:regeringen.se elbilspremie 2026 elbil prisgräns"
    assert rows[0]["validationStage"] == "search query candidate"
    assert rows[0]["requiredFields"] == "URL, title, publish date, official scope, policy effect"
    assert rows[0]["canUseInAnswer"] == "No - validate first"
    assert "official source" in rows[0]["nextStep"].lower()


def test_policy_artifacts_show_business_evidence_before_source_repair_when_both_exist() -> None:
    artifacts = build_visual_artifacts(
        question="CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        answer={
            "title": "Policy",
            "direct": "PHEV 是否有利要先看官方公式和 TCO。",
            "recommendedActions": [{"action": "核对 PHEV 认证 CO2、税率阶梯、company car 计算公式和发布日期"}],
        },
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "confidence": "low",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 1,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {"refId": "policy_source", "label": "CO2 tax.source", "value": "https://example.test/co2-tax", "source": "external_research"},
                        {"refId": "policy_claim", "label": "CO2 tax.claim", "value": "CO2 tax bands affect PHEV qualification.", "source": "external_research"},
                    ],
                    "coverageDiagnostics": {
                        "sourceRepairCandidates": {
                            "dataStatus": "external_policy_source_candidates",
                            "missingOwnModelSource": False,
                            "ownModel": [],
                            "competitorCorridor": [
                                {
                                    "brand": "official",
                                    "model": "Sweden CO2/company-car official source",
                                    "sourceCode": "policy-official-sweden-co2",
                                },
                            ],
                            "candidateCount": 1,
                            "materializedCandidateCount": 0,
                        },
                    },
                },
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "phev_sales", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.sales", "value": 6498, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [
                {"name": "minimum_external_sources", "impact": "blocking"},
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    assert ids[0] == "artifact_news_policy_search_table"
    assert ids.index("artifact_policy_market_context_table") < ids.index("artifact_external_source_repair_table")
    assert ids.index("artifact_tco_validation_table") < ids.index("artifact_external_source_repair_table")


def test_policy_source_validation_matrix_prefers_co2_tax_sources_over_bev_subsidy() -> None:
    artifacts = build_visual_artifacts(
        question="CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        answer={"title": "Policy", "direct": "先补 CO2 税率官方来源。"},
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "confidence": "low",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "web",
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "sourceRepairCandidates": {
                            "dataStatus": "external_policy_source_candidates",
                            "missingOwnModelSource": False,
                            "ownModel": [],
                            "competitorCorridor": [
                                {
                                    "brand": "official",
                                    "model": "Sweden BEV subsidy source",
                                    "sourceSearchQuery": "site:regeringen.se elbilspremie 2026 elbil prisgrans",
                                    "sourceCode": "policy-official-sweden-1",
                                },
                                {
                                    "brand": "official",
                                    "model": "Sweden vehicle-tax/bonus official source",
                                    "sourceSearchQuery": "site:transportstyrelsen.se elbil bonus malus 2026 prisgrans",
                                    "sourceCode": "policy-official-sweden-2",
                                },
                                {
                                    "brand": "official",
                                    "model": "Sweden company-car/tax official source",
                                    "sourceSearchQuery": "site:skatteverket.se bilforman elbil laddhybrid 2026",
                                    "sourceCode": "policy-official-sweden-3",
                                },
                            ],
                            "candidateCount": 3,
                            "materializedCandidateCount": 0,
                        },
                    },
                }
            ],
            "missingEvidence": [
                {"name": "minimum_external_sources", "impact": "blocking"},
            ],
        },
        charts=[],
    )

    repair_table = next(item for item in artifacts if item["id"] == "artifact_external_source_repair_table")
    queries = [row["queryOrSource"] for row in repair_table["data"]["rows"]]

    assert queries == ["site:skatteverket.se bilforman elbil laddhybrid 2026"]
    assert all("elbilspremie" not in query for query in queries)
    assert all("prisgrans" not in query for query in queries)
    assert "1 candidates" in repair_table["data"]["intentAnalysis"]["coverage"]


def test_policy_company_car_gap_renders_tco_source_validation_matrix() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        answer={"title": "Policy", "direct": "先补 company-car benefit 证据。"},
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "confidence": "low",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "web",
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "sourceRepairCandidates": {
                            "dataStatus": "external_policy_source_candidates",
                            "missingOwnModelSource": False,
                            "ownModel": [],
                            "competitorCorridor": [
                                {
                                    "brand": "official",
                                    "model": "Sweden company-car/tax official source",
                                    "sourceSearchQuery": "site:skatteverket.se bilforman elbil laddhybrid 2026",
                                    "sourceCode": "policy-official-sweden-3",
                                },
                            ],
                            "candidateCount": 1,
                            "materializedCandidateCount": 0,
                        },
                    },
                }
            ],
            "missingEvidence": [
                {"name": "leasing_tco_or_company_car_evidence", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    repair_table = next(item for item in artifacts if item["id"] == "artifact_external_source_repair_table")
    rows = repair_table["data"]["rows"]
    queries = [row["queryOrSource"] for row in rows]

    assert rows[0]["sourceNeed"] == "Leasing/TCO/company-car source"
    assert rows[0]["requiredFields"] == "URL, title, publish date, monthly payment/RV/tax formula, eligible model/scope"
    assert any("skatteverket" in query.lower() for query in queries)
    assert any("monthly payment" in query.lower() or "residual value" in query.lower() for query in queries)
    assert "leasing tco source candidates" in repair_table["data"]["intentAnalysis"]["coverage"]


def test_pricing_leasing_tco_candidates_do_not_render_as_msrp_repair_artifact() -> None:
    artifacts = build_visual_artifacts(
        question="大客户 leasing 场景下，PHEV 还有没有理由？",
        answer={"title": "PHEV fleet leasing", "direct": "先补 leasing/TCO/company-car 证据。"},
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "confidence": "low",
            "sourceRepairCandidates": {
                "dataStatus": "leasing_tco_source_candidates",
                "missingOwnModelSource": False,
                "ownModel": [],
                "competitorCorridor": [
                    {
                        "brand": "official",
                        "model": "TCO company-car tax / benefit formula source",
                        "sourceSearchQuery": "site:skatteverket.se bilförmån laddhybrid Sweden 2026",
                        "draftStatus": "candidate_search_query",
                    },
                    {
                        "brand": "official",
                        "model": "TCO leasing monthly payment / residual value source",
                        "sourceSearchQuery": "Sweden PHEV leasing monthly payment residual value fleet TCO",
                        "draftStatus": "candidate_search_query",
                    },
                ],
                "candidateCount": 2,
                "materializedCandidateCount": 0,
            },
            "toolResults": [],
            "missingEvidence": [
                {"name": "leasing_tco_or_company_car_evidence", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    repair_table = next(item for item in artifacts if item["id"] == "artifact_external_source_repair_table")
    tco_table = next(item for item in artifacts if item["id"] == "artifact_tco_validation_table")

    assert "artifact_external_source_repair_table" in ids
    assert "artifact_tco_validation_table" in ids
    assert "artifact_msrp_source_repair_table" not in ids
    assert repair_table["data"]["rows"][0]["sourceNeed"] == "Leasing/TCO/company-car source"
    assert "monthly payment/RV/tax formula" in repair_table["data"]["rows"][0]["requiredFields"]
    assert tco_table["title"] == "TCO / company-car validation table"
    assert "创建当前价格记录" not in str(artifacts)


def test_voc_external_research_queries_render_as_source_repair_table() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        answer={
            "title": "VOC",
            "direct": "当前只能给候选痛点，不能写成高频结论。",
            "recommendedActions": [{"action": "补 VOC/媒体/论坛来源后重跑验证"}],
        },
        evidence_package={
            "intent": "voc_analysis",
            "country": "Sweden",
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "web",
                    "evidenceRefs": [],
                    "coverageDiagnostics": {
                        "externalResearchQueries": [
                            "OMODA JAECOO Sweden Sverige owner review complaint forum",
                            "OMODA JAECOO Sverige ägare recension problem forum klagomål",
                        ],
                    },
                }
            ],
            "missingEvidence": [
                {"name": "external_research_claims_unavailable", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    repair_table = next(item for item in artifacts if item["id"] == "artifact_external_source_repair_table")
    rows = repair_table["data"]["rows"]

    assert ids.index("artifact_external_source_repair_table") < ids.index("artifact_voc_analysis_framework_table")
    assert repair_table["sourceEvidenceRefs"] == []
    assert repair_table["spec"]["evidenceMode"] == "external_source_repair_candidates_not_citations"
    assert rows[0]["sourceNeed"] == "VOC owner/media source"
    assert rows[0]["queryOrSource"] == "OMODA JAECOO Sweden Sverige owner review complaint forum"
    assert rows[0]["validationStage"] == "search query candidate"
    assert rows[0]["evidenceUse"] == "Identify recurring pain points and map them to product, dealer or warranty actions."
    assert rows[0]["requiredFields"] == "URL, title, publish date, claim text, market relevance"
    assert rows[0]["canUseInAnswer"] == "No - validate first"


def test_voc_missing_sources_without_candidates_renders_source_validation_first() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典用户会不会把 V2H 当成真实购买卖点？",
        answer={
            "title": "VOC",
            "direct": "当前只能把 V2H 写成待验证卖点。",
            "recommendedActions": [{"action": "补 V2H 用户原声和媒体测评来源"}],
        },
        evidence_package={
            "intent": "voc_analysis",
            "country": "Sweden",
            "confidence": "low",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "web",
                    "evidenceRefs": [],
                }
            ],
            "missingEvidence": [
                {"name": "external_research_claims_unavailable", "impact": "weakens_answer"},
                {"name": "minimum_external_sources", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    assert artifacts[0]["id"] == "artifact_external_source_repair_table"
    repair_table = artifacts[0]
    rows = repair_table["data"]["rows"]
    assert repair_table["spec"]["evidenceMode"] == "external_source_repair_candidates_not_citations"
    assert rows[0]["sourceNeed"] == "VOC owner/media source"
    assert "V2H EV purchase driver" in rows[0]["queryOrSource"]
    assert rows[0]["canUseInAnswer"] == "No - validate first"
    assert any(item["id"] == "artifact_voc_analysis_framework_table" for item in artifacts)


def test_competitor_table_groups_refs_into_model_decision_rows() -> None:
    artifacts = build_visual_artifacts(
        question="J8 7 座四驱为什么能打 Sorento？",
        answer={
            "title": "J8 competitor view",
            "direct": "先看竞品池和销量/价格证据。",
            "recommendedActions": [{"action": "生成 J8 vs Sorento / XC60 价格配置矩阵"}],
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento", "XC60"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_sor_model", "label": "competitor.1.model", "value": "Sorento", "source": "jato"},
                        {"refId": "ev_sor_sales", "label": "Sorento.sales", "value": 882, "unit": "units", "source": "jato"},
                        {"refId": "ev_sor_seg", "label": "Sorento.segment", "value": "SUV B", "source": "jato"},
                        {"refId": "ev_sor_power", "label": "Sorento.powertrain", "value": "PHEV", "source": "jato"},
                        {"refId": "ev_xc60_model", "label": "competitor.2.model", "value": "XC60", "source": "jato"},
                        {"refId": "ev_xc60_sales", "label": "XC60.sales", "value": 2893, "unit": "units", "source": "jato"},
                        {"refId": "ev_xc60_price", "label": "XC60.avgPrice", "value": 53165, "unit": "EUR", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_competitor_compare_table")
    chart = next(item for item in artifacts if item["id"] == "artifact_competitor_evidence_chart")
    ids = [item["id"] for item in artifacts]
    rows = table["data"]["rows"]
    chart_rows = chart["data"]

    assert ids[:2] == ["artifact_competitor_evidence_chart", "artifact_competitor_compare_table"]
    assert table["title"] == "竞品对比表"
    assert chart["title"] == "Competitor sales chart"
    assert chart["spec"]["chartType"] == "bar"
    assert [row["label"] for row in chart_rows] == ["XC60", "Sorento"]
    assert chart_rows[0]["value"] == 2893
    assert rows[0]["model"] == "Sorento"
    assert rows[0]["segment"] == "SUV B"
    assert rows[0]["powertrain"] == "PHEV"
    assert "Sales 882 units" in rows[0]["keyAdvantage"]
    assert rows[1]["model"] == "XC60"
    assert "Price 53,165 EUR" in rows[1]["keyAdvantage"]
    assert rows[1]["priceEvidence"] == "当前价格 53,165 EUR"
    assert rows[1]["productImplication"] == "用于判断正面对抗、错位定价或高配价值支撑。"
    assert "ev_sor_sales" in table["sourceEvidenceRefs"]
    assert all(set(row) == set(table["spec"]["columns"]) for row in rows)


def test_competitor_price_artifacts_show_actual_price_refs_for_scoring() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利 T7 HEV 是否应该比 Corolla Cross 和 Tucson 更便宜？",
        answer={
            "title": "T7 HEV competitor price view",
            "direct": "T7 HEV 当前价格低于竞品低端，可作为价格锚点，但仍需补配置和月供证据。",
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Hungary",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_t7_price", "label": "T7 HEV.avgPrice", "value": 33000, "unit": "EUR", "source": "jato"},
                        {"refId": "ev_corolla_price", "label": "Corolla Cross.avgPrice", "value": 38500, "unit": "EUR", "source": "jato"},
                        {"refId": "ev_tucson_price", "label": "Tucson.avgPrice", "value": 41000, "unit": "EUR", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    chart = next(item for item in artifacts if item["id"] == "artifact_competitor_evidence_chart")
    table = next(item for item in artifacts if item["id"] == "artifact_competitor_compare_table")
    rows = table["data"]["rows"]

    assert chart["title"] == "Competitor price chart"
    assert chart["data"] == [
        {"label": "Tucson", "value": 41000.0, "unit": "EUR", "series": "price"},
        {"label": "Corolla Cross", "value": 38500.0, "unit": "EUR", "series": "price"},
        {"label": "T7 HEV", "value": 33000.0, "unit": "EUR", "series": "price"},
    ]
    assert [row["model"] for row in rows] == ["T7 HEV", "Corolla Cross", "Tucson"]
    assert rows[0]["priceEvidence"] == "当前价格 33,000 EUR"
    assert rows[0]["gapVsOj"] == "低于已查竞品价格下沿 38,500，价格进入风险低但价值感待验证"
    assert "低位切入/价格锚点" in rows[0]["productImplication"]
    assert "低价不等于低价值" in rows[0]["productImplication"]
    assert rows[1]["priceEvidence"] == "当前价格 38,500 EUR"
    assert rows[1]["gapVsOj"] == "高于 T7 HEV 5,500，作为上方价格锚点"
    assert "配置价值、TCO 和品牌信任" in rows[1]["productImplication"]
    assert rows[2]["priceEvidence"] == "当前价格 41,000 EUR"
    assert rows[2]["gapVsOj"] == "高于 T7 HEV 8,000，作为上方价格锚点"
    assert "瑞典" not in str(table)


def test_competitor_table_does_not_treat_price_source_status_as_price() -> None:
    artifacts = build_visual_artifacts(
        question="J8 7座四驱为什么能打 Sorento？",
        answer={
            "title": "J8 vs Sorento",
            "direct": "市场结构支持先验证家庭/公司车场景，但价格仍待补源。",
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_sor_sales", "label": "Sorento.sales", "value": 0, "unit": "units", "source": "jato_cross_reference"},
                        {"refId": "ev_sor_status", "label": "Sorento.priceEvidenceStatus", "value": "candidate_search_query", "unit": "currency", "source": "jato_cross_reference"},
                        {"refId": "ev_sor_domain", "label": "Sorento.candidateDomain", "value": "kia.com/se", "source": "jato_cross_reference"},
                    ],
                }
            ],
            "missingEvidence": [{"name": "current_msrp", "impact": "weakens_answer"}],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_competitor_compare_table")
    row = table["data"]["rows"][0]

    assert row["model"] == "Sorento"
    assert row["priceEvidence"] == "需检索/确认官方价格源: kia.com/se"
    assert "candidate_search_query EUR" not in str(table)
    assert "Price candidate_search_query" not in row["keyAdvantage"]


def test_competitor_table_does_not_treat_zero_sales_as_advantage() -> None:
    artifacts = build_visual_artifacts(
        question="J7 HEV 是否应该比 Sportage HEV 便宜？",
        answer={
            "title": "J7 HEV vs Sportage HEV",
            "direct": "当前只有价格来源草稿，不能写成已验证胜出。",
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"], "competitors": ["Sportage HEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "sp_sales", "label": "Sportage HEV.sales", "value": 0, "unit": "units", "source": "jato_cross_reference"},
                        {"refId": "sp_status", "label": "Sportage HEV.priceEvidenceStatus", "value": "source_draft_available", "source": "jato_cross_reference"},
                        {"refId": "sp_path", "label": "Sportage HEV.sourceDraftPath", "value": "se/13_kia_sportage_se.yaml", "source": "jato_cross_reference"},
                    ],
                }
            ],
            "missingEvidence": [{"name": "competitor_sales", "impact": "weakens_answer"}],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_competitor_compare_table")
    row = table["data"]["rows"][0]

    assert row["model"] == "Sportage HEV"
    assert "Sales 0" not in row["keyAdvantage"]
    assert "0 units" not in str(table)
    assert row["keyAdvantage"] == "价格来源待物化，尚不能作为已验证 MSRP / 销量 / 配置优势。"
    assert row["priceEvidence"] == "有官方价格源草稿待物化: se/13_kia_sportage_se.yaml"


def test_competitor_price_question_can_show_reference_price_sample_chart() -> None:
    artifacts = build_visual_artifacts(
        question="J7 HEV 是否应该比 Sportage HEV 便宜？请用价格说明。",
        answer={
            "title": "J7 HEV vs Sportage HEV",
            "direct": "当前没有两车 current MSRP，只能先看参考价格样本。",
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"], "competitors": ["Sportage HEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "coverageDiagnostics": {
                        "referencePriceSample": {
                            "priceStats": {
                                "min": 39121.74,
                                "max": 53165.22,
                                "avg": 48467.39,
                                "median": 52130.43,
                                "currency": "EUR",
                            }
                        }
                    },
                    "evidenceRefs": [],
                }
            ],
            "missingEvidence": [
                {"name": "current_msrp", "impact": "weakens_answer"},
                {"name": "competitor_price_range", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    chart = next(item for item in artifacts if item["id"] == "artifact_pricing_corridor_chart")

    assert ids[0] == "artifact_pricing_corridor_chart"
    assert chart["title"] == "参考价格样本图"
    assert "参考样本" in chart["subtitle"]
    assert "仅为参考样本" in chart["spec"]["note"]
    assert [row["label"] for row in chart["data"]] == [
        "参考样本下沿",
        "参考样本上沿",
        "参考样本均值",
        "参考样本中位数",
    ]


def test_competitor_market_context_percent_refs_render_market_structure_chart() -> None:
    artifacts = build_visual_artifacts(
        question="J8 7座四驱为什么能打 Sorento？请给出市场数据支撑、竞品逻辑和图表。",
        answer={
            "title": "J8 vs Sorento",
            "direct": "7座四驱应先验证 SUV A 4WD、PHEV 和公司车场景。",
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_suv_a_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 7544, "unit": "units", "source": "dashboardContext"},
                        {"refId": "ev_suv_a_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct", "value": 60.1, "unit": "%", "source": "dashboardContext"},
                        {"refId": "ev_suv_a_phev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.PHEV_pct", "value": 38.2, "unit": "%", "source": "dashboardContext"},
                        {"refId": "ev_phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "dashboardContext"},
                    ],
                },
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_sorento_sales", "label": "Sorento.sales", "value": 0, "unit": "units", "source": "jato_cross_reference"},
                    ],
                },
            ],
            "missingEvidence": [{"name": "price_or_config_gap", "impact": "weakens_answer"}],
        },
        charts=[],
    )

    chart = next(item for item in artifacts if item["id"] == "artifact_market_structure_chart")
    metric_cards = next(item for item in artifacts if item["id"] == "artifact_metric_cards")
    rows = chart["data"]
    labels = {row["label"] for row in rows}
    analysis = metric_cards["data"]["intentAnalysis"]

    assert chart["title"] == "市场结构图"
    assert "SUV A 4WD" in labels
    assert "SUV A PHEV" in labels
    assert "PHEV Business" in labels
    assert "ev_suv_a_4wd" in chart["sourceEvidenceRefs"]
    assert metric_cards["subtitle"] == "竞品锚点和市场场景信号，均来自本轮 evidence refs。"
    assert analysis["template"] == "competitor_compare"
    assert "SUV A 4WD 占比" in analysis["marketContext"]
    assert "SUV A PHEV 渗透率" in analysis["marketContext"]
    assert analysis["competitorTable"] == []


def test_competitor_market_chart_prioritizes_direct_sorento_and_suv_b_context() -> None:
    artifacts = build_visual_artifacts(
        question="J8 7座四驱为什么能打 Sorento？请给出市场数据支撑、竞品逻辑和图表。",
        answer={
            "title": "J8 vs Sorento",
            "direct": "Sorento 是 SUV B / PHEV / 4WD 场景锚点，J8 需要补价格和配置证据。",
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_sorento_sales", "label": "Sorento.sales", "value": 309, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_sorento_seg", "label": "Sorento.segment", "value": "SUV B", "source": "jato_country_chart_deck"},
                        {"refId": "ev_sorento_power", "label": "Sorento.powertrain", "value": "PHEV", "source": "jato_country_chart_deck"},
                        {"refId": "ev_sorento_4wd_sales", "label": "Sorento.4WD_sales", "value": 309, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_sorento_business_sales", "label": "Sorento.Business_sales", "value": 152, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_b_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV B.sales", "value": 4259, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_b_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV B.4WD_pct", "value": 65.9, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev_4wd", "label": "contextSnapshot.crossTabs.driveByFuel.PHEV.4WD_pct", "value": 68.0, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_b_phev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV B.PHEV_pct", "value": 21.1, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [{"name": "price_or_config_gap", "impact": "weakens_answer"}],
        },
        charts=[],
    )

    chart = next(item for item in artifacts if item["id"] == "artifact_market_structure_chart")
    metric_cards = next(item for item in artifacts if item["id"] == "artifact_metric_cards")
    table = next(item for item in artifacts if item["id"] == "artifact_competitor_compare_table")
    labels = [row["label"] for row in chart["data"]]
    metric_labels = [row["label"] for row in metric_cards["data"]["rows"]]
    table_rows = table["data"]["rows"]

    assert labels[:3] == ["Sorento 销量", "Sorento 4WD", "Sorento 公司车"]
    assert "SUV B 4WD" in labels
    assert "PHEV 4WD" in labels
    assert "SUV B PHEV" in labels
    assert metric_labels[:3] == ["Sorento 销量", "Sorento 4WD", "Sorento 公司车"]
    assert table_rows[0]["model"] == "Sorento"
    assert table_rows[0]["segment"] == "SUV B"
    assert table_rows[0]["powertrain"] == "PHEV"
    assert "ev_sorento_sales" in chart["sourceEvidenceRefs"]


def test_competitor_table_adds_role_rows_for_o5_ex30_ev3_positioning() -> None:
    artifacts = build_visual_artifacts(
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        answer={
            "title": "O5 BEV competitor positioning",
            "direct": (
                "对标判断：O5 BEV 应优先用 EX30 做主对标，EV3 做价格/配置校验锚点。"
                "竞品角色：EX30 帮 O5 判断目标用户、品牌心智和产品定位；"
                "EV3 帮 O5 验证价格带、配置价值和购买替代理由。"
            ),
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato"},
                        {
                            "refId": "ev_ev3_position",
                            "label": "EV3.role",
                            "value": "price / configuration anchor",
                            "source": "jato",
                        },
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_competitor_compare_table")
    rows = table["data"]["rows"]
    ex30 = next(row for row in rows if row["model"] == "EX30")
    ev3 = next(row for row in rows if row["model"] == "EV3")

    assert ex30["segment"] == "主对标"
    assert ex30["powertrain"] == "BEV"
    assert "Sales 1,518 units" in ex30["keyAdvantage"]
    assert "目标用户、品牌心智和产品定位" in ex30["keyAdvantage"]
    assert "配置可赢点和短板" in ex30["gapVsOj"]
    assert "作为主对标" in ex30["productImplication"]
    assert ev3["segment"] == "价格/配置校验锚点"
    assert ev3["powertrain"] == "BEV"
    assert "价格带、配置价值和购买替代理由" in ev3["keyAdvantage"]
    assert "官方 MSRP" in ev3["gapVsOj"]
    assert "判断价差是否成立" in ev3["productImplication"]
    assert table["sourceEvidenceRefs"] == ["ev_ex30_sales", "ev_ev3_position"]


def test_competitor_table_adds_explicit_pending_role_without_model_evidence() -> None:
    artifacts = build_visual_artifacts(
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        answer={
            "title": "O5 BEV competitor positioning",
            "direct": (
                "对标判断：O5 BEV 应优先用 EX30 做主对标，EV3 做价格/配置校验锚点。"
                "竞品角色：EX30 帮 O5 判断目标用户、品牌心智和产品定位；"
                "EV3 帮 O5 验证价格带、配置价值和购买替代理由。"
            ),
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_competitor_compare_table")
    rows = table["data"]["rows"]
    ex30 = next(row for row in rows if row["model"] == "EX30")
    ev3 = next(row for row in rows if row["model"] == "EV3")

    assert ex30["segment"] == "主对标"
    assert ev3["segment"] == "价格/配置校验锚点"
    assert "待验证校验锚点" in ev3["keyAdvantage"]
    assert "直接车型证据待补" in ev3["keyAdvantage"]
    assert table["sourceEvidenceRefs"] == ["ev_ex30_sales"]


def test_competitor_table_keeps_source_repair_actions_out_of_product_implication() -> None:
    artifacts = build_visual_artifacts(
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        answer={
            "title": "O5 competitor view",
            "direct": "先看销量和场景，再补价格证据。",
            "recommendedActions": [
                {
                    "action": (
                        "先按官方价格搜索候选补齐本车型 MSRP 来源（O5 BEV, O5, EX30），"
                        "再写入 MSRP 来源验证表；这些搜索候选只是补证线索，不是最终价格引用。"
                    )
                }
            ],
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_ex30_model", "label": "competitor.1.model", "value": "EX30", "source": "jato"},
                        {"refId": "ev_ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [{"name": "current_price_rows", "impact": "weakens_answer"}],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_competitor_compare_table")
    rows = table["data"]["rows"]
    serialized = str(rows)

    assert rows[0]["model"] == "EX30"
    assert rows[0]["productImplication"] == "用于判断竞品池优先级和主销场景。"
    assert "MSRP 来源验证表" not in serialized
    assert "补证线索" not in serialized
    assert "当前价格记录" not in serialized


def test_competitor_artifacts_suppress_unrequested_competitor_refs_when_requested_missing() -> None:
    artifacts = build_visual_artifacts(
        question="J8 7 座四驱为什么能打 Sorento？",
        answer={
            "title": "J8 competitor view",
            "direct": "当前缺少可引用的 Sorento/J8 价格、配置和销量证据。",
            "recommendedActions": [{"action": "补 J8 / Sorento 价格配置矩阵"}],
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_ex40_sales", "label": "EX40.sales", "value": 2945, "unit": "units", "source": "jato_cross_reference"},
                        {"refId": "ev_xc60_sales", "label": "XC60.sales", "value": 2893, "unit": "units", "source": "jato_cross_reference"},
                        {"refId": "ev_modely_sales", "label": "MODEL Y.sales", "value": 2412, "unit": "units", "source": "jato_cross_reference"},
                        {"refId": "ev_ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato_cross_reference"},
                    ],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_price_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_msrp_postgres"},
                        {"refId": "ev_price_count", "label": "priceStats.count", "value": 4, "unit": "currency", "source": "jato_msrp_postgres"},
                    ],
                },
            ],
            "missingEvidence": [
                {
                    "name": "competitor_sales",
                    "reason": "No Sorento/J8 direct competitor metric rows.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    serialized = str(artifacts)

    assert "artifact_competitor_evidence_chart" not in ids
    assert "artifact_competitor_compare_table" not in ids
    assert "artifact_metric_cards" not in ids
    assert "MODEL Y" not in serialized
    assert "EX30" not in serialized
    assert "XC60.sales" not in serialized
    assert any(item["id"] == "artifact_report_block" for item in artifacts)
    report = next(item for item in artifacts if item["id"] == "artifact_report_block")
    assert report["sourceEvidenceRefs"] == []
    assert report["fallbackReason"] == "evidence_refs_missing"
    assert report["data"]["evidence"] == ["competitor_sales：待补可引用证据"]


def test_competitor_artifacts_show_market_context_chart_for_j8_sorento() -> None:
    artifacts = build_visual_artifacts(
        question="J8 7 座四驱为什么能打 Sorento？",
        answer={
            "title": "J8 competitor view",
            "direct": "J8 vs Sorento 需要用市场场景和竞品矩阵一起判断。",
            "recommendedActions": [{"action": "输出 J8 vs Sorento 价格配置/TCO 矩阵"}],
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_modely_sales", "label": "MODEL Y.sales", "value": 2412, "unit": "units", "source": "jato_cross_reference"},
                    ],
                },
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_suv_a_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 7544, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_a_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct", "value": 60.1, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev_sales", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.sales", "value": 6498, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "No J8/Sorento current price rows.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    chart = next(item for item in artifacts if item["id"] == "artifact_market_structure_chart")
    serialized = str(artifacts)

    assert ids[0] == "artifact_market_structure_chart"
    assert "artifact_competitor_evidence_chart" not in ids
    assert "artifact_competitor_compare_framework_table" in ids
    assert chart["title"] == "市场结构图"
    assert {row["label"] for row in chart["data"]} >= {"SUV A", "PHEV"}
    table = next(item for item in artifacts if item["id"] == "artifact_competitor_compare_framework_table")
    table_rows = table["data"]["rows"]
    assert table_rows[0]["model"] == "市场场景证据"
    assert "SUV A 4WD 占比 = 60.1 %" in str(table_rows)
    assert "MODEL Y" not in serialized
    assert "contextSnapshot.crossTabs" not in serialized


def test_competitor_market_context_artifacts_use_entities_instead_of_hardcoded_j8_sorento() -> None:
    artifacts = build_visual_artifacts(
        question="O5 BEV 应该对标 EV3 吗？",
        answer={
            "title": "O5 BEV competitor view",
            "direct": "O5 BEV vs EV3 需要用市场场景和竞品矩阵一起判断。",
            "recommendedActions": [{"action": "输出 O5 BEV vs EV3 价格配置/TCO 矩阵"}],
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"], "competitors": ["EV3"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_suv_a_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 7544, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suv_a_4wd", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct", "value": 60.1, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [
                {
                    "name": "coverage_diagnostic:no_current_prices_for_requested_models",
                    "reason": "No O5 BEV/EV3 current price rows.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    serialized = str(artifacts)

    assert "O5 BEV" in serialized
    assert "EV3" in serialized
    assert "J8" not in serialized
    assert "Sorento" not in serialized


def test_competitor_report_block_filters_method_lines_when_evidence_refs_missing() -> None:
    artifacts = build_visual_artifacts(
        question="J8 7 座四驱为什么能打 Sorento？",
        answer={
            "title": "J8 competitor positioning",
            "answerPreview": "竞品问题先补可引用证据再写结论：当前只能判断 J8 具备 7 座、四驱和家庭/公司车场景进入理由，不能证明已经赢过 Sorento。 产品动作：输出 J8 vs Sorento 的 7座/4WD/价格/配置/TCO 矩阵，销售话术落到家庭/公司车、冬季/长途和高配价值。 缺 MSRP、月供/RV 和配置证据时，不能说已验证胜出。",
            "reportReadyBullets": [
                "瑞典市场的竞品判断应先锁定竞品池，再拆价格、尺寸/级别、动力、配置和用户场景。",
                "正面对抗、错位竞争或价格锚点，而不是只列车型名称；",
                "Next action：生成竞品矩阵并补齐价格/配置证据",
                "建议动作：补 J8 / Sorento 价格配置矩阵",
            ],
            "businessImplications": ["竞品定位方法：竞品对比先定义对标关系，再判断正面对抗、错位竞争或价格锚点。"],
            "recommendedActions": [{"action": "生成 J8 vs Sorento 的 7座/四驱/价格/配置/TCO 决策矩阵"}],
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "confidence": "medium",
            "toolResults": [],
            "missingEvidence": [],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["id"] == "artifact_report_block")

    assert report["sourceEvidenceRefs"] == []
    assert report["fallbackReason"] == "evidence_refs_missing"
    assert report["data"]["keyMessage"].startswith("竞品问题先补可引用证据再写结论")
    assert report["data"]["evidence"] == ["Competitor table：待补可引用证据", "Feature delta：待补可引用证据"]
    assert report["data"]["productImplication"].startswith("输出 J8 vs Sorento 的 7座/4WD/价格/配置/TCO 矩阵")
    assert report["data"]["nextAction"] == "生成 J8 vs Sorento 的 7座/四驱/价格/配置/TCO 决策矩阵"
    assert "竞品判断应先锁定" not in str(report["data"]["evidence"])
    assert "正面对抗、错位竞争" not in str(report["data"]["evidence"])


def test_competitor_report_block_uses_pm_judgment_over_generic_implication() -> None:
    artifacts = build_visual_artifacts(
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        answer={
            "title": "O5 competitor positioning",
            "answerPreview": (
                "对标判断：O5 BEV 应优先用 EX30 做主对标，EV3 做价格/配置校验锚点。 "
                "## 产品经理判断\n"
                "- 不要把 EX30 和 EV3 等权罗列，应输出 O5 的可赢点、短板、价格边界和补证清单。\n"
                "## 下一步动作\n"
                "- 生成竞品矩阵"
            ),
            "businessImplications": [
                "竞品定位方法：竞品对比先定义对标关系，再判断正面对抗、错位竞争或价格锚点。",
                "结论要能转成配置、价格、销售话术和报告页。",
            ],
            "recommendedActions": [{"action": "生成竞品矩阵"}],
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["id"] == "artifact_report_block")

    assert report["data"]["productImplication"] == "不要把 EX30 和 EV3 等权罗列，应输出 O5 的可赢点、短板、价格边界和补证清单。"
    assert "结论要能转成" not in str(report["data"])
    assert "竞品定位方法" not in str(report["data"])


def test_competitor_report_block_trims_repair_tail_from_inline_product_action() -> None:
    artifacts = build_visual_artifacts(
        question="O9 和 XC60 / EX60 的定位差异是什么？",
        answer={
            "title": "O9 competitor positioning",
            "answerPreview": (
                "对标判断：O9 与 XC60 / EX60 应先写成错位定位判断。 "
                "产品动作：把已验证的销量/价格/级别锚点先转成定位差异，再补目标车型价格、配置和用户场景；不要只停在生成矩阵。\n\n"
                "## 下一步动作\n"
                "- P0 · 先按官方价格搜索候选补齐本车型 MSRP 来源（O9, XC60, EX60），确认 URL、版本/配置、币种、发布日期后生成当前价格记录；这些候选只是补证线索。"
            ),
            "businessImplications": ["结论要能转成配置、价格、销售话术和报告页。"],
            "recommendedActions": [{"action": "生成竞品矩阵"}],
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["O9"], "competitors": ["XC60", "EX60"]},
            "confidence": "medium",
            "toolResults": [],
            "missingEvidence": [],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["id"] == "artifact_report_block")
    product_implication = report["data"]["productImplication"]

    assert product_implication == "把已验证的销量/价格/级别锚点先转成定位差异，再补目标车型价格、配置和用户场景；不要只停在生成矩阵。"
    assert "补证线索" not in product_implication
    assert "当前价格记录" not in product_implication


def test_competitor_table_expands_user_material_competitor_pool() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典 J7 HEV 核心竞品是谁？",
        answer={
            "title": "J7 competitor pool",
            "direct": "竞品池先按用户材料锁定。",
            "recommendedActions": [{"action": "补齐 Corolla Cross / RAV4 / Qashqai 价格配置矩阵"}],
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {
                            "refId": "ev_pool",
                            "label": "J7 HEV user material competitor pool",
                            "value": "Corolla Cross, RAV4, C-HR, Qashqai",
                            "source": "J7_HEV_V4.pptx",
                        }
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_competitor_compare_table")
    rows = table["data"]["rows"]

    assert [row["model"] for row in rows[:4]] == ["Corolla Cross", "RAV4", "C-HR", "Qashqai"]
    assert rows[0]["segment"] == "用户材料竞品池"
    assert rows[0]["keyAdvantage"] == "已被用户材料列为核心对标对象"
    assert rows[0]["productImplication"] == "作为竞品池行，用于确认正面对抗、错位竞争或价格锚点。"
    assert table["sourceEvidenceRefs"] == ["ev_pool"]


def test_competitor_framework_table_keeps_source_repair_actions_out_of_product_implication() -> None:
    artifacts = build_visual_artifacts(
        question="O5 BEV 应该对标 EX30 还是 EV3？",
        answer={
            "title": "O5 competitor framework",
            "direct": "当前价格证据不足，先给竞品判断框架。",
            "recommendedActions": [
                {
                    "action": (
                        "先在 MSRP 来源验证表 中补齐 O5 BEV、EX30、EV3 当前价格记录；"
                        "这些候选只是补证线索，不能当最终结论。"
                    )
                }
            ],
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
            "confidence": "low",
            "toolResults": [],
            "missingEvidence": [
                {"name": "current_price_rows", "impact": "weakens_answer"},
                {"name": "configuration_delta", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_competitor_compare_framework_table")
    report = next(item for item in artifacts if item["id"] == "artifact_report_block")
    rows = table["data"]["rows"]
    serialized = str(rows)
    key_message = report["data"]["keyMessage"]

    assert rows[1]["model"] == "核心竞品池"
    assert rows[1]["productImplication"] == "补齐价格/配置/销量矩阵后确认正面对抗、错位竞争或价格锚点。"
    assert "MSRP 来源验证表" not in serialized
    assert "补证线索" not in serialized
    assert "当前价格记录" not in serialized


def test_competitor_framework_table_uses_market_context_rows_for_j8_sorento() -> None:
    artifacts = build_visual_artifacts(
        question="J8 7 座四驱为什么能打 Sorento？",
        answer={
            "title": "J8 vs Sorento",
            "direct": "J8 7 座四驱对 Sorento 的打法有市场场景支撑。",
            "businessImplications": [
                "输出 J8 vs Sorento 的 7座/4WD/价格/配置/TCO 矩阵，销售话术落到家庭/公司车、冬季/长途和高配价值。"
            ],
            "recommendedActions": [
                {"action": "验证 J8/Sorento 官方 MSRP"},
                {"action": "生成 J8 vs Sorento 配置/TCO 矩阵"},
            ],
        },
        evidence_package={
            "intent": "competitor_compare",
            "country": "Sweden",
            "entities": {"models": ["J8"], "competitors": ["Sorento"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "rowCount": 4,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {
                            "refId": "ev_suv_a_4wd",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct",
                            "value": 60.1,
                            "unit": "%",
                            "source": "jato_country_chart_deck",
                        },
                        {
                            "refId": "ev_suv_a_sales",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales",
                            "value": 7544,
                            "unit": "units",
                            "source": "jato_country_chart_deck",
                        },
                        {
                            "refId": "ev_suv_a_phev",
                            "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.PHEV_pct",
                            "value": 38.2,
                            "unit": "%",
                            "source": "jato_country_chart_deck",
                        },
                        {
                            "refId": "ev_phev_business",
                            "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct",
                            "value": 64.8,
                            "unit": "%",
                            "source": "jato_country_chart_deck",
                        },
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "coverage_diagnostic:no_current_prices_for_requested_models", "impact": "weakens_answer"},
                {"name": "coverage_diagnostic:no_config_projects_for_country", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    table = next(
        item
        for item in artifacts
        if item["id"] == "artifact_competitor_compare_framework_table"
    )
    report = next(item for item in artifacts if item["id"] == "artifact_report_block")
    rows = table["data"]["rows"]
    serialized = str(rows)
    key_message = report["data"]["keyMessage"]

    assert rows[0]["model"] == "市场场景证据"
    assert rows[0]["segment"] == "SUV A"
    assert rows[0]["powertrain"] == "4WD"
    assert "SUV A 4WD 占比 = 60.1 %" in rows[0]["keyAdvantage"]
    assert "PHEV 公司车注册占比 = 64.8 %" in serialized
    assert "SUV A 细分销量 = 7,544 units" in serialized
    assert "目标车型价格、配置、销量证据待补" in serialized
    assert "直接竞品价格/配置/TCO 证据待补" in serialized
    assert "J8/Sorento" in serialized
    assert "不能只凭市场场景判定已胜出" in serialized
    assert "ev_suv_a_4wd" in table["sourceEvidenceRefs"]
    assert "missing_refs_framework" not in str(table.get("spec", {}))
    assert key_message.startswith("对标判断：J8 vs Sorento 应先写成证据驱动的场景验证")
    assert "SUV A 4WD 占比 60.1 %" in key_message
    assert "SUV A 细分销量 7,544 units" in key_message
    assert "SUV A PHEV 渗透率 38.2 %" in key_message
    assert "PHEV 公司车注册占比 64.8 %" in key_message
    assert "不能替代车型级官方 MSRP、配置和 TCO 交叉验证" in key_message


def test_configuration_table_groups_variant_delta_refs_into_feature_rows() -> None:
    artifacts = build_visual_artifacts(
        question="O5 BEV 应该对标 EX30 还是 EV3？配置差异是什么？",
        answer={
            "title": "O5 BEV configuration gap",
            "direct": "配置差异要转成用户价值。",
            "recommendedActions": [{"action": "生成 O5 BEV vs EV3/EX30 配置价值矩阵"}],
        },
        evidence_package={
            "intent": "configuration_analysis",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"], "competitors": ["EV3", "EX30"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "sourceType": "engineering",
                    "evidenceRefs": [
                        {"refId": "ev_battery_delta", "label": "configuration_delta.Battery size", "value": "O5 smaller than EV3 long range", "source": "variant_compare"},
                        {"refId": "ev_battery_target", "label": "Battery size.targetValue", "value": "61 kWh", "source": "variant_compare"},
                        {"refId": "ev_battery_comp", "label": "Battery size.competitorValue", "value": "81 kWh", "source": "variant_compare"},
                        {"refId": "ev_heat", "label": "configuration_delta.Winter package", "value": "Heat pump and pre-conditioning need cross-check", "source": "variant_compare"},
                        {"refId": "ev_heat_priority", "label": "Winter package.priority", "value": "P0", "source": "variant_compare"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_configuration_analysis_table")
    rows = table["data"]["rows"]

    assert rows[0]["feature"] == "Battery size"
    assert rows[0]["targetModel"] == "O5 BEV"
    assert "目标：61 kWh" in rows[0]["validationData"]
    assert "竞品：81 kWh" in rows[0]["validationData"]
    assert rows[0]["sourceOrTool"] == "variant_compare"
    assert "证明冬季真实续航" in rows[0]["acceptanceCriteria"]
    assert rows[0]["currentStatus"] == "已有 evidenceRef: ev_battery_delta"
    assert rows[1]["feature"] == "Winter package"
    assert rows[1]["priority"] == "P0"
    assert rows[1]["validationData"] == "Heat pump and pre-conditioning need cross-check"
    assert "北欧竞品标配" in rows[1]["acceptanceCriteria"]
    assert "ev_battery_delta" in table["sourceEvidenceRefs"]
    assert table["spec"]["columns"] == [
        "feature",
        "targetModel",
        "validationData",
        "sourceOrTool",
        "acceptanceCriteria",
        "currentStatus",
        "priority",
    ]
    assert all(set(row) == set(table["spec"]["columns"]) for row in rows)


def test_configuration_artifacts_prioritize_table_and_report_before_metric_cards() -> None:
    artifacts = build_visual_artifacts(
        question="A0 SUV BEV 为什么需要 80kWh 电池？",
        answer={
            "title": "80kWh validation",
            "direct": "电池判断：80kWh 应定位为长续航/高配安全边界。",
            "reportReadyBullets": ["产品动作：高配/长续航版用 80kWh、热泵和快充打价值感。"],
            "recommendedActions": [{"action": "生成 80kWh 续航-价格-重量验证表"}],
        },
        evidence_package={
            "intent": "configuration_analysis",
            "country": "Sweden",
            "entities": {"models": ["A0 SUV BEV"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "sourceType": "engineering",
                    "evidenceRefs": [
                        {"refId": "ev_battery", "label": "configuration_delta.Battery size", "value": "80kWh high-trim boundary", "source": "variant_compare"},
                        {"refId": "ev_winter", "label": "Winter package.priority", "value": "P0", "source": "variant_compare"},
                        {"refId": "ev_range", "label": "range", "value": 500, "unit": "km", "source": "variant_compare"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    assert [(item["id"], item["type"]) for item in artifacts[:3]] == [
        ("artifact_configuration_analysis_table", "table"),
        ("artifact_report_block", "report_block"),
        ("artifact_metric_cards", "metric_cards"),
    ]
    report = next(item for item in artifacts if item["id"] == "artifact_report_block")
    report_data = report["data"]
    joined_evidence = " ".join(report_data["evidence"])
    assert "Battery size" in joined_evidence
    assert "80kWh high-trim boundary" in joined_evidence
    assert "Winter package" in joined_evidence
    assert "P0" in joined_evidence
    assert report_data["productImplication"].startswith("A0 SUV BEV 配置页应把")
    assert "Battery size=80kWh high-trim boundary" in report_data["productImplication"]
    assert "Winter package=P0" in report_data["productImplication"]
    assert "生成 80kWh 续航-价格-重量验证表" not in report_data["productImplication"]


def test_configuration_artifacts_filter_generic_market_auto_charts() -> None:
    artifacts = build_visual_artifacts(
        question="A0 SUV BEV 为什么需要 80kWh 电池？",
        answer={
            "title": "80kWh validation",
            "direct": "电池判断：80kWh 应定位为长续航/高配安全边界。",
            "recommendedActions": [{"action": "生成 80kWh 续航-价格-重量验证表"}],
        },
        evidence_package={
            "intent": "configuration_analysis",
            "country": "Sweden",
            "entities": {"models": ["A0 SUV BEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_version", "label": "versionCount", "value": 9204, "unit": "units", "source": "jato_country_snapshot"},
                    ],
                }
            ],
            "missingEvidence": [{"name": "competitive_or_configuration_data_unavailable", "reason": "No trim matrix."}],
        },
        charts=[
            {
                "chartId": "auto_top_models",
                "chartType": "bar",
                "title": "Top Models",
                "data": [{"x": [2945], "y": ["EX40"], "type": "bar"}],
                "layout": {},
            },
            {
                "chartId": "battery_range_validation",
                "chartType": "bar",
                "title": "Battery range validation",
                "data": [{"x": [80, 64], "y": ["Long range", "Entry"], "type": "bar"}],
                "layout": {},
            },
        ],
    )

    artifact_ids = [item["id"] for item in artifacts]

    assert "auto_top_models" not in artifact_ids
    assert "battery_range_validation" in artifact_ids
    assert "artifact_metric_cards" not in artifact_ids


def test_configuration_artifacts_show_market_structure_chart_from_cross_tab_refs() -> None:
    artifacts = build_visual_artifacts(
        question="A0 SUV BEV 为什么需要 80kWh 电池？",
        answer={
            "title": "80kWh validation",
            "direct": "电池判断：80kWh 应定位为长续航/高配安全边界，同时要用 A0/A SUV BEV 市场结构验证需求规模。",
            "reportReadyBullets": ["产品动作：先看 SUV A0/A BEV 体量，再补竞品电池/续航配置矩阵。"],
            "recommendedActions": [{"action": "生成 80kWh 续航-价格-重量验证表"}],
        },
        evidence_package={
            "intent": "configuration_analysis",
            "country": "Sweden",
            "entities": {"models": ["A0 SUV BEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "compare_vehicle_variants",
                    "success": True,
                    "sourceType": "engineering",
                    "evidenceRefs": [
                        {"refId": "ev_battery", "label": "configuration_delta.Battery size", "value": "80kWh high-trim boundary", "source": "variant_compare"},
                    ],
                },
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_suva0_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales", "value": 5416, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suva_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.sales", "value": 7544, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_suva_bev", "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.BEV_pct", "value": 40.0, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [{"name": "competitive_or_configuration_data_unavailable", "reason": "No trim matrix."}],
        },
        charts=[],
    )

    artifact_ids = [item["id"] for item in artifacts]
    assert artifact_ids[:2] == ["artifact_market_structure_chart", "artifact_configuration_analysis_table"]

    chart = artifacts[0]
    assert chart["title"] == "市场结构图"
    assert [(row["label"], row["value"], row["series"]) for row in chart["data"]] == [
        ("SUV A0", 5416.0, "级别销量"),
        ("SUV A", 7544.0, "级别销量"),
    ]
    assert chart["sourceEvidenceRefs"] == ["ev_suva0_sales", "ev_suva_sales"]

    table = next(item for item in artifacts if item["id"] == "artifact_configuration_analysis_table")
    assert table["title"] == "配置验证矩阵"
    assert "contextSnapshot.crossTabs" not in str(table["data"]["rows"])
    rows = table["data"]["rows"]
    assert rows[0]["feature"] == "Battery size"
    assert rows[0]["currentStatus"] == "已有 evidenceRef: ev_battery"


def test_configuration_table_uses_market_context_when_config_refs_are_missing() -> None:
    artifacts = build_visual_artifacts(
        question="A0 SUV BEV 为什么需要 80kWh 电池？",
        answer={
            "title": "80kWh validation",
            "direct": "80kWh 可以作为 A0/A SUV 长续航/高配安全边界继续验证。",
            "reportReadyBullets": ["产品动作：先看 SUV A0/A BEV 体量，再补竞品电池/续航配置矩阵。"],
            "recommendedActions": [{"action": "生成 80kWh 续航-价格-重量验证表"}],
        },
        evidence_package={
            "intent": "configuration_analysis",
            "country": "Sweden",
            "entities": {"models": ["A0 SUV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {
                            "refId": "ev_suva_bev",
                            "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.BEV_pct",
                            "value": 40.0,
                            "unit": "%",
                            "source": "jato_country_chart_deck",
                        },
                        {
                            "refId": "ev_suva_4wd",
                            "label": "contextSnapshot.crossTabs.driveBySegment.SUV A.4WD_pct",
                            "value": 60.1,
                            "unit": "%",
                            "source": "jato_country_chart_deck",
                        },
                    ],
                }
            ],
            "missingEvidence": [{"name": "competitive_or_configuration_data_unavailable", "reason": "No trim matrix."}],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_configuration_analysis_table")
    rows = table["data"]["rows"]
    serialized = str(rows)

    assert rows[0]["feature"] == "80kWh long-range battery"
    assert rows[0]["priority"] == "P0"
    assert "冬季真实续航、竞品长续航版、重量、成本、MSRP/价格压力" in rows[0]["validationData"]
    assert "市场场景证据 · SUV A BEV 渗透率" in serialized
    assert "SUV A BEV 渗透率 = 40 %" in serialized
    assert "支持 A0/A SUV BEV 需求和长续航版本继续验证" in serialized
    assert "市场场景证据 · SUV A 4WD 占比" in serialized
    assert "不能证明 80kWh 应全系标配" in serialized
    assert "竞品电池、续航、价格和重量证据" in serialized


def test_configuration_artifacts_include_competitor_context_table_from_competitive_set() -> None:
    artifacts = build_visual_artifacts(
        question="A0 SUV BEV 为什么需要 80kWh 电池？请给出市场结构、竞品配置逻辑和图表。",
        answer={
            "title": "80kWh validation",
            "direct": "配置结论：当前缺少配置矩阵，但已经找到竞品池和市场结构。",
            "reportReadyBullets": ["下一步：生成 A0 SUV BEV 80kWh 续航-价格-重量验证表。"],
            "recommendedActions": [{"action": "生成 A0 SUV BEV 80kWh 续航-价格-重量验证表"}],
        },
        evidence_package={
            "intent": "configuration_analysis",
            "country": "Sweden",
            "entities": {"models": ["A0 SUV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_cross_reference",
                    "evidenceRefs": [
                        {"refId": "ev_ex40_model", "label": "competitor.1.model", "value": "EX40", "source": "jato_cross_reference"},
                        {"refId": "ev_ex40_sales", "label": "EX40.sales", "value": 2945, "unit": "units", "source": "jato_cross_reference"},
                        {"refId": "ev_ex40_status", "label": "EX40.priceEvidenceStatus", "value": "source_draft_available", "source": "jato_cross_reference"},
                        {"refId": "ev_ex40_url", "label": "EX40.sourceUrl", "value": "https://www.volvocars.com/se/build/ex40-electric/", "source": "jato_cross_reference"},
                        {"refId": "ev_ex30_model", "label": "competitor.2.model", "value": "EX30", "source": "jato_cross_reference"},
                        {"refId": "ev_ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato_cross_reference"},
                        {"refId": "ev_ex30_status", "label": "EX30.priceEvidenceStatus", "value": "review_pending_not_current_price", "source": "jato_cross_reference"},
                        {"refId": "ev_ex30_pending", "label": "EX30.reviewPendingRows", "value": 3, "source": "jato_cross_reference"},
                    ],
                },
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_suva0_sales", "label": "contextSnapshot.crossTabs.driveBySegment.SUV A0.sales", "value": 5416, "unit": "units", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [{"name": "competitive_or_configuration_data_unavailable", "reason": "No trim matrix."}],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    assert "artifact_configuration_competitor_context_table" in ids
    assert ids.index("artifact_configuration_competitor_context_table") > ids.index("artifact_configuration_analysis_table")

    table = next(item for item in artifacts if item["id"] == "artifact_configuration_competitor_context_table")
    rows = table["data"]["rows"]
    assert rows[0]["model"] == "EX40"
    assert rows[0]["sales"] == "2,945 units"
    assert rows[0]["priceEvidence"] == "待审核价格候选；非当前价格证据"
    assert rows[0]["source"] == "volvocars.com"
    assert "配置差异仍需" in rows[0]["configurationUse"]
    assert rows[1]["model"] == "EX30"
    assert rows[1]["priceEvidence"] == "待审核价格观察；非当前价格证据"
    assert "ev_ex40_sales" in table["sourceEvidenceRefs"]


def test_configuration_report_block_filters_key_message_from_evidence() -> None:
    artifacts = build_visual_artifacts(
        question="A0 SUV BEV 为什么需要 80kWh 电池？",
        answer={
            "title": "80kWh validation",
            "direct": "80kWh 主要解决冬季续航折损、跨城出行和里程焦虑；",
            "reportReadyBullets": [
                "Key message：80kWh 主要解决冬季续航折损、跨城出行和里程焦虑；",
                "Evidence：80kWh 主要解决冬季续航折损、跨城出行和里程焦虑；",
                "Product implication：A0 SUV BEV 的 80kWh 应定位为长续航/高配安全边界，低配仍要保留价格锚点。",
                "Next action：生成 A0 SUV BEV 80kWh 续航-价格-重量验证表。",
            ],
            "recommendedActions": [{"action": "生成 80kWh 续航-价格-重量验证表"}],
        },
        evidence_package={
            "intent": "configuration_analysis",
            "country": "Sweden",
            "entities": {"models": ["A0 SUV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {
                            "refId": "ev_suva_bev",
                            "label": "contextSnapshot.crossTabs.segmentByFuel.SUV A.BEV_pct",
                            "value": 40.0,
                            "unit": "%",
                            "source": "jato_country_chart_deck",
                        },
                    ],
                }
            ],
            "missingEvidence": [{"name": "competitive_or_configuration_data_unavailable", "reason": "No trim matrix."}],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["id"] == "artifact_report_block")
    evidence = report["data"]["evidence"]

    assert report["data"]["keyMessage"] == "80kWh 主要解决冬季续航折损、跨城出行和里程焦虑；"
    assert "80kWh 主要解决冬季续航折损、跨城出行和里程焦虑；" not in evidence
    assert any("市场场景证据 · SUV A BEV 渗透率" in line for line in evidence)


def test_configuration_artifacts_show_powertrain_mix_chart_when_structure_tabs_missing() -> None:
    artifacts = build_visual_artifacts(
        question="4.7m A-SUV 为什么要 95kWh + 双电机 + 800V？",
        answer={
            "title": "High spec BEV validation",
            "direct": "95kWh + 双电机 + 800V 应作为高价值 BEV 架构继续验证。",
            "reportReadyBullets": ["产品动作：补齐电池、续航、800V、双电机、价格和竞品配置矩阵。"],
            "recommendedActions": [{"action": "生成 95kWh / 双电机 / 800V 配置价值-成本验证表"}],
        },
        evidence_package={
            "intent": "configuration_analysis",
            "country": "Sweden",
            "entities": {"models": ["A-SUV BEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_bev_sales", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev_sales", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 15028, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_hev_sales", "label": "contextSnapshot.powertrainMix.HEV.sales", "value": 5051, "unit": "units", "source": "jato_country_chart_deck"},
                    ],
                }
            ],
            "missingEvidence": [{"name": "competitive_or_configuration_data_unavailable", "reason": "No trim matrix."}],
        },
        charts=[],
    )

    artifact_ids = [item["id"] for item in artifacts]
    assert artifact_ids[:2] == ["artifact_market_powertrain_mix_chart", "artifact_configuration_analysis_table"]

    chart = artifacts[0]
    assert chart["title"] == "Powertrain mix chart"
    assert [(row["label"], row["value"], row["series"]) for row in chart["data"]] == [
        ("BEV", 25235.0, "powertrain sales"),
        ("PHEV", 15028.0, "powertrain sales"),
        ("HEV", 5051.0, "powertrain sales"),
    ]
    assert chart["sourceEvidenceRefs"] == ["ev_bev_sales", "ev_phev_sales", "ev_hev_sales"]
    table = next(item for item in artifacts if item["id"] == "artifact_configuration_analysis_table")
    rows = table["data"]["rows"]
    serialized = str(rows)
    assert rows[0]["feature"] == "95kWh + dual motor + 800V architecture"
    assert rows[0]["priority"] == "P0"
    assert "续航、牵引/四驱、补能效率、成本、竞品高配价格带" in rows[0]["validationData"]
    assert "不能证明 95kWh、双电机或 800V 已是必选" in serialized
    assert "95kWh + dual motor + 800V architecture" in serialized


def test_configuration_table_uses_nordic_bev_market_context_for_winter_package() -> None:
    artifacts = build_visual_artifacts(
        question="北欧市场冬季包应该包含什么？",
        answer={
            "title": "Nordic winter package",
            "direct": "北欧冬季包应先保证冬季可用性，再做舒适和户外价值。",
            "reportReadyBullets": [
                "Must-have：热泵、电池预热、座椅/方向盘加热、除霜、冬季胎/TPMS 和充电预热。"
            ],
            "recommendedActions": [{"action": "生成北欧冬季包 must-have / value / optional 配置清单"}],
        },
        evidence_package={
            "intent": "configuration_analysis",
            "country": "Sweden",
            "entities": {"models": ["Nordic winter package"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {
                            "refId": "ev_se_bev",
                            "label": "crossCountry.Sweden.powertrainMix.BEV.sales",
                            "value": 25235,
                            "unit": "units",
                            "source": "jato_cross_country",
                        },
                        {
                            "refId": "ev_fi_bev",
                            "label": "crossCountry.Finland.powertrainMix.BEV.sales",
                            "value": 8062,
                            "unit": "units",
                            "source": "jato_cross_country",
                        },
                        {
                            "refId": "ev_no_bev",
                            "label": "crossCountry.Norway.powertrainMix.BEV.sales",
                            "value": 26617,
                            "unit": "units",
                            "source": "jato_cross_country",
                        },
                    ],
                }
            ],
            "missingEvidence": [{"name": "external_research_claims_unavailable", "reason": "Need winter package source."}],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    repair_table = next(item for item in artifacts if item["id"] == "artifact_external_source_repair_table")
    table = next(item for item in artifacts if item["id"] == "artifact_configuration_analysis_table")
    repair_rows = repair_table["data"]["rows"]
    rows = table["data"]["rows"]
    serialized = str(rows)

    assert ids.index("artifact_external_source_repair_table") < ids.index("artifact_configuration_analysis_table")
    assert repair_rows[0]["sourceNeed"] == "Configuration/media source"
    assert "Nordic winter package heat pump battery preconditioning EV SUV" in repair_rows[0]["queryOrSource"]
    assert repair_rows[0]["requiredFields"] == "URL, title, publish date, test condition, feature claim, model relevance"
    assert repair_rows[0]["canUseInAnswer"] == "No - validate first"
    assert rows[0]["feature"] == "Nordic winter package"
    assert rows[0]["priority"] == "P0"
    assert "热泵、电池预热、座椅/方向盘加热" in rows[0]["validationData"]
    assert "支持北欧 BEV/冬季使用场景继续验证" in serialized
    assert "不能证明具体冬季包配置已是标配或高频需求" in serialized
    assert "95kWh" not in rows[0]["acceptanceCriteria"]
    assert "市场场景证据 · Sweden BEV 动力销量" in serialized


def test_configuration_table_prefers_validation_matrix_over_market_snapshot_refs() -> None:
    artifacts = build_visual_artifacts(
        question="北欧市场冬季包应该包含什么？A0 SUV BEV 为什么需要 80kWh 电池？",
        answer={
            "title": "Configuration strategy",
            "direct": (
                "配置判断：北欧冬季包和 A0 SUV BEV 80kWh 不是两个孤立配置；"
                "冬季包先保低温可用性，80kWh 作为长续航/高配安全边界。"
            ),
            "summary": "Sweden A0 SUV BEV 的冬季包和 80kWh 应按版本策略判断。",
            "pmInsight": "北欧冬季包和 80kWh 电池要一起转成版本策略。",
            "reportReadyBullets": [
                "配置判断：冬季包先解决低温可用性，80kWh 解决长续航/高配安全边界。",
                "产品动作：低配保价格锚点，高配/长续航版用 80kWh、快充、冬季舒适配置、拖车钩预留和 roof load 打北欧可感知价值。",
            ],
            "recommendedActions": [{"action": "生成北欧冬季包 + A0 SUV BEV 80kWh 版本策略验证表"}],
            "businessSynthesisPlan": {
                "executiveConclusion": "直接结论：北欧冬季包和 A0 SUV BEV 80kWh 不是两个孤立配置。",
                "reportReadyBullets": ["下一步：生成北欧冬季包 + A0 SUV BEV 80kWh 版本策略验证表。"],
                "businessImplications": ["80kWh 承担长续航/高配安全边界。"],
            },
        },
        evidence_package={
            "intent": "configuration_analysis",
            "country": "Sweden",
            "entities": {"models": ["A0 SUV BEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_sales", "label": "cumulativeSales", "value": 1182452, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "ev_avg", "label": "avgMsrp", "value": 57954, "unit": "currency", "source": "jato_country_snapshot"},
                        {"refId": "ev_ex40", "label": "EX40", "value": 2945, "source": "jato_country_snapshot"},
                    ],
                }
            ],
            "missingEvidence": [{"name": "competitive_or_configuration_data_unavailable", "reason": "No trim matrix."}],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_configuration_analysis_table")
    rows = table["data"]["rows"]

    assert table["title"] == "配置验证矩阵"
    assert table["sourceEvidenceRefs"] == []
    assert rows[0]["feature"] == "80kWh long-range battery"
    assert rows[0]["targetModel"] == "A0 SUV BEV"
    assert "冬季真实续航" in rows[0]["validationData"]
    assert "compare_vehicle_variants" in rows[0]["sourceOrTool"]
    assert "证明冬季真实续航" in rows[0]["acceptanceCriteria"]
    assert rows[0]["currentStatus"] == "待补竞品配置/价格证据"
    assert rows[1]["feature"] == "Nordic winter package"
    assert any(row["feature"] == "Visible Nordic value features" for row in rows)
    assert all(row["feature"] not in {"cumulativeSales", "avgMsrp", "EX40"} for row in rows)
    assert all(set(row) == set(table["spec"]["columns"]) for row in rows)


def test_policy_news_table_keeps_source_date_claim_and_business_action() -> None:
    artifacts = build_visual_artifacts(
        question="Elbilspremien 2026 会影响哪些车型？",
        answer={
            "title": "Policy impact",
            "direct": "先核对官方政策边界。",
            "recommendedActions": [{"action": "补官方资格门槛并生成车型影响表"}],
            "businessImplications": ["BEV SUV A0/A 需要先验证价格门槛和私人零售适用性。"],
        },
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {
                            "refId": "ev_irrelevant_source",
                            "label": "Sweden Adjustable Mode Beam Laser - Market Analysis.source",
                            "value": "https://www.indexbox.io/store/sweden-adjustable-mode-beam-laser-market-analysis",
                            "source": "external_research",
                        },
                        {
                            "refId": "ev_irrelevant_claim",
                            "label": "Sweden Adjustable Mode Beam Laser - Market Analysis.claim",
                            "value": "We use cookies to improve your experience and for marketing.",
                            "source": "external_research",
                        },
                        {"refId": "ev_policy_source", "label": "Elbilspremien 2026.source", "value": "https://example.test/policy", "source": "external_research"},
                        {"refId": "ev_policy_date", "label": "Elbilspremien 2026.date", "value": "2026-03-01", "source": "external_research"},
                        {"refId": "ev_policy_claim", "label": "Elbilspremien 2026.claim", "value": "Proposed BEV subsidy targets private buyers under a price cap.", "source": "external_research"},
                        {"refId": "ev_policy_models", "label": "Elbilspremien 2026.affectedModels", "value": "BEV SUV A0/A under price cap", "source": "external_research"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_news_policy_search_table")
    rows = table["data"]["rows"]

    assert len(rows) == 1
    assert table["title"] == "政策/新闻证据表"
    assert table["spec"]["columns"] == ["policyTopic", "sourceDate", "source", "policyEffect", "affectedModels", "businessAction", "risk"]
    assert rows[0]["policyTopic"] == "Elbilspremien 2026"
    assert rows[0]["sourceDate"] == "2026-03-01"
    assert rows[0]["source"] == "https://example.test/policy"
    assert "private buyers" in rows[0]["policyEffect"]
    assert rows[0]["affectedModels"] == "BEV SUV A0/A under price cap"
    assert rows[0]["businessAction"] == "补官方资格门槛并生成车型影响表"
    assert "资格" in rows[0]["risk"]
    assert table["sourceEvidenceRefs"] == ["ev_policy_source"]
    assert "cookie" not in str(rows).casefold()
    assert "laser" not in str(rows).casefold()
    assert table["data"]["intentAnalysis"]["template"] == "news_policy_search"


def test_policy_company_car_adds_supplemental_market_context_table() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        answer={
            "title": "Company car benefit",
            "direct": "BEV 和 PHEV 的公司车差异要拆到 benefit tax、月供、残值和渠道结构。",
            "recommendedActions": [{"action": "建立 BEV/PHEV company car benefit 对比表"}],
            "businessImplications": ["BEV 绝对盘更大，PHEV 需要验证公司车依赖度和 TCO。"],
        },
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "entities": {"models": ["BEV", "PHEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {"refId": "policy_source", "label": "Company car benefit.source", "value": "https://example.test/company-car", "source": "external_research"},
                        {"refId": "policy_claim", "label": "Company car benefit.claim", "value": "Company car benefit affects BEV and PHEV differently.", "source": "external_research"},
                    ],
                },
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "bev_sales", "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "bev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.Business_pct", "value": 60.3, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "bev_other", "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.Other_pct", "value": 0, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "bev_private", "label": "contextSnapshot.crossTabs.registrationByFuel.BEV.Private_pct", "value": 39.7, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "phev_sales", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.sales", "value": 15028, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [],
        },
        charts=[
            {
                "chartId": "top_ranking",
                "chartType": "bar",
                "title": "Top Models",
                "data": [{"x": [2945], "y": ["EX40"], "type": "bar"}],
                "layout": {},
            },
            {
                "chartId": "company_car_channel_mix",
                "chartType": "bar",
                "title": "Company car channel mix",
                "data": [{"x": [60.3, 64.8], "y": ["BEV Business", "PHEV Business"], "type": "bar"}],
                "layout": {},
            },
        ],
    )

    ids = [item["id"] for item in artifacts]
    assert ids[:2] == ["artifact_news_policy_search_table", "artifact_policy_market_context_table"]
    assert "artifact_tco_validation_table" in ids
    assert "top_ranking" not in ids
    assert "company_car_channel_mix" in ids

    market_table = next(item for item in artifacts if item["id"] == "artifact_policy_market_context_table")
    rows = market_table["data"]["rows"]
    assert market_table["title"] == "Market context table"
    assert rows[0]["dimension"] == "Powertrain mix"
    assert rows[0]["signal"] == "BEV"
    assert rows[0]["evidence"] == "25,235 units"
    assert "25,235 units" in rows[0]["businessImplication"]
    assert "验证 BEV" in rows[0]["businessImplication"]
    assert any(row["dimension"] == "Powertrain mix" and row["signal"] == "PHEV" for row in rows)
    business_row = next(row for row in rows if row["dimension"] == "Channel mix" and row["signal"] == "Business 注册占比")
    assert "60.3 %" in business_row["businessImplication"]
    assert "拆分私人零售和公司车逻辑" in business_row["businessImplication"]
    assert any(row["dimension"] == "Channel mix" and row["signal"] == "Private 注册占比" for row in rows)
    assert not any("Other" in row["signal"] or "其他" in row["signal"] for row in rows)
    assert "Other_pct" not in str(market_table["data"]["intentAnalysis"])
    assert "其他注册占比" not in str(market_table["data"]["intentAnalysis"])
    assert "channel" in market_table["subtitle"].lower()

    tco_table = next(item for item in artifacts if item["id"] == "artifact_tco_validation_table")
    tco_rows = tco_table["data"]["rows"]
    assert tco_table["title"] == "TCO / company-car validation table"
    assert tco_table["spec"]["columns"] == [
        "scenario",
        "evidenceNeeded",
        "sourceOrTool",
        "acceptanceCriteria",
        "currentStatus",
        "businessUse",
        "priority",
    ]
    assert tco_rows[0]["scenario"] == "Channel / fleet exposure"
    assert "BEV 公司车注册占比" in tco_rows[0]["currentStatus"]
    assert any(row["scenario"] == "Monthly payment / lease quote" for row in tco_rows)
    assert any(row["currentStatus"] == "待补可引用证据" for row in tco_rows)


def test_policy_phev_co2_tax_adds_supplemental_market_context_table() -> None:
    artifacts = build_visual_artifacts(
        question="CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
        answer={
            "title": "PHEV CO2 tax",
            "direct": "PHEV 是否有利要拆到认证 CO2、company car 税费、月供和渠道结构。",
            "recommendedActions": [{"action": "输出 PHEV vs HEV/BEV 的 company car TCO 场景表"}],
            "businessImplications": ["PHEV 需要验证公司车依赖度和 TCO。"],
        },
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "entities": {"powertrains": ["PHEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {"refId": "policy_source", "label": "CO2 tax.source", "value": "https://example.test/co2-tax", "source": "external_research"},
                        {"refId": "policy_claim", "label": "CO2 tax.claim", "value": "CO2 tax bands affect low-emission PHEV qualification.", "source": "external_research"},
                    ],
                },
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "phev_sales", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.sales", "value": 15028, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "phev_business", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Business_pct", "value": 64.8, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "phev_private", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Private_pct", "value": 35.2, "unit": "%", "source": "jato_country_chart_deck"},
                        {"refId": "phev_other", "label": "contextSnapshot.crossTabs.registrationByFuel.PHEV.Other_pct", "value": 0.0, "unit": "%", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    assert ids[:2] == ["artifact_news_policy_search_table", "artifact_policy_market_context_table"]
    assert "artifact_tco_validation_table" in ids

    market_table = next(item for item in artifacts if item["id"] == "artifact_policy_market_context_table")
    rows = market_table["data"]["rows"]
    assert market_table["title"] == "Market context table"
    assert rows[0]["dimension"] == "Powertrain mix"
    assert rows[0]["signal"] == "PHEV"
    assert rows[0]["evidence"] == "15,028 units"
    assert "15,028 units" in rows[0]["businessImplication"]
    assert "验证 PHEV" in rows[0]["businessImplication"]
    assert any(
        row["dimension"] == "Channel mix"
        and row["signal"] == "Business 注册占比"
        and row["evidence"] == "64.8 %"
        and "64.8 %" in row["businessImplication"]
        for row in rows
    )
    assert not any("Other" in row["signal"] or "其他" in row["signal"] for row in rows)
    assert "Other_pct" not in str(market_table["data"]["intentAnalysis"])
    assert "其他注册占比" not in str(market_table["data"]["intentAnalysis"])

    tco_table = next(item for item in artifacts if item["id"] == "artifact_tco_validation_table")
    tco_rows = tco_table["data"]["rows"]
    assert any(row["scenario"] == "Tax / company-car benefit formula" for row in tco_rows)
    assert any("policy_claim" in row["currentStatus"] for row in tco_rows)
    assert any(row["scenario"] == "Residual value / RV risk" for row in tco_rows)


def test_policy_news_table_falls_back_to_validation_framework_without_refs() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典 company car benefit 对 BEV 和 PHEV 的影响有什么不同？",
        answer={
            "title": "Policy gap",
            "direct": "当前证据不足。",
            "businessFrame": {"verdict": "需要政策来源验证。"},
            "recommendedActions": [{"action": "补 company car benefit 官方公式和车型影响表"}],
        },
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "entities": {"models": ["BEV", "PHEV"]},
            "confidence": "low",
            "toolResults": [],
            "missingEvidence": [{"name": "official_source", "reason": "No official policy source.", "impact": "blocking"}],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_news_policy_search_framework_table")
    rows = table["data"]["rows"]

    assert table["fallbackReason"] == "evidence_refs_missing"
    assert rows[0]["policyTopic"] == "Sweden"
    assert rows[0]["sourceDate"] == "待补发布日期/有效期"
    assert rows[0]["source"] == "待补官方/高质量来源"
    assert rows[0]["businessAction"] == "补 company car benefit 官方公式和车型影响表"
    assert "official_source" in rows[0]["risk"]
    assert table["sourceEvidenceRefs"] == []


def test_policy_price_cap_framework_table_outputs_scenario_rows_without_refs() -> None:
    artifacts = build_visual_artifacts(
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        answer={
            "title": "O5 BEV price cap",
            "direct": (
                "政策边界：瑞典 BEV 补贴价格上限不能默认当成现行约束。"
                "情景矩阵：A 有效且 O5 适用，B 失效或不适用，C 新计划未确认。"
            ),
            "recommendedActions": [
                {"action": "准备补贴内资格锚点价格页"},
                {"action": "生成竞品价格走廊和补贴外高配价值页"},
                {"action": "准备双价格页并标记需要补的官方条文"},
            ],
        },
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"]},
            "confidence": "low",
            "toolResults": [],
            "missingEvidence": [{"name": "official_source", "reason": "No official policy source.", "impact": "blocking"}],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_news_policy_search_framework_table")
    rows = table["data"]["rows"]

    assert table["fallbackReason"] == "evidence_refs_missing"
    assert [row["policyTopic"] for row in rows] == [
        "Scenario A · 价格上限仍有效且 O5 适用",
        "Scenario B · 价格上限失效或 O5 不适用",
        "Scenario C · 新计划或细则未确认",
    ]
    assert rows[0]["affectedModels"] == "O5 BEV"
    assert "补贴资格锚点" in rows[0]["policyEffect"]
    assert rows[0]["businessAction"] == "准备补贴内资格锚点价格页"
    assert rows[0]["risk"] == "缺少官方来源，不能写成现行政策"
    assert "竞品走廊" in rows[1]["policyEffect"]
    assert "双价格页" in rows[2]["businessAction"]
    assert table["sourceEvidenceRefs"] == []


def test_policy_price_cap_uses_framework_when_only_market_context_refs_exist() -> None:
    artifacts = build_visual_artifacts(
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        answer={
            "title": "O5 BEV price cap",
            "direct": (
                "政策边界：瑞典 BEV 补贴价格上限不能默认当成现行约束。"
                "情景矩阵：A 有效且 O5 适用，B 失效或不适用，C 新计划未确认。"
            ),
            "recommendedActions": [
                {"action": "核对瑞典 BEV 补贴价格上限是否仍有效及 O5 BEV 是否适用"},
                {"action": "生成竞品价格走廊和补贴外高配价值页"},
                {"action": "准备双价格页并标记需要补的官方条文"},
            ],
        },
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_market_sales", "label": "cumulativeSales", "value": 1182452.0, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "ev_market_msrp", "label": "avgMsrp", "value": 57954.1, "unit": "currency", "source": "jato_country_snapshot"},
                    ],
                }
            ],
            "missingEvidence": [{"name": "official_source", "reason": "No official policy source.", "impact": "blocking"}],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    table = next(item for item in artifacts if item["id"] == "artifact_news_policy_search_framework_table")
    rows = table["data"]["rows"]

    assert "artifact_news_policy_search_table" not in ids
    assert "artifact_metric_cards" not in ids
    assert artifacts[0]["id"] == "artifact_external_source_repair_table"
    assert artifacts[1]["id"] == "artifact_news_policy_search_framework_table"
    repair_rows = artifacts[0]["data"]["rows"]
    assert repair_rows[0]["sourceNeed"] == "Official policy/news source"
    assert "official policy source" in repair_rows[0]["queryOrSource"]
    assert repair_rows[0]["canUseInAnswer"] == "No - validate first"
    assert table["fallbackReason"] == "evidence_refs_missing"
    assert rows[0]["policyTopic"] == "Scenario A · 价格上限仍有效且 O5 适用"
    assert rows[0]["affectedModels"] == "O5 BEV"
    assert "jato_country_snapshot" not in str(rows).lower()
    assert table["sourceEvidenceRefs"] == []


def test_policy_price_cap_prioritizes_policy_table_and_filters_market_snapshot_noise() -> None:
    artifacts = build_visual_artifacts(
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        answer={
            "title": "O5 BEV price cap",
            "direct": "官方 bonus 已结束后，O5 BEV 定价应回到竞品价格走廊、配置价值和月供/TCO。",
            "recommendedActions": [{"action": "补齐当前 O5 BEV MSRP、竞品价格走廊和 24/36 个月月供"}],
            "businessImplications": ["官方 bonus 已结束后，不应围绕历史补贴门槛倒推定价。"],
        },
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {
                            "refId": "policy_bonus_source",
                            "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.source",
                            "value": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                        },
                        {
                            "refId": "policy_bonus_claim",
                            "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.claim",
                            "value": "The bonus for low emission vehicles has ended.",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                        },
                        {
                            "refId": "policy_bonus_date",
                            "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.date",
                            "value": "2026-04-17",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                        },
                        {
                            "refId": "policy_bonus_rank",
                            "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.rank",
                            "value": 1,
                            "source": "jato_external_research_web",
                        },
                    ],
                },
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_market_sales", "label": "cumulativeSales", "value": 1182452.0, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "ev_market_msrp", "label": "avgMsrp", "value": 57954.1, "unit": "currency", "source": "jato_country_snapshot"},
                    ],
                },
            ],
            "missingEvidence": [],
        },
        charts=[
            {
                "chartId": "top_ranking",
                "chartType": "bar",
                "title": "Top Models",
                "data": [{"x": [2945, 2893], "y": ["EX40", "XC60"], "type": "bar"}],
                "layout": {},
            }
        ],
    )

    assert artifacts[0]["id"] == "artifact_news_policy_search_table"
    assert not any(item["type"] == "metric_cards" for item in artifacts)

    table = artifacts[0]
    rows = table["data"]["rows"]
    assert rows[0]["policyTopic"] == "Bonus - for low emission vehicles has ended - Transportstyrelsen"
    assert rows[0]["sourceDate"] == "2026-04-17"
    assert "has ended" in rows[0]["policyEffect"]
    assert "rank" not in str(rows).lower()
    assert "cumulativeSales" not in str(rows)
    assert table["sourceEvidenceRefs"] == ["policy_bonus_source"]


def test_policy_price_cap_adds_supplemental_pricing_artifacts_when_price_refs_exist() -> None:
    artifacts = build_visual_artifacts(
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        answer={
            "title": "O5 BEV price cap",
            "direct": "官方 bonus 已结束后，O5 BEV 定价应回到竞品价格走廊、配置价值和月供/TCO。",
            "recommendedActions": [{"action": "补齐当前 O5 BEV MSRP、竞品价格走廊和 24/36 个月月供"}],
            "businessImplications": ["官方 bonus 已结束后，不应围绕历史补贴门槛倒推定价。"],
        },
        evidence_package={
            "intent": "news_policy_search",
            "country": "Sweden",
            "entities": {"models": ["O5 BEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {
                            "refId": "policy_bonus_source",
                            "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.source",
                            "value": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                        },
                        {
                            "refId": "policy_bonus_claim",
                            "label": "Bonus - for low emission vehicles has ended - Transportstyrelsen.claim",
                            "value": "The bonus for low emission vehicles has ended.",
                            "source": "https://www.transportstyrelsen.se/en/road/vehicles/taxes-and-fees/bonus/",
                        },
                    ],
                },
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "price_enyaq", "label": "ENYAQ.msrp", "value": 52130.43, "unit": "currency", "source": "jato_msrp_postgres"},
                        {"refId": "price_tayron_min", "label": "TAYRON.minPrice", "value": 39121.74, "unit": "currency", "source": "jato_msrp_postgres"},
                        {"refId": "price_tayron_max", "label": "TAYRON.maxPrice", "value": 53165.22, "unit": "currency", "source": "jato_msrp_postgres"},
                    ],
                },
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    assert ids[:2] == ["artifact_news_policy_search_table", "artifact_policy_pricing_table"]
    assert "artifact_pricing_corridor_chart" in ids

    pricing_table = next(item for item in artifacts if item["id"] == "artifact_policy_pricing_table")
    rows = pricing_table["data"]["rows"]
    assert pricing_table["title"] == "价格证据表"
    assert rows[0]["model"] == "ENYAQ"
    assert rows[0]["msrp"] == "52,130.4 EUR"
    assert rows[1]["model"] == "TAYRON"
    assert "39,121.7 EUR-53,165.2 EUR" in rows[1]["msrp"]
    assert pricing_table["sourceEvidenceRefs"] == ["price_enyaq", "price_tayron_min"]

    chart = next(item for item in artifacts if item["id"] == "artifact_pricing_corridor_chart")
    chart_labels = [row["label"] for row in chart["data"]]
    assert "ENYAQ" in chart_labels
    assert "TAYRON min" in chart_labels
    assert "TAYRON max" in chart_labels


def test_voc_table_keeps_source_signal_implication_and_validation_status() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        answer={
            "title": "VOC themes",
            "direct": "候选吐槽主题需要转成产品动作。",
            "recommendedActions": [{"action": "生成 VOC 主题表和产品动作表"}],
            "businessImplications": ["把售后、车机和冬季使用场景拆成可验证主题。"],
        },
        evidence_package={
            "intent": "voc_analysis",
            "country": "Sweden",
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {"refId": "ev_src", "label": "Sweden OMODA launch.source", "value": "https://example.test/omoda", "source": "external_research"},
                        {"refId": "ev_claim", "label": "Sweden OMODA launch.claim", "value": "New brand entry depends on dealer confidence and delivery experience.", "source": "external_research"},
                        {"refId": "ev_rank", "label": "Sweden OMODA launch.rank", "value": 1, "source": "external_research"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_voc_analysis_table")
    rows = table["data"]["rows"]

    assert table["title"] == "VOC 证据表"
    assert table["spec"]["columns"] == [
        "theme",
        "source",
        "evidenceSignal",
        "productImplication",
        "validationStatus",
        "recommendedAction",
        "confidence",
    ]
    assert rows[0]["theme"] == "Brand trust / service risk"
    assert rows[0]["source"] == "https://example.test/omoda"
    assert "dealer confidence" in rows[0]["evidenceSignal"]
    assert "质保" in rows[0]["productImplication"]
    assert rows[0]["validationStatus"] == "可作为候选 VOC 主题，仍需频次和代表性验证"
    assert rows[0]["recommendedAction"] == "生成 VOC 主题表和产品动作表"
    assert table["sourceEvidenceRefs"] == ["ev_src"]


def test_voc_artifacts_do_not_surface_generic_market_metric_cards_or_snapshot_rows() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？",
        answer={
            "title": "OMODA/JAECOO VOC",
            "direct": "候选吐槽主题需要转成产品动作。",
            "recommendedActions": [{"action": "生成 VOC 主题表和产品动作表"}],
            "businessImplications": ["把售后、车机和冬季使用场景拆成可验证主题。"],
        },
        evidence_package={
            "intent": "voc_analysis",
            "country": "Sweden",
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {
                            "refId": "ev_src",
                            "label": "Agera PR launches the new car brand OMODA in Sweden.source",
                            "value": "https://agerapr.se/en/agera-pr-launches-the-new-car-brand-omoda-in-sweden",
                            "source": "https://agerapr.se/en/agera-pr-launches-the-new-car-brand-omoda-in-sweden",
                        },
                        {
                            "refId": "ev_claim",
                            "label": "Agera PR launches the new car brand OMODA in Sweden.claim",
                            "value": "OMODA market entry depends on dealer confidence and delivery experience.",
                            "source": "https://agerapr.se/en/agera-pr-launches-the-new-car-brand-omoda-in-sweden",
                        },
                    ],
                },
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {
                            "refId": "ev_sales",
                            "label": "cumulativeSales",
                            "value": 1182452.0,
                            "unit": "units",
                            "source": "jato_country_snapshot",
                        },
                        {
                            "refId": "ev_msrp",
                            "label": "avgMsrp",
                            "value": 57954.07,
                            "unit": "currency",
                            "source": "jato_country_snapshot",
                        },
                    ],
                },
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    assert artifacts[0]["id"] == "artifact_voc_analysis_table"
    assert not any(item["type"] == "metric_cards" for item in artifacts)

    table = artifacts[0]
    rows = table["data"]["rows"]
    assert rows[0]["theme"] == "Brand trust / service risk"
    assert rows[0]["source"] == "https://agerapr.se/en/agera-pr-launches-the-new-car-brand-omoda-in-sweden"
    assert str(rows).lower().find("cumulativesales") == -1
    assert str(rows).lower().find("avgmsrp") == -1
    assert table["sourceEvidenceRefs"] == ["ev_src"]


def test_voc_artifacts_filter_generic_market_charts_when_external_research_is_missing() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典用户会不会把 V2H 当成真实购买卖点？",
        answer={
            "title": "V2H VOC",
            "direct": "V2H 暂时不能定位为真实高频购买卖点，应定位为高感知但待验证的技术型加分项。",
            "reportReadyBullets": [
                "瑞典 V2H 暂时应定位为高感知但待验证的技术加分项，不能直接写成高频购买卖点。",
                "建议动作：抓取瑞典/北欧 V2H 用户原声和媒体测评证据。",
            ],
            "recommendedActions": [{"action": "抓取瑞典/北欧 V2H 用户原声和媒体测评证据"}],
        },
        evidence_package={
            "intent": "voc_analysis",
            "country": "Sweden",
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_bev", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_phev", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 15028, "unit": "units", "source": "jato_country_chart_deck"},
                    ],
                }
            ],
            "missingEvidence": [{"name": "external_research_failed", "reason": "external_research timed out after 20s", "impact": "weakens_answer"}],
        },
        charts=[
            {
                "chartId": "auto_top_models",
                "chartType": "bar",
                "title": "Top Models",
                "data": [{"x": [2945], "y": ["EX40"], "type": "bar"}],
                "layout": {},
            },
            {
                "chartId": "auto_powertrain_mix",
                "chartType": "bar",
                "title": "Powertrain mix",
                "data": [{"x": [25235, 15028], "y": ["BEV", "PHEV"], "type": "bar"}],
                "layout": {},
            },
            {
                "chartId": "auto_year_trend",
                "chartType": "line",
                "title": "Year trend",
                "data": [{"x": [2024, 2025], "y": [30, 40], "type": "scatter"}],
                "layout": {},
            },
        ],
    )

    artifact_ids = [item["id"] for item in artifacts]
    assert "artifact_voc_analysis_framework_table" in artifact_ids
    assert "artifact_report_block" in artifact_ids
    assert "auto_top_models" not in artifact_ids
    assert "auto_powertrain_mix" not in artifact_ids
    assert "auto_year_trend" not in artifact_ids
    assert not any(item["type"] == "chart" for item in artifacts)


def test_voc_table_falls_back_to_validation_framework_without_refs() -> None:
    artifacts = build_visual_artifacts(
        question="拖车钩、roof load、冬季胎在北欧用户声音里是不是高频需求？",
        answer={
            "title": "VOC gap",
            "direct": "当前只能给候选主题。",
            "businessFrame": {"verdict": "需要 VOC 来源。"},
            "recommendedActions": [{"action": "先在 External source repair table 中补源入口，再补媒体测评、论坛评论、用户原声并按主题聚类"}],
        },
        evidence_package={
            "intent": "voc_analysis",
            "country": "Sweden",
            "confidence": "low",
            "toolResults": [],
            "missingEvidence": [{"name": "consumer_signal", "reason": "No source-backed VOC.", "impact": "weakens_answer"}],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_voc_analysis_framework_table")
    rows = table["data"]["rows"]

    assert table["fallbackReason"] == "evidence_refs_missing"
    assert rows[0]["theme"] == "VOC source signal"
    assert rows[0]["source"] == "待补 VOC 来源"
    assert rows[0]["recommendedAction"] == "先在 外部来源验证矩阵 中补证线索，再补媒体测评、论坛评论、用户原声并按主题聚类"
    assert table["sourceEvidenceRefs"] == []
    assert "External source repair table" not in str(table["data"])


def test_business_table_rows_are_display_limited_and_preserve_source_refs() -> None:
    long_value = "Visible value proof " * 12
    artifacts = build_visual_artifacts(
        question="J7 HEV pricing",
        answer={"title": "Pricing", "direct": "Use core corridor."},
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {
                            "refId": "ev_price",
                            "label": "J7 HEV Premium MSRP",
                            "value": long_value,
                            "unit": "EUR",
                            "source": "pricing_fixture",
                        }
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    assert artifacts[0]["id"] == "artifact_pricing_analysis_table"
    table = next(item for item in artifacts if item["id"] == "artifact_pricing_analysis_table")
    row = table["data"]["rows"][0]

    assert table["sourceEvidenceRefs"] == ["ev_price"]
    assert set(row) == set(table["spec"]["columns"])
    assert "evidenceRef" not in row
    assert "source" not in row
    assert len(str(row["msrp"])) <= 96
    assert str(row["msrp"]).endswith("...")


def test_inventory_table_uses_bom_readability_columns() -> None:
    package = _evidence_package("inventory_analysis")
    artifacts = build_visual_artifacts(
        question="OMODA9 一个版型多个物料号应该怎么解释？",
        answer={"title": "BOM", "direct": "Split version and material lifecycle."},
        evidence_package=package,
        charts=[],
    )

    assert artifacts[0]["id"] == "artifact_bom_entity_validation_table"
    table = next(item for item in artifacts if item["id"] == "artifact_inventory_analysis_table")
    assert table["spec"]["columns"] == [
        "market",
        "model",
        "version",
        "colorSpec",
        "materialCode",
        "availableUnits",
        "risk",
    ]
    assert table["spec"]["rawColumns"] == [
        "market",
        "model",
        "version",
        "exterior",
        "interior",
        "materialCode",
        "availableUnits",
        "risk",
    ]
    assert len(table["spec"]["columns"]) == 7
    assert len(table["data"]["rows"]) <= 10


def test_inventory_artifacts_prioritize_bom_table_over_generic_market_metrics() -> None:
    artifacts = build_visual_artifacts(
        question="SE/FI 合并 PI 但车辆分市场生成，逻辑是否正确？",
        answer={
            "title": "Inventory mapping",
            "direct": "PI header 可以合并，但车辆生成和物料号应按市场保留 overlay。",
            "recommendedActions": [{"action": "建立 PI header + market overlay + material mapping"}],
        },
        evidence_package={
            "intent": "inventory_analysis",
            "country": "Sweden",
            "entities": {"countries": ["Sweden", "Finland"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "query_cross_country",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {
                            "refId": "ev_sales",
                            "label": "crossCountry.Sweden.kpis.cumulativeSales",
                            "value": 1182452,
                            "unit": "units",
                            "source": "jato_cross_country",
                        },
                        {
                            "refId": "ev_msrp",
                            "label": "results.kpis.avgMsrp",
                            "value": 57954,
                            "unit": "currency",
                            "source": "jato_country_snapshot",
                        },
                        {
                            "refId": "ev_version",
                            "label": "results.kpis.versionCount",
                            "value": 9204,
                            "unit": "units",
                            "source": "jato_filtered_query",
                        },
                        {
                            "refId": "ev_material",
                            "label": "BOM material lifecycle risk",
                            "value": "market overlay required",
                            "source": "bom",
                        },
                        {
                            "refId": "ev_units",
                            "label": "inventory.records.SE_O9.availableUnits",
                            "value": 12,
                            "unit": "units",
                            "source": "inventory",
                        },
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    assert artifacts[0]["id"] == "artifact_bom_entity_validation_table"
    assert artifacts[1]["id"] == "artifact_inventory_analysis_table"
    metric_cards = next((item for item in artifacts if item["type"] == "metric_cards"), None)
    assert metric_cards is not None
    assert "ev_sales" not in metric_cards["sourceEvidenceRefs"]
    assert "ev_msrp" not in metric_cards["sourceEvidenceRefs"]
    assert "ev_version" not in metric_cards["sourceEvidenceRefs"]
    assert metric_cards["sourceEvidenceRefs"] == ["ev_units"]


def test_inventory_table_groups_material_records_into_operational_rows() -> None:
    artifacts = build_visual_artifacts(
        question="当月选品表如何从物料号转成客户可编辑数量？",
        answer={
            "title": "Inventory mapping",
            "direct": "按版本、颜色和生命周期拆分。",
            "recommendedActions": [{"action": "建立版本-颜色-物料号-生命周期映射表"}],
        },
        evidence_package={
            "intent": "inventory_analysis",
            "country": "Sweden",
            "entities": {"models": ["OMODA9"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "query_with_filters",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_m1_market", "label": "inventory.records.SE_O9_BLACK.market", "value": "Sweden", "source": "inventory"},
                        {"refId": "ev_m1_model", "label": "inventory.records.SE_O9_BLACK.model", "value": "OMODA9", "source": "inventory"},
                        {"refId": "ev_m1_version", "label": "inventory.records.SE_O9_BLACK.version", "value": "Premium AWD", "source": "inventory"},
                        {"refId": "ev_m1_ext", "label": "inventory.records.SE_O9_BLACK.exterior", "value": "Black", "source": "inventory"},
                        {"refId": "ev_m1_int", "label": "inventory.records.SE_O9_BLACK.interior", "value": "Beige", "source": "inventory"},
                        {"refId": "ev_m1_mat", "label": "inventory.records.SE_O9_BLACK.materialCode", "value": "MAT-SE-001", "source": "inventory"},
                        {"refId": "ev_m1_units", "label": "inventory.records.SE_O9_BLACK.availableUnits", "value": 12, "unit": "units", "source": "inventory"},
                        {"refId": "ev_m1_life", "label": "inventory.records.SE_O9_BLACK.lifecycle", "value": "active", "source": "inventory"},
                        {"refId": "ev_m2_version", "label": "inventory.records.SE_O9_WHITE.version", "value": "Premium AWD", "source": "inventory"},
                        {"refId": "ev_m2_ext", "label": "inventory.records.SE_O9_WHITE.exterior", "value": "White", "source": "inventory"},
                        {"refId": "ev_m2_int", "label": "inventory.records.SE_O9_WHITE.interior", "value": "Black", "source": "inventory"},
                        {"refId": "ev_m2_mat", "label": "inventory.records.SE_O9_WHITE.materialCode", "value": "MAT-SE-002", "source": "inventory"},
                        {"refId": "ev_m2_risk", "label": "inventory.records.SE_O9_WHITE.risk", "value": "duplicate material for same business version", "source": "inventory"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_inventory_analysis_table")
    rows = table["data"]["rows"]

    assert rows[0]["market"] == "Sweden"
    assert rows[0]["model"] == "OMODA9"
    assert rows[0]["version"] == "Premium AWD"
    assert rows[0]["colorSpec"] == "Black / Beige"
    assert rows[0]["materialCode"] == "MAT-SE-001"
    assert rows[0]["availableUnits"] == "12 units"
    assert "生命周期状态：active" in rows[0]["risk"]
    assert rows[1]["version"] == "Premium AWD"
    assert rows[1]["colorSpec"] == "White / Black"
    assert rows[1]["materialCode"] == "MAT-SE-002"
    assert "同一业务版本存在多个物料号" in rows[1]["risk"]
    assert table["sourceEvidenceRefs"] == ["ev_m1_market", "ev_m2_version"]
    assert all(set(row) == set(table["spec"]["columns"]) for row in rows)


def test_inventory_report_block_uses_bom_evidence_instead_of_generic_framework() -> None:
    artifacts = build_visual_artifacts(
        question="OMODA9 一个版型多个物料号应该怎么解释？",
        answer={
            "title": "BOM",
            "direct": "OMODA9 一个版型多个物料号不能直接判错。",
            "reportReadyBullets": [
                "Title：BOM",
                "Key message：车型版本、物料号、市场、颜色、PI、订单和客户可编辑数量。",
                "Evidence：车型版本、物料号、市场、颜色、PI、订单和客户可编辑数量。",
                "Product implication：如果没有底表证据，仍可以先定义实体关系和异常处理规则。",
                "Next action：画实体关系，再补底表字段验证异常规则。",
            ],
            "recommendedActions": [{"action": "建立版本-颜色-物料号-生命周期映射表"}],
        },
        evidence_package={
            "intent": "inventory_analysis",
            "country": "Sweden",
            "entities": {"models": ["OMODA9"]},
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "inspect_bom_materials",
                    "success": True,
                    "sourceType": "bom",
                    "evidenceRefs": [
                        {"refId": "ev_market", "label": "bom.records.OMODA9.market", "value": "Sweden", "source": "bom"},
                        {"refId": "ev_version", "label": "bom.records.OMODA9.version", "value": "Premium AWD", "source": "bom"},
                        {"refId": "ev_material", "label": "bom.records.OMODA9.materialCode", "value": "MTRL-001 / MTRL-002", "source": "bom"},
                        {"refId": "ev_risk", "label": "BOM material lifecycle risk", "value": "duplicate material mapping", "source": "bom"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["type"] == "report_block")
    evidence_text = " ".join(report["data"]["evidence"])

    assert "Premium AWD" in evidence_text
    assert "MTRL-001 / MTRL-002" in evidence_text
    assert "同一业务版本存在多个物料号" in evidence_text
    assert "客户可编辑数量、可下单状态和订单生命周期待补" in evidence_text
    assert "Premium AWD 已命中物料号 MTRL-001 / MTRL-002" in report["data"]["keyMessage"]
    assert "车型版本、物料号、市场、颜色、PI、订单和客户可编辑数量。" not in evidence_text
    assert "版本-颜色/内饰-市场-物料号-生命周期映射" in report["data"]["productImplication"]
    assert "客户可编辑数量、可下单状态和订单生命周期" in report["data"]["nextAction"]
    assert "画实体关系" not in report["data"]["nextAction"]


def test_inventory_artifacts_suppress_metric_cards_for_market_only_bom_refs() -> None:
    artifacts = build_visual_artifacts(
        question="BOM、车型版本、内外饰颜色之间应该怎么建模？",
        answer={
            "title": "BOM entity model",
            "direct": "市场销量和平均 MSRP 不能证明 BOM 实体映射。",
            "recommendedActions": [{"action": "补齐 BOM/entity mapping 底表"}],
        },
        evidence_package={
            "intent": "inventory_analysis",
            "country": "Sweden",
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_sales", "label": "results.kpis.cumulativeSales", "value": 1182452, "unit": "units", "source": "jato_filtered_query"},
                        {"refId": "ev_avg", "label": "results.kpis.avgMsrp", "value": 57954.1, "unit": "currency", "source": "jato_filtered_query"},
                        {"refId": "ev_version", "label": "results.kpis.versionCount", "value": 9204, "unit": "units", "source": "jato_filtered_query"},
                    ],
                }
            ],
            "missingEvidence": [
                {
                    "name": "bom_entity_mapping_evidence",
                    "reason": "Need entity relationship evidence.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    assert artifacts[0]["id"] == "artifact_bom_entity_validation_table"
    assert all(item["type"] != "metric_cards" for item in artifacts)


def test_inventory_table_turns_bom_risk_ref_into_entity_relationship_rows() -> None:
    artifacts = build_visual_artifacts(
        question="OMODA9 一个版型多个物料号应该怎么解释？",
        answer={
            "title": "BOM logic",
            "direct": "先建立实体关系。",
            "recommendedActions": [{"action": "画出版本-颜色-物料号-生命周期关系"}],
        },
        evidence_package={
            "intent": "inventory_analysis",
            "country": "Sweden",
            "entities": {"models": ["OMODA9"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_with_filters",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_bom_risk", "label": "BOM material lifecycle risk", "value": "duplicate material", "source": "bom"}
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_inventory_analysis_table")
    rows = table["data"]["rows"]

    assert rows[0]["market"] == "Sweden"
    assert rows[0]["model"] == "OMODA9"
    assert rows[0]["version"] == "业务版本"
    assert rows[0]["materialCode"] == "多个物料号需拆分"
    assert rows[0]["availableUnits"] == "客户可编辑数量待计算"
    assert "同一业务版本存在多个物料号" in rows[0]["risk"]
    assert rows[1]["model"] == "实体关系"
    assert rows[1]["materialCode"] == "物料号"
    assert table["sourceEvidenceRefs"] == ["ev_bom_risk"]


def test_inventory_artifacts_include_bom_entity_validation_matrix() -> None:
    artifacts = build_visual_artifacts(
        question="BOM、车型版本、内外饰颜色之间应该怎么建模？",
        answer={
            "title": "BOM entity model",
            "direct": "先验证 PI、市场、版本、颜色、物料号和生命周期关系。",
            "recommendedActions": [{"action": "建立版本-颜色-物料号-生命周期映射表"}],
        },
        evidence_package={
            "intent": "inventory_analysis",
            "country": "Sweden",
            "entities": {"models": ["OMODA9"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_with_filters",
                    "success": True,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_variant", "label": "inventory.records.SE_O9.version", "value": "Premium AWD", "source": "inventory"},
                        {"refId": "ev_color", "label": "inventory.records.SE_O9.exterior", "value": "Black", "source": "inventory"},
                        {"refId": "ev_material", "label": "inventory.records.SE_O9.materialCode", "value": "MAT-SE-001", "source": "material_master"},
                        {"refId": "ev_lifecycle", "label": "inventory.records.SE_O9.lifecycle", "value": "active", "source": "material_master"},
                    ],
                }
            ],
            "missingEvidence": [
                {
                    "name": "bom_entity_mapping_evidence",
                    "reason": "Need entity relationship evidence.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_bom_entity_validation_table")
    rows = table["data"]["rows"]

    assert artifacts[0]["id"] == "artifact_bom_entity_validation_table"
    assert table["spec"]["columns"] == [
        "entityLayer",
        "mappingNeeded",
        "sourceOrTool",
        "acceptanceCriteria",
        "currentStatus",
        "businessUse",
        "priority",
    ]
    assert rows[0]["entityLayer"] == "PI / shared header"
    assert any(row["entityLayer"] == "Material code" and "ev_material" in row["currentStatus"] for row in rows)
    assert any(row["entityLayer"] == "Color / interior" and "ev_color" in row["currentStatus"] for row in rows)
    assert any(row["entityLayer"] == "Lifecycle / orderability" and "ev_lifecycle" in row["currentStatus"] for row in rows)
    assert "ev_material" in table["sourceEvidenceRefs"]
    assert table["spec"]["evidenceMode"] == "validation_matrix_not_final_bom_evidence"


def test_report_ready_answer_returns_report_block() -> None:
    package = _evidence_package("pricing_analysis")
    artifacts = build_visual_artifacts(
        question="Generate one PPT block",
        answer={
            "title": "J7 HEV pricing",
            "direct": "J7 HEV should use core corridor plus high-trim push. This full direct answer is intentionally longer than the PPT key message.",
            "reportReadyBullets": ["Title：J7 HEV pricing", "Key message：Use core corridor plus high-trim push.", "Evidence one", "Next action"],
            "businessImplications": ["高配主推，低配做锚点。"],
            "recommendedActions": [{"action": "Build competitor price matrix"}],
        },
        evidence_package=package,
        charts=[],
    )

    report = next(item for item in artifacts if item["type"] == "report_block")
    assert report["data"]["title"] == "J7 HEV pricing"
    assert report["data"]["keyMessage"] == "Use core corridor plus high-trim push."
    assert report["data"]["evidence"] == ["Evidence one"]
    assert report["data"]["productImplication"] == "高配主推，低配做锚点。"
    assert report["data"]["nextAction"] == "Build competitor price matrix"


def test_report_block_skips_playbook_label_implication() -> None:
    package = _evidence_package("pricing_analysis")
    artifacts = build_visual_artifacts(
        question="瑞典 J7 HEV 应该怎么定价？",
        answer={
            "title": "J7 HEV pricing",
            "direct": "J7 HEV 应围绕核心竞争带中段 + 高配主推。",
            "reportReadyBullets": [
                "瑞典 J7 HEV 定价方法应围绕“核心竞争带中段 + 高配主推”展开。",
                "配置价值：J7 HEV 的打法不是单点油耗压制，而是把质保、540°影像、HUD、座椅舒适、电尾门和天窗转成可见高配价值。",
            ],
            "businessImplications": [
                "定价走廊方法：J7 HEV 方法样例：核心竞争带中段 + 高配主推，先用市场窗口和竞品走廊定位。",
                "竞品池应锁定 Corolla Cross、RAV4、C-HR、Qashqai，价格判断落在 30,000-40,000 EUR 核心竞争带。",
                "定价判断不能套用单一车型模板，应先验证目标车型所属价格走廊、竞品池、配置价值和购买场景。",
                "J7 HEV 的打法不是单点油耗压制，而是把质保、540°影像、HUD、座椅舒适、电尾门和天窗转成可见高配价值。",
            ],
            "recommendedActions": [{"action": "把 J7 HEV 低配锚点和高配主推写成一页定价建议"}],
        },
        evidence_package=package,
        charts=[],
    )

    report = next(item for item in artifacts if item["type"] == "report_block")
    assert "定价走廊方法" not in report["data"]["productImplication"]
    assert "定价判断不能" not in report["data"]["productImplication"]
    assert "质保" in report["data"]["productImplication"]
    assert "可见高配价值" in report["data"]["productImplication"]


def test_pricing_report_block_prioritizes_user_material_over_reference_price_stats() -> None:
    artifacts = build_visual_artifacts(
        question="瑞典 J7 HEV 应该怎么定价？请给出竞品价格走廊、数据支撑和图表。",
        answer={
            "title": "瑞典 · J7 HEV 定价逻辑",
            "direct": "J7 HEV 可以先按核心竞争带中段 + 高配主推推进定价假设。",
            "evidenceDigest": [
                "背景价格样本最低值 = 39,121.7",
                "背景价格样本最高值 = 53,165.2",
            ],
            "reportReadyBullets": [
                "Title：瑞典 J7 HEV 定价逻辑",
                "Key message：低配做价格锚点，高配做主推版本。",
                "Evidence：参考价格样本最低值和最高值。",
                "Product implication：用可见高配解释价差。",
                "Next action：补官方 MSRP 和竞品月供/RV。",
            ],
            "recommendedActions": [{"action": "补官方 MSRP 和竞品月供/RV"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "query_msrp_pricing",
                    "success": True,
                    "rowCount": 0,
                    "sourceType": "postgres",
                    "evidenceRefs": [
                        {"refId": "ev_min", "label": "priceStats.min", "value": 39121.74, "unit": "currency", "source": "jato_price_positioning"},
                        {"refId": "ev_max", "label": "priceStats.max", "value": 53165.22, "unit": "currency", "source": "jato_price_positioning"},
                    ],
                },
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_j7_price", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_gap", "label": "J7 HEV user material price gap", "value": 3230, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                    ],
                },
                {
                    "toolName": "build_market_chart",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_market_hev", "label": "contextSnapshot.powertrainMix.HEV.sales", "value": 5051, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_market_bev", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                        {"refId": "ev_market_phev", "label": "contextSnapshot.powertrainMix.PHEV.sales", "value": 15028, "unit": "units", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [
                {"name": "current_msrp", "reason": "Requested official MSRP is not materialized.", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["type"] == "report_block")
    evidence = report["data"]["evidence"]
    joined = " ".join(evidence)
    assert "JATO 图表口径：HEV 5,051 units，BEV 25,235 units，PHEV 15,028 units" in evidence[0]
    assert "官方 MSRP / 核心竞品当前价格：待补可引用记录" in evidence[1]
    assert "主销高配假设 34,720 EUR" in joined
    assert "用户材料竞品价格带 30,000-40,000 EUR" in joined
    assert "高低配价差 3,230 EUR" in joined
    assert "PVA 覆盖 118 %" in joined
    assert "背景价格样本" not in joined
    assert "参考价格样本最低值" not in joined


def test_visual_artifacts_filter_sweden_j7_material_for_hungary_pricing() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利 J7 HEV 应该怎么定价？请不要回答瑞典。",
        answer={
            "title": "匈牙利 J7 HEV 定价验证",
            "direct": "匈牙利 J7 HEV 定价现在不能给确定数字，需要补官方 MSRP 和竞品价格走廊。",
            "evidenceDigest": [
                "本题车型官方 MSRP = 待补当前价格记录 / 官方来源验证",
                "竞品价格走廊 = 待补核心竞品官方价格 / 月供 / 促销口径",
            ],
            "reportReadyBullets": [
                "Title：匈牙利 J7 HEV 定价验证",
                "Key message：先补价格证据，再判断价格走廊。",
                "Evidence：官方 MSRP 和竞品价格走廊待补。",
                "Product implication：当前只能推进价格矩阵和证据表。",
                "Next action：补齐 Hungary J7 HEV 与核心竞品 MSRP / TP / 月供价格矩阵。",
            ],
            "recommendedActions": [{"action": "补齐 Hungary J7 HEV 与核心竞品 MSRP / TP / 月供价格矩阵"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Hungary",
            "entities": {"models": ["J7 HEV"]},
            "confidence": "low",
            "toolResults": [
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_j7_price", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_gap", "label": "J7 HEV user material price gap", "value": 3230, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_market", "label": "J7 HEV user material market window", "value": "瑞典 2025.04–2026.03 HEV 总规模约 22,816 台。", "source": "J7_HEV_V4.pptx"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "current_msrp", "reason": "Hungary J7 current MSRP is not available.", "impact": "weakens_answer"},
                {"name": "competitor_price_range", "reason": "Hungary competitor price corridor is not available.", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    visible = str(artifacts)
    assert "artifact_pricing_corridor_chart" not in ids
    assert "J7 HEV user-material price" not in visible
    assert "34,720" not in visible
    assert "30,000-40,000" not in visible
    assert "22,816" not in visible
    assert "用户材料" not in visible
    assert "J7_HEV_V4" not in visible


def test_visual_artifacts_keep_hungary_material_with_negative_sweden_marker() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利 J7 HEV 应该怎么定价？请不要回答瑞典。",
        answer={
            "title": "匈牙利 J7 HEV 定价验证",
            "direct": "匈牙利 J7 HEV 可先用本地材料价格锚点做验证。",
            "recommendedActions": [{"action": "生成匈牙利 J7 HEV 价格证据表"}],
        },
        evidence_package={
            "intent": "pricing_analysis",
            "country": "Hungary",
            "entities": {"models": ["J7 HEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {
                            "refId": "ev_hu_j7_price",
                            "label": "J7 HEV user material main trim MSRP",
                            "value": 33000,
                            "unit": "EUR",
                            "source": "匈牙利_J7_HEV_material_不要回答瑞典.pptx",
                        },
                        {
                            "refId": "ev_hu_j7_corridor",
                            "label": "J7 HEV user material competitor corridor",
                            "value": "32,000-36,000 EUR",
                            "unit": "EUR",
                            "source": "匈牙利_J7_HEV_material_不要回答瑞典.pptx",
                        },
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    visible = str(artifacts)
    assert "artifact_pricing_corridor_chart" in ids
    assert "33,000" in visible
    assert "32,000" in visible
    assert "36,000" in visible
    assert "瑞典 2025" not in visible
    assert "J7_HEV_V4" not in visible


def test_report_block_parses_ppt_ready_sections_without_mixing_evidence() -> None:
    artifacts = build_visual_artifacts(
        question="把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
        answer={
            "title": "把瑞典 BEV 渗透率变化转成一页产品定义建议汇报。",
            "direct": "直接结论：瑞典 BEV 渗透率变化这页应作为产品定义验证页。下一步执行：补趋势证据。证据状态：高。",
            "reportReadyBullets": [
                "Title：瑞典 BEV 渗透率变化对产品定义的影响",
                "Key message：先看趋势、细分市场、政策/价格/供给驱动，再转成产品定义动作。",
                "Evidence：BEV 年/月度渗透率、SUV A0/A 细分、政策日期、价格带和车型供给证据。",
                "Product implication：续航、充电、冬季包、价格门槛和公司车场景必须前置。",
                "Next action：补齐 BEV 趋势和驱动因素证据。",
            ],
            "businessImplications": ["fallback implication"],
            "recommendedActions": [],
        },
        evidence_package=_evidence_package("report_generation"),
        charts=[],
    )

    report = next(item for item in artifacts if item["type"] == "report_block")

    assert report["data"]["title"] == "瑞典 BEV 渗透率变化对产品定义的影响"
    assert report["data"]["keyMessage"] == "先看趋势、细分市场、政策/价格/供给驱动，再转成产品定义动作。"
    assert report["data"]["evidence"] == ["BEV 年/月度渗透率、SUV A0/A 细分、政策日期、价格带和车型供给证据。"]
    assert report["data"]["productImplication"] == "续航、充电、冬季包、价格门槛和公司车场景必须前置。"
    assert report["data"]["nextAction"] == "补齐 BEV 趋势和驱动因素证据。"


def test_report_generation_returns_evidence_appendix_table() -> None:
    artifacts = build_visual_artifacts(
        question="生成一页 O5 BEV 对标 EX30 和 EV3 的汇报结构。",
        answer={
            "title": "O5 BEV competitor report",
            "direct": "一页汇报应先给定位结论，再给证据 appendix。",
            "reportReadyBullets": [
                "Title：O5 BEV vs EX30 / EV3 竞品定位页",
                "Key message：EX30 做主对标，EV3 做价格/配置校验锚点。",
                "Evidence：竞品定位、BEV 市场份额和价格/配置差异。",
                "Product implication：O5 BEV 需要证明价格差和配置价值。",
                "Next action：生成 O5/EX30/EV3 一页竞品对标框架。",
            ],
            "recommendedActions": [{"action": "生成 O5/EX30/EV3 一页竞品对标框架"}],
        },
        evidence_package={
            "intent": "report_generation",
            "country": "Sweden",
            "confidence": "high",
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {"refId": "ev_ext_source", "label": "O5 BEV EX30 EV3 benchmark article.source", "value": "https://example.com/research/o5-ex30-ev3", "source": "external_research"},
                        {"refId": "ev_ext_claim", "label": "O5 BEV EX30 EV3 benchmark article.claim", "value": "Benchmark article compares O5 BEV positioning against EX30 and EV3.", "source": "https://example.com/research/o5-ex30-ev3"},
                        {"refId": "ev_ext_date", "label": "O5 BEV EX30 EV3 benchmark article.date", "value": "2026-03-03", "source": "external_research"},
                        {"refId": "ev_ext_rank", "label": "O5 BEV EX30 EV3 benchmark article.rank", "value": 1, "source": "external_research"},
                    ],
                },
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_bev_sales", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_report_generation_table")
    report = next(item for item in artifacts if item["type"] == "report_block")
    rows = table["data"]["rows"]

    assert [item["id"] for item in artifacts[:2]] == ["artifact_report_block", "artifact_report_generation_table"]
    assert table["title"] == "汇报证据附录"
    assert table["spec"]["columns"] == ["section", "evidence", "source", "businessUse", "nextAction", "confidence"]
    assert rows[0]["section"] == "Competitor evidence"
    assert rows[0]["source"] == "example.com · 2026-03-03"
    assert "O5 BEV positioning" in rows[0]["evidence"]
    assert "主对标" in rows[0]["businessUse"]
    assert rows[0]["nextAction"] == "生成 O5/EX30/EV3 一页竞品对标框架"
    assert rows[1]["section"] == "Market evidence"
    assert rows[1]["source"] == "JATO 图表数据"
    assert rows[1]["evidence"] == "BEV 动力销量: 25,235 units"
    assert table["sourceEvidenceRefs"] == ["ev_ext_source", "ev_bev_sales"]
    assert report["data"]["title"] == "O5 BEV vs EX30 / EV3 竞品定位页"


def test_policy_report_generation_filters_low_relevance_external_news_from_artifacts() -> None:
    artifacts = build_visual_artifacts(
        question="Elbilspremien 2026 会影响哪些车型？请给出来源、JATO 数据交叉验证、结论、风险和一页汇报结构。",
        answer={
            "title": "Elbilspremien 2026 policy impact",
            "direct": "需要先核对官方政策来源，再用 JATO BEV 结构判断候选车型池。",
            "reportReadyBullets": [
                "Title：Elbilspremien 2026 车型影响判断",
                "Key message：不能用普通 EV 新闻替代政策原文。",
                "Evidence：[R1] EV maker Polestar's quarterly sales volumes slide amid US market ban - Reuters（reuters.com，2026-07-09）。",
                "Product implication：先用 JATO BEV SUV A0/A 结构判断候选池，官方政策缺失时不点名确定受益车型。",
                "Next action：补官方 Elbilspremien 2026 原文、资格、价格上限和车型清单。",
            ],
            "recommendedActions": [{"action": "补官方 Elbilspremien 2026 原文、资格、价格上限和车型清单"}],
        },
        evidence_package={
            "intent": "report_generation",
            "country": "Sweden",
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "search_market_news",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {
                            "refId": "ev_polestar_source",
                            "label": "EV maker Polestar's quarterly sales volumes slide amid US market ban - Reuters.source",
                            "value": "https://www.reuters.com/business/autos-transportation/ev-maker-polestars-quarterly-sales-volumes-slide-amid-us-market-ban-2026-07-09/",
                            "source": "jato_web_search_service",
                        },
                        {
                            "refId": "ev_polestar_claim",
                            "label": "EV maker Polestar's quarterly sales volumes slide amid US market ban - Reuters.claim",
                            "value": "Polestar quarterly sales volumes declined amid a US market ban.",
                            "source": "https://www.reuters.com/business/autos-transportation/ev-maker-polestars-quarterly-sales-volumes-slide-amid-us-market-ban-2026-07-09/",
                        },
                        {
                            "refId": "ev_polestar_date",
                            "label": "EV maker Polestar's quarterly sales volumes slide amid US market ban - Reuters.date",
                            "value": "2026-07-09",
                            "source": "jato_web_search_service",
                        },
                    ],
                },
                {
                    "toolName": "query_country_snapshot",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_bev_sales", "label": "contextSnapshot.powertrainMix.BEV.sales", "value": 25235, "unit": "units", "source": "jato_country_chart_deck"},
                    ],
                },
            ],
            "missingEvidence": [
                {"name": "external_research_failed", "reason": "Official Elbilspremien source was not retrieved.", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    table = next(item for item in artifacts if item["id"] == "artifact_report_generation_table")
    report = next(item for item in artifacts if item["type"] == "report_block")
    visible = f"{table} {report}".casefold()

    assert "polestar" not in visible
    assert "reuters" not in visible
    assert "us market ban" not in visible
    assert any(row["source"] == "JATO 图表数据" for row in table["data"]["rows"])
    assert table["sourceEvidenceRefs"] == ["ev_bev_sales"]


def test_report_generation_pricing_page_adds_corridor_chart_and_pricing_table() -> None:
    artifacts = build_visual_artifacts(
        question="把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
        answer={
            "title": "J7 HEV pricing report",
            "direct": "瑞典 J7 HEV 定价应围绕核心竞争带中段 + 高配主推。",
            "reportReadyBullets": [
                "Title：瑞典 J7 HEV 定价逻辑：核心竞争带中段 + 高配主推",
                "Key message：低配做价格锚点，高配做主推版本。",
                "Evidence：主销高配价位、竞品价格带、PVA 覆盖和可见配置。",
                "Product implication：用可见高配和质保解释价差。",
                "Next action：补官方 MSRP 和竞品月供/RV。",
            ],
            "recommendedActions": [{"action": "补官方 MSRP 和竞品月供/RV"}],
        },
        evidence_package={
            "intent": "report_generation",
            "country": "Sweden",
            "entities": {"models": ["J7 HEV"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_j7_price", "label": "J7 HEV user material main trim MSRP", "value": 34720, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_corridor", "label": "J7 HEV user material competitor corridor", "value": "30,000-40,000 EUR", "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_gap", "label": "J7 HEV user material price gap", "value": 3230, "unit": "EUR", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_pva", "label": "J7 HEV user material PVA coverage", "value": 118, "unit": "%", "source": "J7_HEV_V4.pptx"},
                        {"refId": "ev_j7_pool", "label": "J7 HEV user material competitor pool", "value": "Corolla Cross, RAV4, C-HR, Qashqai", "source": "J7_HEV_V4.pptx"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    assert ids[:4] == [
        "artifact_report_block",
        "artifact_pricing_corridor_chart",
        "artifact_report_pricing_table",
        "artifact_report_generation_table",
    ]

    chart = next(item for item in artifacts if item["id"] == "artifact_pricing_corridor_chart")
    chart_rows = chart["data"]
    assert [row["label"] for row in chart_rows[:3]] == [
        "J7 HEV 用户材料主销价假设",
        "J7 HEV 用户材料竞品价格带下沿",
        "J7 HEV 用户材料竞品价格带上沿",
    ]
    assert chart_rows[0]["value"] == 34720
    assert chart_rows[1]["value"] == 30000
    assert chart_rows[2]["value"] == 40000

    pricing_table = next(item for item in artifacts if item["id"] == "artifact_report_pricing_table")
    pricing_rows = pricing_table["data"]["rows"]
    assert pricing_table["title"] == "价格证据表"
    assert pricing_rows[0]["model"] == "J7 HEV"
    assert pricing_rows[0]["msrp"] == "34,720 EUR"
    assert "用户材料价格锚点；不是当前官方 MSRP" in pricing_rows[0]["pricePosition"]


def test_report_generation_uses_generic_user_material_pricing_refs() -> None:
    artifacts = build_visual_artifacts(
        question="生成瑞典 O9 一页产品定位汇报结构。",
        answer={
            "title": "O9 positioning report",
            "direct": "O9 应以高端 SUV 公司车和家庭场景验证价格位置。",
            "reportReadyBullets": [
                "Title：瑞典 O9 产品定位页",
                "Key message：53k-55k EUR 需要配置、空间和公司车价值支撑。",
                "Evidence：主销高配价位、竞品价格带和 PVA 覆盖。",
                "Product implication：用大尺寸、高配和质保解释溢价。",
                "Next action：补官方 MSRP 和月供/RV。",
            ],
        },
        evidence_package={
            "intent": "report_generation",
            "country": "Sweden",
            "entities": {"models": ["O9"]},
            "confidence": "medium",
            "toolResults": [
                {
                    "toolName": "business_method_material",
                    "success": True,
                    "sourceType": "generated",
                    "evidenceRefs": [
                        {"refId": "ev_o9_price", "label": "O9 user material main trim MSRP", "value": 54000, "unit": "EUR", "source": "O9_user_material.pptx"},
                        {"refId": "ev_o9_corridor", "label": "O9 user material competitor corridor", "value": "53,000-55,000 EUR", "unit": "EUR", "source": "O9_user_material.pptx"},
                        {"refId": "ev_o9_gap", "label": "O9 user material price gap", "value": 2000, "unit": "EUR", "source": "O9_user_material.pptx"},
                        {"refId": "ev_o9_pva", "label": "O9 user material PVA coverage", "value": 105, "unit": "%", "source": "O9_user_material.pptx"},
                    ],
                }
            ],
            "missingEvidence": [],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    assert "artifact_pricing_corridor_chart" in ids
    assert "artifact_report_pricing_table" in ids

    chart = next(item for item in artifacts if item["id"] == "artifact_pricing_corridor_chart")
    assert [row["label"] for row in chart["data"][:3]] == [
        "O9 用户材料主销价假设",
        "O9 用户材料竞品价格带下沿",
        "O9 用户材料竞品价格带上沿",
    ]

    pricing_table = next(item for item in artifacts if item["id"] == "artifact_report_pricing_table")
    pricing_rows = pricing_table["data"]["rows"]
    assert pricing_rows[0]["model"] == "O9"
    assert pricing_rows[0]["msrp"] == "54,000 EUR"
    assert "用户材料价格锚点；不是当前官方 MSRP" in pricing_rows[0]["pricePosition"]
    assert "PVA 105 %" in str(pricing_rows)
    assert "53,000-55,000 EUR" in str(pricing_rows)


def test_report_generation_returns_model_coverage_matrix_when_competitor_evidence_is_partial() -> None:
    artifacts = build_visual_artifacts(
        question="生成 O5 BEV 对标 EX30 和 EV3 的一页竞品汇报框架。",
        answer={
            "title": "O5 BEV competitor report",
            "direct": "EX30 做主对标，EV3 做价格/配置校验锚点。",
            "reportReadyBullets": [
                "Title：O5 BEV vs EX30 / EV3 竞品定位页",
                "Key message：EX30 做主对标，EV3 做价格/配置校验锚点。",
                "Next action：补齐三车 MSRP、版本、续航/电池、ADAS 和冬季配置矩阵。",
            ],
            "recommendedActions": [{"action": "补齐三车 MSRP、版本、续航/电池、ADAS 和冬季配置矩阵"}],
        },
        evidence_package={
            "intent": "report_generation",
            "country": "Sweden",
            "confidence": "medium",
            "entities": {"models": ["O5 BEV", "EX30", "EV3"], "competitors": ["EX30", "EV3"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ev_ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato_cross_reference"},
                        {"refId": "ev_ex30_model", "label": "competitor.1.model", "value": "EX30", "source": "jato_cross_reference"},
                        {"refId": "ev_xc60_sales", "label": "XC60.sales", "value": 2893, "unit": "units", "source": "jato_cross_reference"},
                        {"refId": "ev_market_sales", "label": "cumulativeSales", "value": 1182452, "unit": "units", "source": "jato_country_snapshot"},
                        {"refId": "ev_market_msrp", "label": "avgMsrp", "value": 57954, "unit": "currency", "source": "jato_country_snapshot"},
                    ],
                },
            ],
            "missingEvidence": [
                {
                    "name": "competitive_or_configuration_data_unavailable",
                    "reason": "Multi-model report question has no evidence refs covering all requested models.",
                    "impact": "weakens_answer",
                }
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    coverage_chart = next(item for item in artifacts if item["id"] == "artifact_report_model_coverage_chart")
    coverage = next(item for item in artifacts if item["id"] == "artifact_report_model_coverage_table")
    appendix = next(item for item in artifacts if item["id"] == "artifact_report_generation_table")
    report = next(item for item in artifacts if item["id"] == "artifact_report_block")
    rows = coverage["data"]["rows"]

    assert ids[:4] == [
        "artifact_report_block",
        "artifact_report_model_coverage_chart",
        "artifact_report_model_coverage_table",
        "artifact_report_generation_table",
    ]
    assert coverage_chart["title"] == "Competitor report evidence coverage"
    assert coverage_chart["data"] == [
        {
            "label": "O5 BEV",
            "value": 0,
            "unit": "refs",
            "series": "available evidence refs",
            "coverageStatus": "待补",
            "missingEvidence": "MSRP、版本、续航/电池、ADAS/冬季配置、来源日期",
        },
        {
            "label": "EX30",
            "value": 1,
            "unit": "refs",
            "series": "available evidence refs",
            "coverageStatus": "部分覆盖",
            "missingEvidence": "MSRP/价格、配置/电池/续航、来源日期",
        },
        {
            "label": "EV3",
            "value": 0,
            "unit": "refs",
            "series": "available evidence refs",
            "coverageStatus": "待补",
            "missingEvidence": "MSRP、版本、续航/电池、ADAS/冬季配置、来源日期",
        },
    ]
    assert coverage["title"] == "Competitor report coverage matrix"
    assert coverage["spec"]["columns"] == [
        "model",
        "role",
        "coverageStatus",
        "availableEvidence",
        "missingEvidence",
        "nextAction",
        "source",
    ]
    assert rows[0]["model"] == "O5 BEV"
    assert rows[0]["coverageStatus"] == "待补"
    assert "MSRP、版本、续航/电池" in rows[0]["missingEvidence"]
    assert rows[1]["model"] == "EX30"
    assert rows[1]["role"] == "竞品"
    assert rows[1]["coverageStatus"] == "部分覆盖"
    assert "EX30.sales=1,518 units" in rows[1]["availableEvidence"]
    assert "competitor.1.model" not in rows[1]["availableEvidence"]
    assert rows[2]["model"] == "EV3"
    assert rows[2]["role"] == "竞品"
    assert rows[2]["source"] == "待补来源"
    assert "证据覆盖验证页" in report["data"]["keyMessage"]
    assert "EX30 做主对标" not in report["data"]["keyMessage"]
    assert "EV3 做价格/配置校验锚点" not in report["data"]["keyMessage"]
    assert "competitor.1.model" not in str(appendix["data"]["rows"])
    assert "XC60" not in str(appendix["data"]["rows"])
    assert "cumulativeSales" not in str(appendix["data"]["rows"])
    assert "avgMsrp" not in str(appendix["data"]["rows"])
    metric_cards = next((item for item in artifacts if item["id"] == "artifact_metric_cards"), None)
    if metric_cards:
        assert "cumulativeSales" not in str(metric_cards["data"]["rows"])
        assert "avgMsrp" not in str(metric_cards["data"]["rows"])


def test_report_coverage_matrix_marks_role_claims_as_unverified_when_evidence_is_partial() -> None:
    artifacts = build_visual_artifacts(
        question="生成 O5 BEV 对标 EX30 和 EV3 的一页竞品汇报框架。",
        answer={
            "title": "O5 BEV competitor report",
            "direct": "EX30 做主对标，EV3 做价格/配置校验锚点。",
            "reportReadyBullets": [
                "Title：O5 BEV vs EX30 / EV3 竞品定位页",
                "Key message：EX30 做主对标，EV3 做价格/配置校验锚点。",
                "Next action：补齐三车 MSRP、版本、续航/电池、ADAS 和冬季配置矩阵。",
            ],
        },
        evidence_package={
            "intent": "report_generation",
            "country": "Sweden",
            "confidence": "medium",
            "entities": {"models": ["O5 BEV"], "competitors": ["EX30", "EV3"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "ex30_role", "label": "EX30.primaryBenchmark", "value": "主对标候选", "source": "jato_cross_reference"},
                        {"refId": "ex30_sales", "label": "EX30.sales", "value": 1518, "unit": "units", "source": "jato_cross_reference"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "current_msrp", "reason": "No O5/EX30/EV3 MSRP rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No feature matrix.", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["id"] == "artifact_report_block")
    coverage = next(item for item in artifacts if item["id"] == "artifact_report_model_coverage_table")
    rows = coverage["data"]["rows"]
    ex30 = next(row for row in rows if row["model"] == "EX30")

    assert ex30["role"] == "待验证主对标"
    assert "证据覆盖验证页" in report["data"]["keyMessage"]
    assert "EX30 做主对标" not in report["data"]["keyMessage"]
    assert "EV3 做价格/配置校验锚点" not in report["data"]["keyMessage"]


def test_report_generation_appendix_groups_requested_competitor_refs() -> None:
    artifacts = build_visual_artifacts(
        question="生成一页匈牙利 T7 HEV 对标 Corolla Cross 和 Tucson 的汇报结构。",
        answer={
            "title": "Hungary T7 competitor report",
            "direct": "T7 HEV 与 Corolla Cross / Tucson 不应等权罗列。",
            "reportReadyBullets": [
                "Title：匈牙利 T7 HEV 对标 Corolla Cross / Tucson",
                "Key message：Corolla Cross 是当前最可引用竞品锚点。",
                "Evidence：Corolla Cross 销量和 Corolla Cross / Tucson 级别证据可用；T7 价格和配置缺口待补。",
                "Product implication：先给对标角色和验证路径，不写确定胜负。",
                "Next action：补齐目标车型 MSRP、竞品价格走廊和月供/促销口径，再生成最终 PPT block。",
            ],
            "recommendedActions": [{"action": "生成一页 PPT block"}],
        },
        evidence_package={
            "intent": "report_generation",
            "country": "Hungary",
            "confidence": "medium",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "cor_sales", "label": "Corolla Cross.sales", "value": 1250, "unit": "units", "source": "jato"},
                        {"refId": "cor_segment", "label": "Corolla Cross.segment", "value": "SUV A", "source": "jato"},
                        {"refId": "tuc_segment", "label": "Tucson.segment", "value": "SUV A", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "target_model_price", "reason": "No T7 price rows.", "impact": "weakens_answer"},
                {"name": "configuration_delta", "reason": "No config diff.", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    ids = [item["id"] for item in artifacts]
    assert ids[:4] == [
        "artifact_report_block",
        "artifact_report_model_coverage_chart",
        "artifact_report_model_coverage_table",
        "artifact_report_generation_table",
    ]

    appendix = next(item for item in artifacts if item["id"] == "artifact_report_generation_table")
    rows = appendix["data"]["rows"]
    assert len(rows) == 2
    assert rows[0]["section"] == "Competitor evidence"
    assert rows[0]["evidence"] == "Corolla Cross 销量: 1,250 units；Corolla Cross.segment: SUV A"
    assert rows[0]["businessUse"] == "支撑主对标、价格锚点、配置校验锚点和销售替代对象判断。"
    assert rows[0]["nextAction"] == "补齐目标车型 MSRP、竞品价格走廊和月供/促销口径，再生成最终 PPT block"
    assert rows[1]["section"] == "Competitor evidence"
    assert rows[1]["evidence"] == "Tucson.segment: SUV A"

    coverage = next(item for item in artifacts if item["id"] == "artifact_report_model_coverage_table")
    coverage_rows = coverage["data"]["rows"]
    assert coverage_rows[0]["model"] == "T7 HEV"
    assert coverage_rows[0]["coverageStatus"] == "待补"
    assert coverage_rows[1]["model"] == "Corolla Cross"
    assert coverage_rows[1]["availableEvidence"] == "Corolla Cross.sales=1,250 units；Corolla Cross.segment=SUV A"


def test_report_block_uses_price_evidence_instead_of_generic_report_implication() -> None:
    artifacts = build_visual_artifacts(
        question="生成一页匈牙利 T7 HEV 对标 Corolla Cross 和 Tucson 的汇报结构。",
        answer={
            "title": "Hungary T7 price report",
            "direct": "T7 HEV 与 Corolla Cross / Tucson 不应等权罗列。",
            "reportReadyBullets": [
                "Title：匈牙利 T7 HEV 对标 Corolla Cross / Tucson",
                "Key message：T7 HEV 需要先用价格和配置证据确认打法。",
                "Evidence：待补完整竞品矩阵。",
                "Product implication：先给对标角色和验证路径，不写确定胜负。",
                "Next action：补齐配置差异和月供/RV。",
            ],
            "recommendedActions": [{"action": "补齐配置差异和月供/RV"}],
        },
        evidence_package={
            "intent": "report_generation",
            "country": "Hungary",
            "confidence": "medium",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "t7_price", "label": "T7 HEV.avgPrice", "value": 33000, "unit": "EUR", "source": "jato"},
                        {"refId": "cor_price", "label": "Corolla Cross.avgPrice", "value": 38500, "unit": "EUR", "source": "jato"},
                        {"refId": "tuc_price", "label": "Tucson.avgPrice", "value": 41000, "unit": "EUR", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "configuration_delta", "reason": "No configuration diff.", "impact": "weakens_answer"},
                {"name": "monthly_payment", "reason": "No monthly payment rows.", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["id"] == "artifact_report_block")

    assert report["data"]["productImplication"].startswith("T7 HEV 可作为低位切入/价格锚点")
    assert "低价不等于低价值" in report["data"]["productImplication"]
    assert "先给对标角色" not in report["data"]["productImplication"]
    assert report["data"]["evidence"][0] == (
        "价格证据：T7 HEV 当前价格 33,000 EUR；"
        "低于已查竞品价格下沿 38,500，价格进入风险低但价值感待验证。"
    )
    assert report["data"]["evidence"][1] == (
        "竞品价格锚点：Corolla Cross 当前价格 38,500 EUR；Tucson 当前价格 41,000 EUR。"
    )


def test_report_block_uses_evidence_backed_lead_when_report_bullets_are_generic() -> None:
    artifacts = build_visual_artifacts(
        question="生成一页匈牙利 T7 HEV 对标 Corolla Cross 和 Tucson 的汇报结构。",
        answer={
            "title": "Hungary T7 price report",
            "direct": "T7 HEV 与 Corolla Cross / Tucson 不应等权罗列。",
            "evidenceBackedLead": (
                "已查数据：匈牙利 T7 HEV.avgPrice = 33,000 EUR；"
                "Corolla Cross.avgPrice = 38,500 EUR；Tucson.avgPrice = 41,000 EUR。"
                "业务判断：T7 HEV 可作为低位切入/价格锚点，但低价不等于低价值。"
            ),
            "reportReadyBullets": [
                "Title：匈牙利 T7 HEV 对标 Corolla Cross / Tucson",
                "Key message：先补完整竞品矩阵再判断打法。",
                "Evidence：待补完整竞品矩阵。",
                "Product implication：先给对标角色和验证路径，不写确定胜负。",
                "Next action：补齐配置差异和月供/RV。",
            ],
            "recommendedActions": [{"action": "补齐配置差异和月供/RV"}],
        },
        evidence_package={
            "intent": "report_generation",
            "country": "Hungary",
            "confidence": "medium",
            "entities": {"models": ["T7 HEV"], "competitors": ["Corolla Cross", "Tucson"]},
            "toolResults": [
                {
                    "toolName": "compare_competitive_set",
                    "success": True,
                    "sourceType": "jato_parquet",
                    "evidenceRefs": [
                        {"refId": "t7_price", "label": "T7 HEV.avgPrice", "value": 33000, "unit": "EUR", "source": "jato"},
                        {"refId": "cor_price", "label": "Corolla Cross.avgPrice", "value": 38500, "unit": "EUR", "source": "jato"},
                        {"refId": "tuc_price", "label": "Tucson.avgPrice", "value": 41000, "unit": "EUR", "source": "jato"},
                    ],
                }
            ],
            "missingEvidence": [
                {"name": "configuration_delta", "reason": "No configuration diff.", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["id"] == "artifact_report_block")

    assert report["data"]["keyMessage"] == "T7 HEV 可作为低位切入/价格锚点，但低价不等于低价值。"
    assert report["data"]["productImplication"] == "T7 HEV 可作为低位切入/价格锚点，但低价不等于低价值。"
    assert report["data"]["evidence"][:3] == [
        "匈牙利 T7 HEV.avgPrice = 33,000 EUR",
        "Corolla Cross.avgPrice = 38,500 EUR",
        "Tucson.avgPrice = 41,000 EUR",
    ]
    assert "待补完整竞品矩阵" not in str(report["data"])
    assert "配置差异待补" in report["data"]["evidence"][3]
    assert "瑞典" not in str(report)


def test_report_block_cleans_policy_direct_fallback_key_message() -> None:
    artifacts = build_visual_artifacts(
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        answer={
            "title": "瑞典 · 政策影响判断",
            "direct": "直接结论：瑞典 BEV 补贴价格上限会把 O5 BEV 定价转成资格门槛与配置价值平衡问题。下一步执行：核对政策。证据状态：部分对齐。",
            "reportReadyBullets": [
                "瑞典 BEV 补贴价格上限问题要先确认政策是否仍有效、是否有新计划、价格门槛和适用人群。",
                "如果价格上限有效，低配/主销版承担补贴资格锚点，高配需要证明补贴外的配置价值。",
                "建议动作：核对瑞典 BEV 补贴价格上限是否仍有效及 O5 BEV 是否适用。",
            ],
            "businessImplications": ["低配做资格锚点，高配证明补贴外价值。"],
            "recommendedActions": [],
        },
        evidence_package={
            **_evidence_package("news_policy_search"),
            "toolResults": [
                {
                    "toolName": "external_research",
                    "success": True,
                    "sourceType": "web",
                    "evidenceRefs": [
                        {"refId": "ev_policy", "label": "O5 BEV price cap claim", "value": "Price cap changes trim strategy.", "source": "external_research"}
                    ],
                }
            ],
        },
        charts=[],
    )

    report = next(item for item in artifacts if item["type"] == "report_block")

    assert report["data"]["keyMessage"] == "瑞典 BEV 补贴价格上限问题要先确认政策是否仍有效、是否有新计划、价格门槛和适用人群。"
    assert report["data"]["evidence"] == [
        "如果价格上限有效，低配/主销版承担补贴资格锚点，高配需要证明补贴外的配置价值。",
    ]
    assert report["data"]["nextAction"] == "核对瑞典 BEV 补贴价格上限是否仍有效及 O5 BEV 是否适用。"


def test_leasing_tco_repair_queries_do_not_default_to_sweden() -> None:
    artifacts = build_visual_artifacts(
        question="匈牙利大客户 leasing 场景下，PHEV 还有没有理由？",
        answer={"title": "Hungary PHEV leasing", "direct": "需要补 TCO 来源。"},
        evidence_package={
            "evidenceId": "evpkg_hu_leasing",
            "intent": "pricing_analysis",
            "country": "",
            "confidence": "low",
            "toolResults": [],
            "missingEvidence": [
                {"name": "leasing_tco_or_company_car_evidence", "reason": "No TCO rows.", "impact": "weakens_answer"},
            ],
        },
        charts=[],
    )

    repair = next(item for item in artifacts if item["id"] == "artifact_external_source_repair_table")
    visible = str(repair)

    assert "Hungary" in visible
    assert "Sweden" not in visible
    assert "skatteverket" not in visible.casefold()
