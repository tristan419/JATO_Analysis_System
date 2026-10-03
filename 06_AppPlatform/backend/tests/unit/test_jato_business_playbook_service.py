from __future__ import annotations

from app.services.jato_business_playbook_service import build_business_playbook_context
from app.services.jato_business_playbook_service import enhance_answer_with_business_playbook
from app.services.jato_business_playbook_service import infer_business_failure_tags
from app.services.jato_business_playbook_service import list_business_playbooks


def test_business_playbooks_cover_core_validation_intents() -> None:
    playbooks = list_business_playbooks()

    assert "pricing_analysis" in playbooks
    assert "market_overview" in playbooks
    assert "competitor_compare" in playbooks
    assert "configuration_analysis" in playbooks
    assert "news_policy_search" in playbooks
    assert "inventory_analysis" in playbooks
    assert "voc_analysis" in playbooks
    assert "report_generation" in playbooks


def test_pricing_playbook_prioritizes_own_model_and_competitor_price_matrix() -> None:
    playbook = list_business_playbooks()["pricing_analysis"]

    assert playbook["nextActions"][0] == "补齐本车型与竞品 MSRP / TP / 月供价格矩阵"
    assert playbook["nextActions"][0] != "补齐竞品 MSRP"


def test_business_playbook_context_uses_category_alias() -> None:
    context = build_business_playbook_context(
        country="Sweden",
        question="OMODA9 一个版型多个物料号应该怎么解释？",
        category="inventory_bom",
    )

    assert context["id"] == "inventory_bom"
    assert "实体关系" in context["requiredSections"]
    assert "query_with_filters" in context["recommendedTools"]


def test_insufficient_evidence_answer_is_business_actionable() -> None:
    answer = enhance_answer_with_business_playbook(
        {
            "direct": "当前证据不足。",
            "bullets": [],
            "limitations": [],
            "answerStatus": "insufficient_evidence",
        },
        {
            "evidenceId": "evpkg_test",
            "intent": "pricing_analysis",
            "country": "Sweden",
            "toolResults": [],
            "missingEvidence": [{"name": "own_model_price", "reason": "empty", "impact": "blocking"}],
            "confidence": "low",
        },
        country="Sweden",
        question="瑞典 J7 HEV 应该怎么定价？",
    )

    assert "不能停在" in answer["direct"]
    assert any("下一步动作" in item for item in answer["bullets"])
    assert any(item.startswith("建议查数动作：") for item in answer["bullets"])
    assert any(item.startswith("建议输出形态：") for item in answer["bullets"])
    assert not any(item.startswith("建议调用工具：") for item in answer["bullets"])
    assert answer["businessPlaybook"]["id"] == "pricing_analysis"


def test_infer_failure_tags_marks_countrycopilot_win_and_weak_business_scores() -> None:
    tags = infer_business_failure_tags(
        {
            "country": "Sweden",
            "expectedIntent": "pricing_analysis",
            "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
            "astrbot": {
                "answerStatus": "insufficient_evidence",
                "answerPreview": "当前工具没有查到足够可引用证据。",
                "selectedTool": "query_country_snapshot",
                "evidenceRefCount": 0,
            },
            "comparison": {
                "astrbotAnswerChars": 20,
                "countryCopilotAnswerChars": 800,
                "answerLengthDelta": -780,
            },
            "humanScoring": {
                "winner": "countryCopilot",
                "scoreTotals": {"complete": True},
                "astrbotScores": {
                    "pmInsight": 2,
                    "actionability": 2,
                    "artifactQuality": 2,
                    "presentationReadiness": 2,
                },
            },
        }
    )

    assert "answer_too_conservative" in tags
    assert "answer_too_generic" in tags
    assert "tool_missing" in tags
    assert "pm_insight_weak" in tags
    assert "presentation_not_ready" in tags


def test_report_generation_with_report_block_is_not_marked_chart_not_useful() -> None:
    tags = infer_business_failure_tags(
        {
            "country": "Sweden",
            "category": "report_generation",
            "expectedIntent": "report_generation",
            "expectedTools": ["query_msrp_pricing", "compare_competitive_set"],
            "question": "把瑞典 J7 HEV 定价逻辑生成一页产品定位汇报结构。",
            "astrbot": {
                "answerStatus": "partially_answered",
                "answerPreview": "直接结论：Sweden report-ready structure.",
                "selectedTool": "query_msrp_pricing",
                "evidenceRefCount": 1,
                "chartCount": 0,
                "evidencePackage": {
                    "toolResults": [
                        {"toolName": "query_msrp_pricing", "evidenceRefs": [{"refId": "ev_1"}]},
                        {"toolName": "compare_competitive_set", "evidenceRefs": []},
                    ]
                },
                "visualArtifacts": [
                    {"type": "metric_cards"},
                    {"type": "report_block"},
                ],
                "businessSynthesisPlan": {"reportReadyBullets": ["Title / Evidence / Action"]},
            },
            "comparison": {"astrbotAnswerChars": 500, "answerLengthDelta": 0},
            "humanScoring": {"scoreTotals": {"complete": False}},
        }
    )

    assert "chart_not_useful" not in tags
    assert "presentation_not_ready" not in tags
    assert "tool_missing" not in tags


def test_country_scoped_config_evidence_without_country_word_is_not_generic() -> None:
    tags = infer_business_failure_tags(
        {
            "country": "Sweden",
            "category": "configuration",
            "expectedIntent": "configuration_analysis",
            "expectedTools": ["compare_vehicle_variants", "build_market_chart"],
            "question": "A0 SUV BEV 为什么需要 80kWh 电池？",
            "astrbot": {
                "answerStatus": "partially_answered",
                "answerPreview": (
                    "直接结论：80kWh 可以作为 A0/A SUV 长续航/高配安全边界继续验证，"
                    "因为市场结构已有相关场景信号：SUV A0 细分销量 5,416 units，"
                    "SUV A 细分销量 7,544 units。"
                ),
                "evidenceRefCount": 8,
                "selectedTool": "compare_vehicle_variants",
                "evidenceDigest": [
                    "SUV A0 细分销量 = 5,416 units（JATO 图表数据）",
                    "SUV A 细分销量 = 7,544 units（JATO 图表数据）",
                    "配置验证项 = 80kWh 长续航/高配安全边界、热泵、电池预热、快充和冬季舒适配置",
                ],
                "displayPlan": "用配置差异表展示 trim、must-have、visible value、cost/risk。",
                "recommendedActions": [
                    {"action": "生成 A0 SUV BEV 80kWh 续航-价格-重量验证表。"},
                ],
                "businessImplications": [
                    "A0 SUV BEV 的 80kWh 应定位为长续航/高配安全边界，低配仍要保留价格锚点。",
                ],
                "visualArtifacts": [
                    {"type": "chart", "title": "Market structure chart"},
                    {"type": "table", "title": "Configuration validation matrix"},
                ],
                "evidencePackage": {
                    "country": "Sweden",
                    "intent": "configuration_analysis",
                    "toolResults": [
                        {
                            "toolName": "build_market_chart",
                            "evidenceRefs": [{"refId": "ev_suv_a0_sales"}],
                        }
                    ],
                },
                "qualityScore": {"totalScore": 0.88, "failures": []},
            },
            "comparison": {
                "astrbotAnswerChars": 900,
                "countryCopilotAnswerChars": 1100,
                "answerLengthDelta": -200,
            },
            "humanScoring": {"scoreTotals": {"complete": False}},
        }
    )

    assert "answer_too_generic" not in tags


def test_missing_blocking_evidence_quality_failure_maps_to_evidence_missing() -> None:
    tags = infer_business_failure_tags(
        {
            "country": "Sweden",
            "category": "pricing",
            "expectedIntent": "pricing_analysis",
            "expectedTools": ["query_msrp_pricing"],
            "question": "O9 在瑞典 53k-55k 欧元是否合理？",
            "astrbot": {
                "answerStatus": "partially_answered",
                "answerPreview": "当前只能做方向判断，仍缺少 MSRP 和竞品价格走廊。",
                "selectedTool": "query_msrp_pricing",
                "evidenceRefCount": 4,
                "qualityScore": {
                    "totalScore": 0.74,
                    "failures": ["missing_blocking_evidence"],
                },
                "evidencePackage": {
                    "toolResults": [
                        {"toolName": "query_msrp_pricing", "evidenceRefs": [{"refId": "ev_1"}]},
                    ],
                    "missingEvidence": [
                        {"name": "current_msrp", "reason": "No MSRP evidence.", "impact": "blocking"},
                    ],
                },
            },
            "comparison": {"astrbotAnswerChars": 420, "answerLengthDelta": 0},
            "humanScoring": {"scoreTotals": {"complete": False}},
        }
    )

    assert "evidence_missing" in tags


def test_explicit_chart_request_without_chart_is_marked_chart_not_useful() -> None:
    tags = infer_business_failure_tags(
        {
            "country": "Sweden",
            "category": "market_overview",
            "expectedIntent": "market_overview",
            "expectedTools": ["query_country_snapshot"],
            "question": "画一张瑞典 BEV 趋势图表。",
            "astrbot": {
                "answerStatus": "answered",
                "answerPreview": "Sweden market chart answer.",
                "selectedTool": "query_country_snapshot",
                "evidenceRefCount": 3,
                "chartCount": 0,
                "evidencePackage": {
                    "toolResults": [{"toolName": "query_country_snapshot", "evidenceRefs": [{"refId": "ev_1"}]}]
                },
                "visualArtifacts": [{"type": "metric_cards"}],
            },
            "comparison": {"astrbotAnswerChars": 500, "answerLengthDelta": 0},
            "humanScoring": {"scoreTotals": {"complete": False}},
        }
    )

    assert "chart_not_useful" in tags
