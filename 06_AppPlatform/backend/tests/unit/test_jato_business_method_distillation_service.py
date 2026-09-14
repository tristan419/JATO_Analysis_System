from __future__ import annotations

from app.services.jato_business_method_distillation_service import extract_business_method_from_text
from app.services.jato_business_method_distillation_service import get_active_pricing_method


J7_METHOD_TEXT = """
基于瑞典市场、竞品格局和配置差异，J7 HEV 的定价逻辑应围绕“核心竞争带中段 + 高配主推”展开。
市场层面，瑞典 HEV 规模稳定，2025.04–2026.03 总规模约 22,816 台，需求高度集中在 SUV A0 / SUV A 层级。
RAV4 换代、Corolla Cross 改款和交付周期拉长，为 J7 HEV 提供进入窗口。
竞品层面，J7 应重点锁定 Corolla Cross、RAV4、C-HR、Qashqai 等同价带、同级别、同使用场景车型。
主销高配 34,720€ 落在 3.0–4.0 万欧核心价格带内。
配置层面，J7 的优势在 7年/15万公里质保、540°全景影像、HUD、座椅通风/记忆、感应电尾门、全景天窗。
高低配价差为 3,230€，高配 PVA 估值约 3,820€，覆盖率约 118%，Appendix PVA TOTAL 3400€。
P08 建议主销价 34,720 元。P10 包含购置税、增值税、上牌费、保险费、新能源免征。
Source: JATO / user PPT.
"""


def test_extracts_j7_pricing_method_from_text() -> None:
    method = extract_business_method_from_text(
        J7_METHOD_TEXT,
        source_name="J7_HEV_V4.pptx",
        market="Sweden",
        model="J7 HEV",
    )

    assert method["methodType"] == "pricing_positioning"
    assert method["market"] == "Sweden"
    assert method["model"] == "J7 HEV"
    assert "competitor_corridor" in method["analysisFlow"]
    assert method["priceCorridor"]["positioning"] == "核心竞争带中段 + 高配主推"
    assert "Corolla Cross" in method["competitorPool"]
    assert "RAV4" in method["competitorPool"]


def test_generates_pricing_playbook_and_feature_value_dictionary() -> None:
    method = extract_business_method_from_text(J7_METHOD_TEXT, market="Sweden", model="J7 HEV")
    playbook = method["pricingPlaybook"]
    feature_names = {item["featureName"] for item in method["featureValueClaims"]}

    assert "SUV A0 / SUV A" in playbook["market_window"]
    assert "2025.04–2026.03" in playbook["market_window"]
    assert "22,816" in playbook["market_window"]
    assert "核心竞争带" in playbook["competitor_corridor"]
    assert "低配" in playbook["price_anchor"]
    assert "高配" in playbook["main_trim_strategy"]
    assert "7年/15万公里质保" in feature_names
    assert "540°全景影像" in feature_names
    assert "HUD" in feature_names


def test_builds_golden_answer_spec() -> None:
    method = extract_business_method_from_text(J7_METHOD_TEXT, market="Sweden", model="J7 HEV")

    assert "market window" in method["goldenAnswer"]["expectedMustMention"]
    assert "high trim main version" in method["goldenAnswer"]["expectedMustMention"]
    assert "pm_insight" in method["goldenAnswer"]["answerQualityRubric"]


def test_detects_data_quality_warnings() -> None:
    method = extract_business_method_from_text(J7_METHOD_TEXT, market="Sweden", model="J7 HEV")
    warning_codes = {warning["code"] for warning in method["dataQualityWarnings"]}

    assert "mixed_currency_unit" in warning_codes
    assert "multiple_pva_values" in warning_codes
    assert "non_target_market_template_residue" in warning_codes


def test_active_method_matches_sweden_j7_hev_pricing() -> None:
    method = get_active_pricing_method(country="Sweden", model="J7 HEV", question="瑞典 J7 HEV 应该怎么定价？")

    assert method is not None
    assert method["methodType"] == "pricing_positioning"
    assert method["priceCorridor"]["positioning"] == "核心竞争带中段 + 高配主推"
