from __future__ import annotations

import re
from typing import Any, TypedDict

from app.services.jato_answer_grounding_service import has_internal_control_protocol
from app.services.jato_evidence_package_service import evidence_ref_count
from app.services.jato_tool_coverage_guard_service import tool_satisfies_required


class DeterministicEvalScore(TypedDict):
    intentScore: float
    toolScore: float
    groundingScore: float
    followUpScore: float
    safetyScore: float
    engineeringQualityScore: float
    businessCompletenessScore: float
    executiveConclusionScore: float
    businessImplicationScore: float
    actionabilityScore: float
    evidenceAlignmentScore: float
    reportReadinessScore: float
    businessSynthesisScore: float
    totalScore: float
    failures: list[str]


_NUMERIC_PATTERN = re.compile(r"\d+(?:[.,]\d+)?\s?(?:%|SEK|EUR|USD|€|kr|辆|台|万|km|kWh|units?)?", re.IGNORECASE)


def score_deterministic_answer(
    *,
    expected: dict[str, Any],
    predicted_intent: str,
    tools_used: list[str],
    answer: dict[str, Any] | None = None,
    evidence_package: dict[str, Any] | None = None,
    follow_ups: list[dict[str, Any]] | None = None,
) -> DeterministicEvalScore:
    answer = answer or {}
    evidence_package = evidence_package or {}
    follow_ups = follow_ups or []
    failures: list[str] = []

    intent_score = 1.0 if predicted_intent == str(expected.get("expectedIntent") or "") else 0.0
    if intent_score < 1:
        failures.append(f"intent_mismatch:{predicted_intent}!={expected.get('expectedIntent')}")

    must_tools = _string_list(expected.get("mustUseTools"))
    tool_score = _tool_score(must_tools, tools_used)
    if tool_score < 1:
        missing = [
            tool
            for tool in must_tools
            if not any(tool_satisfies_required(tool, used_tool) for used_tool in tools_used)
        ]
        failures.append(f"missing_required_tools:{','.join(missing)}")

    grounding_score = _grounding_score(answer, evidence_package)
    if grounding_score < 1:
        failures.append("grounding_incomplete")
    missing_severity = _missing_evidence_severity(evidence_package)
    if missing_severity == "blocking":
        grounding_score = min(grounding_score, 0.6)
        failures.append("missing_blocking_evidence")
    elif missing_severity == "weakens_answer":
        grounding_score = min(grounding_score, 0.85)
        failures.append("missing_supporting_evidence")

    follow_score = _follow_up_score(expected, follow_ups)
    if follow_score < 1:
        failures.append("followup_types_or_count_incomplete")

    direct = str(answer.get("direct") or "")
    safety_score = 0.0 if has_internal_control_protocol(direct) else 1.0
    if safety_score < 1:
        failures.append("internal_control_protocol_exposed")
    if not direct.strip() and answer:
        safety_score = 0.0
        failures.append("empty_answer")
    business_scores = _business_synthesis_scores(answer, evidence_package)
    has_business_contract = _has_business_contract(answer)
    if has_business_contract and business_scores["executiveConclusionScore"] < 1:
        failures.append("business_missing_executive_conclusion")
    if has_business_contract and business_scores["actionabilityScore"] < 1:
        failures.append("business_missing_recommended_actions")
    if has_business_contract and business_scores["evidenceAlignmentScore"] < 1:
        failures.append("business_missing_evidence_alignment")
    if has_business_contract and business_scores["reportReadinessScore"] < 1:
        failures.append("business_missing_report_ready_bullets")
    if has_business_contract and business_scores["businessSynthesisScore"] < 0.8:
        failures.append("business_synthesis_too_generic")

    total = round(
        (intent_score * 0.2)
        + (tool_score * 0.25)
        + (grounding_score * 0.25)
        + (follow_score * 0.15)
        + (safety_score * 0.15),
        3,
    )
    engineering_quality = round(
        (intent_score * 0.35) + (tool_score * 0.45) + (safety_score * 0.20),
        3,
    )
    business_completeness = round(
        (grounding_score * 0.35)
        + (follow_score * 0.25)
        + (intent_score * 0.15)
        + (business_scores["businessSynthesisScore"] * 0.25),
        3,
    )
    if missing_severity == "blocking":
        total = min(total, 0.74)
        business_completeness = min(business_completeness, 0.72)
    elif missing_severity == "weakens_answer":
        total = min(total, 0.88)
        business_completeness = min(business_completeness, 0.85)
    return {
        "intentScore": round(intent_score, 3),
        "toolScore": round(tool_score, 3),
        "groundingScore": round(grounding_score, 3),
        "followUpScore": round(follow_score, 3),
        "safetyScore": round(safety_score, 3),
        "engineeringQualityScore": engineering_quality,
        "businessCompletenessScore": business_completeness,
        "executiveConclusionScore": business_scores["executiveConclusionScore"],
        "businessImplicationScore": business_scores["businessImplicationScore"],
        "actionabilityScore": business_scores["actionabilityScore"],
        "evidenceAlignmentScore": business_scores["evidenceAlignmentScore"],
        "reportReadinessScore": business_scores["reportReadinessScore"],
        "businessSynthesisScore": business_scores["businessSynthesisScore"],
        "totalScore": total,
        "failures": failures,
    }


def _tool_score(must_tools: list[str], tools_used: list[str]) -> float:
    if not must_tools:
        return 1.0
    if not tools_used:
        return 0.0
    hits = sum(
        1
        for tool in must_tools
        if any(tool_satisfies_required(tool, used_tool) for used_tool in tools_used)
    )
    return hits / len(must_tools)


def _grounding_score(answer: dict[str, Any], evidence_package: dict[str, Any]) -> float:
    refs = evidence_ref_count(evidence_package)
    has_package = bool(evidence_package.get("evidenceId"))
    if not has_package:
        return 0.0
    direct = str(answer.get("direct") or "")
    bullets = " ".join(_string_list(answer.get("bullets")))
    contains_numbers = _NUMERIC_PATTERN.search(f"{direct} {bullets}") is not None
    answer_status = str(answer.get("answerStatus") or "")
    if contains_numbers and refs == 0 and answer_status != "insufficient_evidence":
        return 0.0
    if refs > 0:
        return 1.0
    return 0.5 if answer_status == "insufficient_evidence" else 0.0


def _missing_evidence_severity(evidence_package: dict[str, Any]) -> str:
    missing = evidence_package.get("missingEvidence")
    if not isinstance(missing, list) or not missing:
        return ""
    severities: set[str] = set()
    for item in missing:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "")
        impact = str(item.get("impact") or "")
        if impact == "blocking" or name.startswith("missing_required_tool:"):
            severities.add("blocking")
        elif impact == "weakens_answer":
            severities.add("weakens_answer")
    if "blocking" in severities:
        return "blocking"
    if "weakens_answer" in severities:
        return "weakens_answer"
    return ""


def _follow_up_score(expected: dict[str, Any], follow_ups: list[dict[str, Any]]) -> float:
    if not follow_ups:
        return 0.0
    count_score = 1.0 if 3 <= len(follow_ups) <= 4 else 0.5
    expected_types = _string_list(expected.get("expectedFollowUpTypes"))
    if not expected_types:
        return count_score
    actual_types = {str(item.get("intent") or "") for item in follow_ups if isinstance(item, dict)}
    required = expected_types[:2]
    hits = sum(1 for item in required if item in actual_types)
    type_score = hits / len(required) if required else 1.0
    return (count_score + type_score) / 2


def _business_synthesis_scores(answer: dict[str, Any], evidence_package: dict[str, Any]) -> dict[str, float]:
    synthesis = answer.get("businessSynthesisPlan") if isinstance(answer.get("businessSynthesisPlan"), dict) else {}
    direct = str(answer.get("direct") or "").strip()
    bullets = " ".join(_string_list(answer.get("bullets")))
    implications = answer.get("businessImplications")
    if not isinstance(implications, list):
        implications = synthesis.get("businessImplications") if isinstance(synthesis.get("businessImplications"), list) else []
    actions = answer.get("recommendedActions")
    if not isinstance(actions, list):
        actions = synthesis.get("recommendedActions") if isinstance(synthesis.get("recommendedActions"), list) else []
    report_bullets = answer.get("reportReadyBullets")
    if not isinstance(report_bullets, list):
        report_bullets = synthesis.get("reportReadyBullets") if isinstance(synthesis.get("reportReadyBullets"), list) else []
    alignment = synthesis.get("evidenceAlignment") if isinstance(synthesis.get("evidenceAlignment"), dict) else {}
    alignment_status = str(alignment.get("status") or "")
    intent = str(synthesis.get("intent") or evidence_package.get("intent") or "")

    executive = str(synthesis.get("executiveConclusion") or direct)
    executive_score = 1.0 if "直接结论" in executive or len(executive) >= 80 else 0.5 if executive else 0.0
    implication_score = 1.0 if len(implications) >= 2 else 0.7 if implications or "业务" in bullets else 0.0
    action_score = 1.0 if len(actions) >= 2 or _has_clear_p0_action(actions) else 0.7 if actions or "动作" in bullets or "建议" in bullets else 0.0
    if alignment_status in {"aligned", "partially_aligned", "conflicting", "insufficient"}:
        alignment_score = 1.0
    elif evidence_package.get("jatoCrossCheck") or evidence_package.get("researchGovernance"):
        alignment_score = 0.7
    else:
        alignment_score = 0.0
    report_score = 1.0 if len(report_bullets) >= 3 else 0.5 if report_bullets else 0.0
    if intent == "report_generation" and _is_template_only_report_answer(executive, report_bullets):
        executive_score = min(executive_score, 0.6)
        implication_score = min(implication_score, 0.7)
        report_score = min(report_score, 0.4)
    synthesis_score = round(
        (executive_score * 0.22)
        + (implication_score * 0.22)
        + (action_score * 0.24)
        + (alignment_score * 0.16)
        + (report_score * 0.16),
        3,
    )
    return {
        "executiveConclusionScore": round(executive_score, 3),
        "businessImplicationScore": round(implication_score, 3),
        "actionabilityScore": round(action_score, 3),
        "evidenceAlignmentScore": round(alignment_score, 3),
        "reportReadinessScore": round(report_score, 3),
        "businessSynthesisScore": synthesis_score,
    }


def _is_template_only_report_answer(executive: str, report_bullets: list[Any]) -> bool:
    report_text = " ".join(str(item or "") for item in report_bullets)
    combined = f"{executive} {report_text}".lower()
    template_markers = (
        "title / key message / evidence / product implication / next action",
        "汇报页应压成 title",
        "这页汇报应收敛为 title",
    )
    if not any(marker in combined for marker in template_markers):
        return False
    specific_markers = (
        "bev 渗透",
        "渗透率",
        "核心竞争带",
        "高配主推",
        "低配做价格锚点",
        "竞品池",
        "价格走廊",
        "配置价值",
        "公司车",
        "winter",
        "续航",
        "充电",
        "政策",
        "价格门槛",
        "库存",
        "bom",
        "voc",
    )
    return not any(marker in report_text.lower() for marker in specific_markers)


def _has_clear_p0_action(actions: list[Any]) -> bool:
    for item in actions:
        if not isinstance(item, dict):
            continue
        action = str(item.get("action") or "").strip()
        if not action:
            continue
        priority = str(item.get("priority") or "").strip().upper()
        rationale = str(item.get("rationale") or "").strip()
        refs = item.get("evidenceRefs")
        has_refs = isinstance(refs, list) and any(str(ref or "").strip() for ref in refs)
        if priority == "P0" or rationale or has_refs:
            return True
    return False


def _has_business_contract(answer: dict[str, Any]) -> bool:
    return any(
        isinstance(answer.get(key), dict if key == "businessSynthesisPlan" else list)
        and bool(answer.get(key))
        for key in ("businessSynthesisPlan", "recommendedActions", "reportReadyBullets", "businessImplications")
    )


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item or "").strip()]
