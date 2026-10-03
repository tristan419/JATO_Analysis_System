from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.request import Request, urlopen

from app.core.config import ASTRBOT_JUDGE_API_BASE
from app.core.config import ASTRBOT_JUDGE_KEY_ENV
from app.core.config import ASTRBOT_JUDGE_MODEL
from app.core.config import ASTRBOT_JUDGE_PROVIDER_ID
from app.services.jato_agent_provider_service import parse_agent_answer_content


_REFERENCE_PATH_SPECS = [
    {
        "id": "gpt5_5",
        "label": "GPT5.5 / GPT Judge",
        "role": "Scalable Judge / Teacher Loop for side-by-side business scoring.",
        "evidence": (
            "Implemented through the active side-by-side judge provider config: "
            "APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED, APP_ASTRBOT_JUDGE_MODEL, "
            "APP_ASTRBOT_JUDGE_API_BASE, and APP_ASTRBOT_JUDGE_KEY_ENV."
        ),
        "providerEnv": "APP_ASTRBOT_JUDGE_PROVIDER_ID",
        "modelEnv": "APP_ASTRBOT_JUDGE_MODEL",
        "apiBaseEnv": "APP_ASTRBOT_JUDGE_API_BASE",
        "keyEnvVar": "APP_ASTRBOT_JUDGE_KEY_ENV",
        "defaultProvider": ASTRBOT_JUDGE_PROVIDER_ID,
        "defaultModel": ASTRBOT_JUDGE_MODEL,
        "defaultApiBase": ASTRBOT_JUDGE_API_BASE,
        "defaultKeySource": ASTRBOT_JUDGE_KEY_ENV,
        "implemented": True,
        "nextAction": "Configure a valid judge key, run a 2-record smoke, then generate a 30-record GPT judged baseline.",
    },
    {
        "id": "opus_4_8",
        "label": "Opus 4.8",
        "role": "External implementation path requested by user; can be mapped into the judge-provider interface once a concrete spec is supplied.",
        "evidence": "No local repo/Codex attachment spec is known yet; optional env hooks are reserved for this path.",
        "providerEnv": "APP_ASTRBOT_OPUS48_JUDGE_PROVIDER_ID",
        "modelEnv": "APP_ASTRBOT_OPUS48_JUDGE_MODEL",
        "apiBaseEnv": "APP_ASTRBOT_OPUS48_JUDGE_API_BASE",
        "keyEnvVar": "APP_ASTRBOT_OPUS48_JUDGE_KEY_ENV",
        "defaultProvider": "",
        "defaultModel": "",
        "defaultApiBase": "",
        "defaultKeySource": "",
        "implemented": False,
        "nextAction": "Provide the Opus 4.8 path/spec or set the reserved env hooks before claiming alignment.",
    },
    {
        "id": "fable_5",
        "label": "Fable 5",
        "role": "External implementation path requested by user; can be mapped into the judge-provider interface once a concrete spec is supplied.",
        "evidence": "No local repo/Codex attachment spec is known yet; optional env hooks are reserved for this path.",
        "providerEnv": "APP_ASTRBOT_FABLE5_JUDGE_PROVIDER_ID",
        "modelEnv": "APP_ASTRBOT_FABLE5_JUDGE_MODEL",
        "apiBaseEnv": "APP_ASTRBOT_FABLE5_JUDGE_API_BASE",
        "keyEnvVar": "APP_ASTRBOT_FABLE5_JUDGE_KEY_ENV",
        "defaultProvider": "",
        "defaultModel": "",
        "defaultApiBase": "",
        "defaultKeySource": "",
        "implemented": False,
        "nextAction": "Provide the Fable 5 path/spec or set the reserved env hooks before claiming alignment.",
    },
]


def preflight_judge_provider(*, live_check: bool = True) -> dict[str, Any]:
    """Check whether the side-by-side GPT judge can run before baseline evals.

    This is deliberately separate from Business Validation execution. It never
    writes eval records; it only validates enablement, key presence, and
    optionally a tiny live chat-completions request.
    """
    enabled = _side_by_side_enabled()
    api_key = _judge_api_key()
    missing_key = not bool(api_key)
    base = {
        "ready": False,
        "enabled": enabled,
        "missingKey": missing_key,
        "missing_key": missing_key,
        "liveCheck": live_check,
        "provider": _provider_metadata(),
        "referenceJudgePaths": list_reference_judge_paths(),
    }
    if not enabled:
        return {
            **base,
            "status": "disabled",
            "reason": "APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED is not enabled",
        }
    if missing_key:
        return {
            **base,
            "status": "missing_key",
            "reason": f"{ASTRBOT_JUDGE_KEY_ENV} is not configured",
        }
    if not live_check:
        return {
            **base,
            "ready": True,
            "status": "ready",
            "reason": "Judge provider is enabled and key is configured; live check skipped.",
        }
    messages = [
        {
            "role": "system",
            "content": "You are a judge provider preflight. Return only JSON.",
        },
        {
            "role": "user",
            "content": '{"task":"preflight","output":{"ok":true}}',
        },
    ]
    try:
        content = _post_chat(messages, api_key, max_tokens=40)
    except Exception as exc:  # noqa: BLE001
        return {
            **base,
            "status": "failed",
            "reason": str(exc),
        }
    return {
        **base,
        "ready": True,
        "status": "ok",
        "reason": "Judge provider live check succeeded.",
        "responsePreview": str(content or "")[:120],
    }


def list_reference_judge_paths() -> dict[str, Any]:
    """Return auditable judge/reference paths without exposing secret values.

    The active GPT judge path is the only implemented execution path today.
    Opus/Fable are tracked as reserved provider slots so reports can distinguish
    "not configured yet" from "not possible to integrate".
    """
    active_provider = _provider_metadata()
    active_id = _active_reference_path_id(active_provider)
    return {
        "source": "jato_agent_llm_judge_service",
        "activePathId": active_id,
        "activeProvider": active_provider,
        "paths": [
            _reference_path_payload(spec, active_path_id=active_id)
            for spec in _REFERENCE_PATH_SPECS
        ],
    }


def judge_answer_with_llm(
    *,
    question: str,
    answer: dict[str, Any],
    evidence_package: dict[str, Any],
    follow_ups: list[dict[str, Any]],
) -> dict[str, Any]:
    if not _enabled("APP_ASTRBOT_LLM_JUDGE_ENABLED"):
        return {"status": "disabled", "scores": {}, "reason": "APP_ASTRBOT_LLM_JUDGE_ENABLED is not enabled"}
    api_key = _judge_api_key()
    if not api_key:
        return {"status": "missing_key", "scores": {}, "reason": f"{ASTRBOT_JUDGE_KEY_ENV} is not configured"}
    prompt = {
        "task": "judge_automotive_market_agent_answer",
        "question": question,
        "answer": _shrink(answer),
        "evidencePackage": _shrink(evidence_package),
        "followUps": _shrink(follow_ups),
        "outputContract": {
            "answeredQuestion": "1-5",
            "usedNecessaryEvidence": "1-5",
            "unsupportedNumbers": "1-5 where 5 means no unsupported numbers",
            "productManagerPerspective": "1-5",
            "followUpBusinessValue": "1-5",
            "overall": "1-5",
        },
    }
    messages = [
        {
            "role": "system",
            "content": (
                "你是汽车产品分析 Agent 的评估器。"
                "只输出 JSON，不要输出解释性长文。"
                "从 1-5 分评价回答是否回答用户问题、是否使用必要证据、是否存在无证据数字、"
                "是否有产品经理视角、follow-up 是否能推动下一步业务分析。"
            ),
        },
        {"role": "user", "content": json.dumps(prompt, ensure_ascii=False, separators=(",", ":"))},
    ]
    try:
        content = _post_chat(messages, api_key, max_tokens=500)
        parsed = parse_agent_answer_content(content)
    except Exception as exc:
        return {"status": "failed", "scores": {}, "reason": str(exc), "provider": _provider_metadata()}
    return {"status": "ok", "scores": parsed, "provider": _provider_metadata()}


def judge_side_by_side_with_llm(
    *,
    record: dict[str, Any],
    score_dimensions: list[str],
    failure_taxonomy: list[str],
) -> dict[str, Any]:
    """Use a separate GPT judge to replace repetitive manual side-by-side scoring.

    The judge provider is intentionally independent from the AstrBot answer
    provider, so DPV4/DeepSeek can keep composing answers while OpenAI/GPT acts
    as a stricter product-review teacher.
    """
    if not _side_by_side_enabled():
        return {
            "status": "disabled",
            "scores": {},
            "reason": "APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED is not enabled",
            "provider": _provider_metadata(),
        }
    api_key = _judge_api_key()
    if not api_key:
        return {
            "status": "missing_key",
            "scores": {},
            "reason": f"{ASTRBOT_JUDGE_KEY_ENV} is not configured",
            "provider": _provider_metadata(),
        }
    prompt = _build_side_by_side_prompt(record, score_dimensions, failure_taxonomy)
    messages = [
        {
            "role": "system",
            "content": (
                "你是严苛的汽车产品经理和汽车市场分析 Agent 评审员。"
                "你的任务是比较 AstrBot 与 CountryCopilot 对同一个业务问题的回答。"
                "不要偏袒 AstrBot；谁更可信、更能推进业务动作，谁就赢。"
                "必须按给定 8 个维度给 1-5 的整数分。"
                "分数 5 代表该维度非常强，1 代表不可用。"
                "只输出一个 JSON 对象，不要输出解释性长文。"
            ),
        },
        {"role": "user", "content": json.dumps(prompt, ensure_ascii=False, separators=(",", ":"))},
    ]
    try:
        content = _post_chat(messages, api_key, max_tokens=1200)
        parsed = parse_agent_answer_content(content)
        scores = _normalize_side_by_side_scores(parsed, score_dimensions, failure_taxonomy)
        scores = _apply_side_by_side_rubric_caps(
            scores,
            record=record,
            score_dimensions=score_dimensions,
            failure_taxonomy=failure_taxonomy,
        )
    except Exception as exc:
        return {"status": "failed", "scores": {}, "reason": str(exc), "provider": _provider_metadata()}
    return {
        "status": "ok",
        "scores": scores,
        "provider": _provider_metadata(),
        "raw": _shrink(parsed),
    }


def _build_side_by_side_prompt(
    record: dict[str, Any],
    score_dimensions: list[str],
    failure_taxonomy: list[str],
) -> dict[str, Any]:
    astrbot = record.get("astrbot") if isinstance(record.get("astrbot"), dict) else {}
    copilot = record.get("countryCopilot") if isinstance(record.get("countryCopilot"), dict) else {}
    return {
        "task": "side_by_side_business_validation_judge",
        "question": {
            "id": record.get("questionId"),
            "category": record.get("category"),
            "country": record.get("country"),
            "text": record.get("question"),
            "expectedIntent": record.get("expectedIntent"),
            "expectedTools": record.get("expectedTools", []),
            "expectedFollowUpTypes": record.get("expectedFollowUpTypes", []),
        },
        "scoreDimensions": score_dimensions,
        "failureTaxonomy": failure_taxonomy,
        "astrbot": {
            "answer": astrbot.get("answerPreview", ""),
            "bullets": astrbot.get("bullets", []),
            "limitations": astrbot.get("limitations", []),
            "answerStatus": astrbot.get("answerStatus"),
            "selectedTool": astrbot.get("selectedTool"),
            "evidenceRefCount": astrbot.get("evidenceRefCount"),
            "qualityScore": _shrink(astrbot.get("qualityScore", {})),
            "evidencePackage": _shrink(astrbot.get("evidencePackage", {})),
            "visualArtifacts": _shrink(astrbot.get("visualArtifacts", [])),
            "followUps": _shrink(astrbot.get("followUps", [])),
            "businessPlaybook": _shrink(record.get("businessPlaybook", {})),
        },
        "countryCopilot": {
            "answer": copilot.get("answerPreview", ""),
            "answerMode": copilot.get("answerMode"),
            "intentRoute": copilot.get("intentRoute"),
            "confidence": copilot.get("confidence"),
            "sourceCount": copilot.get("sourceCount"),
            "chartLinkCount": copilot.get("chartLinkCount"),
        },
        "outputContract": {
            "astrbotScores": {key: "integer 1-5" for key in score_dimensions},
            "countryCopilotScores": {key: "integer 1-5" for key in score_dimensions},
            "winner": "astrbot|countryCopilot|tie|unclear",
            "notes": "one concise Chinese paragraph explaining why",
            "failureTags": failure_taxonomy,
            "confidence": "low|medium|high",
        },
        "scoringGuidance": [
            "grounding 看数字和结论是否有证据，不是看回答是否更长。",
            "pmInsight 看是否有产品经理视角、场景拆解和定位判断。",
            "actionability 看是否能转成价格、配置、竞品、报告或查数动作。",
            "artifactQuality 看图表/表格/report block 是否帮助业务判断。",
            "presentationReadiness 看是否接近可复制进汇报材料。",
            "如果 AstrBot 过度保守、只说证据不足，应降低 actionability/pmInsight。",
            "如果 CountryCopilot 更完整、更像业务报告，可以让 CountryCopilot 胜出。",
            "硬性扣分：没有 evidenceRef 或来源支撑的确定数字，grounding 最高 2 分。",
            "硬性扣分：只复述数据、没有定位/场景/竞品/动作判断，pmInsight 最高 3 分。",
            "硬性扣分：没有下一步动作或数据补齐建议，actionability 最高 3 分。",
            "硬性扣分：问题需要图表、表格、矩阵或汇报结构但没有有效 artifact，artifactQuality 最高 3 分。",
            "硬性扣分：follow-up 泛泛而谈或不存在，followUpValue 最高 3 分。",
            "硬性扣分：出现 hallucination_risk 时，winner 仍可判断，但 replacement readiness 不能判 ready。",
        ],
    }


def _normalize_side_by_side_scores(
    parsed: dict[str, Any],
    score_dimensions: list[str],
    failure_taxonomy: list[str],
) -> dict[str, Any]:
    astrbot_scores = _normalize_score_map(parsed.get("astrbotScores"), score_dimensions)
    copilot_scores = _normalize_score_map(
        parsed.get("countryCopilotScores") or parsed.get("copilotScores"),
        score_dimensions,
    )
    winner = str(parsed.get("winner") or "").strip()
    if winner not in {"astrbot", "countryCopilot", "tie", "unclear"}:
        winner = _winner_from_scores(astrbot_scores, copilot_scores, score_dimensions)
    return {
        "astrbotScores": astrbot_scores,
        "countryCopilotScores": copilot_scores,
        "winner": winner,
        "notes": str(parsed.get("notes") or parsed.get("reason") or "")[:1600],
        "failureTags": [
            tag
            for tag in _string_list(parsed.get("failureTags"))
            if tag in set(failure_taxonomy)
        ],
        "confidence": str(parsed.get("confidence") or "medium").strip() or "medium",
    }


def _normalize_score_map(value: Any, dimensions: list[str]) -> dict[str, int]:
    raw = value if isinstance(value, dict) else {}
    result: dict[str, int] = {}
    for key in dimensions:
        try:
            numeric = int(round(float(raw.get(key))))
        except (TypeError, ValueError):
            continue
        if 1 <= numeric <= 5:
            result[key] = numeric
    return result


def _apply_side_by_side_rubric_caps(
    scores: dict[str, Any],
    *,
    record: dict[str, Any],
    score_dimensions: list[str],
    failure_taxonomy: list[str],
) -> dict[str, Any]:
    normalized = {
        **scores,
        "astrbotScores": dict(scores.get("astrbotScores") if isinstance(scores.get("astrbotScores"), dict) else {}),
        "countryCopilotScores": dict(scores.get("countryCopilotScores") if isinstance(scores.get("countryCopilotScores"), dict) else {}),
    }
    tags = [
        tag
        for tag in _string_list(scores.get("failureTags"))
        if tag in set(failure_taxonomy)
    ]
    _cap_side_scores(
        normalized["astrbotScores"],
        side="astrbot",
        record=record,
        tags=tags,
        score_dimensions=score_dimensions,
        failure_taxonomy=failure_taxonomy,
    )
    _cap_side_scores(
        normalized["countryCopilotScores"],
        side="countryCopilot",
        record=record,
        tags=tags,
        score_dimensions=score_dimensions,
        failure_taxonomy=failure_taxonomy,
    )
    normalized["failureTags"] = _dedupe_tags(tags, failure_taxonomy)
    return normalized


def _cap_side_scores(
    scores: dict[str, int],
    *,
    side: str,
    record: dict[str, Any],
    tags: list[str],
    score_dimensions: list[str],
    failure_taxonomy: list[str],
) -> None:
    side_data = record.get("astrbot" if side == "astrbot" else "countryCopilot")
    if not isinstance(side_data, dict):
        side_data = {}
    answer = str(side_data.get("answerPreview") or "")
    question = str(record.get("question") or "")

    evidence_refs = int(side_data.get("evidenceRefCount") or 0) if side == "astrbot" else int(side_data.get("sourceCount") or 0)
    if "grounding" in score_dimensions and evidence_refs == 0 and _has_definite_number(answer):
        _cap_score(scores, "grounding", 2)
        _append_tag(tags, "hallucination_risk", failure_taxonomy)
        _append_tag(tags, "evidence_missing", failure_taxonomy)

    if "pmInsight" in score_dimensions and _looks_like_data_restatement(answer):
        _cap_score(scores, "pmInsight", 3)
        _append_tag(tags, "pm_insight_weak", failure_taxonomy)

    if "actionability" in score_dimensions and not _has_action_recommendation(answer):
        _cap_score(scores, "actionability", 3)
        _append_tag(tags, "answer_too_conservative", failure_taxonomy)

    artifact_count = _side_artifact_count(side_data, side=side)
    if "artifactQuality" in score_dimensions and _question_needs_artifact(question, record) and artifact_count == 0:
        _cap_score(scores, "artifactQuality", 3)
        _append_tag(tags, "chart_not_useful", failure_taxonomy)

    if "followUpValue" in score_dimensions and _side_has_weak_followups(side_data, side=side):
        _cap_score(scores, "followUpValue", 3)
        _append_tag(tags, "followup_low_value", failure_taxonomy)


def _cap_score(scores: dict[str, int], key: str, cap: int) -> None:
    current = scores.get(key)
    if isinstance(current, int) and current > cap:
        scores[key] = cap


def _append_tag(tags: list[str], tag: str, taxonomy: list[str]) -> None:
    if tag in taxonomy and tag not in tags:
        tags.append(tag)


def _dedupe_tags(tags: list[str], taxonomy: list[str]) -> list[str]:
    result: list[str] = []
    allowed = set(taxonomy)
    for tag in tags:
        if tag in allowed and tag not in result:
            result.append(tag)
    return result


def _has_definite_number(text: str) -> bool:
    value = str(text or "")
    return bool(re.search(r"(?<![A-Za-z])\d+(?:[.,]\d+)?\s*(?:%|台|辆|SEK|EUR|欧元|万|k|公里|km|kWh|个月|月供|份额|销量)", value))


def _looks_like_data_restatement(text: str) -> bool:
    value = str(text or "")
    if len(value) < 120:
        return True
    insight_markers = ("因此", "意味着", "建议", "定位", "场景", "动作", "策略", "话术", "主推", "机会", "风险", "next", "recommend")
    data_markers = ("数据", "销量", "份额", "市场", "占比", "refs", "evidence")
    return any(marker in value for marker in data_markers) and not any(marker in value for marker in insight_markers)


def _has_action_recommendation(text: str) -> bool:
    value = str(text or "")
    markers = ("建议", "下一步", "动作", "应该", "需要", "主推", "补齐", "生成", "验证", "recommend", "next")
    return any(marker in value for marker in markers)


def _question_needs_artifact(question: str, record: dict[str, Any]) -> bool:
    value = f"{question} {record.get('category') or ''}".lower()
    return any(token in value for token in ("chart", "table", "matrix", "report", "图", "图表", "表", "矩阵", "汇报", "报告", "对比"))


def _side_artifact_count(side_data: dict[str, Any], *, side: str) -> int:
    if side == "astrbot":
        artifacts = side_data.get("visualArtifacts")
        return len(artifacts) if isinstance(artifacts, list) else 0
    return int(side_data.get("chartLinkCount") or side_data.get("evidenceTableCount") or 0)


def _side_has_weak_followups(side_data: dict[str, Any], *, side: str) -> bool:
    if side == "astrbot":
        follow_ups = side_data.get("followUps")
        if not isinstance(follow_ups, list) or len(follow_ups) == 0:
            return True
        labels = " ".join(str(item.get("label") or item.get("question") or "") if isinstance(item, dict) else str(item) for item in follow_ups)
        generic_markers = ("继续", "了解更多", "还有什么", "更多信息")
        return len(labels.strip()) < 24 or any(marker == labels.strip() for marker in generic_markers)
    answer = str(side_data.get("answerPreview") or "")
    return "？" not in answer and "?" not in answer and "下一步" not in answer


def _winner_from_scores(
    astrbot_scores: dict[str, int],
    copilot_scores: dict[str, int],
    dimensions: list[str],
) -> str:
    if not dimensions:
        return "unclear"
    if any(key not in astrbot_scores or key not in copilot_scores for key in dimensions):
        return "unclear"
    astrbot_avg = sum(astrbot_scores[key] for key in dimensions) / len(dimensions)
    copilot_avg = sum(copilot_scores[key] for key in dimensions) / len(dimensions)
    if abs(astrbot_avg - copilot_avg) < 0.01:
        return "tie"
    return "astrbot" if astrbot_avg > copilot_avg else "countryCopilot"


def _enabled(name: str) -> bool:
    return os.getenv(name, "false").strip().lower() in {"1", "true", "yes", "on"}


def _side_by_side_enabled() -> bool:
    return _enabled("APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED") or _enabled("APP_ASTRBOT_AUTO_HUMAN_JUDGE_ENABLED")


def _judge_api_key() -> str:
    return os.getenv(ASTRBOT_JUDGE_KEY_ENV, "").strip()


def _provider_metadata() -> dict[str, Any]:
    return {
        "provider": ASTRBOT_JUDGE_PROVIDER_ID,
        "model": ASTRBOT_JUDGE_MODEL,
        "apiBase": ASTRBOT_JUDGE_API_BASE,
        "keySource": ASTRBOT_JUDGE_KEY_ENV,
    }


def _active_reference_path_id(active_provider: dict[str, Any]) -> str:
    active_model = str(active_provider.get("model") or "").strip().lower()
    active_base = str(active_provider.get("apiBase") or "").strip().rstrip("/").lower()
    active_key = str(active_provider.get("keySource") or "").strip()
    for spec in _REFERENCE_PATH_SPECS:
        model = _reference_env_value(spec["modelEnv"], spec["defaultModel"]).lower()
        api_base = _reference_env_value(spec["apiBaseEnv"], spec["defaultApiBase"]).rstrip("/").lower()
        key_source = _reference_env_value(spec["keyEnvVar"], spec["defaultKeySource"])
        if model and model == active_model:
            return str(spec["id"])
        if api_base and key_source and api_base == active_base and key_source == active_key:
            return str(spec["id"])
    return "custom"


def _reference_path_payload(spec: dict[str, Any], *, active_path_id: str) -> dict[str, Any]:
    provider = _reference_env_value(spec["providerEnv"], spec["defaultProvider"])
    model = _reference_env_value(spec["modelEnv"], spec["defaultModel"])
    api_base = _reference_env_value(spec["apiBaseEnv"], spec["defaultApiBase"]).rstrip("/")
    key_source = _reference_env_value(spec["keyEnvVar"], spec["defaultKeySource"])
    active = str(spec["id"]) == active_path_id
    key_configured = bool(key_source and os.getenv(key_source, "").strip())
    env_configured = any(
        bool(os.getenv(str(spec[key]), "").strip())
        for key in ("providerEnv", "modelEnv", "apiBaseEnv", "keyEnvVar")
    )
    implemented = bool(spec.get("implemented"))
    if implemented:
        status = "implemented"
    elif env_configured:
        status = "configured"
    else:
        status = "reference_missing"
    if active and _side_by_side_enabled() and key_configured:
        readiness = "ready"
    elif active and not _side_by_side_enabled():
        readiness = "disabled"
    elif active and not key_configured:
        readiness = "missing_key"
    elif key_source and key_configured:
        readiness = "configured_inactive"
    else:
        readiness = "not_configured"
    return {
        "id": spec["id"],
        "label": spec["label"],
        "status": status,
        "readinessStatus": readiness,
        "active": active,
        "implemented": implemented,
        "role": spec["role"],
        "evidence": spec["evidence"],
        "provider": provider,
        "model": model,
        "apiBase": api_base,
        "keySource": key_source,
        "keyConfigured": key_configured,
        "env": {
            "provider": spec["providerEnv"],
            "model": spec["modelEnv"],
            "apiBase": spec["apiBaseEnv"],
            "keySource": spec["keyEnvVar"],
        },
        "nextAction": spec["nextAction"],
    }


def _reference_env_value(env_name: str, default: str) -> str:
    return str(os.getenv(env_name, default) or "").strip()


def _post_chat(messages: list[dict[str, str]], api_key: str, *, max_tokens: int) -> str:
    payload = {
        "model": ASTRBOT_JUDGE_MODEL,
        "messages": messages,
        "temperature": 0.0,
        "max_tokens": max_tokens,
        "stream": False,
    }
    request = Request(
        _chat_completions_url(),
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    with urlopen(request, timeout=35) as response:
        body = json.loads(response.read().decode("utf-8"))
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    message = choices[0].get("message") if isinstance(choices[0], dict) else {}
    return str(message.get("content") or "") if isinstance(message, dict) else ""


def _chat_completions_url() -> str:
    base = ASTRBOT_JUDGE_API_BASE.rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    return f"{base}/chat/completions"


def _shrink(value: Any, *, depth: int = 0) -> Any:
    if depth >= 3:
        return str(value)[:300]
    if isinstance(value, dict):
        return {str(key): _shrink(item, depth=depth + 1) for key, item in list(value.items())[:12]}
    if isinstance(value, list):
        return [_shrink(item, depth=depth + 1) for item in value[:8]]
    if isinstance(value, str):
        return value[:1000]
    return value


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item or "").strip()]
