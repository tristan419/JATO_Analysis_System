"""LLM composer for engineering config business summaries.

The parser and compare layers own facts. This service only asks Config's
optional summary provider to turn already-structured facts into business text.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import subprocess
import tempfile
from collections import OrderedDict
from typing import Any
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.request import Request
from urllib.request import urlopen

from app.core.config import ENGINEERING_CONFIG_SUMMARY_PROVIDER_API_BASE
from app.core.config import ENGINEERING_CONFIG_SUMMARY_PROVIDER_ID
from app.core.config import ENGINEERING_CONFIG_SUMMARY_PROVIDER_KEY_ENV
from app.core.config import ENGINEERING_CONFIG_SUMMARY_PROVIDER_MODEL
from app.core.config import ENGINEERING_CONFIG_SUMMARY_RUNTIME_URL
SUMMARY_CACHE_LIMIT = 64
DEFAULT_FINAL_COMPOSER_TIMEOUT_SECONDS = 25
DEFAULT_FINAL_COMPOSER_MAX_TOKENS = 1200
_SUMMARY_CACHE: OrderedDict[str, dict[str, Any]] = OrderedDict()
_COMPACT_LIST_LIMITS = {
    "targets": 6,
    "evidenceFacts": 40,
    "categoryFacts": 12,
    "businessFocusGroups": 8,
    "upgradeSignals": 8,
    "addedFeatures": 16,
    "removedFeatures": 16,
    "changedFeatures": 16,
    "sourceSheetNames": 8,
}


def compose_engineering_config_business_summary(payload: dict[str, Any]) -> dict[str, Any]:
    """Compose business-readable trim summaries from deterministic facts."""
    api_key = os.getenv(ENGINEERING_CONFIG_SUMMARY_PROVIDER_KEY_ENV, "").strip()
    if not api_key:
        return _fallback_response(
            "missing_key",
            f"{ENGINEERING_CONFIG_SUMMARY_PROVIDER_KEY_ENV} is not configured",
        )

    payload_without_cache_control, force_refresh = _payload_without_cache_control(payload)
    prompt_payload = _compact_payload(payload_without_cache_control)
    cache_key = _business_summary_cache_key(prompt_payload)
    if not force_refresh:
        cached_response = _cached_summary_response(cache_key)
        if cached_response is not None:
            return cached_response

    try:
        provider_payload = _compose_provider_payload(api_key, prompt_payload)
    except Exception as exc:
        return _fallback_response("failed", str(exc), prompt_payload=prompt_payload)

    content = _choice_content(provider_payload)
    parsed = _parse_summary_json(content) or {}
    raw_summaries = (
        parsed.get("summaries")
        or parsed.get("businessSummaries")
        or parsed.get("items")
        or parsed.get("summaryItems")
    )
    if raw_summaries is None and _looks_like_summary_item(parsed):
        raw_summaries = [parsed]
    summaries = _sanitize_summaries(raw_summaries, prompt_payload.get("targets"))
    retry_payload = _missing_target_retry_payload(prompt_payload, summaries)
    retry_provider_payloads: list[dict[str, Any]] = []
    if retry_payload:
        try:
            retry_provider_payload = _compose_provider_payload(api_key, retry_payload)
        except Exception:
            retry_provider_payload = None
        if retry_provider_payload:
            retry_provider_payloads.append(retry_provider_payload)
            retry_content = _choice_content(retry_provider_payload)
            retry_parsed = _parse_summary_json(retry_content) or {}
            retry_raw_summaries = (
                retry_parsed.get("summaries")
                or retry_parsed.get("businessSummaries")
                or retry_parsed.get("items")
                or retry_parsed.get("summaryItems")
            )
            if retry_raw_summaries is None and _looks_like_summary_item(retry_parsed):
                retry_raw_summaries = [retry_parsed]
            retry_summaries = _sanitize_summaries(retry_raw_summaries, retry_payload.get("targets"))
            summaries = _merge_retry_summaries(summaries, retry_summaries)
    for single_retry_payload in _single_missing_target_retry_payloads(prompt_payload, summaries):
        try:
            single_provider_payload = _compose_single_target_provider_payload(api_key, single_retry_payload)
        except Exception:
            continue
        retry_provider_payloads.append(single_provider_payload)
        single_content = _choice_content(single_provider_payload)
        single_parsed = _parse_summary_json(single_content) or {}
        single_raw_summaries = (
            single_parsed.get("summaries")
            or single_parsed.get("businessSummaries")
            or single_parsed.get("items")
            or single_parsed.get("summaryItems")
        )
        if single_raw_summaries is None and _looks_like_summary_item(single_parsed):
            single_raw_summaries = [single_parsed]
        single_summaries = _sanitize_summaries(single_raw_summaries, single_retry_payload.get("targets"))
        summaries = _merge_retry_summaries(summaries, single_summaries)
    usage = _read_usage(provider_payload)
    for retry_provider_payload in retry_provider_payloads:
        usage = _merge_usage(usage, _read_usage(retry_provider_payload))
    response = {
        "summaries": summaries,
        "usage": {
            "provider": ENGINEERING_CONFIG_SUMMARY_PROVIDER_ID,
            "model": ENGINEERING_CONFIG_SUMMARY_PROVIDER_MODEL,
            "status": "ok",
            "promptTokens": usage["promptTokens"],
            "completionTokens": usage["completionTokens"],
            "totalTokens": usage["totalTokens"],
            "promptCacheHitTokens": usage["promptCacheHitTokens"],
            "promptCacheMissTokens": usage["promptCacheMissTokens"],
            "finishReason": _choice_finish_reason(provider_payload),
            "estimated": False,
        },
    }
    transport_fallback = _read_text(provider_payload.get("_transportFallback"))
    if transport_fallback:
        response["usage"]["transportFallback"] = transport_fallback
    _remember_summary_response(cache_key, response)
    return response


def clear_engineering_config_business_summary_cache() -> None:
    _SUMMARY_CACHE.clear()


def get_engineering_config_business_summary_readiness() -> dict[str, Any]:
    """Return provider readiness without calling the LLM provider."""
    api_key_configured = bool(
        os.getenv(ENGINEERING_CONFIG_SUMMARY_PROVIDER_KEY_ENV, "").strip()
    )
    return {
        "ready": api_key_configured,
        "status": "ready" if api_key_configured else "missing_key",
        "provider": ENGINEERING_CONFIG_SUMMARY_PROVIDER_ID,
        "model": ENGINEERING_CONFIG_SUMMARY_PROVIDER_MODEL,
        "apiBase": ENGINEERING_CONFIG_SUMMARY_PROVIDER_API_BASE,
        "keySource": ENGINEERING_CONFIG_SUMMARY_PROVIDER_KEY_ENV,
        "providerConfigured": api_key_configured,
        "runtimeUrl": ENGINEERING_CONFIG_SUMMARY_RUNTIME_URL,
        "runtimeUsed": False,
        "runtimeStatus": "not_used_by_compare_runtime_compose",
        "liveCheck": "not_performed",
        "cacheSize": len(_SUMMARY_CACHE),
        "cacheLimit": SUMMARY_CACHE_LIMIT,
        "pipeline": "compare_runtime_compose",
        "persisted": False,
        "message": (
            "Engineering config AI summaries are composed from the current compare facts at runtime."
            if api_key_configured
            else (
                f"{ENGINEERING_CONFIG_SUMMARY_PROVIDER_KEY_ENV} is not configured; "
                "compare tables remain usable without AI summary."
            )
        ),
        "notes": [
            "Source Digest upload stores source files and extracted facts only.",
            "Business summaries are generated from the currently selected config columns.",
            "Cached summaries are reused for the same compare scope but are not persisted as digest artifacts.",
        ],
    }


def _payload_without_cache_control(payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    normalized = copy.deepcopy(payload)
    context = normalized.get("context")
    force_refresh = False
    if isinstance(context, dict):
        cache_control = context.pop("cacheControl", None)
        if isinstance(cache_control, dict):
            force_refresh = bool(cache_control.get("forceRefresh"))
        force_refresh = force_refresh or bool(context.pop("forceRefresh", False))
    return normalized, force_refresh


def _business_summary_cache_key(prompt_payload: dict[str, Any]) -> str:
    serialized = json.dumps(prompt_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    seed = (
        f"{ENGINEERING_CONFIG_SUMMARY_PROVIDER_ID}:"
        f"{ENGINEERING_CONFIG_SUMMARY_PROVIDER_MODEL}:{serialized}"
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _cached_summary_response(cache_key: str) -> dict[str, Any] | None:
    cached = _SUMMARY_CACHE.get(cache_key)
    if cached is None:
        return None
    _SUMMARY_CACHE.move_to_end(cache_key)
    response = copy.deepcopy(cached)
    usage = response.get("usage")
    if isinstance(usage, dict):
        usage["cacheHit"] = True
    return response


def _remember_summary_response(cache_key: str, response: dict[str, Any]) -> None:
    summaries = response.get("summaries")
    if not isinstance(summaries, list) or len(summaries) == 0:
        return
    _SUMMARY_CACHE[cache_key] = copy.deepcopy(response)
    _SUMMARY_CACHE.move_to_end(cache_key)
    while len(_SUMMARY_CACHE) > SUMMARY_CACHE_LIMIT:
        _SUMMARY_CACHE.popitem(last=False)


def _compose_provider_payload(api_key: str, prompt_payload: dict[str, Any]) -> dict[str, Any]:
    messages = [
        {"role": "system", "content": _system_prompt()},
        {"role": "user", "content": json.dumps(prompt_payload, ensure_ascii=False, separators=(",", ":"))},
    ]
    return _post_chat_completion_with_transport_fallback(
        api_key=api_key,
        model=ENGINEERING_CONFIG_SUMMARY_PROVIDER_MODEL,
        messages=messages,
        response_format={"type": "json_object"},
    )


def _compose_single_target_provider_payload(api_key: str, prompt_payload: dict[str, Any]) -> dict[str, Any]:
    messages = [
        {"role": "system", "content": _single_target_system_prompt()},
        {"role": "user", "content": json.dumps(prompt_payload, ensure_ascii=False, separators=(",", ":"))},
    ]
    return _post_chat_completion_with_transport_fallback(
        api_key=api_key,
        model=ENGINEERING_CONFIG_SUMMARY_PROVIDER_MODEL,
        messages=messages,
        response_format={"type": "json_object"},
    )


def _post_chat_completion_with_transport_fallback(
    *,
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    response_format: dict[str, str] | None = None,
) -> dict[str, Any]:
    try:
        return _post_chat_completion(
            api_key=api_key,
            model=model,
            messages=messages,
            response_format=response_format,
        )
    except Exception as primary_exc:
        try:
            payload = _post_chat_completion_with_curl(
                api_key=api_key,
                model=model,
                messages=messages,
                response_format=response_format,
            )
        except Exception as fallback_exc:
            primary_reason = _safe_error_summary(primary_exc, api_key=api_key)
            fallback_reason = _safe_error_summary(fallback_exc, api_key=api_key)
            raise RuntimeError(f"{primary_reason}; curl transport fallback failed: {fallback_reason}") from fallback_exc
        payload["_transportFallback"] = "curl"
        payload["_transportFallbackReason"] = _safe_error_summary(primary_exc, api_key=api_key)
        return payload


def _post_chat_completion(
    *,
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    response_format: dict[str, str] | None = None,
) -> dict[str, Any]:
    request_body: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": DEFAULT_FINAL_COMPOSER_MAX_TOKENS,
        "stream": False,
    }
    if response_format:
        request_body["response_format"] = response_format
    request = Request(
        _chat_completions_url(),
        data=json.dumps(request_body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=DEFAULT_FINAL_COMPOSER_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"provider HTTP {exc.code}") from exc
    except (URLError, TimeoutError, ValueError) as exc:
        raise RuntimeError(str(exc)) from exc
    if not isinstance(payload, dict):
        raise RuntimeError("provider response was not a JSON object")
    return payload


def _choice_content(payload: dict[str, Any]) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return ""
    message = choices[0].get("message")
    if not isinstance(message, dict):
        return ""
    return _read_text(message.get("content"))


def _choice_finish_reason(payload: dict[str, Any]) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return ""
    return _read_text(choices[0].get("finish_reason"))


def _read_usage(payload: dict[str, Any]) -> dict[str, int]:
    usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else {}
    prompt_tokens = _read_non_negative_int(usage.get("prompt_tokens"))
    completion_tokens = _read_non_negative_int(usage.get("completion_tokens"))
    return {
        "promptTokens": prompt_tokens,
        "completionTokens": completion_tokens,
        "totalTokens": _read_non_negative_int(usage.get("total_tokens")) or prompt_tokens + completion_tokens,
        "promptCacheHitTokens": _read_non_negative_int(usage.get("prompt_cache_hit_tokens")),
        "promptCacheMissTokens": _read_non_negative_int(usage.get("prompt_cache_miss_tokens")),
    }


def _post_chat_completion_with_curl(
    *,
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    response_format: dict[str, str] | None = None,
) -> dict[str, Any]:
    request_body: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": DEFAULT_FINAL_COMPOSER_MAX_TOKENS,
        "stream": False,
    }
    if response_format:
        request_body["response_format"] = response_format

    body_file_name = ""
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as body_file:
            body_file_name = body_file.name
            json.dump(request_body, body_file, ensure_ascii=False, separators=(",", ":"))
        curl_config = "\n".join([
            f'url = "{_curl_config_quote(_chat_completions_url())}"',
            'request = "POST"',
            'header = "Content-Type: application/json"',
            f'header = "Authorization: Bearer {_curl_config_quote(api_key)}"',
            f'data-binary = "@{_curl_config_quote(body_file_name)}"',
        ])
        completed = subprocess.run(
            [
                "curl",
                "--silent",
                "--show-error",
                "--fail-with-body",
                "--max-time",
                str(DEFAULT_FINAL_COMPOSER_TIMEOUT_SECONDS),
                "--config",
                "-",
            ],
            input=curl_config,
            text=True,
            capture_output=True,
            timeout=DEFAULT_FINAL_COMPOSER_TIMEOUT_SECONDS + 5,
            check=False,
        )
    finally:
        if body_file_name:
            try:
                os.unlink(body_file_name)
            except OSError:
                pass
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(stderr or f"curl exited with code {completed.returncode}")
    try:
        parsed = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("curl provider response was not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("curl provider response was not a JSON object")
    return parsed


def _chat_completions_url() -> str:
    base = ENGINEERING_CONFIG_SUMMARY_PROVIDER_API_BASE.rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    return f"{base}/chat/completions"


def _curl_config_quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _safe_error_summary(exc: Exception, *, api_key: str) -> str:
    message = str(exc)
    if api_key:
        message = message.replace(api_key, "[redacted]")
    return message[:240]


def _system_prompt() -> str:
    return (
        "你是 JATO 配置对比业务摘要 composer。"
        "输入 facts 已由 parser/compare/evidence 层确定，你不能重新判断配置事实，不能添加 facts 中没有的配置。"
        "你的任务是把结构化配置差异改写成中文业务摘要，给产品、销售和市场业务用户直接阅读。"
        "摘要要像业务同事口述配置差异：headline 必须使用“{target} 相比 {base} ...”的方向表达，"
        "不要写成规则计数说明。mainUpgrades 优先按业务维度归纳，例如泊车辅助、音响、舒适便利、座椅、灯光；"
        "优先读取 categoryFacts 判断哪些配置大类是业务重点，再结合 businessFocusGroups、upgradeSignals 和 evidenceFacts 组织句子；"
        "每个 target 必须先读取 sourceEvidenceSummary：differenceCount、withSourceEvidenceCount、missingSourceEvidenceCount、"
        "inferredCount、unknownCount、mergedCellExpandedCount、sourceSheetNames 是该目标的证据边界。"
        "replacementsOrReductions 用于表达减少、替换、降配或从手动到电动/从普通到高配的互斥变化。"
        "如果能从 upgradeSignals 看出升级链路，优先写成“倒车影像升级为 360 全景影像”这类句子。"
        "避免只复述 feature code；需要保留中英配置名时，也要先给业务维度。"
        "必须读取 context.compareScope：marketScope/sourceScope/modelYearScope/identityScope/caution 是本次对比的全局口径。"
        "如果 compareScope.sourceReviewHints 或 sourceGroups 存在，必须优先把同国家同年款多来源边界写入 evidenceStatus，"
        "不要把不同网站、不同 sheet 或不同上传人的差异直接写成确定配置升级。"
        "如果 compareScope.marketScope 是 cross_market，必须在 evidenceStatus 提醒跨国家/市场配置差异不一定等于版本升级；"
        "如果 sourceScope 是 multi_source，必须提醒需要核对不同网站、sheet 或上传来源；"
        "如果 modelYearScope 是 cross_model_year，必须提醒可能存在改款换代影响；"
        "如果 identityScope 是 own_vs_competitor，要按本品 vs 竞品写业务语气，避免假设竞品有物料号。"
        "如果 compareScope.missingSourceEvidenceCount > 0，也必须提醒缺 source evidence 的差异不能直接引用为确定卖点。"
        "如果 target.sourceEvidenceSummary.missingSourceEvidenceCount > 0，也必须在该 target 的 evidenceStatus 提醒缺来源证据。"
        "如果 target.sourceEvidenceSummary.mergedCellExpandedCount > 0，必须说明部分值来自合并格展开，可引用但要保留来源边界。"
        "如果 target.sourceEvidenceSummary.sourceSheetNames 有多个 sheet 或来源，必须提醒多 sheet/source 需要核对。"
        "保留证据边界：如果 inferredCount > 0，必须说明规则推断不是 Excel 原文，引用前需要核对 source evidence。"
        "如果 evidenceFacts 中有 requiresReview=true 或 businessNote 包含需核对、待确认、缺失、缺少、回看，"
        "必须在 evidenceStatus 中提醒这些配置来自需核对事实，不能直接当作确定卖点引用。"
        "如果 context 里有跨市场、年款待补、来源差异，也必须在 evidenceStatus 中提醒。"
        "输出必须是一个合法 JSON 对象，字段为 summaries。"
        "summaries 数组长度必须等于输入 targets 数量，并按输入 targets 顺序返回。"
        "即使某个 target 差异少或证据不足，也要返回该 target 的摘要，并在 evidenceStatus 说明边界。"
        "当 context.retryReason 为 single_target_summary_rescue 时，输入只包含一个 target，必须只返回这一项，不能返回空 summaries。"
        "summaries 是数组，每项字段必须为 targetTrimId、targetLabel、headline、mainUpgrades、"
        "replacementsOrReductions、evidenceStatus、recommendedUse、evidenceRefs。"
        "mainUpgrades、replacementsOrReductions、evidenceStatus 必须是字符串数组。"
        "evidenceRefs 必须是数组；每项包含 section、itemIndex、evidenceKey、reason。"
        "section 只能引用 mainUpgrades、replacementsOrReductions 或 evidenceStatus；itemIndex 是对应数组下标。"
        "evidenceKey 必须来自输入 target 的 evidenceFacts 或 upgradeSignals，不能自造。"
        "每个 target 最多 5 条 mainUpgrades、最多 4 条 replacementsOrReductions、最多 3 条 evidenceStatus。"
        "不要输出 Markdown，不要输出解释，不要提 AstrBot 或模型名称。"
    )


def _single_target_system_prompt() -> str:
    return (
        "你是 JATO 配置对比单目标业务摘要 composer。"
        "输入只包含 baseTrim 和 targets[0] 一个目标配置列。"
        "必须输出合法 JSON 对象，格式为 {\"summaries\":[...]}，summaries 必须且只能有 1 项，不能返回空数组。"
        "targetTrimId 和 targetLabel 必须原样复制输入 targets[0]。"
        "headline 必须用“{target} 相比 {base} ...”表达方向。"
        "优先用 categoryFacts、businessFocusGroups、upgradeSignals、addedFeatures、removedFeatures、changedFeatures 写业务可读结论。"
        "必须读取 targets[0].sourceEvidenceSummary，把 missingSourceEvidenceCount、inferredCount、"
        "mergedCellExpandedCount 和 sourceSheetNames 写进 evidenceStatus 的证据边界。"
        "如果 context.compareScope.sourceReviewHints 或 sourceGroups 存在，必须提醒同国家同年款多来源边界，"
        "不要把不同来源差异直接写成确定配置升级。"
        "mainUpgrades 至少 1 条；如果事实很弱，就写差异主要集中在哪些 category，不能虚构配置。"
        "replacementsOrReductions 可为空；evidenceStatus 至少 1 条，必须说明 inferred / source evidence 边界。"
        "如果 evidenceFacts 中有 requiresReview=true 或 businessNote 提示需核对，也必须写入 evidenceStatus。"
        "evidenceRefs 只能引用输入 evidenceFacts 或 upgradeSignals 中出现的 evidenceKey；没有可引用 key 就返回空数组。"
        "每项字段为 targetTrimId、targetLabel、headline、mainUpgrades、replacementsOrReductions、"
        "evidenceStatus、recommendedUse、evidenceRefs。不要输出 Markdown，不要解释。"
    )


def _compact_payload(value: Any, *, depth: int = 0, key: str | None = None) -> Any:
    if isinstance(value, list) and all(isinstance(item, (str, int, float, bool)) or item is None for item in value):
        limit = _COMPACT_LIST_LIMITS.get(key or "", 12)
        return value[:limit]
    if depth >= 5:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)[:800]
    if isinstance(value, dict):
        return {
            str(item_key): _compact_payload(item, depth=depth + 1, key=str(item_key))
            for item_key, item in list(value.items())[:80]
        }
    if isinstance(value, list):
        limit = _COMPACT_LIST_LIMITS.get(key or "", 12)
        return [_compact_payload(item, depth=depth + 1, key=key) for item in value[:limit]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)[:800]


def _parse_summary_json(content: str) -> dict[str, Any] | None:
    text = str(content or "").strip()
    if text.startswith("```"):
        text = text.strip("`").removeprefix("json").strip()
    try:
        parsed = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        parsed = None
    if isinstance(parsed, list):
        return {"summaries": parsed}
    if isinstance(parsed, dict) and isinstance(parsed.get("summaries"), list):
        return parsed
    if isinstance(parsed, dict) and _looks_like_summary_item(parsed):
        return parsed
    for candidate in _iter_balanced_json_objects(text):
        try:
            parsed = json.loads(candidate)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(parsed, list):
            return {"summaries": parsed}
        if isinstance(parsed, dict) and isinstance(parsed.get("summaries"), list):
            return parsed
        if isinstance(parsed, dict) and _looks_like_summary_item(parsed):
            return parsed
    return parsed if isinstance(parsed, dict) else None


def _looks_like_summary_item(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and "headline" in value
        and (
            "mainUpgrades" in value
            or "replacementsOrReductions" in value
            or "evidenceStatus" in value
        )
    )


def _iter_balanced_json_objects(text: str):
    for start, char in enumerate(text):
        if char != "{":
            continue
        depth = 0
        in_string = False
        escaped = False
        for index in range(start, len(text)):
            current = text[index]
            if in_string:
                if escaped:
                    escaped = False
                elif current == "\\":
                    escaped = True
                elif current == '"':
                    in_string = False
                continue
            if current == '"':
                in_string = True
            elif current == "{":
                depth += 1
            elif current == "}":
                depth -= 1
                if depth == 0:
                    yield text[start : index + 1]
                    break


def _sanitize_summaries(raw_summaries: Any, raw_targets: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_summaries, list):
        raw_summaries = []
    targets = raw_targets if isinstance(raw_targets, list) else []
    target_by_index = [target for target in targets if isinstance(target, dict)]
    raw_items = [item if isinstance(item, dict) else {} for item in raw_summaries]

    if target_by_index:
        used_indexes: set[int] = set()
        return [
            _sanitize_summary_item(
                _match_summary_item_for_target(raw_items, target, index, used_indexes),
                target,
            )
            for index, target in enumerate(target_by_index)
        ]

    results: list[dict[str, Any]] = []
    for item in raw_items:
        target_trim_id = _read_text(item.get("targetTrimId"))
        target_label = _read_text(item.get("targetLabel"))
        if not target_trim_id and not target_label:
            continue
        results.append(_sanitize_summary_item(item, {}))
    return results


def _match_summary_item_for_target(
    raw_items: list[dict[str, Any]],
    target: dict[str, Any],
    index: int,
    used_indexes: set[int],
) -> dict[str, Any]:
    target_trim_id = _read_text(target.get("targetTrimId"))
    target_label = _read_text(target.get("targetLabel"))
    for item_index, item in enumerate(raw_items):
        if item_index in used_indexes:
            continue
        item_trim_id = _read_text(item.get("targetTrimId"))
        if target_trim_id and item_trim_id == target_trim_id:
            used_indexes.add(item_index)
            return item
    for item_index, item in enumerate(raw_items):
        if item_index in used_indexes:
            continue
        item_label = _read_text(item.get("targetLabel"))
        if target_label and item_label == target_label:
            used_indexes.add(item_index)
            return item
    if index < len(raw_items) and index not in used_indexes:
        used_indexes.add(index)
        return raw_items[index]
    return {}


def _sanitize_summary_item(item: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    target_trim_id = _read_text(item.get("targetTrimId")) or _read_text(target.get("targetTrimId"))
    target_label = _read_text(item.get("targetLabel")) or _read_text(target.get("targetLabel"))
    headline = _read_text(item.get("headline"))[:260]
    main_upgrades = _read_string_list(item.get("mainUpgrades"), limit=5)
    replacements_or_reductions = _read_string_list(item.get("replacementsOrReductions"), limit=4)
    evidence_status = _read_string_list(item.get("evidenceStatus"), limit=3)
    if not headline and target_label:
        headline = f"{target_label} 的 AI 摘要暂未返回，请以配置表和 source evidence 为准。"
    if not evidence_status and target:
        evidence_status = _fallback_evidence_status(target)
    evidence_status = _merge_required_evidence_status(evidence_status, target)
    section_lengths = {
        "mainUpgrades": len(main_upgrades),
        "replacementsOrReductions": len(replacements_or_reductions),
        "evidenceStatus": len(evidence_status),
    }
    summary_sections = {
        "mainUpgrades": main_upgrades,
        "replacementsOrReductions": replacements_or_reductions,
        "evidenceStatus": evidence_status,
    }
    evidence_refs = _read_evidence_refs(
        item.get("evidenceRefs"),
        target,
        section_lengths=section_lengths,
    )
    bound_claims = {
        (str(ref.get("section") or ""), int(ref.get("itemIndex", -1)))
        for ref in evidence_refs
        if str(ref.get("section") or "") in {"mainUpgrades", "replacementsOrReductions"}
    }
    claim_count = len(main_upgrades) + len(replacements_or_reductions)
    unsupported_evidence_count = sum(
        1
        for section, items in (
            ("mainUpgrades", main_upgrades),
            ("replacementsOrReductions", replacements_or_reductions),
        )
        for item_index, _item in enumerate(items)
        if (section, item_index) not in bound_claims
    )
    if unsupported_evidence_count:
        evidence_status = _dedupe_strings([
            *evidence_status,
            f"{unsupported_evidence_count} 条 AI 结论未匹配到配置证据，不可直接引用。",
        ], limit=4)
    return {
        "targetTrimId": target_trim_id,
        "targetLabel": target_label,
        "headline": headline,
        "mainUpgrades": main_upgrades,
        "replacementsOrReductions": replacements_or_reductions,
        "evidenceStatus": evidence_status,
        "evidenceRefs": evidence_refs,
        "evidenceBoundClaimCount": claim_count - unsupported_evidence_count,
        "unsupportedEvidenceCount": unsupported_evidence_count,
        "recommendedUse": (_read_text(item.get("recommendedUse")) or _fallback_recommended_use(target))[:360],
    }


def _summary_needs_retry(summary: dict[str, Any]) -> bool:
    headline = _read_text(summary.get("headline"))
    return (
        "AI 摘要暂未返回" in headline
        and not summary.get("mainUpgrades")
        and not summary.get("replacementsOrReductions")
    )


def _missing_target_retry_payload(prompt_payload: Any, summaries: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not isinstance(prompt_payload, dict):
        return None
    targets = prompt_payload.get("targets")
    if not isinstance(targets, list):
        return None
    missing_targets = [
        target
        for target, summary in zip(targets, summaries, strict=False)
        if isinstance(target, dict) and _summary_needs_retry(summary)
    ]
    if not missing_targets:
        return None
    retry_payload = dict(prompt_payload)
    retry_payload["targets"] = missing_targets
    retry_context = dict(retry_payload.get("context")) if isinstance(retry_payload.get("context"), dict) else {}
    retry_context["retryReason"] = "previous_llm_response_omitted_targets"
    retry_context["instruction"] = (
        "Only compose summaries for the provided missing targets. Return every provided target in order. "
        "Do not omit a target even when evidence is weak."
    )
    retry_payload["context"] = retry_context
    return retry_payload


def _single_missing_target_retry_payloads(
    prompt_payload: Any,
    summaries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(prompt_payload, dict):
        return []
    targets = prompt_payload.get("targets")
    if not isinstance(targets, list):
        return []
    retry_payloads: list[dict[str, Any]] = []
    for target, summary in zip(targets, summaries, strict=False):
        if not isinstance(target, dict) or not _summary_needs_retry(summary):
            continue
        retry_payload = dict(prompt_payload)
        retry_payload["baseTrim"] = _slim_retry_base_trim(prompt_payload.get("baseTrim"))
        retry_payload["targets"] = [_slim_single_retry_target(target)]
        retry_context = dict(retry_payload.get("context")) if isinstance(retry_payload.get("context"), dict) else {}
        retry_context["retryReason"] = "single_target_summary_rescue"
        retry_context["instruction"] = (
            "Compose exactly one summary for the single provided target. "
            "Return summaries with one item only. Keep targetTrimId and targetLabel from the input target."
        )
        retry_payload["context"] = retry_context
        retry_payloads.append(retry_payload)
    return retry_payloads


def _slim_retry_base_trim(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    keys = (
        "trimId",
        "label",
        "brand",
        "modelName",
        "trimName",
        "market",
        "modelYear",
        "materialNo",
        "salesVersion",
        "dataOrigin",
        "source",
    )
    return {key: value.get(key) for key in keys if value.get(key) not in (None, "", [])}


def _slim_single_retry_target(target: dict[str, Any]) -> dict[str, Any]:
    slim: dict[str, Any] = {}
    for key in (
        "targetTrimId",
        "targetLabel",
        "targetTrim",
        "differenceCounts",
        "upgradeSignals",
        "addedFeatures",
        "removedFeatures",
        "changedFeatures",
        "businessFocusGroups",
        "categoryFacts",
        "sourceEvidenceSummary",
        "context",
        "evidence",
    ):
        if key in target:
            slim[key] = target[key]
    evidence_facts = target.get("evidenceFacts")
    if isinstance(evidence_facts, list):
        slim["evidenceFacts"] = evidence_facts[:24]
    return slim


def _summary_identity_key(summary: dict[str, Any]) -> str:
    return _read_text(summary.get("targetTrimId")) or _read_text(summary.get("targetLabel"))


def _merge_retry_summaries(
    summaries: list[dict[str, Any]],
    retry_summaries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    retry_by_key = {
        _summary_identity_key(summary): summary
        for summary in retry_summaries
        if _summary_identity_key(summary)
    }
    if not retry_by_key:
        return summaries
    merged: list[dict[str, Any]] = []
    for summary in summaries:
        key = _summary_identity_key(summary)
        retry_summary = retry_by_key.get(key)
        if retry_summary and _summary_needs_retry(summary) and not _summary_needs_retry(retry_summary):
            merged.append(retry_summary)
        else:
            merged.append(summary)
    return merged


def _merge_usage(primary: dict[str, int], retry: dict[str, int]) -> dict[str, int]:
    merged = dict(primary)
    for key in (
        "promptTokens",
        "completionTokens",
        "totalTokens",
        "promptCacheHitTokens",
        "promptCacheMissTokens",
    ):
        merged[key] = int(primary.get(key, 0) or 0) + int(retry.get(key, 0) or 0)
    return merged


def _fallback_evidence_status(target: dict[str, Any]) -> list[str]:
    evidence = target.get("evidence") if isinstance(target.get("evidence"), dict) else {}
    warning = _read_text(evidence.get("warning"))
    return [
        "LLM 未返回该目标摘要，当前不能引用为卖点。",
        warning or "请点开 source evidence 核对后再使用。",
    ][:3]


def _merge_required_evidence_status(existing: list[str], target: dict[str, Any]) -> list[str]:
    required = _required_evidence_status(target)
    if not required:
        return existing[:3]

    existing_kept: list[str] = []
    for item in existing:
        if not item:
            continue
        if item not in existing_kept:
            existing_kept.append(item)

    missing_required = [
        item
        for item in required
        if not _evidence_status_covers(item["kind"], existing_kept)
    ]
    if not missing_required:
        return existing_kept[:3]

    if len(missing_required) >= 3:
        return [item["text"] for item in missing_required[:3]]
    slots_for_existing = max(0, 3 - len(missing_required))
    merged = existing_kept[:slots_for_existing] + [item["text"] for item in missing_required]
    return merged[:3]


def _required_evidence_status(target: dict[str, Any]) -> list[dict[str, str]]:
    source_summary = target.get("sourceEvidenceSummary")
    if not isinstance(source_summary, dict):
        source_summary = {}
    evidence = target.get("evidence") if isinstance(target.get("evidence"), dict) else {}

    inferred_count = _read_non_negative_int(source_summary.get("inferredCount"))
    if inferred_count == 0:
        inferred_count = _read_non_negative_int(evidence.get("inferredCount"))
    missing_source_count = _read_non_negative_int(source_summary.get("missingSourceEvidenceCount"))
    unknown_count = _read_non_negative_int(source_summary.get("unknownCount"))
    merged_count = _read_non_negative_int(source_summary.get("mergedCellExpandedCount"))
    source_sheet_count = len(_read_string_list(source_summary.get("sourceSheetNames"), limit=8))

    notes: list[dict[str, str]] = []
    if _target_has_review_required_fact(target):
        notes.append({
            "kind": "review",
            "text": "存在需核对配置事实，引用到卖点前需要打开 source evidence 核对。",
        })
    if inferred_count > 0:
        notes.append({
            "kind": "inferred",
            "text": f"{inferred_count} 项来自规则推断，不是 Excel 原文。",
        })
    if missing_source_count > 0:
        notes.append({
            "kind": "missing_source",
            "text": f"{missing_source_count} 项缺 source evidence，不能直接引用为确定卖点。",
        })
    if unknown_count > 0:
        notes.append({
            "kind": "unknown",
            "text": f"{unknown_count} 项仍为待确认，需补来源或人工核对。",
        })
    if merged_count > 0:
        notes.append({
            "kind": "merged",
            "text": f"{merged_count} 项来自合并格展开，可引用但需保留来源边界。",
        })
    if source_sheet_count > 1:
        notes.append({
            "kind": "multi_source",
            "text": "差异涉及多个 sheet/source，引用前需核对来源口径。",
        })
    return notes


def _read_non_negative_int(value: Any) -> int:
    parsed = _read_int(value)
    if parsed is None or parsed < 0:
        return 0
    return parsed


def _target_has_review_required_fact(target: dict[str, Any]) -> bool:
    evidence_facts = target.get("evidenceFacts")
    if not isinstance(evidence_facts, list):
        return False
    for fact in evidence_facts:
        if not isinstance(fact, dict):
            continue
        if fact.get("requiresReview") is True:
            return True
        business_note = _read_text(fact.get("businessNote"))
        if business_note and any(token in business_note for token in ("需核对", "待核对", "待确认", "缺失", "缺少", "回看", "人工核对")):
            return True
    return False


def _evidence_status_covers(kind: str, existing: list[str]) -> bool:
    text = " ".join(existing).lower()
    if kind == "review":
        return any(token in text for token in ("需核对", "待核对", "待确认", "人工核对", "review"))
    if kind == "inferred":
        return any(token in text for token in ("规则推断", "不是 excel 原文", "not excel", "inferred", "不配备*"))
    if kind == "missing_source":
        return any(token in text for token in ("缺 source", "缺少 source", "缺来源", "缺少来源", "missing source"))
    if kind == "unknown":
        return any(token in text for token in ("待确认", "unknown", "需补来源"))
    if kind == "merged":
        return any(token in text for token in ("合并格", "merged"))
    if kind == "multi_source":
        return any(token in text for token in ("多个 sheet", "多 sheet", "多个 source", "多来源", "multi_source", "multi source"))
    return False


def _fallback_recommended_use(target: dict[str, Any]) -> str:
    if not target:
        return ""
    return "请先在配置表中核对差异行和 source evidence，再形成业务话术。"


def _fallback_business_summary_items(prompt_payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(prompt_payload, dict):
        return []
    targets = prompt_payload.get("targets")
    if not isinstance(targets, list):
        return []
    base_trim = prompt_payload.get("baseTrim") if isinstance(prompt_payload.get("baseTrim"), dict) else {}
    base_label = _read_text(base_trim.get("label")) or _read_text(base_trim.get("trimName")) or "基准配置列"
    summaries: list[dict[str, Any]] = []
    for target in targets:
        if not isinstance(target, dict):
            continue
        target_label = _read_text(target.get("targetLabel")) or "目标配置列"
        item = {
            "targetTrimId": _read_text(target.get("targetTrimId")),
            "targetLabel": target_label,
            "headline": f"{target_label} 相比 {base_label} 暂用配置事实生成兜底摘要。",
            "mainUpgrades": _fallback_main_upgrades(target),
            "replacementsOrReductions": _fallback_replacements_or_reductions(target),
            "evidenceStatus": ["LLM provider 暂不可用，以下结论只作为配置事实导读。"],
            "evidenceRefs": [],
            "recommendedUse": "请优先使用完整配置表和 source evidence；外部引用前需等待 LLM 摘要恢复或人工核对。",
        }
        summaries.append(_sanitize_summary_item(item, target))
    return summaries


def _fallback_main_upgrades(target: dict[str, Any]) -> list[str]:
    upgrades: list[str] = []
    for signal in _read_dict_list(target.get("upgradeSignals"), limit=5):
        dimension = _read_text(signal.get("dimension")) or _read_text(signal.get("category")) or "配置"
        from_value = _read_text(signal.get("from"))
        to_value = _read_text(signal.get("to"))
        if from_value and to_value:
            upgrades.append(f"{dimension}：{from_value} → {to_value}")
        else:
            label = _feature_fact_label(signal)
            if label:
                upgrades.append(label)
    for feature in _read_feature_list(target.get("addedFeatures"), limit=5):
        upgrades.append(f"新增 {feature}")
    if upgrades:
        return _dedupe_strings(upgrades, limit=5)

    focus_groups = _read_dict_list(target.get("businessFocusGroups"), limit=3)
    for group in focus_groups:
        label = _read_text(group.get("label")) or _read_text(group.get("category")) or _read_text(group.get("title"))
        count = _read_non_negative_int(group.get("count") or group.get("differenceCount") or group.get("total"))
        if label:
            upgrades.append(f"差异集中在 {label}{f' {count} 项' if count else ''}")
    category_facts = _read_dict_list(target.get("categoryFacts"), limit=3)
    for fact in category_facts:
        category = _read_text(fact.get("category")) or _read_text(fact.get("label"))
        count = _read_non_negative_int(fact.get("differenceCount") or fact.get("count"))
        if category:
            upgrades.append(f"{category} 有{count or ''}项配置差异".replace("有项", "存在"))
    if upgrades:
        return _dedupe_strings(upgrades, limit=5)

    difference_counts = target.get("differenceCounts") if isinstance(target.get("differenceCounts"), dict) else {}
    total_difference = _read_non_negative_int(
        difference_counts.get("totalDifference")
        or difference_counts.get("differenceCount")
        or target.get("differenceCount")
    )
    if total_difference > 0:
        return [f"该配置列存在 {total_difference} 项差异，需在下方配置表逐项核对。"]
    return ["当前缺少足够可归纳的升级事实，需以配置表逐项核对。"]


def _fallback_replacements_or_reductions(target: dict[str, Any]) -> list[str]:
    reductions: list[str] = []
    for feature in _read_feature_list(target.get("removedFeatures"), limit=4):
        reductions.append(f"减少或替换 {feature}")
    for feature in _read_feature_list(target.get("changedFeatures"), limit=4):
        reductions.append(f"参数变化 {feature}")
    return _dedupe_strings(reductions, limit=4)


def _read_dict_list(value: Any, *, limit: int) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value[:limit] if isinstance(item, dict)]


def _read_feature_list(value: Any, *, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    features: list[str] = []
    for item in value[:limit]:
        label = _feature_fact_label(item)
        if label:
            features.append(label)
    return features


def _feature_fact_label(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("featureName", "label", "name", "title", "dimension", "feature", "to", "from"):
            text = _read_text(value.get(key))
            if text:
                return text[:180]
        return ""
    return _read_text(value)[:180]


def _dedupe_strings(values: list[str], *, limit: int) -> list[str]:
    results: list[str] = []
    for value in values:
        text = _read_text(value)
        if text and text not in results:
            results.append(text[:260])
        if len(results) >= limit:
            break
    return results


def _read_text(value: Any) -> str:
    return str(value or "").strip()


def _read_string_list(value: Any, *, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item or "").strip()[:260] for item in value[:limit] if str(item or "").strip()]


def _target_evidence_keys(target: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    evidence_facts = target.get("evidenceFacts")
    if isinstance(evidence_facts, list):
        for item in evidence_facts:
            if not isinstance(item, dict):
                continue
            key = _read_text(item.get("evidenceKey"))
            if key:
                keys.add(key)
    upgrade_signals = target.get("upgradeSignals")
    if isinstance(upgrade_signals, list):
        for item in upgrade_signals:
            if not isinstance(item, dict):
                continue
            for key_name in ("evidenceKey", "fromEvidenceKey", "toEvidenceKey"):
                key = _read_text(item.get(key_name))
                if key:
                    keys.add(key)
    return keys


def _read_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _read_evidence_refs(
    value: Any,
    target: dict[str, Any],
    *,
    section_lengths: dict[str, int] | None = None,
    limit: int = 16,
) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    allowed_keys = _target_evidence_keys(target)
    if not allowed_keys:
        return []
    candidates_by_key: dict[str, list[dict[str, str]]] = {}
    for candidate in _evidence_ref_candidates(target):
        candidates_by_key.setdefault(candidate["evidenceKey"], []).append(candidate)
    refs: list[dict[str, Any]] = []
    allowed_sections = {"mainUpgrades", "replacementsOrReductions", "evidenceStatus"}
    for item in value[:limit]:
        if not isinstance(item, dict):
            continue
        section = _read_text(item.get("section"))
        item_index = _read_int(item.get("itemIndex"))
        evidence_key = _read_text(item.get("evidenceKey"))
        if section not in allowed_sections or item_index is None or item_index < 0 or not evidence_key:
            continue
        if section_lengths is not None and item_index >= section_lengths.get(section, 0):
            continue
        if allowed_keys and evidence_key not in allowed_keys:
            continue
        candidates = candidates_by_key.get(evidence_key, [])
        feature_code = next(
            (candidate["featureCode"] for candidate in candidates if candidate.get("featureCode")),
            "",
        )
        category = next(
            (candidate["category"] for candidate in candidates if candidate.get("category")),
            "",
        )
        refs.append(
            {
                "section": section,
                "itemIndex": item_index,
                "evidenceKey": evidence_key,
                "featureCode": feature_code,
                "category": category,
                "reason": _read_text(item.get("reason"))[:260],
            }
        )
    return refs


def _evidence_ref_candidates(target: dict[str, Any]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    evidence_facts = target.get("evidenceFacts")
    if isinstance(evidence_facts, list):
        for item in evidence_facts:
            if not isinstance(item, dict):
                continue
            evidence_key = _read_text(item.get("evidenceKey"))
            if not evidence_key:
                continue
            candidates.append({
                "evidenceKey": evidence_key,
                "featureCode": _read_text(item.get("featureCode"))[:160],
                "category": _read_text(item.get("category"))[:160],
            })
    upgrade_signals = target.get("upgradeSignals")
    if isinstance(upgrade_signals, list):
        for item in upgrade_signals:
            if not isinstance(item, dict):
                continue
            for key_name in ("evidenceKey", "toEvidenceKey", "fromEvidenceKey"):
                evidence_key = _read_text(item.get(key_name))
                if not evidence_key:
                    continue
                candidates.append({
                    "evidenceKey": evidence_key,
                    "featureCode": _read_text(item.get("featureCode"))[:160],
                    "category": _read_text(item.get("category") or item.get("dimension"))[:160],
                })
    return candidates


def _fallback_response(status: str, reason: str, *, prompt_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    _ = prompt_payload
    return {
        "summaries": [],
        "usage": {
            "provider": ENGINEERING_CONFIG_SUMMARY_PROVIDER_ID,
            "model": ENGINEERING_CONFIG_SUMMARY_PROVIDER_MODEL,
            "status": status,
            "providerStatus": status,
            "promptTokens": 0,
            "completionTokens": 0,
            "totalTokens": 0,
            "promptCacheHitTokens": 0,
            "promptCacheMissTokens": 0,
            "finishReason": "",
            "estimated": False,
            "fallbackReason": reason[:240],
        },
    }
