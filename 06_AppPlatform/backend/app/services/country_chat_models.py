from __future__ import annotations

import itertools
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import Request
from urllib.request import urlopen

from app.services import news_digest_service

AUTO_CHAT_MODEL_ID = "auto"
DEFAULT_NVIDIA_CHAT_MODEL = "meta/llama-3.3-70b-instruct"
NVIDIA_MODELS_URL = "https://integrate.api.nvidia.com/v1/models"
GEMINI_MODELS_URL = "https://generativelanguage.googleapis.com/v1beta/models"
DISCOVERY_ALL_MODELS = "*"
_ROTATION_COUNTER = itertools.count()
_ROTATION_LOCK = threading.Lock()
_DISCOVERY_CACHE_LOCK = threading.Lock()
_DISCOVERY_CACHE: dict[str, tuple[float, tuple[str, ...]]] = {}

_STATIC_NVIDIA_CHAT_MODELS = (
    "meta/llama-3.3-70b-instruct",
    "meta/llama-3.1-70b-instruct",
    "meta/llama-3.1-8b-instruct",
    "nvidia/llama-3.1-nemotron-70b-instruct",
    "nvidia/llama-3.1-nemotron-51b-instruct",
    "nvidia/llama-3.1-nemotron-ultra-253b-v1",
    "mistralai/mistral-large",
    "mistralai/mistral-medium-3-instruct",
    "qwen/qwen3-next-80b-a3b-instruct",
    "deepseek-ai/deepseek-v3.2",
    "openai/gpt-oss-120b",
)
_STATIC_GEMINI_CHAT_MODELS = (
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
)
_NVIDIA_MODEL_SKIP_TOKENS = (
    "embed",
    "retriever",
    "rerank",
    "clip",
    "guard",
    "safety",
    "topic-control",
    "pii",
    "deplot",
    "parse",
    "coder",
    "code-",
    "/code",
    "codegemma",
    "codellama",
    "starcoder",
    "fuyu",
    "kosmos",
    "vision",
    "multimodal",
    "-vl",
    "/vl",
    "translate",
    "reward",
    "recurrentgemma",
    "neva",
)
_NVIDIA_MODEL_ALLOW_TOKENS = (
    "instruct",
    "chat",
    "reason",
    "thinking",
    "nemotron",
    "mistral-large",
    "mistral-medium",
    "magistral",
    "ministral",
    "minimax-m2.",
    "yi-large",
    "deepseek-v3",
    "gpt-oss",
    "glm",
    "sarvam-m",
    "palmyra",
)
_GEMINI_SEARCH_PREFIXES = ("gemini-2.",)
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CountryChatModelOption:
    id: str
    provider: str
    model: str | None
    label: str
    description: str | None = None
    available: bool = True

    def to_payload(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "provider": self.provider,
            "model": self.model,
            "label": self.label,
            "description": self.description,
            "available": self.available,
        }


def get_default_nvidia_chat_model() -> str:
    return (
        os.getenv("APP_NVIDIA_CHAT_MODEL", DEFAULT_NVIDIA_CHAT_MODEL).strip()
        or DEFAULT_NVIDIA_CHAT_MODEL
    )


def get_default_gemini_chat_model() -> str:
    return (
        os.getenv(
            "APP_GEMINI_CHAT_MODEL",
            news_digest_service.DEFAULT_GEMINI_MODEL,
        ).strip()
        or news_digest_service.DEFAULT_GEMINI_MODEL
    )


def nvidia_provider_available() -> bool:
    return bool(
        os.getenv("NVIDIA_API_KEY", "").strip()
        or os.getenv("NVAPI_KEY", "").strip()
    )


def gemini_provider_available() -> bool:
    return bool(news_digest_service._gemini_api_key())  # noqa: SLF001


def gemini_model_supports_google_search(model: str | None) -> bool:
    normalized = str(model or "").strip().lower()
    return bool(normalized) and normalized.startswith(_GEMINI_SEARCH_PREFIXES)


def get_country_chat_model_metadata() -> dict[str, Any]:
    provider_options = _preferred_provider_model_options()
    primary = provider_options[0] if provider_options else None
    provider_available = bool(primary)
    provider_reason = None
    if not provider_available:
        provider_reason = (
            "当前环境未配置可用聊天模型；页面会退回本地摘要回答。"
        )

    return {
        "provider": primary.provider if primary else "fallback",
        "providerAvailable": provider_available,
        "providerReason": provider_reason,
        "defaultModel": primary.model if primary else None,
        "defaultChatModel": default_country_chat_model_id(),
        "availableChatModels": [
            option.to_payload()
            for option in list_country_chat_model_options()
        ],
    }


def list_country_chat_model_options() -> list[CountryChatModelOption]:
    provider_options = _provider_model_options()
    return [
        CountryChatModelOption(
            id=AUTO_CHAT_MODEL_ID,
            provider="auto",
            model=None,
            label="Auto (Recommended)",
            description=(
                "自动在各 provider 的默认模型之间轮换，并在失败时切换。"
                if provider_options else
                "当前没有可用聊天模型，将退回本地摘要回答。"
            ),
            available=True,
        ),
        *provider_options,
    ]


def default_country_chat_model_id() -> str:
    return AUTO_CHAT_MODEL_ID


def resolve_country_chat_model_id(requested_model: str | None) -> str:
    requested = str(requested_model or "").strip()
    if not requested:
        return default_country_chat_model_id()

    normalized = requested.lower()
    for option in list_country_chat_model_options():
        if option.id.lower() == normalized:
            return option.id

    allowed = ", ".join(
        option.id for option in list_country_chat_model_options()
    )
    raise ValueError(f"不支持的聊天模型: {requested}（可选: {allowed}）")


def build_country_chat_execution_chain(
    requested_model: str | None,
) -> tuple[str, list[CountryChatModelOption]]:
    selected_id = resolve_country_chat_model_id(requested_model)
    preferred_options = _preferred_provider_model_options()
    if not preferred_options:
        return selected_id, []

    if selected_id == AUTO_CHAT_MODEL_ID:
        with _ROTATION_LOCK:
            offset = next(_ROTATION_COUNTER) % len(preferred_options)
        ordered = preferred_options[offset:] + preferred_options[:offset]
        return selected_id, ordered

    all_options = _provider_model_options()
    selected = next(
        option for option in all_options if option.id == selected_id
    )
    ordered: list[CountryChatModelOption] = [selected]
    same_provider_default = next(
        (
            option
            for option in preferred_options
            if option.provider == selected.provider and option.id != selected.id
        ),
        None,
    )
    if same_provider_default is not None:
        ordered.append(same_provider_default)
    ordered.extend(
        option
        for option in preferred_options
        if option.provider != selected.provider
    )
    return selected_id, _dedupe_options(ordered)


def describe_model_option(option: CountryChatModelOption | None) -> str:
    if option is None:
        return "fallback"
    if option.model:
        return f"{option.provider} · {option.model}"
    return option.provider


def _provider_model_options() -> list[CountryChatModelOption]:
    configured = _configured_provider_model_specs()
    specs = (
        _expand_provider_model_specs(configured)
        if configured else
        _discover_default_provider_model_specs()
    )

    options: list[CountryChatModelOption] = []
    seen_ids: set[str] = set()
    for provider, model in specs:
        normalized_provider = provider.strip().lower()
        normalized_model = str(model or "").strip()
        if not normalized_model:
            continue
        if normalized_provider == "nvidia" and not nvidia_provider_available():
            continue
        if normalized_provider == "gemini" and not gemini_provider_available():
            continue
        option_id = f"{normalized_provider}:{normalized_model}"
        if option_id in seen_ids:
            continue
        seen_ids.add(option_id)
        options.append(
            CountryChatModelOption(
                id=option_id,
                provider=normalized_provider,
                model=normalized_model,
                label=_model_label(normalized_provider, normalized_model),
                description=_model_description(
                    normalized_provider,
                    normalized_model,
                ),
            )
        )
    return options


def _preferred_provider_model_options() -> list[CountryChatModelOption]:
    options = _provider_model_options()
    if not options:
        return []

    preferred: list[CountryChatModelOption] = []
    for provider in ("nvidia", "gemini"):
        candidates = [option for option in options if option.provider == provider]
        if not candidates:
            continue
        preferred_model = _preferred_model_name_for_provider(provider)
        match = next(
            (option for option in candidates if option.model == preferred_model),
            None,
        )
        preferred.append(match or candidates[0])
    return preferred


def _preferred_model_name_for_provider(provider: str) -> str:
    if provider == "gemini":
        return get_default_gemini_chat_model()
    return get_default_nvidia_chat_model()


def _configured_provider_model_specs() -> list[tuple[str, str]]:
    raw = os.getenv("APP_COUNTRY_CHAT_MODEL_OPTIONS", "").strip()
    if not raw:
        return []

    specs: list[tuple[str, str]] = []
    for chunk in raw.replace("\n", ",").split(","):
        item = chunk.strip()
        if not item:
            continue
        provider, model = _parse_model_spec(item)
        specs.append((provider, model))
    return specs


def _expand_provider_model_specs(
    specs: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    expanded: list[tuple[str, str]] = []
    for provider, model in specs:
        if model == DISCOVERY_ALL_MODELS:
            expanded.extend(
                (provider, discovered)
                for discovered in _discover_provider_model_names(provider)
            )
            continue
        expanded.append((provider, model))
    return expanded


def _discover_default_provider_model_specs() -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    if nvidia_provider_available():
        specs.extend(
            ("nvidia", model)
            for model in _discover_provider_model_names("nvidia")
        )
    if gemini_provider_available():
        specs.extend(
            ("gemini", model)
            for model in _discover_provider_model_names("gemini")
        )
    return specs


def _discover_provider_model_names(provider: str) -> list[str]:
    normalized_provider = provider.strip().lower()
    cache_ttl_seconds = _model_discovery_ttl_seconds()
    now = time.time()
    with _DISCOVERY_CACHE_LOCK:
        cached = _DISCOVERY_CACHE.get(normalized_provider)
        if cached and now - cached[0] < cache_ttl_seconds:
            return list(cached[1])

    models: list[str]
    try:
        if normalized_provider == "gemini":
            models = _fetch_gemini_model_names()
        else:
            models = _fetch_nvidia_model_names()
    except (
        HTTPError,
        URLError,
        TimeoutError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        log.warning(
            "Country chat model discovery failed for %s: %s",
            normalized_provider,
            exc,
        )
        models = _static_provider_model_names(normalized_provider)

    normalized = tuple(_normalize_provider_models(normalized_provider, models))
    with _DISCOVERY_CACHE_LOCK:
        _DISCOVERY_CACHE[normalized_provider] = (now, normalized)
    return list(normalized)


def _fetch_nvidia_model_names() -> list[str]:
    api_key = (
        os.getenv("NVIDIA_API_KEY", "").strip()
        or os.getenv("NVAPI_KEY", "").strip()
    )
    if not api_key:
        return []

    request = Request(
        NVIDIA_MODELS_URL,
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
        method="GET",
    )
    with urlopen(request, timeout=_model_discovery_timeout_seconds()) as response:
        payload = json.loads(response.read().decode("utf-8"))

    items = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise ValueError("NVIDIA models endpoint returned an unexpected payload")

    models: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        model_name = str(item.get("id") or "").strip()
        if _looks_like_nvidia_chat_model(model_name):
            models.append(model_name)
    return models


def _fetch_gemini_model_names() -> list[str]:
    api_key = news_digest_service._gemini_api_key()  # noqa: SLF001
    if not api_key:
        return []

    models: list[str] = []
    page_token = ""
    while True:
        url = f"{GEMINI_MODELS_URL}?key={quote(api_key, safe='')}"
        if page_token:
            url = f"{url}&pageToken={quote(page_token, safe='')}"
        request = Request(url, method="GET")
        with urlopen(
            request,
            timeout=_model_discovery_timeout_seconds(),
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))

        items = payload.get("models") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            raise ValueError("Gemini models endpoint returned an unexpected payload")

        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            methods = item.get("supportedGenerationMethods")
            if not (
                name.startswith("models/")
                and isinstance(methods, list)
                and "generateContent" in methods
            ):
                continue
            model_name = name.removeprefix("models/")
            if _looks_like_gemini_chat_model(model_name):
                models.append(model_name)

        page_token = str(payload.get("nextPageToken") or "").strip()
        if not page_token:
            break
    return models


def _normalize_provider_models(provider: str, models: list[str]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in models:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        if provider == "gemini" and not _looks_like_gemini_chat_model(normalized):
            continue
        if provider == "nvidia" and not _looks_like_nvidia_chat_model(normalized):
            continue
        seen.add(normalized)
        cleaned.append(normalized)

    preferred_model = _preferred_model_name_for_provider(provider)
    if preferred_model and preferred_model not in seen:
        cleaned.insert(0, preferred_model)
    elif preferred_model:
        cleaned.remove(preferred_model)
        cleaned.insert(0, preferred_model)
    return cleaned


def _static_provider_model_names(provider: str) -> list[str]:
    if provider == "gemini":
        return _normalize_provider_models(provider, list(_STATIC_GEMINI_CHAT_MODELS))
    return _normalize_provider_models(provider, list(_STATIC_NVIDIA_CHAT_MODELS))


def _looks_like_nvidia_chat_model(model: str) -> bool:
    normalized = str(model or "").strip().lower()
    if not normalized:
        return False
    if any(token in normalized for token in _NVIDIA_MODEL_SKIP_TOKENS):
        return False
    return any(token in normalized for token in _NVIDIA_MODEL_ALLOW_TOKENS)


def _looks_like_gemini_chat_model(model: str) -> bool:
    normalized = str(model or "").strip().lower()
    return normalized.startswith("gemini-")


def _parse_model_spec(spec: str) -> tuple[str, str]:
    normalized = spec.strip()
    lowered = normalized.lower()
    if ":" not in normalized:
        if lowered in {"nvidia", "gemini"}:
            return lowered, DISCOVERY_ALL_MODELS
        return "nvidia", normalized

    provider, model = normalized.split(":", 1)
    normalized_provider = provider.strip().lower()
    normalized_model = model.strip() or DISCOVERY_ALL_MODELS
    if normalized_provider not in {"nvidia", "gemini"}:
        return "nvidia", normalized
    return normalized_provider, normalized_model


def _dedupe_options(
    options: list[CountryChatModelOption],
) -> list[CountryChatModelOption]:
    ordered: list[CountryChatModelOption] = []
    seen: set[str] = set()
    for option in options:
        if option.id in seen:
            continue
        seen.add(option.id)
        ordered.append(option)
    return ordered


def _model_label(provider: str, model: str) -> str:
    if provider == "gemini":
        return f"Gemini · {model}"
    return f"NVIDIA · {model}"


def _model_description(provider: str, model: str) -> str:
    if provider == "gemini":
        if gemini_model_supports_google_search(model):
            return "Gemini 聊天模型，可按语义触发 Google Search 联网检索。"
        return "Gemini 聊天模型，直接基于国家快照生成分析回答。"
    return "NVIDIA NIM 聊天模型，支持 tool-calling 与结构化分析。"


def _model_discovery_ttl_seconds() -> int:
    raw = os.getenv("APP_COUNTRY_CHAT_MODEL_DISCOVERY_TTL_SECONDS", "1800")
    return max(60, int((raw or "1800").strip()))


def _model_discovery_timeout_seconds() -> int:
    raw = os.getenv("APP_COUNTRY_CHAT_MODEL_DISCOVERY_TIMEOUT_SECONDS", "10")
    return max(3, int((raw or "10").strip()))
