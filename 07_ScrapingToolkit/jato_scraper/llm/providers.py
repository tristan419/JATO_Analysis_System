"""Provider-specific LLM clients for Hugging Face and NVIDIA."""

from __future__ import annotations

import os
from typing import Any

from jato_scraper.llm.client import OpenAICompatibleChatClient


HUGGING_FACE_BASE_URL = "https://router.huggingface.co/v1"
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"


class HuggingFaceChatClient(OpenAICompatibleChatClient):
    """Client for Hugging Face Inference Providers chat completions."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        default_model: str | None = None,
        base_url: str = HUGGING_FACE_BASE_URL,
        provider: str | None = None,
    ) -> None:
        resolved_key = api_key or _first_env(
            "HF_TOKEN",
            "HF_API_TOKEN",
            "HUGGINGFACE_API_KEY",
        )
        if not resolved_key:
            raise ValueError(
                "Hugging Face API key not found in HF_TOKEN, "
                "HF_API_TOKEN, or HUGGINGFACE_API_KEY"
            )
        self.provider = provider.strip() if provider else None
        super().__init__(
            provider_name="Hugging Face",
            base_url=base_url,
            api_key=resolved_key,
            default_model=default_model,
        )

    def _resolve_model_name(self, model: str | None) -> str:
        selected_model = super()._resolve_model_name(model)
        if not self.provider or ":" in selected_model:
            return selected_model
        return f"{selected_model}:{self.provider}"


class NvidiaChatClient(OpenAICompatibleChatClient):
    """Client for NVIDIA NIM hosted chat completions."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        default_model: str | None = None,
        base_url: str = NVIDIA_BASE_URL,
    ) -> None:
        resolved_key = api_key or _first_env(
            "NVIDIA_API_KEY",
            "NVAPI_KEY",
        )
        if not resolved_key:
            raise ValueError(
                "NVIDIA API key not found in NVIDIA_API_KEY or NVAPI_KEY"
            )
        super().__init__(
            provider_name="NVIDIA",
            base_url=base_url,
            api_key=resolved_key,
            default_model=default_model,
        )


def create_chat_client(
    provider: str,
    **kwargs: Any,
) -> OpenAICompatibleChatClient:
    """Instantiate a provider client by name."""

    normalized = provider.strip().lower()
    if normalized in {"hf", "huggingface", "hugging_face"}:
        return HuggingFaceChatClient(**kwargs)
    if normalized in {"nvidia", "nim"}:
        return NvidiaChatClient(**kwargs)
    raise ValueError(f"Unsupported LLM provider: {provider!r}")


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()
    return None
