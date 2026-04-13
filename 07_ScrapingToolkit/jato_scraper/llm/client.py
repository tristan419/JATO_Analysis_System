"""Shared LLM client primitives for the scraping toolkit.

Both Hugging Face Inference Providers and NVIDIA NIM expose
OpenAI-compatible chat completion APIs. This module keeps the common
HTTP transport and response parsing in one place so provider wrappers
stay small.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import requests


DEFAULT_TIMEOUT = 60


class LlmApiError(RuntimeError):
    """Raised when a remote LLM provider returns an error response."""


@dataclass(frozen=True)
class ChatMessage:
    """One chat message in OpenAI-compatible format."""

    role: str
    content: str | list[dict[str, Any]]
    name: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "role": self.role,
            "content": self.content,
        }
        if self.name:
            payload["name"] = self.name
        return payload


@dataclass(frozen=True)
class ChatChoice:
    """One completion choice returned by a provider."""

    index: int
    message: dict[str, Any]
    finish_reason: str | None = None


@dataclass(frozen=True)
class ChatUsage:
    """Token usage metadata returned by a provider."""

    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> ChatUsage | None:
        if not isinstance(payload, dict):
            return None
        return cls(
            prompt_tokens=_maybe_int(payload.get("prompt_tokens")),
            completion_tokens=_maybe_int(payload.get("completion_tokens")),
            total_tokens=_maybe_int(payload.get("total_tokens")),
        )


@dataclass(frozen=True)
class ChatResponse:
    """Normalized chat completion response."""

    response_id: str | None
    model: str | None
    choices: tuple[ChatChoice, ...]
    usage: ChatUsage | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)

    @property
    def first_message(self) -> dict[str, Any] | None:
        if not self.choices:
            return None
        return self.choices[0].message

    @property
    def text(self) -> str | None:
        message = self.first_message
        if not isinstance(message, dict):
            return None
        content = message.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            text_parts = []
            for item in content:
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "text":
                    text_value = item.get("text")
                    if isinstance(text_value, str):
                        text_parts.append(text_value)
            if text_parts:
                return "\n".join(text_parts)
        return None


class BaseChatClient(ABC):
    """Abstract chat client interface."""

    @abstractmethod
    def chat(
        self,
        messages: list[ChatMessage | dict[str, Any]],
        *,
        model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        stream: bool = False,
        stop: str | list[str] | None = None,
        response_format: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        extra_payload: dict[str, Any] | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> ChatResponse:
        """Submit a chat-completions request."""


class OpenAICompatibleChatClient(BaseChatClient):
    """HTTP client for OpenAI-compatible chat completion APIs."""

    def __init__(
        self,
        *,
        provider_name: str,
        base_url: str,
        api_key: str,
        default_model: str | None = None,
        default_headers: dict[str, str] | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.provider_name = provider_name
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.default_model = default_model.strip() if default_model else None
        self.default_headers = default_headers or {}
        self.session = session or requests.Session()

    @property
    def chat_completions_url(self) -> str:
        return f"{self.base_url}/chat/completions"

    def _resolve_model_name(self, model: str | None) -> str:
        selected_model = (model or self.default_model or "").strip()
        if not selected_model:
            raise ValueError(
                f"{self.provider_name} client requires a model name"
            )
        return selected_model

    def _build_headers(self) -> dict[str, str]:
        return {
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
            **self.default_headers,
        }

    def _build_payload(
        self,
        messages: list[ChatMessage | dict[str, Any]],
        *,
        model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        stream: bool = False,
        stop: str | list[str] | None = None,
        response_format: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        extra_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": self._resolve_model_name(model),
            "messages": [to_message_payload(message) for message in messages],
            "stream": stream,
        }
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if temperature is not None:
            payload["temperature"] = temperature
        if top_p is not None:
            payload["top_p"] = top_p
        if stop is not None:
            payload["stop"] = stop
        if response_format is not None:
            payload["response_format"] = response_format
        if tools is not None:
            payload["tools"] = tools
        if tool_choice is not None:
            payload["tool_choice"] = tool_choice
        if extra_payload:
            payload.update(extra_payload)
        return payload

    def chat(
        self,
        messages: list[ChatMessage | dict[str, Any]],
        *,
        model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        stream: bool = False,
        stop: str | list[str] | None = None,
        response_format: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        extra_payload: dict[str, Any] | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> ChatResponse:
        if stream:
            raise ValueError(
                "Streaming responses are not implemented in this client yet"
            )
        payload = self._build_payload(
            messages,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p,
            stream=stream,
            stop=stop,
            response_format=response_format,
            tools=tools,
            tool_choice=tool_choice,
            extra_payload=extra_payload,
        )
        response = self.session.post(
            self.chat_completions_url,
            headers=self._build_headers(),
            json=payload,
            timeout=timeout,
        )
        if not response.ok:
            raise LlmApiError(
                _build_error_message(self.provider_name, response)
            )
        data = response.json()
        if not isinstance(data, dict):
            raise LlmApiError(
                f"{self.provider_name} returned a non-object JSON payload"
            )
        return parse_chat_response(data)


def to_message_payload(
    message: ChatMessage | dict[str, Any],
) -> dict[str, Any]:
    """Convert a typed or raw message into request payload format."""

    if isinstance(message, ChatMessage):
        return message.to_payload()
    if isinstance(message, dict):
        return dict(message)
    raise TypeError(f"Unsupported message type: {type(message)!r}")


def parse_chat_response(payload: dict[str, Any]) -> ChatResponse:
    """Normalize an OpenAI-compatible response payload."""

    raw_choices = payload.get("choices")
    choices: list[ChatChoice] = []
    if isinstance(raw_choices, list):
        for index, raw_choice in enumerate(raw_choices):
            if not isinstance(raw_choice, dict):
                continue
            raw_message = raw_choice.get("message")
            if not isinstance(raw_message, dict):
                continue
            choice_index = _maybe_int(raw_choice.get("index"))
            choices.append(
                ChatChoice(
                    index=index if choice_index is None else choice_index,
                    message=dict(raw_message),
                    finish_reason=_maybe_str(
                        raw_choice.get("finish_reason")
                    ),
                )
            )
    return ChatResponse(
        response_id=_maybe_str(payload.get("id")),
        model=_maybe_str(payload.get("model")),
        choices=tuple(choices),
        usage=ChatUsage.from_payload(payload.get("usage")),
        raw_payload=dict(payload),
    )


def _build_error_message(
    provider_name: str,
    response: requests.Response,
) -> str:
    body = _decode_error_body(response)
    return (
        f"{provider_name} request failed with status "
        f"{response.status_code}: {body}"
    )


def _decode_error_body(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text.strip() or "unknown error"
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if isinstance(message, str) and message.strip():
                return message.strip()
        message = payload.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return str(payload)


def _maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _maybe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
