import json

import pytest

from app.services import country_chat_models
from app.services import country_chat_service


@pytest.fixture(autouse=True)
def _clear_country_chat_model_cache() -> None:
    country_chat_models._DISCOVERY_CACHE.clear()  # noqa: SLF001
    yield
    country_chat_models._DISCOVERY_CACHE.clear()  # noqa: SLF001


def test_list_country_chat_model_options_expands_provider_wildcards(
    monkeypatch,
) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "nvidia-secret")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-secret")
    monkeypatch.setenv("APP_COUNTRY_CHAT_MODEL_OPTIONS", "nvidia:*,gemini:*")
    monkeypatch.setenv("APP_NVIDIA_CHAT_MODEL", "meta/llama-3.3-70b-instruct")
    monkeypatch.setenv("APP_GEMINI_CHAT_MODEL", "gemini-2.5-flash")
    monkeypatch.setattr(
        country_chat_models,
        "_fetch_nvidia_model_names",
        lambda: [
            "meta/llama-3.3-70b-instruct",
            "mistralai/mistral-large",
            "nvidia/embed-qa-4",
        ],
    )
    monkeypatch.setattr(
        country_chat_models,
        "_fetch_gemini_model_names",
        lambda: ["gemini-2.5-flash", "gemini-2.5-pro"],
    )

    options = country_chat_models.list_country_chat_model_options()

    assert [item.id for item in options] == [
        "auto",
        "gemini:gemini-2.5-flash",
        "gemini:gemini-2.5-pro",
        "nvidia:meta/llama-3.3-70b-instruct",
        "nvidia:mistralai/mistral-large",
    ]
    gemini_option = next(item for item in options if item.provider == "gemini")
    assert "Google Search" in str(gemini_option.description)


def test_execution_chain_keeps_auto_on_provider_defaults(monkeypatch) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "nvidia-secret")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-secret")
    monkeypatch.setenv("APP_COUNTRY_CHAT_MODEL_OPTIONS", "nvidia:*,gemini:*")
    monkeypatch.setenv("APP_NVIDIA_CHAT_MODEL", "meta/llama-3.3-70b-instruct")
    monkeypatch.setenv("APP_GEMINI_CHAT_MODEL", "gemini-2.5-flash")
    monkeypatch.setattr(
        country_chat_models,
        "_fetch_nvidia_model_names",
        lambda: [
            "meta/llama-3.3-70b-instruct",
            "mistralai/mistral-large",
        ],
    )
    monkeypatch.setattr(
        country_chat_models,
        "_fetch_gemini_model_names",
        lambda: ["gemini-2.5-flash", "gemini-2.5-pro"],
    )
    selected_id, execution_chain = country_chat_models.build_country_chat_execution_chain(
        "auto"
    )

    assert selected_id == "auto"
    assert [item.id for item in execution_chain] == [
        "gemini:gemini-2.5-flash",
        "nvidia:meta/llama-3.3-70b-instruct",
    ]


def test_default_model_options_use_static_models_without_discovery(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-secret")
    monkeypatch.delenv("APP_COUNTRY_CHAT_MODEL_OPTIONS", raising=False)
    monkeypatch.delenv("APP_COUNTRY_CHAT_MODEL_DISCOVER_ON_REQUEST", raising=False)
    monkeypatch.setattr(
        country_chat_models,
        "_fetch_gemini_model_names",
        lambda: (_ for _ in ()).throw(AssertionError("should not discover models")),
    )

    metadata = country_chat_models.get_country_chat_model_metadata()

    assert metadata["defaultChatModel"] == "gemini:gemini-flash-latest"
    assert "gemini:gemini-flash-latest" in [
        item["id"] for item in metadata["availableChatModels"]
    ]


def test_selected_model_falls_back_to_provider_default_then_other_provider(
    monkeypatch,
) -> None:
    monkeypatch.setenv("NVIDIA_API_KEY", "nvidia-secret")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-secret")
    monkeypatch.setenv("APP_COUNTRY_CHAT_MODEL_OPTIONS", "nvidia:*,gemini:*")
    monkeypatch.setenv("APP_NVIDIA_CHAT_MODEL", "meta/llama-3.3-70b-instruct")
    monkeypatch.setenv("APP_GEMINI_CHAT_MODEL", "gemini-2.5-flash")
    monkeypatch.setattr(
        country_chat_models,
        "_fetch_nvidia_model_names",
        lambda: [
            "meta/llama-3.3-70b-instruct",
            "mistralai/mistral-large",
        ],
    )
    monkeypatch.setattr(
        country_chat_models,
        "_fetch_gemini_model_names",
        lambda: ["gemini-2.5-flash", "gemini-2.5-pro"],
    )

    _, execution_chain = country_chat_models.build_country_chat_execution_chain(
        "nvidia:mistralai/mistral-large"
    )

    assert [item.id for item in execution_chain] == [
        "nvidia:mistralai/mistral-large",
        "nvidia:meta/llama-3.3-70b-instruct",
        "gemini:gemini-2.5-flash",
    ]


class _StubUrlopenResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self) -> "_StubUrlopenResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_gemini_request_summarizes_external_search_for_market_context(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        country_chat_service.news_digest_service,
        "_gemini_api_key",
        lambda: "gemini-secret",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_select_context_for_intents",
        lambda *_: {"overviewSummary": {"totalVolume": 1}},
    )
    monkeypatch.setattr(
        country_chat_service.web_search_service,
        "search_market_news",
        lambda **_: [
            {
                "title": "Volvo EX60 production starts",
                "source": "Reuters",
                "publishedAt": "2026-04-22",
                "snippet": "Demand for the new EX60 exceeds expectations.",
                "url": "https://example.test/ex60",
                "provider": "test",
            }
        ],
    )
    captured: dict[str, object] = {}

    def _fake_urlopen(request, timeout):  # noqa: ANN001
        captured["request_body"] = json.loads(request.data.decode("utf-8"))
        return _StubUrlopenResponse(
            {
                "candidates": [
                    {
                        "content": {
                            "parts": [{"text": "gemini ok"}],
                        }
                    }
                ]
            }
        )

    monkeypatch.setattr(country_chat_service, "urlopen", _fake_urlopen)

    result = country_chat_service._answer_with_gemini(
        country="瑞典",
        question="最近瑞典新能源补贴和关税政策有什么变化？",
        intents=["market-context"],
        user_params={},
        snapshot={},
        history=[],
        chat_model="gemini-2.5-flash",
    )

    assert result == "gemini ok"
    assert "tools" not in captured["request_body"]
    prompt = captured["request_body"]["contents"][0]["parts"][0]["text"]
    assert "Volvo EX60 production starts" in prompt


def test_gemini_market_context_returns_search_results_when_summary_times_out(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        country_chat_service.news_digest_service,
        "_gemini_api_key",
        lambda: "gemini-secret",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_select_context_for_intents",
        lambda *_: {"overviewSummary": {"totalVolume": 1}},
    )
    monkeypatch.setattr(
        country_chat_service.web_search_service,
        "search_market_news",
        lambda **_: [
            {
                "title": "Volvo EX60 production starts",
                "source": "Reuters",
                "publishedAt": "2026-04-22",
                "snippet": "Demand for the new EX60 exceeds expectations.",
                "url": "https://example.test/ex60",
                "provider": "test",
            }
        ],
    )
    monkeypatch.setattr(
        country_chat_service,
        "urlopen",
        lambda *_, **__: (_ for _ in ()).throw(TimeoutError("timeout")),
    )

    result = country_chat_service._answer_with_gemini(
        country="瑞典",
        question="最近瑞典 Volvo EX60 有什么新闻？",
        intents=["market-context"],
        user_params={},
        snapshot={},
        history=[],
        chat_model="gemini-flash-latest",
    )

    assert "Volvo EX60 production starts" in result
    assert "模型总结超时" in result


def test_gemini_request_skips_google_search_for_regular_snapshot_question(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        country_chat_service.news_digest_service,
        "_gemini_api_key",
        lambda: "gemini-secret",
    )
    monkeypatch.setattr(
        country_chat_service,
        "_select_context_for_intents",
        lambda *_: {"segmentMatrix": {"rows": []}},
    )
    captured: dict[str, object] = {}

    def _fake_urlopen(request, timeout):  # noqa: ANN001
        captured["request_body"] = json.loads(request.data.decode("utf-8"))
        return _StubUrlopenResponse(
            {
                "candidates": [
                    {
                        "content": {
                            "parts": [{"text": "gemini ok"}],
                        }
                    }
                ]
            }
        )

    monkeypatch.setattr(country_chat_service, "urlopen", _fake_urlopen)

    result = country_chat_service._answer_with_gemini(
        country="瑞典",
        question="SUV 细分市场分析",
        intents=["segment-analysis"],
        intent_route="market-scan-scope",
        user_params={},
        snapshot={"newsDigest": {"headline": "old"}, "marketEvents": []},
        history=[],
        chat_model="gemini-2.5-flash",
    )

    assert result == "gemini ok"
    assert "tools" not in captured["request_body"]
