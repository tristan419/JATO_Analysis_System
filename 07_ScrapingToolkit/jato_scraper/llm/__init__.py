"""LLM helpers for future scraping and review automation workflows.

Current providers:
  - Hugging Face Inference Providers via
    https://router.huggingface.co/v1/chat/completions
  - NVIDIA NIM via
    https://integrate.api.nvidia.com/v1/chat/completions
"""

from jato_scraper.llm.client import (
    BaseChatClient,
    ChatChoice,
    ChatMessage,
    ChatResponse,
    ChatUsage,
    LlmApiError,
    OpenAICompatibleChatClient,
)
from jato_scraper.llm.msrp_page_analyzer import (
  analyze_page_evidence,
  build_page_evidence,
  fetch_page_html,
  parse_json_object,
)
from jato_scraper.llm.providers import (
    HuggingFaceChatClient,
    NvidiaChatClient,
    create_chat_client,
)

__all__ = [
    "BaseChatClient",
    "analyze_page_evidence",
    "build_page_evidence",
    "ChatChoice",
    "ChatMessage",
    "ChatResponse",
    "ChatUsage",
    "fetch_page_html",
    "HuggingFaceChatClient",
    "LlmApiError",
    "NvidiaChatClient",
    "OpenAICompatibleChatClient",
    "parse_json_object",
    "create_chat_client",
]
