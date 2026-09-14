from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import astrbot_tools
from app.api.routes.astrbot_tools import channel_router
from app.core.config import API_PREFIX


def test_mock_channel_message_route_returns_normalized_reply(monkeypatch) -> None:
    def fake_handle_mock_channel_message(message: dict[str, object]):
        assert message["channel"] == "mock"
        assert message["text"] == "Hello from channel"
        return {
            "replyId": "reply_test",
            "auditId": "chaudit_test",
            "channel": "mock",
            "channelUserId": message["channelUserId"],
            "channelConversationId": message["channelConversationId"],
            "sessionId": "chan_mock_conv",
            "text": "Mock answer",
            "answer": {"title": "Answer", "direct": "Mock answer", "bullets": [], "limitations": []},
            "selectedTool": "query_country_snapshot",
            "toolCount": 1,
            "citations": [],
            "metadata": {},
        }

    monkeypatch.setattr(astrbot_tools, "handle_mock_channel_message", fake_handle_mock_channel_message)

    app = FastAPI()
    app.include_router(channel_router, prefix=API_PREFIX)
    client = TestClient(app)

    response = client.post(
        "/v1/astrbot/channels/mock/message",
        json={
            "channelUserId": "u-1",
            "channelConversationId": "conv-1",
            "text": "Hello from channel",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["replyId"] == "reply_test"
    assert payload["text"] == "Mock answer"
    assert payload["selectedTool"] == "query_country_snapshot"


def test_channel_status_route_returns_adapters() -> None:
    app = FastAPI()
    app.include_router(channel_router, prefix=API_PREFIX)
    client = TestClient(app)

    response = client.get("/v1/astrbot/channels/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "mock_enabled"
    assert any(adapter["id"] == "mock" for adapter in payload["adapters"])
