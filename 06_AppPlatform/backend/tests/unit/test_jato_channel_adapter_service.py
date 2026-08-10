from __future__ import annotations

import json

import pytest

from app.services import jato_channel_adapter_service as channel_service
from app.services import jato_conversation_store


def _fake_agent_result(selected_tool: str = "query_country_snapshot") -> dict[str, object]:
    return {
        "tool": "route_agent_request",
        "metadata": {
            "selectedTool": selected_tool,
            "routeSource": "test",
            "profileId": "pm_coder_market_assistant",
            "skillId": "auto_route",
            "modelUsageStatus": "disabled",
        },
        "data": {
            "answer": {
                "title": "Grounded answer",
                "direct": "Sweden market answer.",
                "bullets": ["Evidence bullet"],
                "limitations": [],
                "citations": [{"label": "JATO", "source": "test", "tool": selected_tool}],
            },
            "secondaryResults": [],
        },
    }


def test_handle_mock_channel_message_routes_to_agent_and_audits(tmp_path, monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_call_jato_mcp_tool(name: str, arguments: dict[str, object]):
        captured["name"] = name
        captured["arguments"] = arguments
        return _fake_agent_result()

    monkeypatch.setattr(channel_service, "_CHANNEL_AUDIT_DIR", tmp_path / "channel_messages")
    monkeypatch.setattr(jato_conversation_store, "_CONV_DIR", tmp_path / "agent_conversations")
    monkeypatch.setattr(channel_service, "call_jato_mcp_tool", fake_call_jato_mcp_tool)

    reply = channel_service.handle_mock_channel_message(
        {
            "channel": "mock",
            "channelUserId": "u-1",
            "channelConversationId": "conv-1",
            "jatoUserName": "tester",
            "country": "Sweden",
            "text": "Summarize Sweden BEV market",
            "metadata": {"source": "unit", "apiToken": "secret-value"},
        }
    )

    assert captured["name"] == "route_agent_request"
    assert captured["arguments"] == {
        "country": "Sweden",
        "question": "Summarize Sweden BEV market",
        "skill_id": "",
        "mode": "",
        "include_secondary_paths": True,
    }
    assert reply["channel"] == "mock"
    assert reply["sessionId"] == "chan_mock_conv-1"
    assert reply["selectedTool"] == "query_country_snapshot"
    assert "Sweden market answer." in reply["text"]
    assert reply["auditId"].startswith("chaudit_")

    audit_path = tmp_path / "channel_messages" / "channel_messages.jsonl"
    records = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 1
    assert records[0]["status"] == "ok"
    assert records[0]["inbound"]["metadata"]["apiToken"] == "[redacted]"
    assert "secret-value" not in str(records[0])

    history = jato_conversation_store.get_history("chan_mock_conv-1")
    assert history["totalTurns"] == 2
    assert [turn["role"] for turn in history["turns"]] == ["user", "assistant"]


def test_handle_mock_channel_message_rejects_attachments_and_audits(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(channel_service, "_CHANNEL_AUDIT_DIR", tmp_path / "channel_messages")

    with pytest.raises(ValueError, match="Attachments are not supported"):
        channel_service.handle_mock_channel_message(
            {
                "channel": "mock",
                "channelUserId": "u-1",
                "channelConversationId": "conv-1",
                "text": "Read this file",
                "attachments": [{"name": "report.pdf"}],
            }
        )

    audit_path = tmp_path / "channel_messages" / "channel_messages.jsonl"
    records = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert records[0]["status"] == "rejected"
    assert records[0]["inbound"]["attachmentCount"] == 1


def test_read_channel_adapter_status_lists_mock_and_planned_channels(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(channel_service, "_CHANNEL_AUDIT_DIR", tmp_path)

    status = channel_service.read_channel_adapter_status()

    assert status["status"] == "mock_enabled"
    assert any(adapter["id"] == "mock" and adapter["status"] == "enabled" for adapter in status["adapters"])
    assert any(adapter["id"] == "work_wechat" and adapter["status"] == "planned" for adapter in status["adapters"])
