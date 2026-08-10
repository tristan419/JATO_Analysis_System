from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.services.jato_conversation_store import add_turn
from app.services.jato_mcp_tools_service import call_jato_mcp_tool


_CHANNEL_AUDIT_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "hermes" / "channel_messages"
_CHANNEL_AUDIT_FILE_NAME = "channel_messages.jsonl"
_SUPPORTED_MOCK_CHANNELS = {"mock"}
_PLANNED_CHANNELS = ("work_wechat", "wechat_official", "qq", "feishu")
_SECRET_KEY_FRAGMENTS = ("key", "token", "secret", "password", "credential")


def read_channel_adapter_status() -> dict[str, Any]:
    adapters = [
        {
            "id": "mock",
            "name": "Mock Channel",
            "status": "enabled",
            "inbound": True,
            "outbound": True,
            "endpoint": "/v1/astrbot/channels/mock/message",
            "notes": "Local normalized-message adapter for testing channel integration.",
        }
    ]
    adapters.extend(
        {
            "id": channel,
            "name": _display_channel(channel),
            "status": "planned",
            "inbound": False,
            "outbound": False,
            "endpoint": "",
            "notes": "Requires platform credentials, signature verification, user mapping, and rate limits.",
        }
        for channel in _PLANNED_CHANNELS
    )
    return {
        "status": "mock_enabled",
        "auditLog": str(_audit_file_path()),
        "adapters": adapters,
        "limitations": [
            "Only the mock adapter is enabled.",
            "Attachments are blocked until document-ingestion policy is defined.",
            "Real WeChat/QQ sending is not enabled.",
        ],
    }


def handle_mock_channel_message(message: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_message(message, forced_channel="mock")
    if normalized["attachments"]:
        audit = _write_audit_record(
            normalized,
            status="rejected",
            reply=None,
            error="Attachments are not supported by the mock channel adapter.",
        )
        raise ValueError(f"Attachments are not supported by the mock channel adapter. auditId={audit['auditId']}")

    add_turn(
        normalized["sessionId"],
        "user",
        normalized["text"],
        {
            "channel": normalized["channel"],
            "channelUserId": normalized["channelUserId"],
            "channelConversationId": normalized["channelConversationId"],
            "jatoUserName": normalized["jatoUserName"],
        },
    )

    agent_result = call_jato_mcp_tool(
        "route_agent_request",
        {
            "country": normalized["country"],
            "question": normalized["text"],
            "skill_id": normalized["skillId"],
            "mode": normalized["mode"],
            "include_secondary_paths": normalized["includeSecondaryPaths"],
        },
    )
    reply = _build_reply(normalized, agent_result)

    add_turn(
        normalized["sessionId"],
        "assistant",
        reply["text"],
        {
            "channel": normalized["channel"],
            "replyId": reply["replyId"],
            "selectedTool": reply["selectedTool"],
            "toolCount": reply["toolCount"],
            "citations": reply["citations"],
        },
    )
    audit = _write_audit_record(normalized, status="ok", reply=reply, error="")
    reply["auditId"] = audit["auditId"]
    return reply


def list_channel_audit_records(limit: int = 20) -> dict[str, Any]:
    path = _audit_file_path()
    records = _read_jsonl(path)
    records.reverse()
    safe_limit = max(1, min(int(limit or 20), 100))
    return {
        "items": records[:safe_limit],
        "total": len(records),
        "limit": safe_limit,
    }


def _normalize_message(message: dict[str, Any], *, forced_channel: str) -> dict[str, Any]:
    raw_channel = _text(message.get("channel")) or forced_channel
    channel = forced_channel or raw_channel
    if channel not in _SUPPORTED_MOCK_CHANNELS:
        raise ValueError(f"Unsupported channel adapter: {channel}")

    text = _text(message.get("text"))
    if not text:
        raise ValueError("text is required")

    channel_user_id = _text(message.get("channelUserId")) or _text(message.get("channel_user_id")) or "mock-user"
    channel_conversation_id = (
        _text(message.get("channelConversationId"))
        or _text(message.get("channel_conversation_id"))
        or f"mock-conv-{channel_user_id}"
    )
    country = _text(message.get("country")) or "Sweden"
    metadata = message.get("metadata") if isinstance(message.get("metadata"), dict) else {}
    return {
        "messageId": _text(message.get("messageId")) or f"msg_{uuid.uuid4().hex[:12]}",
        "channel": channel,
        "channelUserId": channel_user_id,
        "channelConversationId": channel_conversation_id,
        "jatoUserName": _text(message.get("jatoUserName")) or _text(message.get("jato_user_name")) or "channel_mock_user",
        "text": text,
        "country": country,
        "skillId": _text(message.get("skillId")) or _text(message.get("skill_id")),
        "mode": _text(message.get("mode")),
        "includeSecondaryPaths": _bool_value(message.get("includeSecondaryPaths"), default=True),
        "attachments": message.get("attachments") if isinstance(message.get("attachments"), list) else [],
        "metadata": _redact_value(metadata),
        "sessionId": _channel_session_id(channel, channel_conversation_id),
        "receivedAt": _text(metadata.get("receivedAt")) or _now_iso(),
    }


def _build_reply(message: dict[str, Any], agent_result: dict[str, Any]) -> dict[str, Any]:
    data = agent_result.get("data") if isinstance(agent_result.get("data"), dict) else {}
    answer = data.get("answer") if isinstance(data.get("answer"), dict) else {}
    metadata = agent_result.get("metadata") if isinstance(agent_result.get("metadata"), dict) else {}
    text = _reply_text(answer)
    selected_tool = str(metadata.get("selectedTool") or answer.get("tool") or "")
    citations = answer.get("citations") if isinstance(answer.get("citations"), list) else []
    secondary_results = data.get("secondaryResults") if isinstance(data.get("secondaryResults"), list) else []
    return {
        "replyId": f"reply_{uuid.uuid4().hex[:12]}",
        "channel": message["channel"],
        "channelUserId": message["channelUserId"],
        "channelConversationId": message["channelConversationId"],
        "sessionId": message["sessionId"],
        "text": text,
        "answer": {
            "title": answer.get("title") or "AstrBot reply",
            "direct": answer.get("direct") or text,
            "bullets": answer.get("bullets") if isinstance(answer.get("bullets"), list) else [],
            "limitations": answer.get("limitations") if isinstance(answer.get("limitations"), list) else [],
        },
        "selectedTool": selected_tool,
        "toolCount": 1 + len([item for item in secondary_results if item.get("status") == "executed"]),
        "citations": citations[:5],
        "metadata": {
            "routeSource": metadata.get("routeSource"),
            "profileId": metadata.get("profileId"),
            "skillId": metadata.get("skillId"),
            "modelUsageStatus": metadata.get("modelUsageStatus"),
        },
    }


def _reply_text(answer: dict[str, Any]) -> str:
    direct = _text(answer.get("direct"))
    bullets = answer.get("bullets") if isinstance(answer.get("bullets"), list) else []
    limitations = answer.get("limitations") if isinstance(answer.get("limitations"), list) else []
    parts = [direct] if direct else []
    parts.extend(f"- {_text(item)}" for item in bullets[:3] if _text(item))
    if limitations:
        parts.append("限制：" + "；".join(_text(item) for item in limitations[:2] if _text(item)))
    return "\n".join(parts).strip() or "当前没有足够证据生成回复。"


def _write_audit_record(
    message: dict[str, Any],
    *,
    status: str,
    reply: dict[str, Any] | None,
    error: str,
) -> dict[str, Any]:
    record = {
        "auditId": f"chaudit_{uuid.uuid4().hex[:12]}",
        "recordedAt": _now_iso(),
        "status": status,
        "channel": message["channel"],
        "channelUserId": message["channelUserId"],
        "channelConversationId": message["channelConversationId"],
        "jatoUserName": message["jatoUserName"],
        "country": message["country"],
        "sessionId": message["sessionId"],
        "inbound": {
            "messageId": message["messageId"],
            "textPreview": message["text"][:240],
            "attachmentCount": len(message["attachments"]),
            "metadata": message["metadata"],
        },
        "outbound": _audit_reply(reply),
        "error": error[:500] if error else "",
    }
    path = _audit_file_path()
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record


def _audit_reply(reply: dict[str, Any] | None) -> dict[str, Any]:
    if not reply:
        return {}
    return {
        "replyId": reply.get("replyId"),
        "textPreview": str(reply.get("text") or "")[:240],
        "selectedTool": reply.get("selectedTool"),
        "toolCount": reply.get("toolCount"),
        "citationCount": len(reply.get("citations") or []),
    }


def _audit_file_path() -> Path:
    _CHANNEL_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    return _CHANNEL_AUDIT_DIR / _CHANNEL_AUDIT_FILE_NAME


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                records.append(parsed)
    return records


def _channel_session_id(channel: str, conversation_id: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in conversation_id)
    safe = safe.strip("_")[:80] or "default"
    return f"chan_{channel}_{safe}"


def _redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if any(fragment in key_text.lower() for fragment in _SECRET_KEY_FRAGMENTS):
                result[key_text] = "[redacted]"
            else:
                result[key_text] = _redact_value(item)
        return result
    if isinstance(value, list):
        return [_redact_value(item) for item in value[:20]]
    return value


def _bool_value(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _display_channel(channel: str) -> str:
    return channel.replace("_", " ").title()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _text(value: Any) -> str:
    return str(value or "").strip()
