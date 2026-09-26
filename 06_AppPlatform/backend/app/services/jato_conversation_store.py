"""Multi-turn conversation memory for the JATO Agent.

Stores conversations per session as JSONL, injects history into the agent loop context.
Integrates with Hermes memory/audit layers via session_id tracking.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_CONV_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "hermes" / "agent_conversations"
_MAX_TURNS = 20  # Max turns to keep per session
_MAX_CONTEXT_TURNS = 6  # Max recent turns injected into agent context


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_file(session_id: str) -> Path:
    _CONV_DIR.mkdir(parents=True, exist_ok=True)
    safe = session_id.replace("/", "_").replace("..", "_") or "default"
    return _CONV_DIR / f"{safe}.jsonl"


def create_session() -> str:
    """Create a new conversation session and return its ID."""
    return f"sess_{uuid.uuid4().hex[:12]}"


def add_turn(
    session_id: str,
    role: str,  # "user" or "assistant"
    text: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Add a conversation turn to the session."""
    turn = {
        "turnId": f"turn_{uuid.uuid4().hex[:8]}",
        "sessionId": session_id,
        "role": role,
        "text": text[:2000],
        "metadata": metadata or {},
        "recordedAt": _now_iso(),
    }
    path = _session_file(session_id)
    turns = _read_turns(path)
    turns.append(turn)
    # Keep only the last N turns
    turns = turns[-_MAX_TURNS:]
    with open(path, "w", encoding="utf-8") as fh:
        for t in turns:
            fh.write(json.dumps(t, ensure_ascii=False) + "\n")
    return turn


def get_context(
    session_id: str,
    max_turns: int = _MAX_CONTEXT_TURNS,
) -> str:
    """Get recent conversation turns formatted for LLM context injection."""
    path = _session_file(session_id)
    turns = _read_turns(path)
    recent = turns[-max_turns:]
    if not recent:
        return ""
    lines = ["Previous conversation:"]
    for t in recent:
        role_label = "User" if t["role"] == "user" else "Assistant"
        lines.append(f"{role_label}: {t['text'][:500]}")
    return "\n".join(lines)


def get_history(
    session_id: str,
    limit: int = 20,
) -> dict[str, Any]:
    """Get conversation history for display/audit."""
    path = _session_file(session_id)
    turns = _read_turns(path)
    return {
        "sessionId": session_id,
        "turns": turns[-limit:],
        "totalTurns": len(turns),
    }


def list_sessions(limit: int = 20) -> list[dict[str, Any]]:
    """List recent conversation sessions."""
    sessions: list[dict[str, Any]] = []
    if not _CONV_DIR.exists():
        return sessions
    for f in sorted(_CONV_DIR.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True):
        turns = _read_turns(f)
        if turns:
            first = turns[0]
            last = turns[-1]
            sessions.append({
                "sessionId": first.get("sessionId", f.stem),
                "startedAt": first.get("recordedAt", ""),
                "lastActivityAt": last.get("recordedAt", ""),
                "turnCount": len(turns),
                **_summarize_session_turns(turns),
            })
    return sessions[:limit]


def _read_turns(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    turns: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                turns.append(json.loads(stripped))
            except json.JSONDecodeError:
                continue
    return turns


def _summarize_session_turns(turns: list[dict[str, Any]]) -> dict[str, Any]:
    latest_user = _latest_turn_by_role(turns, "user")
    latest_assistant = _latest_turn_by_role(turns, "assistant")
    assistant_metadata = latest_assistant.get("metadata") if latest_assistant else {}
    metadata = assistant_metadata if isinstance(assistant_metadata, dict) else {}
    latest_question = _trim_text(latest_user.get("text", "") if latest_user else "", limit=180)
    latest_answer = _trim_text(latest_assistant.get("text", "") if latest_assistant else "", limit=220)
    tool_calls = metadata.get("toolCalls")
    return {
        "country": _trim_text(metadata.get("country") or metadata.get("requestedCountry") or "", limit=48),
        "latestQuestion": latest_question,
        "latestAnswerPreview": latest_answer,
        "latestAnswerTitle": _trim_text(metadata.get("answerTitle") or metadata.get("summary") or "", limit=120),
        "answerStatus": _trim_text(metadata.get("answerStatus") or metadata.get("status") or "", limit=48),
        "confidence": _trim_text(metadata.get("confidence") or "", limit=32),
        "toolCalls": [str(tool) for tool in tool_calls[:6]] if isinstance(tool_calls, list) else [],
    }


def _latest_turn_by_role(turns: list[dict[str, Any]], role: str) -> dict[str, Any] | None:
    for turn in reversed(turns):
        if turn.get("role") == role:
            return turn
    return None


def _trim_text(value: Any, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 1)].rstrip()}…"
