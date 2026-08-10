from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes.astrbot_tools import agent_router
from app.core.config import API_PREFIX
from app.services import jato_conversation_store


def test_agent_session_history_returns_conversation_turns(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(jato_conversation_store, "_CONV_DIR", tmp_path)
    session_id = "sess_test_history"
    jato_conversation_store.add_turn(session_id, "user", "Show Sweden BEV trend")
    jato_conversation_store.add_turn(
        session_id,
        "assistant",
        "Sweden BEV trend is available.",
        {
            "answerTitle": "Sweden BEV",
            "bullets": ["BEV is the leading fuel type."],
            "toolCalls": ["build_market_chart"],
            "toolCount": 1,
            "chartCount": 1,
            "charts": [
                {
                    "chartId": "bev_trend",
                    "chartType": "line",
                    "title": "BEV trend",
                    "data": [{"x": ["2025-01"], "y": [42], "type": "scatter"}],
                    "layout": {"height": 320},
                }
            ],
        },
    )

    app = FastAPI()
    app.include_router(agent_router, prefix=API_PREFIX)
    client = TestClient(app)

    response = client.get(f"/v1/astrbot/agent/sessions/{session_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["sessionId"] == session_id
    assert payload["totalTurns"] == 2
    assert [turn["role"] for turn in payload["turns"]] == ["user", "assistant"]
    assert payload["turns"][1]["metadata"]["toolCalls"] == ["build_market_chart"]
    assert payload["turns"][1]["metadata"]["answerTitle"] == "Sweden BEV"
    assert payload["turns"][1]["metadata"]["charts"][0]["chartId"] == "bev_trend"


def test_agent_sessions_lists_recent_conversations(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(jato_conversation_store, "_CONV_DIR", tmp_path)
    jato_conversation_store.add_turn("sess_a", "user", "First")
    jato_conversation_store.add_turn("sess_b", "user", "Second")

    app = FastAPI()
    app.include_router(agent_router, prefix=API_PREFIX)
    client = TestClient(app)

    response = client.get("/v1/astrbot/agent/sessions?limit=5")

    assert response.status_code == 200
    payload = response.json()
    session_ids = {item["sessionId"] for item in payload["items"]}
    assert {"sess_a", "sess_b"}.issubset(session_ids)


def test_agent_sessions_include_readable_latest_context(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(jato_conversation_store, "_CONV_DIR", tmp_path)
    session_id = "sess_hungary_context"
    jato_conversation_store.add_turn(session_id, "user", "匈牙利 J7 HEV 是否值得继续验证？")
    jato_conversation_store.add_turn(
        session_id,
        "assistant",
        "匈牙利市场需要先补齐 HEV SUV A0/A 证据。",
        {
            "country": "Hungary",
            "answerTitle": "Hungary J7 HEV validation",
            "answerStatus": "answered",
            "confidence": "high",
            "toolCalls": ["query_country_snapshot", "external_research"],
        },
    )

    app = FastAPI()
    app.include_router(agent_router, prefix=API_PREFIX)
    client = TestClient(app)

    response = client.get("/v1/astrbot/agent/sessions?limit=5")

    assert response.status_code == 200
    session = next(item for item in response.json()["items"] if item["sessionId"] == session_id)
    assert session["country"] == "Hungary"
    assert session["latestQuestion"] == "匈牙利 J7 HEV 是否值得继续验证？"
    assert session["latestAnswerTitle"] == "Hungary J7 HEV validation"
    assert session["answerStatus"] == "answered"
    assert session["confidence"] == "high"
    assert session["toolCalls"] == ["query_country_snapshot", "external_research"]
