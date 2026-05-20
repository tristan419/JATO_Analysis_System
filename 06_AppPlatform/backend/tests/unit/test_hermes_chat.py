from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient
from app.services.hermes_chat_service import HermesIntentRouter, create_session, get_session, add_message, list_sessions

@pytest.fixture
def router_inst():
    return HermesIntentRouter()

@pytest.fixture
def client():
    from fastapi import FastAPI
    from app.api.routes.hermes import router
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class TestIntentRouter:
    def test_gap_query(self, router_inst):
        r = router_inst.classify("show open governance gaps", {})
        assert r["intent"] == "gap_query"
        assert r["executionMode"] == "direct_answer"

    def test_cost_query(self, router_inst):
        assert router_inst.classify("what is the cost status", {})["intent"] == "cost_query"

    def test_source_audit_create_run(self, router_inst):
        r = router_inst.classify("run source audit for Sweden", {})
        assert r["intent"] == "source_audit"
        assert r["executionMode"] == "create_run"

    def test_unknown_clarification(self, router_inst):
        r = router_inst.classify("xyzzy blarg", {})
        assert r["executionMode"] == "clarification_needed"

    def test_dev_request_blocked(self, router_inst):
        r = router_inst.classify("deploy to production", {"userRole": "user"})
        assert r["executionMode"] == "blocked_by_policy"

    def test_entity_extraction(self, router_inst):
        r = router_inst.classify("run source audit for Sweden", {})
        assert "Sweden" in r.get("entities", {}).get("country", [])

    def test_chinese_message(self, router_inst):
        r = router_inst.classify("显示所有 governance gaps 和漏洞问题", {})
        assert r["intent"] == "gap_query"


class TestChatEndpoint:
    def test_post_chat_direct_answer(self, client, tmp_path):
        with patch("app.api.routes.hermes.HERMES_DIR", tmp_path), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.post("/hermes/chat", json={"message": "show open governance gaps"})
        assert resp.status_code == 200
        assert resp.json()["replyType"] == "direct_answer"

    def test_post_chat_run_created(self, client, tmp_path):
        with patch("app.api.routes.hermes.HERMES_DIR", tmp_path), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.post("/hermes/chat", json={"message": "run source audit for Sweden"})
        assert resp.status_code == 200
        assert resp.json()["replyType"] == "run_created"

    def test_empty_message_400(self, client, tmp_path):
        with patch("app.api.routes.hermes.HERMES_DIR", tmp_path), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            assert client.post("/hermes/chat", json={"message": ""}).status_code == 400


class TestSessions:
    def test_create_and_get(self, tmp_path):
        with patch("app.services.hermes_chat_service._get_sessions_dir", return_value=tmp_path):
            s = create_session()
            assert get_session(s["sessionId"]) is not None

    def test_add_message(self, tmp_path):
        with patch("app.services.hermes_chat_service._get_sessions_dir", return_value=tmp_path):
            s = create_session()
            add_message(s["sessionId"], {"messageId": "m1", "role": "user", "content": "hi", "timestamp": ""})
            assert len(get_session(s["sessionId"])["messages"]) == 1

    def test_list_sessions(self, tmp_path):
        with patch("app.services.hermes_chat_service._get_sessions_dir", return_value=tmp_path):
            create_session()
            assert len(list_sessions(10)) == 1


class TestCommands:
    def test_list_commands(self, client):
        resp = client.get("/hermes/commands")
        assert resp.status_code == 200
        assert len(resp.json()) >= 4

    def test_command_execute_unknown_400(self, client, monkeypatch, tmp_path):
        monkeypatch.setenv("HERMES_RUN_ENABLED", "true")
        assert client.post("/hermes/commands/execute", json={"commandId": "nonexistent"}).status_code == 400
