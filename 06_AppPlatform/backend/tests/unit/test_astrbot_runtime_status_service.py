from __future__ import annotations

from app.services import astrbot_runtime_status_service
from app.services import jato_agent_memory_service


def test_read_astrbot_runtime_status_uses_safe_provider_metadata(monkeypatch) -> None:
    def fake_probe(url: str) -> dict[str, object]:
        return {
            "reachable": True,
            "httpStatus": 200 if "6185" in url else 406,
            "latencyMs": 3,
            "detail": "ok",
        }

    monkeypatch.setenv("DEEPSEEK_API_KEY", "secret-value")
    monkeypatch.setenv("PAGEINDEX_API_KEY", "pageindex-secret")
    monkeypatch.delenv("MINIRAG_API_URL", raising=False)
    monkeypatch.setattr(astrbot_runtime_status_service.jato_minirag_client, "MINIRAG_ENABLED", False)
    monkeypatch.setattr(astrbot_runtime_status_service, "_probe_http_url", fake_probe)

    result = astrbot_runtime_status_service.read_astrbot_runtime_status()

    assert result["runtime"]["status"] == "online"
    assert result["mcp"]["status"] == "online"
    assert result["mcp"]["toolCount"] == len(result["mcp"]["tools"])
    assert result["mcp"]["toolCount"] >= 23
    assert result["provider"]["keySource"] == "$DEEPSEEK_API_KEY"
    assert result["provider"]["keyConfigured"] is True
    assert result["retrieval"]["pageIndex"]["keySource"] == "$PAGEINDEX_API_KEY"
    assert result["retrieval"]["pageIndex"]["keyConfigured"] is True
    assert result["retrieval"]["miniRag"]["status"] == "fallback"
    assert result["channels"]["status"] == "mock_enabled"
    assert any(adapter["id"] == "mock" for adapter in result["channels"]["adapters"])
    assert result["profile"]["id"] == "pm_coder_market_assistant"
    assert "汽车市场分析助手" in result["profile"]["positioning"]
    assert result["skills"]["defaultSkillId"] == "auto_route"
    assert any(item["id"] == "policy_news_scan" for item in result["skills"]["items"])
    assert "secret-value" not in str(result)
    assert "pageindex-secret" not in str(result)


def test_read_astrbot_runtime_status_marks_missing_provider_key(monkeypatch) -> None:
    def fake_probe(url: str) -> dict[str, object]:
        return {
            "reachable": False,
            "httpStatus": None,
            "latencyMs": 2,
            "detail": "connection refused",
        }

    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("PAGEINDEX_API_KEY", raising=False)
    monkeypatch.delenv("MINIRAG_API_URL", raising=False)
    monkeypatch.setattr(astrbot_runtime_status_service.jato_minirag_client, "MINIRAG_ENABLED", False)
    monkeypatch.setattr(astrbot_runtime_status_service, "_probe_http_url", fake_probe)

    result = astrbot_runtime_status_service.read_astrbot_runtime_status()

    assert result["runtime"]["status"] == "offline"
    assert result["mcp"]["status"] == "offline"
    assert result["provider"]["status"] == "missing_key"
    assert result["retrieval"]["pageIndex"]["status"] == "fallback"
    assert result["retrieval"]["miniRag"]["status"] == "fallback"


def test_memory_stats_tolerates_non_utf8_agent_log(monkeypatch, tmp_path) -> None:
    memory_dir = tmp_path / "agent_memory"
    memory_dir.mkdir()
    monkeypatch.setattr(jato_agent_memory_service, "_MEMORY_DIR", memory_dir)

    memory_file = memory_dir / "agent_runs.jsonl"
    memory_file.write_bytes(
        b'{"runId":"run_bad","skillId":"auto_route","country":"Sweden",'
        b'"selectedTool":"query_country_snapshot","createdAt":"2026-06-14T00:00:00+00:00",'
        b'"resultSummary":"bad byte \xe5"}\n'
    )

    result = jato_agent_memory_service.get_memory_stats()

    assert result["totalRuns"] == 1
    assert result["bySkill"]["auto_route"] == 1
    assert result["byCountry"]["Sweden"] == 1
    assert result["byTool"]["query_country_snapshot"] == 1
