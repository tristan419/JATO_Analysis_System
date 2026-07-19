from __future__ import annotations

from app.services import jato_agent_llm_judge_service as judge_service
from app.services import jato_eval_service as eval_service


def _path_by_id(matrix: dict, path_id: str) -> dict:
    paths = matrix.get("paths") if isinstance(matrix.get("paths"), list) else []
    for path in paths:
        if path.get("id") == path_id:
            return path
    raise AssertionError(f"reference path missing: {path_id}")


def test_reference_judge_paths_expose_gpt_path_without_secret(monkeypatch) -> None:
    monkeypatch.setenv("APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED", "true")
    monkeypatch.setenv(judge_service.ASTRBOT_JUDGE_KEY_ENV, "sk-test-should-not-leak")
    monkeypatch.delenv("APP_ASTRBOT_OPUS48_JUDGE_MODEL", raising=False)
    monkeypatch.delenv("APP_ASTRBOT_FABLE5_JUDGE_MODEL", raising=False)

    matrix = judge_service.list_reference_judge_paths()

    assert matrix["source"] == "jato_agent_llm_judge_service"
    gpt_path = _path_by_id(matrix, "gpt5_5")
    assert gpt_path["label"] == "GPT5.5 / GPT Judge"
    assert gpt_path["status"] == "implemented"
    assert gpt_path["keySource"] == judge_service.ASTRBOT_JUDGE_KEY_ENV
    assert gpt_path["keyConfigured"] is True
    assert "sk-test-should-not-leak" not in repr(matrix)
    assert _path_by_id(matrix, "opus_4_8")["status"] == "reference_missing"
    assert _path_by_id(matrix, "fable_5")["status"] == "reference_missing"


def test_reference_judge_paths_track_reserved_opus_configuration(monkeypatch) -> None:
    monkeypatch.setenv("APP_ASTRBOT_OPUS48_JUDGE_PROVIDER_ID", "anthropic")
    monkeypatch.setenv("APP_ASTRBOT_OPUS48_JUDGE_MODEL", "opus-4.8")
    monkeypatch.setenv("APP_ASTRBOT_OPUS48_JUDGE_API_BASE", "https://api.anthropic.example/v1")
    monkeypatch.setenv("APP_ASTRBOT_OPUS48_JUDGE_KEY_ENV", "ANTHROPIC_API_KEY")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "anthropic-test-should-not-leak")

    matrix = judge_service.list_reference_judge_paths()
    opus_path = _path_by_id(matrix, "opus_4_8")

    assert opus_path["status"] == "configured"
    assert opus_path["readinessStatus"] == "configured_inactive"
    assert opus_path["provider"] == "anthropic"
    assert opus_path["model"] == "opus-4.8"
    assert opus_path["keySource"] == "ANTHROPIC_API_KEY"
    assert opus_path["keyConfigured"] is True
    assert "anthropic-test-should-not-leak" not in repr(matrix)


def test_business_validation_summary_includes_reference_judge_paths(tmp_path, monkeypatch) -> None:
    side_by_side_file = tmp_path / "eval_side_by_side.jsonl"
    monkeypatch.setattr(eval_service, "_SIDE_BY_SIDE_FILE", side_by_side_file)

    report = eval_service.get_business_validation_report()

    matrix = report["summary"]["referenceJudgePaths"]
    assert matrix["source"] == "jato_agent_llm_judge_service"
    assert _path_by_id(matrix, "gpt5_5")["status"] == "implemented"
