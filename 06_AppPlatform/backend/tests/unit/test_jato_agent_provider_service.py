from __future__ import annotations

from app.services import jato_agent_provider_service as provider


def test_parse_answer_json_handles_fenced_json_block() -> None:
    parsed = provider._parse_answer_json(
        """```json
{"title": "JATO answer", "direct": "DPV4 final answer", "bullets": ["A"], "limitations": []}
```"""
    )

    assert parsed == {
        "title": "JATO answer",
        "direct": "DPV4 final answer",
        "bullets": ["A"],
        "limitations": [],
    }


def test_parse_answer_json_preserves_follow_ups() -> None:
    parsed = provider._parse_answer_json(
        """```json
{"title": "JATO answer", "direct": "DPV4 final answer", "bullets": ["A"], "limitations": [], "followUps": ["继续看品牌贡献？", "对比挪威？"]}
```"""
    )

    assert parsed is not None
    assert parsed["followUps"] == ["继续看品牌贡献？", "对比挪威？"]


def test_parse_answer_json_extracts_balanced_object_from_wrapped_text() -> None:
    parsed = provider._parse_answer_json(
        '模型输出：{"title": "Analysis", "direct": "含有 {braces} 的回答", "bullets": [], "limitations": []}谢谢'
    )

    assert parsed is not None
    assert parsed["title"] == "Analysis"
    assert parsed["direct"] == "含有 {braces} 的回答"


def test_parse_answer_json_prefers_answer_object_over_tool_args_without_marker() -> None:
    parsed = provider._parse_answer_json(
        """TOOL: query_country_snapshot
ARGS: {"country": "Sweden", "question": "BEV market"}
REASON: need market data
Here is the final JSON:
{"title": "Sweden BEV", "direct": "瑞典 BEV 份额继续处于高位。", "bullets": ["BEV mix is strong"], "limitations": []}"""
    )

    assert parsed is not None
    assert parsed["title"] == "Sweden BEV"
    assert parsed["direct"] == "瑞典 BEV 份额继续处于高位。"
    assert "country" not in parsed


def test_parse_answer_json_skips_invalid_fenced_block_and_uses_later_valid_json() -> None:
    parsed = provider._parse_answer_json(
        """```json
{"title": "Broken", "direct": }
```
模型随后修正：
{"title": "Fixed", "direct": "已使用后续有效 JSON。", "bullets": [], "limitations": []}"""
    )

    assert parsed is not None
    assert parsed["title"] == "Fixed"
    assert parsed["direct"] == "已使用后续有效 JSON。"


def test_parse_answer_json_accepts_common_trailing_comma_output() -> None:
    parsed = provider._parse_answer_json(
        """FINAL_ANSWER:
{
  "title": "Trailing comma",
  "direct": "模型多输出了尾逗号。",
  "bullets": ["A"],
  "limitations": [],
}"""
    )

    assert parsed is not None
    assert parsed["title"] == "Trailing comma"
    assert parsed["direct"] == "模型多输出了尾逗号。"


def test_parse_answer_json_accepts_smart_quote_keys() -> None:
    parsed = provider._parse_answer_json(
        "FINAL_ANSWER:\n{“title”: “Smart quotes”, “direct”: “可以解析智能引号。”, “bullets”: [], “limitations”: []}"
    )

    assert parsed is not None
    assert parsed["title"] == "Smart quotes"
    assert parsed["direct"] == "可以解析智能引号。"


def test_parse_agent_json_handles_final_answer_bold_json() -> None:
    parsed = provider._parse_agent_json(
        'FINAL_ANSWER:\n**{"title": "Analysis", "direct": "瑞典 BEV 份额上升", "bullets": ["证据 A"], "limitations": []}**'
    )

    assert parsed is not None
    assert parsed["title"] == "Analysis"
    assert parsed["direct"] == "瑞典 BEV 份额上升"
    assert parsed["bullets"] == ["证据 A"]


def test_parse_agent_json_uses_last_final_answer_marker() -> None:
    parsed = provider._parse_agent_json(
        """FORMAT EXAMPLE:
FINAL_ANSWER:
{"title": "..."}
FINAL_ANSWER:
{"title": "Actual", "direct": "最终答案", "bullets": [], "limitations": []}"""
    )

    assert parsed is not None
    assert parsed["title"] == "Actual"
    assert parsed["direct"] == "最终答案"


def test_parse_agent_json_keeps_tool_selection_json() -> None:
    parsed = provider._parse_agent_json(
        """```json
{"primary_tool": "build_market_chart", "secondary_tool": null, "mode": "chart", "reasoning": "trend request", "confidence": "high"}
```"""
    )

    assert parsed is not None
    assert parsed["primary_tool"] == "build_market_chart"
    assert parsed["mode"] == "chart"


def test_parse_agent_json_fallback_filters_control_lines_after_final_answer() -> None:
    parsed = provider._parse_agent_json(
        """TOOL: query_country_snapshot
ARGS: {"country": "Sweden"}
REASON: need market data
FINAL_ANSWER:
工具没有返回足够证据，无法确认结论。"""
    )

    assert parsed is not None
    assert parsed["direct"] == "工具没有返回足够证据，无法确认结论。"
    assert "TOOL:" not in parsed["direct"]
    assert "ARGS:" not in parsed["direct"]


def test_clean_answer_text_fallback_filters_control_lines_without_marker() -> None:
    direct = provider._clean_answer_text_fallback(
        """TOOL: query_country_snapshot
ARGS: {"country": "Sweden"}
REASON: need market data
无法确认，因为工具没有返回数据。"""
    )

    assert direct == "无法确认，因为工具没有返回数据。"
