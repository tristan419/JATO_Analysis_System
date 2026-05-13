"""Answer Composer — builds structured answers from available data.

No keyword matching. Data presence alone determines which blocks appear.
The DeepSeek text answer handles relevance and format flexibility.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class AnswerBlock(BaseModel):
    block_type: str = "summary"
    title: str = ""
    content: str = ""
    data: dict[str, Any] | None = None


class StructuredAnswer(BaseModel):
    summary: str = ""
    blocks: list[AnswerBlock] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)
    answer_mode: str = "quick_answer"


def compose_answer(
    evidence_pack=None,
    source_plan=None,
    snapshot: dict[str, Any] | None = None,
    country: str = "",
    question: str = "",
) -> StructuredAnswer:
    snapshot = snapshot or {}
    blocks: list[AnswerBlock] = []
    limitations: list[str] = []
    recommendations: list[str] = []

    # ── Summary (always) ──
    overview = snapshot.get("overviewSummary", {})
    kpis = snapshot.get("kpis", {})
    summary_text = overview.get("headline") or f"{country}：{kpis.get('brandCount', '?')} 品牌，{kpis.get('modelCount', '?')} 车型"
    blocks.append(AnswerBlock(block_type="summary", title="市场概况", content=summary_text))

    # ── Data-driven blocks: include if data exists ──
    top_brands = snapshot.get("topBrands", [])
    if isinstance(top_brands, list) and len(top_brands) >= 2:
        rows = "\n".join(f"| {b.get('label', '?')} | {int(b.get('value', 0)):,} |" for b in top_brands[:5])
        blocks.append(AnswerBlock(block_type="table", title="品牌排名", content=f"| 品牌 | 销量 |\n| --- | --- |\n{rows}"))

    powertrain = snapshot.get("powertrainMix", [])
    if isinstance(powertrain, list) and len(powertrain) >= 2:
        rows = "\n".join(f"| {p.get('label', '?')} | {int(p.get('value', 0)):,} |" for p in powertrain[:6])
        blocks.append(AnswerBlock(block_type="table", title="动力结构", content=f"| 动力 | 销量 |\n| --- | --- |\n{rows}"))

    cross_tabs = snapshot.get("crossTabs", {})
    if isinstance(cross_tabs, dict):
        for ct_key, ct_title in [("driveByFuel", "驱动 × 动力"), ("registrationByFuel", "注册 × 动力")]:
            data = cross_tabs.get(ct_key, [])
            if data:
                lines = [f"- {r.get('_index', '?')}：4WD **{r.get('4WD_pct', r.get('Business_pct', 0))}%**" for r in data[:5]]
                blocks.append(AnswerBlock(block_type="evidence", title=ct_title, content="\n".join(lines) if lines else "无数据"))

    segment = snapshot.get("segmentMatrix", {})
    seg_rows = segment.get("rows", []) if isinstance(segment, dict) else []
    if seg_rows:
        rows = "\n".join(f"| {r.get('segment', '?')} | {int(r.get('currentMonth', 0)):,} |" for r in seg_rows[:8])
        blocks.append(AnswerBlock(block_type="table", title="细分市场", content=f"| 细分 | 当月销量 |\n| --- | --- |\n{rows}"))

    # ── Evidence sources ──
    if evidence_pack:
        sources = getattr(evidence_pack, "sources", [])
        if sources:
            src_lines = [f"- {s.source_name or s.source_id} ({s.coverage})" for s in sources]
            blocks.append(AnswerBlock(block_type="evidence", title="证据来源", content="\n".join(src_lines)))
        for lim in getattr(evidence_pack, "limitations", []):
            limitations.append(lim)

    # ── Tax estimate if present ──
    if evidence_pack:
        for table in getattr(evidence_pack, "tables", []):
            if table.get("title") == "税负估算":
                blocks.append(AnswerBlock(block_type="tax_estimate", title="税负估算", content="", data=table))

    # ── Source plan ──
    if source_plan:
        plan_lines = [f"- {item.source_id} ({item.source_lane})" for item in getattr(source_plan, "items", [])]
        if plan_lines:
            blocks.append(AnswerBlock(block_type="evidence", title="数据源计划", content="\n".join(plan_lines)))

    # ── Mode: derived from what we actually output ──
    if len(blocks) <= 2:
        answer_mode = "quick_answer"
    elif len(blocks) <= 4:
        answer_mode = "kpi_answer"
    else:
        answer_mode = "markdown_report"

    return StructuredAnswer(
        summary=summary_text,
        blocks=blocks,
        limitations=limitations,
        recommendations=recommendations,
        answer_mode=answer_mode,
    )
