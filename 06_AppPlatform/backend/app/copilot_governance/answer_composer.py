"""Answer Composer — builds structured answers from evidence packs.

Block selection is driven by a simple principle: only include data that
both EXISTS in the snapshot AND is RELEVANT to the user's question.
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


# ── Question analysis ────────────────────────────────────────────

def _lowered(question: str) -> str:
    return (question or "").lower()


def _has_any(lowered: str, keywords: tuple[str, ...]) -> bool:
    return any(kw in lowered for kw in keywords)


# ── Data availability checks ─────────────────────────────────────

def _has_ranking(snapshot: dict) -> bool:
    brands = snapshot.get("topBrands", [])
    return isinstance(brands, list) and len(brands) >= 2


def _has_powertrain(snapshot: dict) -> bool:
    pt = snapshot.get("powertrainMix", [])
    return isinstance(pt, list) and len(pt) >= 2


def _has_cross_tabs(snapshot: dict) -> bool:
    ct = snapshot.get("crossTabs", {})
    return isinstance(ct, dict) and bool(ct.get("availableDimensions"))


def _has_segment(snapshot: dict) -> bool:
    seg = snapshot.get("segmentMatrix", {})
    rows = seg.get("rows", []) if isinstance(seg, dict) else []
    return len(rows) >= 2


def _has_msrp(snapshot: dict) -> bool:
    return bool(snapshot.get("preciseLookup"))


# ── Question relevance checks ────────────────────────────────────

def _asks_ranking(lowered: str) -> bool:
    return _has_any(lowered, (
        "排名", "top", "前", "卖得好", "卖的最好", "畅销", "哪些", "什么车",
        "ranking", "best", "top brand", "popular",
    ))


def _asks_powertrain(lowered: str) -> bool:
    return _has_any(lowered, (
        "bev", "phev", "hev", "mhev", "ice", "电动", "混动", "插混",
        "纯电", "燃油", "动力", "powertrain", "新能源", "nev",
    ))


def _asks_segment(lowered: str) -> bool:
    return _has_any(lowered, (
        "suv", "sedan", "轿车", "越野", "segment", "细分", "级别",
        "a级", "b级", "c级", "紧凑", "中型", "大型",
    ))


def _asks_causal(lowered: str) -> bool:
    return _has_any(lowered, (
        "为什么", "原因", "为何", "下滑", "上升", "下降", "下跌",
        "why", "decline", "drop", "growth", "factor", "驱动", "影响",
    ))


def _asks_tax(lowered: str) -> bool:
    return _has_any(lowered, (
        "税", "补贴", "malus", "bonus", "碳", "co2", "排放",
        "tax", "subsidy", "incentive", "政策",
    ))


def _asks_price(lowered: str) -> bool:
    return _has_any(lowered, (
        "价格", "多少钱", "价位", "msrp", "price", "定价", "售价",
        "月供", "金融",
    ))


def _asks_compare(lowered: str) -> bool:
    return _has_any(lowered, (
        "对比", "比较", "哪个好", "区别", "差异", "vs", "versus",
        "compare", "comparison", "difference",
    ))


def _is_simple_fact(lowered: str) -> bool:
    """Simple fact questions: single KPI, no analysis needed."""
    if _asks_causal(lowered) or _asks_compare(lowered) or _asks_segment(lowered):
        return False
    if _asks_ranking(lowered) or _asks_powertrain(lowered):
        return False  # even if short, these deserve structured output
    return len(lowered.split()) <= 10


# ── Block builders ───────────────────────────────────────────────

def _build_ranking_table(snapshot: dict) -> AnswerBlock:
    brands = snapshot.get("topBrands", [])
    rows = "\n".join(
        f"| {b.get('label', '?')} | {int(b.get('value', 0)):,} |"
        for b in brands[:5]
    )
    return AnswerBlock(
        block_type="table", title="品牌排名",
        content=f"| 品牌 | 销量 |\n| --- | --- |\n{rows}",
    )


def _build_powertrain_table(snapshot: dict) -> AnswerBlock:
    pt = snapshot.get("powertrainMix", [])
    rows = "\n".join(
        f"| {p.get('label', '?')} | {int(p.get('value', 0)):,} |"
        for p in pt[:6]
    )
    return AnswerBlock(
        block_type="table", title="动力结构",
        content=f"| 动力 | 销量 |\n| --- | --- |\n{rows}",
    )


def _build_cross_tab_block(snapshot: dict) -> AnswerBlock | None:
    ct = snapshot.get("crossTabs", {})
    drive_by_fuel = ct.get("driveByFuel", [])
    if not drive_by_fuel:
        return None
    lines = []
    for row in drive_by_fuel[:5]:
        idx = row.get("_index", "?")
        pct_4wd = row.get("4WD_pct", 0)
        lines.append(f"- {idx}: 四驱占比 **{pct_4wd}%**")
    return AnswerBlock(
        block_type="evidence", title="驱动 × 动力交叉",
        content="\n".join(lines),
    )


def _build_segment_block(snapshot: dict) -> AnswerBlock | None:
    seg = snapshot.get("segmentMatrix", {})
    rows = seg.get("rows", []) if isinstance(seg, dict) else []
    if not rows:
        return None
    lines = "\n".join(
        f"| {r.get('segment', '?')} | {int(r.get('currentMonth', 0)):,} | {r.get('mom', '?')}% |"
        for r in rows[:8]
    )
    return AnswerBlock(
        block_type="table", title="细分市场",
        content=f"| 细分 | 当月销量 | MoM |\n| --- | --- | --- |\n{lines}",
    )


# ── Main composer ────────────────────────────────────────────────

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
    lowered = _lowered(question)
    is_simple = _is_simple_fact(lowered)

    # ── 1. Always: summary (compact for simple questions) ──
    overview = snapshot.get("overviewSummary", {})
    kpis = snapshot.get("kpis", {})
    if overview.get("headline"):
        summary_text = overview["headline"]
    elif kpis:
        summary_text = (
            f"{country}：{kpis.get('brandCount', '?')} 品牌，"
            f"{kpis.get('modelCount', '?')} 车型"
        )
    else:
        summary_text = f"{country} 市场数据"

    blocks.append(AnswerBlock(block_type="summary", title="市场概况", content=summary_text))

    # ── 2. Ranking: only if user asks AND data exists ──
    if _has_ranking(snapshot) and _asks_ranking(lowered) and not is_simple:
        blocks.append(_build_ranking_table(snapshot))
    elif _has_ranking(snapshot) and _asks_ranking(lowered) and is_simple:
        # Simple question → just name the top in summary, no table
        top = (snapshot.get("topBrands") or [{}])[0]
        blocks[0].content += f"，头部品牌 {top.get('label', '?')}"

    # ── 3. Powertrain: only if relevant ──
    if _has_powertrain(snapshot) and _asks_powertrain(lowered):
        blocks.append(_build_powertrain_table(snapshot))

    # ── 4. Cross-tabs: only for causal/analytical questions ──
    if _has_cross_tabs(snapshot) and (_asks_causal(lowered) or _asks_segment(lowered)):
        ct_block = _build_cross_tab_block(snapshot)
        if ct_block:
            blocks.append(ct_block)

    # ── 5. Segment: only for segment questions ──
    if _has_segment(snapshot) and _asks_segment(lowered):
        seg_block = _build_segment_block(snapshot)
        if seg_block:
            blocks.append(seg_block)

    # ── 6. Strategy/comparison gets evidence coverage ──
    has_strategy = _asks_causal(lowered) or _asks_compare(lowered) or _asks_tax(lowered)
    if has_strategy and evidence_pack:
        sources = getattr(evidence_pack, "sources", [])
        if sources:
            src_lines = [f"- {s.source_name or s.source_id} ({s.coverage})" for s in sources]
            blocks.append(AnswerBlock(
                block_type="evidence", title="证据来源",
                content="\n".join(src_lines),
            ))

    # ── 7. Tax estimate ──
    if _asks_tax(lowered) and evidence_pack:
        tables = getattr(evidence_pack, "tables", [])
        for table in tables:
            if table.get("title") == "税负估算":
                blocks.append(AnswerBlock(
                    block_type="tax_estimate", title="税负估算", content="", data=table,
                ))

    # ── Determine answer_mode from actual blocks ──
    block_types = {b.block_type for b in blocks}
    if len(blocks) <= 2:
        answer_mode = "quick_answer"
    elif "tax_estimate" in block_types:
        answer_mode = "strategy_brief"
    elif block_types >= {"table", "evidence"}:
        answer_mode = "markdown_report"
    elif "table" in block_types:
        answer_mode = "kpi_answer"
    else:
        answer_mode = "quick_answer"

    # ── Limitations ──
    if evidence_pack:
        for lim in getattr(evidence_pack, "limitations", []):
            limitations.append(lim)
    if not _has_cross_tabs(snapshot) and _asks_causal(lowered):
        limitations.append("交叉维度数据不可用，因果分析受限。")

    # ── Recommendations only for strategy questions ──
    if has_strategy and limitations:
        recommendations.append("建议获取更完整的证据后再做策略判断。")

    return StructuredAnswer(
        summary=summary_text,
        blocks=blocks,
        limitations=limitations,
        recommendations=recommendations,
        answer_mode=answer_mode,
    )
