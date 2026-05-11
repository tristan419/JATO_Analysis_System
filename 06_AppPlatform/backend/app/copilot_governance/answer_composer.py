"""Answer Composer — builds structured answers from evidence packs."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class AnswerBlock(BaseModel):
    block_type: Literal["summary", "table", "chart", "evidence", "limitation", "tax_estimate", "recommendation"] = "summary"
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

    # Summary from overview
    overview = snapshot.get("overviewSummary", {})
    kpis = snapshot.get("kpis", {})
    if overview or kpis:
        summary_parts = []
        if overview.get("headline"):
            summary_parts.append(overview["headline"])
        elif kpis:
            summary_parts.append(
                f"{country} 市场：{kpis.get('brandCount', '?')} 个品牌，"
                f"{kpis.get('modelCount', '?')} 个车型。"
            )
        blocks.append(AnswerBlock(
            block_type="summary",
            title="市场概况",
            content=" ".join(summary_parts) or f"{country} 市场数据快照。",
        ))

    # Brand ranking table
    top_brands = snapshot.get("topBrands", [])
    if isinstance(top_brands, list) and top_brands:
        brands_text = "\n".join(
            f"| {b.get('label', '?')} | {b.get('value', 0):,} |"
            for b in top_brands[:5]
        )
        blocks.append(AnswerBlock(
            block_type="table",
            title="Top 5 品牌",
            content=f"| 品牌 | 销量 |\n| --- | --- |\n{brands_text}",
        ))

    # Powertrain mix
    powertrain = snapshot.get("powertrainMix", [])
    if isinstance(powertrain, list) and powertrain:
        pt_text = "\n".join(
            f"| {p.get('label', '?')} | {p.get('value', 0):,} |"
            for p in powertrain[:6]
        )
        blocks.append(AnswerBlock(
            block_type="table",
            title="动力结构",
            content=f"| 动力 | 销量 |\n| --- | --- |\n{pt_text}",
        ))

    # Cross-tab insights
    cross_tabs = snapshot.get("crossTabs", {})
    if isinstance(cross_tabs, dict):
        drive_by_fuel = cross_tabs.get("driveByFuel", [])
        if drive_by_fuel:
            lines = []
            for row in drive_by_fuel[:5]:
                idx = row.get("_index", "?")
                pct_4wd = row.get("4WD_pct", 0)
                lines.append(f"- {idx}: 4WD 占比 {pct_4wd}%")
            blocks.append(AnswerBlock(
                block_type="evidence",
                title="驱动 × 动力交叉",
                content="\n".join(lines) or "数据不可用",
            ))

    # Evidence pack sources
    if evidence_pack:
        sources = getattr(evidence_pack, "sources", [])
        if sources:
            src_lines = [f"- {s.source_name or s.source_id} ({s.coverage})" for s in sources]
            blocks.append(AnswerBlock(
                block_type="evidence",
                title="证据覆盖",
                content="\n".join(src_lines),
            ))
        for lim in getattr(evidence_pack, "limitations", []):
            limitations.append(lim)

    # Source plan
    if source_plan:
        plan_lines = [f"- {item.source_id} ({item.source_lane})" for item in getattr(source_plan, "items", [])]
        if plan_lines:
            blocks.append(AnswerBlock(
                block_type="evidence",
                title="数据源计划",
                content="\n".join(plan_lines),
            ))

    # Tax estimate if available
    if evidence_pack:
        tables = getattr(evidence_pack, "tables", [])
        for table in tables:
            if table.get("title") == "税负估算":
                blocks.append(AnswerBlock(
                    block_type="tax_estimate",
                    title="税负估算",
                    content="",
                    data=table,
                ))

    # Build summary
    summary = f"{country} 市场分析" if country else "市场分析"
    if overview.get("subheadline"):
        summary = overview["subheadline"]

    # Recommendations
    if limitations:
        recommendations.append("建议获取更完整的证据后再做策略判断。")
    if not cross_tabs:
        recommendations.append("交叉维度数据缺失，建议检查数据源配置。")

    return StructuredAnswer(
        summary=summary,
        blocks=blocks,
        limitations=limitations,
        recommendations=recommendations,
        answer_mode="quick_answer" if len(blocks) <= 3 else "markdown_report",
    )
