"""Result Verifier — validate query results before they reach the Answer Composer."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class VerificationIssue(BaseModel):
    severity: Literal["error", "warning", "info"] = "warning"
    code: str
    message: str
    affected_source_id: str | None = None
    suggested_fix: str | None = None


class ResultVerificationReport(BaseModel):
    status: Literal["pass", "warning", "fail"] = "pass"
    confidence: Literal["high", "medium", "low"] = "medium"
    issues: list[VerificationIssue] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    verified_sources: list[str] = Field(default_factory=list)


def verify_row_count(
    rows: list[dict[str, Any]],
    min_rows: int = 0,
    max_rows: int = 5000,
) -> list[VerificationIssue]:
    issues: list[VerificationIssue] = []
    count = len(rows)
    if count == 0:
        issues.append(VerificationIssue(
            severity="warning",
            code="EMPTY_RESULT",
            message="查询返回零行，可能是过滤条件过严或数据缺失。",
        ))
    if count > max_rows:
        issues.append(VerificationIssue(
            severity="warning",
            code="ROW_LIMIT_EXCEEDED",
            message=f"查询返回 {count} 行，超过上限 {max_rows}。",
            suggested_fix="考虑添加过滤条件或聚合。",
        ))
    return issues


def verify_freshness(
    period_label: str | None,
    max_staleness_months: int = 3,
) -> list[VerificationIssue]:
    if not period_label:
        return [VerificationIssue(
            severity="warning",
            code="FRESHNESS_UNKNOWN",
            message="无法确定数据时效。",
        )]
    return []


def verify_share_sum(
    items: list[dict[str, Any]],
    share_key: str = "share",
    tolerance: float = 0.05,
    label: str = "",
) -> list[VerificationIssue]:
    if not items:
        return []
    total = sum(item.get(share_key, 0) for item in items if isinstance(item, dict))
    if total <= 0:
        return []
    if abs(total - 100.0) > tolerance * 100:
        return [VerificationIssue(
            severity="warning",
            code="SHARE_SUM_MISMATCH",
            message=f"{label or '份额'}合计 {total:.1f}%，不是 100%。可能存在未分类数据。",
        )]
    return []


def verify_source_coverage(
    source_plan=None,
    available_sources: list[str] | None = None,
) -> list[VerificationIssue]:
    issues: list[VerificationIssue] = []
    if source_plan is None:
        return issues
    planned = {item.source_id for item in getattr(source_plan, "items", [])}
    available = set(available_sources or [])
    missing = planned - available
    for src_id in missing:
        issues.append(VerificationIssue(
            severity="warning",
            code="SOURCE_MISSING",
            message=f"计划源 {src_id} 无可用数据。",
            affected_source_id=src_id,
        ))
    return issues


def verify_no_empty(
    snapshot: dict[str, Any],
    key: str,
    label: str = "",
) -> list[VerificationIssue]:
    value = snapshot.get(key)
    if value is None or (isinstance(value, (list, dict, str)) and not value):
        return [VerificationIssue(
            severity="info",
            code="FIELD_EMPTY",
            message=f"{label or key} 数据为空。",
        )]
    return []


def run_result_verification(
    snapshot: dict[str, Any],
    source_plan=None,
    governed_results: list[dict[str, Any]] | None = None,
) -> ResultVerificationReport:
    issues: list[VerificationIssue] = []
    verified: list[str] = []

    for key, label in [
        ("ytdBrandRanking", "YTD品牌排名"),
        ("powertrainMix", "动力结构"),
        ("topBrands", "头部品牌"),
    ]:
        issues.extend(verify_no_empty(snapshot, key, label))
        if snapshot.get(key):
            verified.append(key)

    if snapshot.get("powertrainMix"):
        issues.extend(verify_share_sum(
            snapshot["powertrainMix"],
            share_key="value" if isinstance(snapshot["powertrainMix"], list) and snapshot["powertrainMix"] and "value" in snapshot["powertrainMix"][0] else "share",
            label="动力份额",
        ))

    issues.extend(verify_freshness(snapshot.get("periodLabel")))

    if governed_results:
        issues.extend(verify_row_count(governed_results))
    if source_plan:
        issues.extend(verify_source_coverage(source_plan, verified))

    if not issues:
        return ResultVerificationReport(
            status="pass",
            confidence="high",
            verified_sources=verified,
        )

    has_errors = any(i.severity == "error" for i in issues)
    has_warnings = any(i.severity == "warning" for i in issues)

    status: Literal["pass", "warning", "fail"] = (
        "fail" if has_errors else "warning" if has_warnings else "pass"
    )
    confidence: Literal["high", "medium", "low"] = (
        "low" if has_errors else "medium" if has_warnings else "high"
    )

    return ResultVerificationReport(
        status=status,
        confidence=confidence,
        issues=issues,
        limitations=[i.message for i in issues],
        verified_sources=verified,
    )
