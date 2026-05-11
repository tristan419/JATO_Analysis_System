"""SQL Validator — ensures QueryPlans are safe, bounded, and governed."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from app.copilot_governance.catalog.registry import get_dataset


class ValidationIssue(BaseModel):
    severity: Literal["error", "warning", "info"] = "error"
    code: str
    message: str
    suggested_fix: str | None = None


class QueryValidationResult(BaseModel):
    valid: bool
    risk_level: Literal["low", "medium", "high"] = "low"
    issues: list[ValidationIssue] = Field(default_factory=list)


_FORBIDDEN_SQL_KEYWORDS = [
    "DELETE", "DROP", "INSERT", "UPDATE", "ALTER", "CREATE",
    "TRUNCATE", "GRANT", "REVOKE", "EXEC", "EXECUTE",
    "INTO OUTFILE", "INTO DUMPFILE", "LOAD_FILE",
    "SLEEP(", "BENCHMARK(", "WAITFOR",
]


def validate_query_plan(
    plan: dict[str, Any],
    dataset_whitelist: list[str] | None = None,
) -> QueryValidationResult:
    issues: list[ValidationIssue] = []

    dataset_id = str(plan.get("dataset_id", ""))
    if not dataset_id:
        issues.append(ValidationIssue(
            severity="error", code="NO_DATASET",
            message="QueryPlan 缺少 dataset_id。",
        ))
        return QueryValidationResult(valid=False, risk_level="high", issues=issues)

    if dataset_whitelist and dataset_id not in dataset_whitelist:
        issues.append(ValidationIssue(
            severity="error", code="DATASET_NOT_ALLOWED",
            message=f"Dataset {dataset_id} 不在允许列表中。",
        ))

    catalog_item = get_dataset(dataset_id)
    if catalog_item is not None:
        required_filters = catalog_item.required_filters
        plan_filters = plan.get("filters", [])
        filter_fields = {f.get("field", "") for f in plan_filters if isinstance(f, dict)}
        for req in required_filters:
            if req not in filter_fields:
                issues.append(ValidationIssue(
                    severity="warning", code="MISSING_REQUIRED_FILTER",
                    message=f"缺少必需过滤字段 '{req}'。",
                    suggested_fix=f"添加 filter: field={req}。",
                ))

        governance = catalog_item.governance
        limit = plan.get("limit", 0)
        max_rows = governance.get("max_rows", 5000)
        if limit > max_rows:
            issues.append(ValidationIssue(
                severity="warning", code="ROW_LIMIT_TOO_HIGH",
                message=f"limit={limit} 超过允许上限 {max_rows}。",
                suggested_fix=f"将 limit 设为 {max_rows} 或更低。",
            ))

        if not governance.get("readonly", True):
            issues.append(ValidationIssue(
                severity="error", code="NOT_READONLY",
                message=f"Dataset {dataset_id} 不允许只读查询。",
            ))

    query_type = str(plan.get("query_type", ""))
    if query_type not in {"aggregate", "detail", "ranking", "trend", "distribution", "correlation"}:
        issues.append(ValidationIssue(
            severity="warning", code="UNKNOWN_QUERY_TYPE",
            message=f"未知查询类型: {query_type}。",
        ))

    if not plan.get("metrics") and query_type not in ("detail",):
        issues.append(ValidationIssue(
            severity="warning", code="NO_METRICS",
            message="QueryPlan 没有定义 metrics。",
        ))

    has_errors = any(i.severity == "error" for i in issues)
    has_warnings = any(i.severity == "warning" for i in issues)

    if has_errors:
        risk_level: Literal["low", "medium", "high"] = "high"
        valid = False
    elif has_warnings:
        risk_level = "medium"
        valid = True
    else:
        risk_level = "low"
        valid = True

    return QueryValidationResult(valid=valid, risk_level=risk_level, issues=issues)


def validate_sql_text(sql: str) -> QueryValidationResult:
    """Basic safety check on raw SQL text — rejects dangerous keywords."""
    issues: list[ValidationIssue] = []
    upper = sql.upper()

    for keyword in _FORBIDDEN_SQL_KEYWORDS:
        if keyword.upper() in upper:
            issues.append(ValidationIssue(
                severity="error",
                code="FORBIDDEN_KEYWORD",
                message=f"SQL 包含禁止关键字: {keyword}",
            ))

    if "SELECT" not in upper:
        issues.append(ValidationIssue(
            severity="error",
            code="NOT_SELECT",
            message="只允许 SELECT 查询。",
        ))

    if "SELECT *" in upper.replace(" ", ""):
        issues.append(ValidationIssue(
            severity="warning",
            code="SELECT_STAR",
            message="不推荐使用 SELECT *。请指定具体字段。",
        ))

    has_errors = any(i.severity == "error" for i in issues)
    return QueryValidationResult(
        valid=not has_errors,
        risk_level="high" if has_errors else "low",
        issues=issues,
    )
