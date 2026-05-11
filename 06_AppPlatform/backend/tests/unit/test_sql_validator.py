"""Tests for SQL validator."""

from __future__ import annotations

from app.copilot_governance.sql_validator import (
    validate_query_plan,
    validate_sql_text,
)


class TestValidateQueryPlan:
    def test_valid_plan_passes(self):
        result = validate_query_plan({
            "dataset_id": "jato_sales_parquet",
            "query_type": "aggregate",
            "filters": [{"field": "Country", "operator": "=", "value": "Sweden"}],
            "metrics": [{"field": "Sales", "aggregation": "sum", "alias": "total"}],
            "limit": 100,
        })
        assert result.valid
        assert result.risk_level in ("low", "medium")

    def test_no_dataset_fails(self):
        result = validate_query_plan({})
        assert not result.valid
        assert result.risk_level == "high"

    def test_unknown_query_type_warns(self):
        result = validate_query_plan({
            "dataset_id": "jato_sales_parquet",
            "query_type": "unknown_type",
            "metrics": [{"field": "Sales", "aggregation": "sum", "alias": "x"}],
        })
        assert any(i.code == "UNKNOWN_QUERY_TYPE" for i in result.issues)

    def test_limit_too_high_warns(self):
        result = validate_query_plan({
            "dataset_id": "jato_sales_parquet",
            "query_type": "aggregate",
            "metrics": [{"field": "Sales", "aggregation": "sum", "alias": "x"}],
            "limit": 10000,
        })
        assert any(i.code == "ROW_LIMIT_TOO_HIGH" for i in result.issues)

    def test_not_whitelisted_dataset_fails(self):
        result = validate_query_plan(
            {"dataset_id": "unknown_dataset", "query_type": "aggregate"},
            dataset_whitelist=["jato_sales_parquet"],
        )
        assert any(i.code == "DATASET_NOT_ALLOWED" for i in result.issues)


class TestValidateSqlText:
    def test_select_ok(self):
        result = validate_sql_text("SELECT country, SUM(sales) FROM t GROUP BY country")
        assert result.valid

    def test_drop_rejected(self):
        result = validate_sql_text("DROP TABLE users")
        assert not result.valid
        assert any("DROP" in i.message for i in result.issues)

    def test_insert_rejected(self):
        result = validate_sql_text("INSERT INTO t VALUES (1)")
        assert not result.valid

    def test_delete_rejected(self):
        result = validate_sql_text("DELETE FROM t WHERE id=1")
        assert not result.valid

    def test_select_star_warns(self):
        result = validate_sql_text("SELECT * FROM t")
        assert any(i.code == "FORBIDDEN_KEYWORD" for i in result.issues) or result.valid
