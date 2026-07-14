from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from app.infra import msrp_repository
from app.services.msrp_official_source_policy import (
    is_enabled_official_msrp_source,
    is_official_msrp_source_type,
)


def test_current_price_alert_keys_subquery_filters_drop_direction_and_window() -> None:
    alert_keys = msrp_repository._current_price_alert_keys_subquery(
        "drops",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        3.0,
    )

    sql = str(
        select(alert_keys.c.country).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "lag(msrp.price_history.msrp_value)" in sql
    assert "price_history_events.valid_from_utc >= '2026-01-01" in sql
    assert "CASE WHEN (price_history_events.source_currency = price_history_events.previous_source_currency)" in sql
    assert "price_history_events.previous_source_msrp_value ELSE price_history_events.previous_msrp_value END != 0" in sql
    assert "abs(" in sql
    assert "price_history_events.source_msrp_value - price_history_events.previous_source_msrp_value" in sql
    assert "price_history_events.msrp_value - price_history_events.previous_msrp_value" in sql
    assert ">= 3.0" in sql
    assert "END < 0" in sql


def test_current_price_alert_keys_subquery_filters_increase_direction() -> None:
    alert_keys = msrp_repository._current_price_alert_keys_subquery("increases")

    sql = str(
        select(alert_keys.c.country).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "price_history_events.source_msrp_value - price_history_events.previous_source_msrp_value" in sql
    assert "price_history_events.msrp_value - price_history_events.previous_msrp_value" in sql
    assert "END > 0" in sql


def test_materializable_queries_require_enabled_canonical_official_sources() -> None:
    class Result:
        def scalars(self):
            return self

        def all(self):
            return []

    class Session:
        def __init__(self) -> None:
            self.statements = []

        def execute(self, statement):
            self.statements.append(statement)
            return Result()

    session = Session()
    msrp_repository.list_materializable_observations(
        session,
        None,
        None,
        None,
        10,
    )
    msrp_repository.list_reconciliation_observations(
        session,
        None,
        None,
        None,
        10,
    )

    assert len(session.statements) == 2
    for statement in session.statements:
        sql = str(
            statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        )
        assert "JOIN msrp.sources" in sql
        assert "msrp.sources.enabled IS true" in sql
        assert "official_pdf" in sql
        assert "official_web" in sql
        assert "reference_catalog" not in sql


def test_canonical_official_policy_excludes_nonofficial_and_disabled_sources() -> None:
    assert is_official_msrp_source_type("official_pdf") is True
    assert is_official_msrp_source_type("official_web") is True
    assert is_official_msrp_source_type("reference_catalog") is False
    assert (
        is_enabled_official_msrp_source(
            type(
                "Source",
                (),
                {"enabled": False, "source_type": "official_pdf"},
            )()
        )
        is False
    )
