from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from app.infra import msrp_repository


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
