import threading

import pandas as pd
import pytest

from app.services import query_service


@pytest.fixture(autouse=True)
def disable_grouped_time_series_disk_cache(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PERSISTENT_CACHE_ENABLED",
        False,
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PERSISTENT_CACHE_DIR",
        tmp_path,
    )
    monkeypatch.setattr(
        query_service,
        "DASHBOARD_OVERVIEW_PERSISTENT_CACHE_ENABLED",
        False,
    )
    monkeypatch.setattr(
        query_service,
        "DASHBOARD_OVERVIEW_PERSISTENT_CACHE_DIR",
        tmp_path,
    )
    query_service._clear_grouped_time_series_cache()
    query_service._clear_dashboard_overview_cache()
    monkeypatch.setattr(query_service, "get_redis_client", lambda: None)


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    def get(self, key: str):
        return self.store.get(key)

    def setex(self, key: str, ttl: int, value: str) -> bool:
        self.store[key] = value
        self.ttls[key] = ttl
        return True

    def setnx(self, key: str, value: str) -> bool:
        if key in self.store:
            return False
        self.store[key] = value
        return True

    def expire(self, key: str, ttl: int) -> bool:
        if key not in self.store:
            return False
        self.ttls[key] = ttl
        return True

    def delete(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            if key in self.store:
                deleted += 1
                self.store.pop(key, None)
            self.ttls.pop(key, None)
        return deleted


def test_filters_options_batch_merges_requests_with_same_filters(
    monkeypatch,
) -> None:
    calls: list[tuple[list[str], dict[str, list[str]]]] = []

    monkeypatch.setattr(
        query_service.repo,
        "load_distinct_options_batch",
        lambda columns, filters: (
            calls.append((columns, filters))
            or {column: [f"{column}-value"] for column in columns}
        ),
    )

    result = query_service.filters_options_batch([
        ("Make", {"Country": ["HU"]}),
        ("Model", {"Country": ["HU"]}),
    ])

    assert calls == [
        (["Make", "Model"], {"Country": ["HU"]}),
    ]
    assert result == [
        {"column": "Make", "options": ["Make-value"]},
        {"column": "Model", "options": ["Model-value"]},
    ]


def test_filters_options_batch_uses_top_level_snapshot(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        query_service,
        "_load_top_level_filter_options_snapshot",
        lambda: {"Country": ["HU", "SE"]},
    )
    monkeypatch.setattr(
        query_service.repo,
        "load_distinct_options_batch",
        lambda columns, filters: (_ for _ in ()).throw(AssertionError("scan")),
    )

    result = query_service.filters_options_batch([
        ("Country", {}),
    ])

    assert result == [
        {"column": "Country", "options": ["HU", "SE"]},
    ]


def test_query_overview_reports_cache_state_and_role_scope(
    monkeypatch,
) -> None:
    calls = 0

    monkeypatch.setattr(
        query_service.repo,
        "current_dataset_token",
        lambda: "dataset-a",
    )

    def fake_query_overview_impl(**kwargs) -> dict:
        nonlocal calls
        if kwargs.get("filters") == {"Country": ["HU"]}:
            calls += 1
        return {
            "route": "dynamic-aggregate",
            "kpis": {"totalRows": 10},
            "monthSeries": [],
            "yearSeries": [],
        }

    monkeypatch.setattr(
        query_service,
        "_query_overview_impl",
        fake_query_overview_impl,
    )

    first = query_service.query_overview_with_cache_state(
        filters={"Country": ["HU"]},
        prefer_precomputed=True,
        top_n=10,
        cache_scope="viewer",
    )
    second = query_service.query_overview_with_cache_state(
        filters={"Country": ["HU"]},
        prefer_precomputed=True,
        top_n=10,
        cache_scope="viewer",
    )
    third = query_service.query_overview_with_cache_state(
        filters={"Country": ["HU"]},
        prefer_precomputed=True,
        top_n=10,
        cache_scope="admin",
    )

    assert first.cache_state == "MISS"
    assert second.cache_state == "MEMORY"
    assert third.cache_state == "MISS"
    assert calls == 2
    assert first.payload == second.payload == third.payload


def test_query_overview_uses_persistent_cache(
    monkeypatch,
    tmp_path,
) -> None:
    calls = 0

    monkeypatch.setattr(
        query_service,
        "DASHBOARD_OVERVIEW_PERSISTENT_CACHE_ENABLED",
        True,
    )
    monkeypatch.setattr(
        query_service,
        "DASHBOARD_OVERVIEW_PERSISTENT_CACHE_DIR",
        tmp_path,
    )
    monkeypatch.setattr(
        query_service.repo,
        "current_dataset_token",
        lambda: "dataset-a",
    )

    def fake_query_overview_impl(**kwargs) -> dict:
        nonlocal calls
        if kwargs.get("filters") == {"Country": ["HU"]}:
            calls += 1
        return {
            "route": "dynamic-aggregate",
            "kpis": {"totalRows": 10},
            "monthSeries": [],
            "yearSeries": [],
        }

    monkeypatch.setattr(
        query_service,
        "_query_overview_impl",
        fake_query_overview_impl,
    )

    first = query_service.query_overview_with_cache_state(
        filters={"Country": ["HU"]},
        prefer_precomputed=True,
        top_n=10,
        cache_scope="viewer",
    )
    query_service._clear_dashboard_overview_cache()
    second = query_service.query_overview_with_cache_state(
        filters={"Country": ["HU"]},
        prefer_precomputed=True,
        top_n=10,
        cache_scope="viewer",
    )

    assert first.cache_state == "MISS"
    assert second.cache_state == "DISK"
    assert calls == 1
    assert first.payload == second.payload
    assert list(tmp_path.glob("*.json"))


def test_query_overview_uses_redis_cache_by_role_scope(
    monkeypatch,
) -> None:
    calls = 0
    redis = _FakeRedis()

    monkeypatch.setattr(query_service, "get_redis_client", lambda: redis)
    monkeypatch.setattr(
        query_service.repo,
        "current_dataset_token",
        lambda: "dataset-a",
    )

    def fake_query_overview_impl(**kwargs) -> dict:
        nonlocal calls
        if kwargs.get("filters") == {"Country": ["HU"]}:
            calls += 1
        return {
            "route": "dynamic-aggregate",
            "kpis": {"totalRows": 10},
            "monthSeries": [],
            "yearSeries": [],
        }

    monkeypatch.setattr(
        query_service,
        "_query_overview_impl",
        fake_query_overview_impl,
    )

    first = query_service.query_overview_with_cache_state(
        filters={"Country": ["HU"]},
        prefer_precomputed=True,
        top_n=10,
        cache_scope="viewer",
    )
    query_service._clear_dashboard_overview_cache()
    second = query_service.query_overview_with_cache_state(
        filters={"Country": ["HU"]},
        prefer_precomputed=True,
        top_n=10,
        cache_scope="viewer",
    )
    query_service._clear_dashboard_overview_cache()
    third = query_service.query_overview_with_cache_state(
        filters={"Country": ["HU"]},
        prefer_precomputed=True,
        top_n=10,
        cache_scope="admin",
    )

    assert first.cache_state == "MISS"
    assert second.cache_state == "REDIS"
    assert third.cache_state == "MISS"
    assert calls == 2
    assert first.payload == second.payload == third.payload
    assert redis.ttls
    assert all(
        ttl == query_service.DASHBOARD_OVERVIEW_CACHE_TTL_SECONDS
        for ttl in redis.ttls.values()
    )


def test_query_overview_waits_for_peer_redis_compute(
    monkeypatch,
) -> None:
    redis = _FakeRedis()
    waited_keys: list[str] = []
    payload = {
        "route": "dynamic-aggregate",
        "kpis": {"totalRows": 10},
        "monthSeries": [],
        "yearSeries": [],
    }
    cache_key = (
        query_service._normalize_cache_scope("viewer"),
        query_service._normalize_query_cache_filters({"Country": ["HU"]}),
        True,
        10,
    )
    redis_key = query_service._dashboard_overview_redis_cache_key(
        cache_key,
        "dataset-a",
    )
    redis.store[f"{redis_key}:lock"] = "1"

    def wait_for_peer_cache(client, key):
        waited_keys.append(key)
        assert client is redis
        return {
            "schema": query_service._DASHBOARD_OVERVIEW_REDIS_CACHE_SCHEMA,
            "dataset": "dataset-a",
            "cachedAt": 1,
            "result": payload,
        }

    monkeypatch.setattr(query_service, "get_redis_client", lambda: redis)
    monkeypatch.setattr(query_service, "wait_for_cache", wait_for_peer_cache)
    monkeypatch.setattr(
        query_service.repo,
        "current_dataset_token",
        lambda: "dataset-a",
    )
    monkeypatch.setattr(
        query_service,
        "_query_overview_impl",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("peer cache wait should avoid overview compute")
        ),
    )

    result = query_service.query_overview_with_cache_state(
        filters={"Country": ["HU"]},
        prefer_precomputed=True,
        top_n=10,
        cache_scope="viewer",
    )

    assert result.cache_state == "REDIS_WAIT"
    assert result.payload == payload
    assert waited_keys == [redis_key]


def test_warm_dashboard_overview_cache_includes_configured_filter_sets(
    monkeypatch,
) -> None:
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        query_service,
        "DASHBOARD_OVERVIEW_PREWARM_SCOPES",
        ["viewer"],
    )
    monkeypatch.setattr(
        query_service,
        "DASHBOARD_OVERVIEW_PREWARM_FILTERS",
        [
            {"Country": ["DK"], "Powertrain": ["ICE", "BEV"]},
            {"Powertrain": ["BEV", "ICE"], "Country": ["DK"]},
        ],
    )

    def fake_query_overview(**kwargs) -> dict:
        calls.append(kwargs)
        return {"items": []}

    monkeypatch.setattr(
        query_service,
        "query_overview",
        fake_query_overview,
    )

    result = query_service.warm_dashboard_overview_cache()

    assert result == {"warmed": 2, "failed": 0}
    assert calls == [
        {
            "filters": {},
            "prefer_precomputed": True,
            "top_n": 10,
            "cache_scope": "viewer",
        },
        {
            "filters": {"Country": ["DK"], "Powertrain": ["BEV", "ICE"]},
            "prefer_precomputed": True,
            "top_n": 10,
            "cache_scope": "viewer",
        },
    ]


def test_dashboard_overview_prewarm_defaults_cover_dashboard_scope() -> None:
    assert query_service.DASHBOARD_OVERVIEW_CACHE_TTL_SECONDS >= 1800
    assert "order_filler" in query_service.DASHBOARD_OVERVIEW_PREWARM_SCOPES
    assert any(
        filters.get("国家") and filters.get("动总规整")
        for filters in query_service.DASHBOARD_OVERVIEW_PREWARM_FILTERS
    )


def test_warm_dashboard_metadata_cache_reuses_service_loaders(
    monkeypatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        query_service,
        "metadata_columns",
        lambda: calls.append("columns") or ["Country"],
    )
    monkeypatch.setattr(
        query_service,
        "get_data_freshness",
        lambda: calls.append("freshness") or [],
    )

    result = query_service.warm_dashboard_metadata_cache()

    assert result == {"warmed": 2, "failed": 0}
    assert calls == ["columns", "freshness"]


def test_query_grouped_time_series_caches_by_params_and_dataset_token(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Brand": ["Alpha", "Beta"],
            "2024": [10.0, 5.0],
        }
    )
    load_calls = 0
    dataset_token = "dataset-a"

    def current_dataset_token() -> str:
        return dataset_token

    def load_slice(columns, filters, limit, offset):
        nonlocal load_calls
        load_calls += 1
        return frame.loc[:, columns].copy()

    query_service._grouped_time_series_cache.clear()
    monkeypatch.setattr(query_service.repo, "current_dataset_token", current_dataset_token)
    monkeypatch.setattr(query_service.repo, "list_columns", lambda: ["Brand", "2024"])
    monkeypatch.setattr(query_service.repo, "load_slice", load_slice)

    first = query_service.query_grouped_time_series(
        filters={},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
    )
    second = query_service.query_grouped_time_series(
        filters={},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
    )

    dataset_token = "dataset-b"
    third = query_service.query_grouped_time_series(
        filters={},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
    )

    assert load_calls == 2
    assert first == second == third
    query_service._grouped_time_series_cache.clear()


def test_query_grouped_time_series_reports_cache_state(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Brand": ["Alpha", "Beta"],
            "2024": [10.0, 5.0],
        }
    )

    monkeypatch.setattr(
        query_service.repo,
        "current_dataset_token",
        lambda: "dataset-a",
    )
    monkeypatch.setattr(query_service.repo, "list_columns", lambda: ["Brand", "2024"])
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: frame.loc[:, columns].copy(),
    )

    first = query_service.query_grouped_time_series_with_cache_state(
        filters={},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
    )
    second = query_service.query_grouped_time_series_with_cache_state(
        filters={},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
    )

    assert first.cache_state == "MISS"
    assert second.cache_state == "MEMORY"
    assert first.payload == second.payload


def test_query_grouped_time_series_uses_persistent_cache(
    monkeypatch,
    tmp_path,
) -> None:
    frame = pd.DataFrame(
        {
            "Brand": ["Alpha", "Beta"],
            "2024": [10.0, 5.0],
        }
    )
    load_calls = 0

    def load_slice(columns, filters, limit, offset):
        nonlocal load_calls
        load_calls += 1
        return frame.loc[:, columns].copy()

    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PERSISTENT_CACHE_ENABLED",
        True,
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PERSISTENT_CACHE_DIR",
        tmp_path,
    )
    monkeypatch.setattr(
        query_service.repo,
        "current_dataset_token",
        lambda: "dataset-a",
    )
    monkeypatch.setattr(query_service.repo, "list_columns", lambda: ["Brand", "2024"])
    monkeypatch.setattr(query_service.repo, "load_slice", load_slice)

    first = query_service.query_grouped_time_series(
        filters={"Country": ["HU"]},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
        cache_scope="viewer",
    )
    query_service._clear_grouped_time_series_cache()
    second = query_service.query_grouped_time_series(
        filters={"Country": ["HU"]},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
        cache_scope="viewer",
    )

    assert load_calls == 1
    assert first == second
    assert list(tmp_path.glob("*.json"))


def test_query_grouped_time_series_uses_redis_cache_by_role_scope(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Brand": ["Alpha", "Beta"],
            "2024": [10.0, 5.0],
        }
    )
    load_calls = 0
    redis = _FakeRedis()

    def load_slice(columns, filters, limit, offset):
        nonlocal load_calls
        load_calls += 1
        return frame.loc[:, columns].copy()

    monkeypatch.setattr(query_service, "get_redis_client", lambda: redis)
    monkeypatch.setattr(
        query_service.repo,
        "current_dataset_token",
        lambda: "dataset-a",
    )
    monkeypatch.setattr(query_service.repo, "list_columns", lambda: ["Brand", "2024"])
    monkeypatch.setattr(query_service.repo, "load_slice", load_slice)

    first = query_service.query_grouped_time_series_with_cache_state(
        filters={"Country": ["HU"]},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
        cache_scope="viewer",
    )
    query_service._clear_grouped_time_series_cache()
    second = query_service.query_grouped_time_series_with_cache_state(
        filters={"Country": ["HU"]},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
        cache_scope="viewer",
    )
    query_service._clear_grouped_time_series_cache()
    third = query_service.query_grouped_time_series_with_cache_state(
        filters={"Country": ["HU"]},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
        cache_scope="admin",
    )

    assert first.cache_state == "MISS"
    assert second.cache_state == "REDIS"
    assert third.cache_state == "MISS"
    assert load_calls == 2
    assert first.payload == second.payload == third.payload
    assert redis.ttls
    assert all(
        ttl == query_service.GROUPED_TIME_SERIES_CACHE_TTL_SECONDS
        for ttl in redis.ttls.values()
    )
    assert all(not key.endswith(":lock") for key in redis.store)


def test_query_grouped_time_series_waits_for_peer_redis_compute(
    monkeypatch,
) -> None:
    redis = _FakeRedis()
    waited_keys: list[str] = []
    payload = {
        "series": [{"group": "Alpha", "values": [{"period": "2024", "value": 10.0}]}],
        "meta": {"grain": "year"},
    }
    cache_key = (
        query_service._normalize_cache_scope("viewer"),
        query_service._normalize_query_cache_filters({"Country": ["HU"]}),
        "year",
        "Brand",
        None,
        2,
        False,
        None,
    )
    redis_key = query_service._grouped_time_series_redis_cache_key(
        cache_key,
        "dataset-a",
    )
    redis.store[f"{redis_key}:lock"] = "1"

    def wait_for_peer_cache(client, key):
        waited_keys.append(key)
        assert client is redis
        return {
            "schema": query_service._GROUPED_TIME_SERIES_REDIS_CACHE_SCHEMA,
            "dataset": "dataset-a",
            "cachedAt": 1,
            "result": payload,
        }

    monkeypatch.setattr(query_service, "get_redis_client", lambda: redis)
    monkeypatch.setattr(query_service, "wait_for_cache", wait_for_peer_cache)
    monkeypatch.setattr(
        query_service.repo,
        "current_dataset_token",
        lambda: "dataset-a",
    )
    monkeypatch.setattr(query_service.repo, "list_columns", lambda: ["Brand", "2024"])
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: (_ for _ in ()).throw(
            AssertionError("peer cache wait should avoid parquet compute")
        ),
    )

    result = query_service.query_grouped_time_series_with_cache_state(
        filters={"Country": ["HU"]},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
        cache_scope="viewer",
    )

    assert result.cache_state == "REDIS_WAIT"
    assert result.payload == payload
    assert waited_keys == [redis_key]


def test_query_grouped_time_series_cache_is_scoped_by_role(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Brand": ["Alpha", "Beta"],
            "2024": [10.0, 5.0],
        }
    )
    load_calls = 0

    def load_slice(columns, filters, limit, offset):
        nonlocal load_calls
        load_calls += 1
        return frame.loc[:, columns].copy()

    monkeypatch.setattr(
        query_service.repo,
        "current_dataset_token",
        lambda: "dataset-a",
    )
    monkeypatch.setattr(query_service.repo, "list_columns", lambda: ["Brand", "2024"])
    monkeypatch.setattr(query_service.repo, "load_slice", load_slice)

    first = query_service.query_grouped_time_series(
        filters={},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
        cache_scope="viewer",
    )
    second = query_service.query_grouped_time_series(
        filters={},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
        cache_scope="viewer",
    )
    third = query_service.query_grouped_time_series(
        filters={},
        grain="year",
        group_by="Brand",
        top_n=2,
        include_others=False,
        cache_scope="admin",
    )

    assert load_calls == 2
    assert first == second == third


def test_query_grouped_time_series_coalesces_concurrent_same_key(
    monkeypatch,
) -> None:
    calls = 0
    compute_started = threading.Event()
    release_compute = threading.Event()
    results: list[dict] = []

    def query() -> None:
        results.append(
            query_service.query_grouped_time_series(
                filters={"Country": ["HU"]},
                grain="year",
                group_by="Brand",
                top_n=2,
                include_others=False,
            )
        )

    def fake_impl(**_kwargs) -> dict:
        nonlocal calls
        calls += 1
        compute_started.set()
        assert release_compute.wait(timeout=2)
        return {
            "grain": "year",
            "rows": 1,
            "items": [{"time": "2024", "value": 10.0, "series": "Alpha"}],
        }

    query_service._clear_grouped_time_series_cache()
    monkeypatch.setattr(
        query_service.repo,
        "current_dataset_token",
        lambda: "dataset-a",
    )
    monkeypatch.setattr(
        query_service,
        "_query_grouped_time_series_impl",
        fake_impl,
    )

    first = threading.Thread(target=query)
    second = threading.Thread(target=query)
    first.start()
    assert compute_started.wait(timeout=2)
    second.start()
    release_compute.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert calls == 1
    assert results == [
        {
            "grain": "year",
            "rows": 1,
            "items": [{"time": "2024", "value": 10.0, "series": "Alpha"}],
        },
        {
            "grain": "year",
            "rows": 1,
            "items": [{"time": "2024", "value": 10.0, "series": "Alpha"}],
        },
    ]
    query_service._clear_grouped_time_series_cache()


def test_query_grouped_time_series_inflight_separates_dataset_tokens(
    monkeypatch,
) -> None:
    dataset_token = "dataset-a"
    calls = 0
    first_started = threading.Event()
    second_started = threading.Event()
    release_compute = threading.Event()
    results: list[dict] = []

    def current_dataset_token() -> str:
        return dataset_token

    def query() -> None:
        results.append(
            query_service.query_grouped_time_series(
                filters={"Country": ["HU"]},
                grain="year",
                group_by="Brand",
                top_n=2,
                include_others=False,
            )
        )

    def fake_impl(**_kwargs) -> dict:
        nonlocal calls
        calls += 1
        token = current_dataset_token()
        if token == "dataset-a":
            first_started.set()
        elif token == "dataset-b":
            second_started.set()
        assert release_compute.wait(timeout=2)
        return {
            "grain": "year",
            "rows": 1,
            "items": [{"time": "2024", "value": 10.0, "series": token}],
        }

    query_service._clear_grouped_time_series_cache()
    monkeypatch.setattr(
        query_service.repo,
        "current_dataset_token",
        current_dataset_token,
    )
    monkeypatch.setattr(
        query_service,
        "_query_grouped_time_series_impl",
        fake_impl,
    )

    first = threading.Thread(target=query)
    first.start()
    assert first_started.wait(timeout=2)
    dataset_token = "dataset-b"
    second = threading.Thread(target=query)
    second.start()
    assert second_started.wait(timeout=2)
    release_compute.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert calls == 2
    assert sorted(item["items"][0]["series"] for item in results) == [
        "dataset-a",
        "dataset-b",
    ]
    query_service._clear_grouped_time_series_cache()


def test_warm_grouped_time_series_cache_includes_configured_filter_sets(
    monkeypatch,
) -> None:
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_GROUP_BY",
        ["Brand"],
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_GRAINS",
        ["month"],
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_SCOPES",
        ["viewer"],
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_FILTERS",
        [
            {"Country": ["DK"], "Powertrain": ["ICE", "BEV"]},
            {"Powertrain": ["BEV", "ICE"], "Country": ["DK"]},
        ],
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_TOP_N",
        12,
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_INCLUDE_OTHERS",
        True,
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_SHARE_SPLIT_BY",
        ["segment"],
    )

    def fake_query_grouped_time_series(**kwargs) -> dict:
        calls.append(kwargs)
        return {"items": []}

    monkeypatch.setattr(
        query_service,
        "query_grouped_time_series",
        fake_query_grouped_time_series,
    )

    result = query_service.warm_grouped_time_series_cache()

    assert result == {"warmed": 2, "failed": 0}
    assert calls == [
        {
            "filters": {},
            "grain": "month",
            "group_by": "Brand",
            "top_n": 12,
            "include_others": True,
            "cache_scope": "viewer",
        },
        {
            "filters": {"Country": ["DK"], "Powertrain": ["BEV", "ICE"]},
            "grain": "month",
            "group_by": "Brand",
            "top_n": 12,
            "include_others": True,
            "cache_scope": "viewer",
        },
    ]


def test_warm_grouped_time_series_cache_includes_share_split_lenses(
    monkeypatch,
) -> None:
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_GROUP_BY",
        ["四驱占比"],
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_GRAINS",
        ["month"],
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_SCOPES",
        ["order_filler"],
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_FILTERS",
        [],
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_TOP_N",
        10,
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_INCLUDE_OTHERS",
        False,
    )
    monkeypatch.setattr(
        query_service,
        "GROUPED_TIME_SERIES_PREWARM_SHARE_SPLIT_BY",
        ["segment", "powertrain", "invalid"],
    )

    def fake_query_grouped_time_series(**kwargs) -> dict:
        calls.append(kwargs)
        return {"items": []}

    monkeypatch.setattr(
        query_service,
        "query_grouped_time_series",
        fake_query_grouped_time_series,
    )

    result = query_service.warm_grouped_time_series_cache()

    assert result == {"warmed": 3, "failed": 0}
    assert calls == [
        {
            "filters": {},
            "grain": "month",
            "group_by": "四驱占比",
            "top_n": 10,
            "include_others": False,
            "cache_scope": "order_filler",
        },
        {
            "filters": {},
            "grain": "month",
            "group_by": "四驱占比",
            "top_n": 10,
            "include_others": False,
            "cache_scope": "order_filler",
            "share_split_by": "segment",
        },
        {
            "filters": {},
            "grain": "month",
            "group_by": "四驱占比",
            "top_n": 10,
            "include_others": False,
            "cache_scope": "order_filler",
            "share_split_by": "powertrain",
        },
    ]


def test_grouped_time_series_prewarm_defaults_cover_dashboard_scope() -> None:
    assert query_service.GROUPED_TIME_SERIES_CACHE_TTL_SECONDS >= 1800
    assert query_service.GROUPED_TIME_SERIES_PREWARM_TOP_N >= 10
    assert query_service.GROUPED_TIME_SERIES_PREWARM_INCLUDE_OTHERS is False
    assert "order_filler" in query_service.GROUPED_TIME_SERIES_PREWARM_SCOPES
    assert {"month", "year"}.issubset(set(query_service.GROUPED_TIME_SERIES_PREWARM_GRAINS))
    assert {"四驱占比", "Business/Private 占比"}.issubset(
        set(query_service.GROUPED_TIME_SERIES_PREWARM_GROUP_BY)
    )
    assert {"segment", "powertrain"}.issubset(
        set(query_service.GROUPED_TIME_SERIES_PREWARM_SHARE_SPLIT_BY)
    )
    assert any(
        filters.get("国家") and filters.get("动总规整")
        for filters in query_service.GROUPED_TIME_SERIES_PREWARM_FILTERS
    )


def test_query_grouped_time_series_respects_time_range_for_topn(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Brand": ["Alpha", "Beta"],
            "2024 Jan": [40.0, 5.0],
            "2024 Feb": [0.0, 20.0],
            "2024 Mar": [0.0, 20.0],
        }
    )

    monkeypatch.setattr(
        query_service.repo,
        "list_columns",
        lambda: ["Brand", "2024 Jan", "2024 Feb", "2024 Mar"],
    )
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: frame.loc[:, columns].copy(),
    )

    result = query_service.query_grouped_time_series(
        filters={},
        grain="month",
        group_by="Brand",
        top_n=1,
        include_others=False,
        time_range={"start": "2024 Jan", "end": "2024 Feb"},
    )

    assert result["rows"] == 2
    assert [item["time"] for item in result["items"]] == ["2024 Jan", "2024 Feb"]
    assert {item["series"] for item in result["items"]} == {"Alpha"}
    assert [item["value"] for item in result["items"]] == [40.0, 0.0]


def test_query_grouped_time_series_respects_year_time_range(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Brand": ["Alpha", "Beta"],
            "2023": [10.0, 5.0],
            "2024": [20.0, 8.0],
            "2025": [30.0, 9.0],
        }
    )

    monkeypatch.setattr(
        query_service.repo,
        "list_columns",
        lambda: ["Brand", "2023", "2024", "2025"],
    )
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: frame.loc[:, columns].copy(),
    )

    result = query_service.query_grouped_time_series(
        filters={},
        grain="year",
        group_by="Brand",
        top_n=1,
        include_others=False,
        time_range={"start": "2024", "end": "2024"},
    )

    assert result["rows"] == 1
    assert result["items"] == [
        {"time": "2024", "value": 20.0, "series": "Alpha"},
    ]


def test_query_grouped_time_series_returns_awd_share_monthly(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Driven wheels": ["4x4", "front", "rear"],
            "2024 Jan": [25.0, 75.0, 0.0],
            "2024 Feb": [10.0, 0.0, 40.0],
        }
    )

    monkeypatch.setattr(
        query_service.repo,
        "list_columns",
        lambda: ["Driven wheels", "2024 Jan", "2024 Feb"],
    )
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: frame.loc[:, columns].copy(),
    )

    result = query_service.query_grouped_time_series(
        filters={},
        grain="month",
        group_by="四驱占比",
        top_n=10,
        include_others=False,
        time_range={"start": "2024 Jan", "end": "2024 Feb"},
    )

    assert result["items"] == [
        {"time": "2024 Jan", "value": 25.0, "series": "4x4"},
        {"time": "2024 Feb", "value": 20.0, "series": "4x4"},
    ]


def test_query_grouped_time_series_returns_business_private_share_yearly(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Registration type": ["Business", "Private", "?"],
            "2024": [60.0, 40.0, 0.0],
            "2025": [50.0, 45.0, 5.0],
        }
    )

    monkeypatch.setattr(
        query_service.repo,
        "list_columns",
        lambda: ["Registration type", "2024", "2025"],
    )
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: frame.loc[:, columns].copy(),
    )

    result = query_service.query_grouped_time_series(
        filters={},
        grain="year",
        group_by="Business/Private 占比",
        top_n=10,
        include_others=False,
        time_range={"start": "2025", "end": "2025"},
    )

    assert result["items"] == [
        {"time": "2025", "value": 50.0, "series": "Business"},
        {"time": "2025", "value": 45.0, "series": "Private"},
    ]


def test_query_grouped_time_series_returns_awd_share_split_by_segment(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Driven wheels": ["4x4", "front", "4x4", "front"],
            "细分市场（按车长）": ["SUV A", "SUV A", "SUV A0", "SUV A0"],
            "2024 Jan": [20.0, 80.0, 30.0, 30.0],
        }
    )

    monkeypatch.setattr(
        query_service.repo,
        "list_columns",
        lambda: ["Driven wheels", "细分市场（按车长）", "2024 Jan"],
    )
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: frame.loc[:, columns].copy(),
    )

    result = query_service.query_grouped_time_series(
        filters={},
        grain="month",
        group_by="四驱占比",
        top_n=10,
        include_others=False,
        share_split_by="segment",
        time_range={"start": "2024 Jan", "end": "2024 Jan"},
    )

    assert result["items"] == [
        {"time": "2024 Jan", "value": 20.0, "series": "SUV A"},
        {"time": "2024 Jan", "value": 50.0, "series": "SUV A0"},
    ]


def test_query_grouped_time_series_returns_business_private_share_split_by_powertrain(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Registration type": ["Business", "Private", "Business", "Private"],
            "动总规整": ["BEV", "BEV", "PHEV", "PHEV"],
            "2025": [30.0, 70.0, 80.0, 20.0],
        }
    )

    monkeypatch.setattr(
        query_service.repo,
        "list_columns",
        lambda: ["Registration type", "动总规整", "2025"],
    )
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: frame.loc[:, columns].copy(),
    )

    result = query_service.query_grouped_time_series(
        filters={},
        grain="year",
        group_by="Business/Private 占比",
        top_n=10,
        include_others=False,
        share_split_by="powertrain",
        time_range={"start": "2025", "end": "2025"},
    )

    assert result["items"] == [
        {"time": "2025", "value": 30.0, "series": "BEV · Business"},
        {"time": "2025", "value": 70.0, "series": "BEV · Private"},
        {"time": "2025", "value": 80.0, "series": "PHEV · Business"},
        {"time": "2025", "value": 20.0, "series": "PHEV · Private"},
    ]


def test_query_advanced_chart_respects_time_range_window(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Powertrain": ["BEV", "ICE"],
            "2024 Jan": [100.0, 0.0],
            "2024 Feb": [0.0, 40.0],
            "2024 Mar": [0.0, 100.0],
        }
    )

    monkeypatch.setattr(
        query_service.repo,
        "list_columns",
        lambda: ["Powertrain", "2024 Jan", "2024 Feb", "2024 Mar"],
    )
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: frame.loc[:, columns].copy(),
    )

    result = query_service.query_advanced_chart(
        group="market_structure",
        chart="powertrain_mix",
        filters={},
        top_n=1,
        time_range={"start": "2024 Jan", "end": "2024 Feb"},
    )

    assert result["rows"] == 1
    assert result["items"][0]["label"] == "BEV"
    assert result["items"][0]["value"] == 100.0


def test_powertrain_bubble_group_top_n_keeps_same_model_names_separate_by_brand(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Make": ["BRAND A", "BRAND B", "BRAND C"],
            "Model": ["Twin", "Twin", "Solo"],
            "Version name": ["Base", "Base", "Base"],
            "细分市场": ["SUV A", "SUV A", "SUV A"],
            "动总规整": ["BEV", "BEV", "BEV"],
            "车长(mm)": [4300.0, 4400.0, 4500.0],
            "MSRP": [30000.0, 31000.0, 32000.0],
            "2026 Jan": [100.0, 1.0, 90.0],
        }
    )

    columns = list(frame.columns)
    monkeypatch.setattr(query_service.repo, "list_columns", lambda: columns)
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: frame.loc[:, columns].copy(),
    )

    result = query_service.query_advanced_chart(
        group="market_structure",
        chart="powertrain_bubble",
        filters={},
        top_n=1,
        options={
            "group_top_n": True,
            "group_dimension": "segment",
            "group_values": ["SUV A"],
            "time_range": {"start": "2026 Jan", "end": "2026 Jan"},
        },
    )

    assert result["rows"] == 1
    assert result["items"][0]["ModelKey"] == "BRAND A::Twin"
    assert result["items"][0]["Sales"] == pytest.approx(100.0)


def test_query_model_versions_respects_time_range_window(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Model": ["Falcon", "Falcon"],
            "Version": ["Base", "Max"],
            "Make": ["A", "A"],
            "动力总成": ["BEV", "BEV"],
            "车长(mm)": [4500.0, 4510.0],
            "MSRP": [30000.0, 35000.0],
            "Trim level": ["Base", "Max"],
            "2024 Jan": [100.0, 0.0],
            "2024 Mar": [0.0, 80.0],
        }
    )

    monkeypatch.setattr(
        query_service.repo,
        "list_columns",
        lambda: [
            "Model",
            "Version",
            "Make",
            "动力总成",
            "车长(mm)",
            "MSRP",
            "Trim level",
            "2024 Jan",
            "2024 Mar",
        ],
    )
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: frame.loc[:, columns].copy(),
    )

    result = query_service.query_model_versions(
        filters={},
        model_name="Falcon",
        top_n=1,
        time_range={"start": "2024 Mar", "end": "2024 Mar"},
    )

    assert result["rows"] == 1
    assert result["items"][0]["Version"] == "Max"
    assert result["items"][0]["Sales"] == 80.0


def test_query_positioning_map_respects_time_range_window(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "Make": ["BrandA", "BrandB"],
            "Model": ["Alpha", "Beta"],
            "细分市场": ["C-SUV", "C-SUV"],
            "动力总成": ["BEV", "BEV"],
            "车长(mm)": [4500.0, 4520.0],
            "MSRP": [30000.0, 32000.0],
            "2024 Jan": [100.0, 0.0],
            "2024 Mar": [0.0, 120.0],
        }
    )

    monkeypatch.setattr(
        query_service.repo,
        "list_columns",
        lambda: [
            "Make",
            "Model",
            "细分市场",
            "动力总成",
            "车长(mm)",
            "MSRP",
            "2024 Jan",
            "2024 Mar",
        ],
    )
    monkeypatch.setattr(
        query_service.repo,
        "load_slice",
        lambda columns, filters, limit, offset: frame.loc[:, columns].copy(),
    )

    result = query_service.query_positioning_map(
        filters={},
        target_length=None,
        target_msrp=None,
        length_range=600,
        manual_competitors=None,
        top_n=1,
        n_clusters=4,
        time_range={"start": "2024 Mar", "end": "2024 Mar"},
    )

    assert result["rows"] == 1
    assert result["items"][0]["Model"] == "Beta"
    assert result["items"][0]["Sales"] == 120.0


def test_build_peer_corridor_includes_stance_verdict() -> None:
    agg = pd.DataFrame(
        {
            "Length": [4688.0, 4700.0],
            "MSRP": [45000.0, 48000.0],
            "Sales": [80000.0, 25000.0],
        }
    )

    peer_corridor = query_service._build_peer_corridor(
        agg,
        target_length=4500.0,
        target_msrp=35000.0,
    )

    assert peer_corridor is not None
    assert peer_corridor["positionLabel"] == "below-peer-range"
    assert peer_corridor["stanceCode"] == "aggressive-share-take"
    assert peer_corridor["stanceLabel"] == "进攻切入价"
    assert "volume / share take" in peer_corridor["stanceDetail"]
