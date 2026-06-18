import threading

import pandas as pd

from app.services import query_service


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
