import pandas as pd

from app.services import query_service


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
