from types import SimpleNamespace

import pandas as pd
import pytest

from app.infra import parquet_repository


class _FakeTable:
    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame

    def to_pandas(self) -> pd.DataFrame:
        return self._frame.copy()


class _FakeDataset:
    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame
        self.schema = SimpleNamespace(names=list(frame.columns))

    def to_table(
        self,
        columns: list[str],
        filter=None,  # noqa: A002
    ) -> _FakeTable:
        return _FakeTable(self._frame.loc[:, columns])


@pytest.fixture(autouse=True)
def clear_repository_caches(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(parquet_repository, "_columns_cache", None)
    monkeypatch.setattr(parquet_repository, "_freshness_cache", None)
    monkeypatch.setattr(
        parquet_repository,
        "METADATA_PERSISTENT_CACHE_DIR",
        tmp_path,
    )


def test_time_series_month_returns_latest_rows(monkeypatch) -> None:
    frame = pd.DataFrame(
        {
            "2025 Nov": [10, 20],
            "2025 Dec": [30, 40],
            "2026 Jan": [50, 60],
            "2026 Feb": [70, 80],
        }
    )

    monkeypatch.setattr(
        parquet_repository,
        "load_precomputed",
        lambda name: pd.DataFrame(),
    )
    monkeypatch.setattr(
        parquet_repository,
        "_open_dataset",
        lambda: _FakeDataset(frame),
    )
    monkeypatch.setattr(
        parquet_repository,
        "_build_filter_expression",
        lambda filters: None,
    )

    result = parquet_repository.time_series(
        filters={"国家": ["芬兰"]},
        grain="month",
        top_n=3,
    )

    assert result["time"].tolist() == ["2025 Dec", "2026 Jan", "2026 Feb"]
    assert result["value"].tolist() == [70.0, 110.0, 150.0]


def test_count_rows_falls_back_to_full_parquet_when_partition_files_exceed_manifest(
    tmp_path,
    monkeypatch,
) -> None:
    processed_root = tmp_path / "04_Processed_data"
    full_parquet = processed_root / "jato_full_archive.parquet"
    partitioned_root = processed_root / "partitioned_dataset_v1"
    partition_dir = partitioned_root / "国家=%E7%91%9E%E5%85%B8"
    processed_root.mkdir(parents=True)
    partition_dir.mkdir(parents=True)

    pd.DataFrame({"国家": ["瑞典"], "2026 Mar": [26576]}).to_parquet(
        full_parquet,
        index=False,
    )
    pd.DataFrame({"2026 Mar": [26576]}).to_parquet(
        partition_dir / "part-a.parquet",
        index=False,
    )
    pd.DataFrame({"2026 Mar": [26576]}).to_parquet(
        partition_dir / "part-b.parquet",
        index=False,
    )
    (partitioned_root / "manifest.json").write_text(
        '{"parquetFileCount": 1}',
        encoding="utf-8",
    )

    monkeypatch.setattr(parquet_repository, "PARQUET_PATH", full_parquet)
    monkeypatch.setattr(parquet_repository, "PARTITIONED_PATH", partitioned_root)
    monkeypatch.setattr(parquet_repository, "_dataset_cache", None)
    monkeypatch.setattr(parquet_repository, "_dataset_cache_token", None)

    assert parquet_repository._resolve_dataset_path() == full_parquet
    assert parquet_repository.count_rows({}) == 1


def test_list_columns_uses_persistent_metadata_cache(monkeypatch) -> None:
    frame = pd.DataFrame({"国家": ["瑞典"], "2026 Jan": [10]})
    open_calls = 0

    def open_dataset() -> _FakeDataset:
        nonlocal open_calls
        open_calls += 1
        return _FakeDataset(frame)

    monkeypatch.setattr(
        parquet_repository,
        "METADATA_PERSISTENT_CACHE_ENABLED",
        True,
    )
    monkeypatch.setattr(
        parquet_repository,
        "current_dataset_token",
        lambda: "dataset-a",
    )
    monkeypatch.setattr(parquet_repository, "_open_dataset", open_dataset)

    first = parquet_repository.list_columns()
    monkeypatch.setattr(parquet_repository, "_columns_cache", None)
    second = parquet_repository.list_columns()

    assert first == ["国家", "2026 Jan"]
    assert second == first
    assert open_calls == 1
    assert list(parquet_repository.METADATA_PERSISTENT_CACHE_DIR.glob("*.json"))


def test_country_data_freshness_uses_persistent_metadata_cache(
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "国家": ["瑞典", "芬兰"],
            "2026 Jan": [10, 0],
            "2026 Feb": [0, 20],
        }
    )
    open_calls = 0

    def open_dataset() -> _FakeDataset:
        nonlocal open_calls
        open_calls += 1
        return _FakeDataset(frame)

    monkeypatch.setattr(
        parquet_repository,
        "METADATA_PERSISTENT_CACHE_ENABLED",
        True,
    )
    monkeypatch.setattr(
        parquet_repository,
        "current_dataset_token",
        lambda: "dataset-a",
    )
    monkeypatch.setattr(parquet_repository, "_open_dataset", open_dataset)

    first = parquet_repository.country_data_freshness()
    monkeypatch.setattr(parquet_repository, "_freshness_cache", None)
    second = parquet_repository.country_data_freshness()

    assert first == [
        {"country": "瑞典", "latestMonth": "2026 Jan", "monthsInWindow": 1},
        {"country": "芬兰", "latestMonth": "2026 Feb", "monthsInWindow": 2},
    ]
    assert second == first
    assert open_calls == 1
    assert list(parquet_repository.METADATA_PERSISTENT_CACHE_DIR.glob("*.json"))
