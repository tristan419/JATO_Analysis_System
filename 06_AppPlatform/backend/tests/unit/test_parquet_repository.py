from types import SimpleNamespace

import pandas as pd

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
