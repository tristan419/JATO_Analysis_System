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
