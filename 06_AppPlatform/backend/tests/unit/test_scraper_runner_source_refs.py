"""Tests for runner source reference resolution."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.scraper import runner


class _EmptyExtractor:
    def __init__(self, code: str):
        self.config = SimpleNamespace(
            source_code=code,
            country="比利时",
            brand="VOLKSWAGEN",
        )

    def extract(self):
        return []


def test_run_scrape_accepts_yaml_file_path(tmp_path, monkeypatch):
    draft_path = tmp_path / "volkswagen_id_4_be.yaml"
    draft_path.write_text("source_code: placeholder\n", encoding="utf-8")

    loaded_paths: list[Path] = []

    def fake_load_all_sources(sources_dir=None):
        return []

    def fake_load_source_file(path: Path):
        loaded_paths.append(path)
        return "volkswagen_id_4_be_draft_scrapling"

    monkeypatch.setattr(runner, "load_all_sources", fake_load_all_sources)
    monkeypatch.setattr(runner, "load_source_file", fake_load_source_file)
    monkeypatch.setattr(
        runner.registry,
        "get",
        lambda code: _EmptyExtractor(code),
    )

    summary = runner.run_scrape([str(draft_path)], dry_run=True)

    assert loaded_paths == [draft_path.resolve()]
    assert list(summary["sources"].keys()) == [
        "volkswagen_id_4_be_draft_scrapling"
    ]
    assert (
        summary["sources"]["volkswagen_id_4_be_draft_scrapling"]["status"]
        == "empty"
    )


def test_run_scrape_accepts_directory_path(tmp_path, monkeypatch):
    draft_dir = tmp_path / "be"
    draft_dir.mkdir()

    load_calls: list[Path | None] = []

    def fake_load_all_sources(sources_dir=None):
        load_calls.append(sources_dir)
        if sources_dir is None:
            return ["bmw_de_scrapling"]
        if Path(sources_dir) == draft_dir.resolve():
            return [
                "volkswagen_id_4_be_draft_scrapling",
                "volkswagen_tiguan_be_draft_scrapling",
            ]
        return []

    monkeypatch.setattr(runner, "load_all_sources", fake_load_all_sources)
    monkeypatch.setattr(
        runner.registry,
        "get",
        lambda code: _EmptyExtractor(code),
    )

    summary = runner.run_scrape([str(draft_dir)], dry_run=True)

    assert load_calls == [None, draft_dir.resolve()]
    assert list(summary["sources"].keys()) == [
        "volkswagen_id_4_be_draft_scrapling",
        "volkswagen_tiguan_be_draft_scrapling",
    ]
