from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "data_pipeline"
    / "fill_msrp_source_powertrain_keywords.py"
)


def load_module():
    module_name = "fill_msrp_source_powertrain_keywords_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


keyword_fill = load_module()


def test_dry_run_reports_replacements_without_writing(tmp_path: Path) -> None:
    source_root = tmp_path / "source_drafts"
    path = source_root / "es" / "01_codex_es.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        "\n".join(
            [
                "structured_fields:",
                "  powertrain_rules:",
                "  - key: powertrain_bev",
                "    powertrain: BEV",
                "    keywords:",
                "      - TODO_BEV_KEYWORD",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = keyword_fill.fill_source_drafts(source_root, execute=False)

    assert report.replacement_count == 1
    assert report.changed_file_count == 1
    assert "TODO_BEV_KEYWORD" in path.read_text(encoding="utf-8")


def test_execute_replaces_country_powertrain_keywords(tmp_path: Path) -> None:
    source_root = tmp_path / "source_drafts"
    path = source_root / "es" / "01_codex_es.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        "\n".join(
            [
                "structured_fields:",
                "  powertrain_rules:",
                "  - key: powertrain_bev",
                "    powertrain: BEV",
                "    keywords:",
                "      - TODO_BEV_KEYWORD",
                "  - key: powertrain_ice",
                "    powertrain: ICE",
                "    keywords:",
                "      - TODO_ICE_KEYWORD",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = keyword_fill.fill_source_drafts(source_root, execute=True)
    updated = path.read_text(encoding="utf-8")

    assert report.replacement_count == 2
    assert "TODO_" not in updated
    assert "      - electrico" in updated
    assert "      - gasolina" in updated
