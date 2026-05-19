"""Unit tests for Hermes governance API endpoints.

Tests the three new/updated endpoints:
  - GET /hermes/gaps
  - GET /hermes/markdown-diagrams
  - GET /hermes/evidence-ledger
"""

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from fastapi.testclient import TestClient

from app.api.routes.hermes import (
    MERMAID_BLOCK_RE,
    _md_diagrams_cache,
    router,
)


@pytest.fixture
def client():
    """Return a TestClient with only the hermes router mounted."""
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


# ── helpers ──────────────────────────────────────────────────────────

def _write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data))


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


# ── /hermes/sentinel + deploy status ─────────────────────────────────

class TestSentinelAndDeploy:
    def test_set_notification_status_route(self, client, tmp_path, monkeypatch):
        from app.services import hermes_sentinel_service as sentinel

        monkeypatch.setattr(sentinel, "_project_root", tmp_path)
        created = sentinel._emit("devsync", "medium", "Missing Docs", "Body")
        assert created is not None

        resp = client.post(
            f"/hermes/sentinel/notifications/{created['id']}/status",
            json={"status": "archived"},
        )

        assert resp.status_code == 200
        assert resp.json()["status"] == "archived"

    def test_deploy_status_endpoint_reports_drift(self, client, tmp_path, monkeypatch):
        from app.services import hermes_deploy_status_service as deploy_status

        monkeypatch.setattr(deploy_status, "_project_root", tmp_path)
        _write_json(tmp_path / "hermes" / "deploy_release.json", {
            "commitSha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "shortSha": "aaaaaaaa",
            "source": "github_actions_archive",
        })
        _write_json(tmp_path / "hermes" / "deploy_expected.json", {
            "commitSha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "shortSha": "bbbbbbbb",
        })

        resp = client.get("/hermes/deploy/status")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "critical"
        assert data["drift"]["isDrift"] is True

    def test_full_design_document_endpoint(self, client, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir(parents=True)
        (reports_dir / "HERMES_FULL_DESIGN_DOCUMENT.md").write_text("# Hermes\n\nDesign", encoding="utf-8")

        with patch("app.api.routes.hermes.HERMES_DIR", tmp_path), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path.parent):
            resp = client.get("/hermes/reports/full-design-document")

        assert resp.status_code == 200
        data = resp.json()
        assert data["exists"] is True
        assert "# Hermes" in data["content"]


# ── /hermes/gaps ──────────────────────────────────────────────────────

class TestGaps:
    def test_returns_all_gaps_when_no_filters(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test", "severity": "high"},
                {"gapId": "g2", "status": "resolved", "category": "docs", "severity": "low"},
                {"gapId": "g3", "status": "in_progress", "category": "pipeline", "severity": "medium"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3

    def test_filters_by_status(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test"},
                {"gapId": "g2", "status": "resolved", "category": "docs"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?status=open")
        assert resp.status_code == 200
        assert len(resp.json()) == 1
        assert resp.json()[0]["gapId"] == "g1"

    def test_filters_by_category(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test"},
                {"gapId": "g2", "status": "open", "category": "docs"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?category=test")
        assert resp.status_code == 200
        assert len(resp.json()) == 1
        assert resp.json()[0]["gapId"] == "g1"

    def test_filters_combined_and_logic(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test"},
                {"gapId": "g2", "status": "resolved", "category": "test"},
                {"gapId": "g3", "status": "open", "category": "docs"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?status=open&category=test")
        assert resp.status_code == 200
        assert len(resp.json()) == 1
        assert resp.json()[0]["gapId"] == "g1"

    def test_unknown_status_returns_empty(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?status=nonexistent")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_unknown_category_returns_empty(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open", "category": "test"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?category=nonexistent")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_file_missing_returns_empty(self, client, tmp_path):
        nonexistent = tmp_path / "nonexistent"
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", nonexistent):
            resp = client.get("/hermes/gaps")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_malformed_yaml_returns_empty(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        gaps_yaml.parent.mkdir(parents=True, exist_ok=True)
        gaps_yaml.write_text(":!!:bad yaml: - [")
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps")
        # Malformed YAML is caught and returns empty list
        assert resp.status_code == 200
        assert resp.json() == []

    def test_gaps_without_status_field_not_filtered_out(self, client, tmp_path):
        """Gaps missing 'status' key should be left in when no filter is active."""
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "category": "test"},
                {"gapId": "g2", "status": "open", "category": "test"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps")
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    def test_gaps_without_category_field_not_filtered_out(self, client, tmp_path):
        gaps_yaml = tmp_path / "governance_gaps.yaml"
        _write_yaml(gaps_yaml, {
            "gaps": [
                {"gapId": "g1", "status": "open"},
                {"gapId": "g2", "status": "open", "category": "test"},
            ]
        })
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/gaps?category=test")
        assert resp.status_code == 200
        assert len(resp.json()) == 1


# ── /hermes/evidence-ledger ──────────────────────────────────────────

class TestEvidenceLedger:
    def _make_entry(self, created_at: str, etype: str = "fact") -> dict:
        return {"createdAt": created_at, "type": etype, "fact": "test fact"}

    def test_returns_empty_when_file_missing(self, client, tmp_path):
        nonexistent = tmp_path / "nonexistent_ledger.jsonl"
        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", nonexistent.parent):
            with patch("app.api.routes.hermes.HERMES_DIR", nonexistent.parent):
                resp = client.get("/hermes/evidence-ledger")
        # file not found → empty return, 200
        assert resp.status_code == 200
        data = resp.json()
        assert data["totalCount"] == 0
        assert data["records"] == []
        assert data["byType"] == {}
        assert data["rangeStart"] == ""
        assert data["rangeEnd"] == ""

    def test_returns_records_with_type_breakdown(self, client, tmp_path):
        now = datetime.now(timezone.utc)
        entries = [
            self._make_entry((now - timedelta(days=i)).isoformat(), "fact" if i % 2 == 0 else "event")
            for i in range(10)
        ]
        ledger = tmp_path / "evidence_ledger.jsonl"
        _write_json(ledger, entries[0])  # won't write array, need one per line
        ledger.write_text("\n".join(json.dumps(e) for e in entries))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger?days=30")
        assert resp.status_code == 200
        data = resp.json()
        assert data["totalCount"] == 10
        assert len(data["records"]) == 10
        assert "fact" in data["byType"]
        assert "event" in data["byType"]
        assert data["rangeStart"] != ""
        assert data["rangeEnd"] != ""

    def test_days_filter_narrows_results(self, client, tmp_path):
        now = datetime.now(timezone.utc)
        entries = [
            self._make_entry((now - timedelta(days=i)).isoformat()) for i in range(20)
        ]
        ledger = tmp_path / "evidence_ledger.jsonl"
        ledger.write_text("\n".join(json.dumps(e) for e in entries))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp_all = client.get("/hermes/evidence-ledger?days=90&limit=100")
            resp_7 = client.get("/hermes/evidence-ledger?days=7")
        all_data = resp_all.json()
        day7_data = resp_7.json()
        # totalCount is all-time regardless of days filter
        assert all_data["totalCount"] == 20
        assert day7_data["totalCount"] == 20
        # But records within the 7-day window should be fewer
        assert len(day7_data["records"]) <= len(all_data["records"])

    def test_limit_caps_records(self, client, tmp_path):
        now = datetime.now(timezone.utc)
        entries = [
            self._make_entry((now - timedelta(hours=i)).isoformat()) for i in range(50)
        ]
        ledger = tmp_path / "evidence_ledger.jsonl"
        ledger.write_text("\n".join(json.dumps(e) for e in entries))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger?days=90&limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["records"]) == 5
        assert data["totalCount"] == 50  # all-time

    def test_default_days_is_7(self, client, tmp_path):
        now = datetime.now(timezone.utc)
        entries = [
            self._make_entry((now - timedelta(days=i)).isoformat()) for i in range(14)
        ]
        ledger = tmp_path / "evidence_ledger.jsonl"
        ledger.write_text("\n".join(json.dumps(e) for e in entries))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["records"]) <= 7 + 1  # ~7 days of unique dates

    def test_entries_without_created_at_are_handled(self, client, tmp_path):
        ledger = tmp_path / "evidence_ledger.jsonl"
        ledger.write_text(json.dumps({"type": "fact", "event": "no date"}) + "\n")

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger")
        assert resp.status_code == 200
        data = resp.json()
        # No crash, total counts
        assert data["totalCount"] == 1

    def test_corrupt_lines_are_skipped(self, client, tmp_path):
        now = datetime.now(timezone.utc)
        ledger = tmp_path / "evidence_ledger.jsonl"
        lines = [
            "not json",
            json.dumps(self._make_entry(now.isoformat(), "fact")),
            "",
            "{broken",
        ]
        ledger.write_text("\n".join(lines))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger?days=30")
        assert resp.status_code == 200
        data = resp.json()
        assert data["totalCount"] == 1  # only the valid JSON line

    def test_by_type_is_always_present(self, client, tmp_path):
        """byType must be present as {} even when records is empty."""
        entries = [
            self._make_entry((datetime.now(timezone.utc) - timedelta(days=60)).isoformat()),
        ]
        ledger = tmp_path / "evidence_ledger.jsonl"
        ledger.write_text("\n".join(json.dumps(e) for e in entries))

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.HERMES_DIR", tmp_path):
            resp = client.get("/hermes/evidence-ledger?days=1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["records"] == []
        assert data["byType"] == {}
        assert isinstance(data["byType"], dict)


# ── /hermes/markdown-diagrams ─────────────────────────────────────────

MD_SINGLE_FLOWCHART = """# Test Doc

## Pipeline Overview

```mermaid
flowchart TD
    A --> B
    B --> C
```

Some text after.
"""

MD_TWO_DIAGRAMS = """# Two Diagrams

## First Diagram

```mermaid
flowchart LR
    X --> Y
```

## Second Diagram

```mermaid
sequenceDiagram
    Alice->>Bob: Hello
```

End.
"""

MD_NO_MERMAID = """# No Diagrams Here

Just some text and a code block:

```
not mermaid
```
"""

MD_BROKEN_MERMAID = """# Broken Mermaid

```mermaid
this is not valid mermaid at all
```
"""


class TestMarkdownDiagrams:
    def test_extracts_single_flowchart(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_file = md_dir / "test.md"
        md_file.parent.mkdir(parents=True, exist_ok=True)
        md_file.write_text(MD_SINGLE_FLOWCHART)
        # Invalidate cache
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        d = data[0]
        assert d["type"] == "flowchart"
        assert "A --> B" in d["raw"]
        assert d["diagramIndex"] == 0
        assert "test.md" in d["file"]

    def test_extracts_multiple_diagrams(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_file = md_dir / "multi.md"
        md_file.parent.mkdir(parents=True, exist_ok=True)
        md_file.write_text(MD_TWO_DIAGRAMS)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["type"] == "flowchart"
        assert data[1]["type"] == "sequenceDiagram"

    def test_returns_empty_when_no_diagrams(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_file = md_dir / "noop.md"
        md_file.parent.mkdir(parents=True, exist_ok=True)
        md_file.write_text(MD_NO_MERMAID)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_handles_broken_mermaid_gracefully(self, client, tmp_path):
        """A syntactically broken mermaid block is still extracted — rendering
        failure is the frontend's responsibility."""
        md_dir = tmp_path / "Markdown_Readme"
        md_file = md_dir / "broken.md"
        md_file.parent.mkdir(parents=True, exist_ok=True)
        md_file.write_text(MD_BROKEN_MERMAID)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert "not valid" in data[0]["raw"]

    def test_file_filter_filters_by_substring(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        (md_dir / "sub").mkdir(parents=True)
        (md_dir / "a.md").write_text(MD_SINGLE_FLOWCHART)
        (md_dir / "sub" / "b.md").write_text(MD_SINGLE_FLOWCHART)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams?file_filter=sub")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert "sub" in data[0]["file"]

    def test_file_filter_case_insensitive(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        (md_dir / "WORKFLOWS").mkdir(parents=True, exist_ok=True)
        (md_dir / "WORKFLOWS" / "test.md").write_text(MD_SINGLE_FLOWCHART)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams?file_filter=workflows")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    def test_cache_invalidates_on_mtime_change(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        md_file = md_dir / "test.md"
        md_file.write_text(MD_SINGLE_FLOWCHART)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp1 = client.get("/hermes/markdown-diagrams")
        assert resp1.status_code == 200
        cached_data = resp1.json()
        assert len(cached_data) == 1

        # Modify the file
        time.sleep(0.1)
        md_file.write_text(MD_TWO_DIAGRAMS)

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp2 = client.get("/hermes/markdown-diagrams")
        assert resp2.status_code == 200
        # Cache should have been invalidated, now returns 2 diagrams
        assert len(resp2.json()) == 2

    def test_cache_invalidates_when_file_deleted(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        f1 = md_dir / "a.md"
        f2 = md_dir / "b.md"
        f1.write_text(MD_SINGLE_FLOWCHART)
        f2.write_text(MD_SINGLE_FLOWCHART)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp1 = client.get("/hermes/markdown-diagrams")
        assert len(resp1.json()) == 2

        # Delete one file
        f1.unlink()

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp2 = client.get("/hermes/markdown-diagrams")
        assert len(resp2.json()) == 1

    def test_empty_md_dir_returns_empty(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_missing_md_dir_returns_empty(self, client, tmp_path):
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path / "no_such_dir"):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_heading_extraction_from_before_block(self, client, tmp_path):
        content = """# Top Level

## My Flowchart

Some description text here.

```mermaid
flowchart TD
    A --> B
```
"""
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        (md_dir / "heading.md").write_text(content)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["title"] == "My Flowchart"

    def test_non_utf8_file_is_skipped(self, client, tmp_path):
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        (md_dir / "bad.md").write_bytes(b"\xff\xfe\x00\x01")
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_unknown_diagram_type_defaults_to_flowchart(self, client, tmp_path):
        content = """```mermaid
erDiagram
    CUSTOMER ||--o{ ORDER : places
```
"""
        md_dir = tmp_path / "Markdown_Readme"
        md_dir.mkdir(parents=True, exist_ok=True)
        (md_dir / "erd.md").write_text(content)
        _md_diagrams_cache["data"] = None
        _md_diagrams_cache["mtimes"] = {}

        with patch.object(router, "routes", []), patch("app.api.routes.hermes.PROJECT_ROOT", tmp_path):
            resp = client.get("/hermes/markdown-diagrams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["type"] == "flowchart"  # default for unknown


# ── Regex unit tests (no server needed) ──────────────────────────────

class TestMermaidRegex:
    def test_finds_single_block(self):
        blocks = MERMAID_BLOCK_RE.findall(MD_SINGLE_FLOWCHART)
        assert len(blocks) == 1
        assert "flowchart TD" in blocks[0]

    def test_finds_multiple_blocks(self):
        blocks = MERMAID_BLOCK_RE.findall(MD_TWO_DIAGRAMS)
        assert len(blocks) == 2

    def test_no_match_on_no_mermaid(self):
        blocks = MERMAID_BLOCK_RE.findall(MD_NO_MERMAID)
        assert blocks == []

    def test_mermaid_with_trailing_whitespace(self):
        src = "```mermaid  \nflowchart TD\n    A\n```"
        blocks = MERMAID_BLOCK_RE.findall(src)
        assert len(blocks) == 1

    def test_empty_mermaid_block_is_extracted(self):
        src = "```mermaid\n```"
        blocks = MERMAID_BLOCK_RE.findall(src)
        assert len(blocks) == 1
        assert blocks[0].strip() == ""
