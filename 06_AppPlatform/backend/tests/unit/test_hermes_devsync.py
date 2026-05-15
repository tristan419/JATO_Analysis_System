import json
from unittest.mock import patch
import pytest

@pytest.fixture(autouse=True)
def _isolate_paths(tmp_path):
    hermes_dir = tmp_path / "hermes"
    hermes_dir.mkdir(exist_ok=True)
    (hermes_dir / "dev_events").mkdir(exist_ok=True)
    (hermes_dir / "registry").mkdir(exist_ok=True)
    (tmp_path / "Markdown_Readme" / "features").mkdir(parents=True, exist_ok=True)
    with (patch("app.services.hermes_devsync_service._project_root", tmp_path),
          patch("app.services.hermes_devsync_service._root", return_value=tmp_path)):
        yield

def _make_event(event_id="dev_evt_001", **overrides):
    base = {"eventId": event_id, "eventType": "implementation_completed",
            "source": "claude_code", "title": "Test Feature", "summary": "Test.",
            "linkedFeatureIds": ["test-feature"], "changedFiles": ["test.py"],
            "addedEndpoints": ["POST /test"], "frontendChanges": ["Test UI"],
            "backendChanges": ["Test BE"], "tests": {"backend": "10 passed"},
            "risks": [], "nextSteps": [], "createdAt": "2026-05-15T17:00:00+08:00"}
    base.update(overrides)
    return base


class TestDevEvents:
    def test_append_and_list(self, tmp_path):
        from app.services.hermes_devsync_service import append_dev_event, list_dev_events
        append_dev_event(_make_event("e1"))
        assert len(list_dev_events()) == 1

    def test_filter_by_event_type(self, tmp_path):
        from app.services.hermes_devsync_service import append_dev_event, list_dev_events
        append_dev_event(_make_event("e1", eventType="test_run"))
        assert len(list_dev_events(event_type="test_run")) == 1
        assert len(list_dev_events(event_type="bug_fix")) == 0

    def test_malformed_lines_skipped(self, tmp_path):
        from app.services.hermes_devsync_service import list_dev_events
        p = tmp_path / "hermes" / "dev_events" / "dev_events.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("not json\n" + json.dumps(_make_event("e1")) + "\n{broken\n")
        assert len(list_dev_events()) == 1

    def test_empty_returns_empty(self, tmp_path):
        from app.services.hermes_devsync_service import list_dev_events
        assert list_dev_events() == []


class TestFeatures:
    def test_upsert_creates_and_updates(self, tmp_path):
        from app.services.hermes_devsync_service import upsert_feature, list_features
        upsert_feature({"featureId": "test", "title": "T", "status": "planned"})
        assert len(list_features()) == 1
        f2 = upsert_feature({"featureId": "test", "status": "implemented"})
        assert f2["status"] == "implemented"
        assert f2["title"] == "T"

    def test_list_filters(self, tmp_path):
        from app.services.hermes_devsync_service import upsert_feature, list_features
        upsert_feature({"featureId": "f1", "title": "One", "status": "done", "category": "gov"})
        upsert_feature({"featureId": "f2", "title": "Two", "status": "planned", "category": "fe"})
        assert len(list_features(status="done")) == 1
        assert len(list_features(category="fe")) == 1


class TestGaps:
    def test_missing_docs_gap(self, tmp_path):
        from app.services.hermes_devsync_service import create_missing_docs_gap
        gap = create_missing_docs_gap({"featureId": "f1", "title": "F1", "docs": []})
        assert gap is not None
        assert gap["gapId"] == "gap.devsync.f1.missing_docs"

    def test_no_gap_when_docs_exist(self, tmp_path):
        from app.services.hermes_devsync_service import create_missing_docs_gap
        assert create_missing_docs_gap({"featureId": "f1", "title": "F1", "docs": ["d.md"]}) is None

    def test_missing_tests_gap(self, tmp_path):
        from app.services.hermes_devsync_service import create_missing_tests_gap
        gap = create_missing_tests_gap({"featureId": "f1", "title": "F1", "tests": {}})
        assert gap is not None
        assert gap["severity"] == "high"


class TestMarkdown:
    def test_generates(self, tmp_path):
        from app.services.hermes_devsync_service import generate_feature_markdown
        path = generate_feature_markdown({
            "featureId": "test", "title": "Test", "status": "done",
            "category": "gov", "source": "cc", "summary": "A test.",
            "endpoints": ["POST /x"], "tests": {"be": "ok"},
            "lastUpdatedAt": "now"})
        assert path.is_file()
        assert "# Test" in path.read_text()


class TestSync:
    def test_sync_creates_feature(self, tmp_path):
        from app.services.hermes_devsync_service import append_dev_event, sync_dev_events, list_features
        append_dev_event(_make_event("e1"))
        r = sync_dev_events()
        assert r["synced"] == 1
        assert len(r["featuresCreated"]) > 0

    def test_sync_empty_returns_zero(self, tmp_path):
        from app.services.hermes_devsync_service import sync_dev_events
        r = sync_dev_events()
        assert r["synced"] == 0

    def test_full_roundtrip(self, tmp_path):
        from app.services.hermes_devsync_service import append_dev_event, sync_dev_events, list_features
        append_dev_event(_make_event("e1"))
        sync_dev_events()
        feats = list_features()
        assert len(feats) >= 1
        md = tmp_path / "Markdown_Readme" / "features" / "test-feature.md"
        assert md.is_file()
        ev = tmp_path / "hermes" / "evidence_ledger.jsonl"
        assert ev.is_file()
