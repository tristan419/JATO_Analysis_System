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

    def test_list_cleans_duplicate_event_titles(self, tmp_path):
        from app.services.hermes_devsync_service import list_features
        p = tmp_path / "hermes" / "registry" / "features.yaml"
        p.write_text(
            "features:\n"
            "- featureId: backend\n"
            "  title: 'feat: UI animation toolkit + Presence Phase 1'\n"
            "  status: implemented\n"
            "- featureId: frontend\n"
            "  title: 'feat: UI animation toolkit + Presence Phase 1'\n"
            "  status: implemented\n"
            "- featureId: presence-websocket\n"
            "  title: 'feat: UI animation toolkit + Presence Phase 1'\n"
            "  status: implemented\n"
        )

        titles = [f["title"] for f in list_features()]
        assert titles == ["Backend", "Frontend", "Presence WebSocket"]

    def test_upsert_cleans_commit_style_title(self, tmp_path):
        from app.services.hermes_devsync_service import upsert_feature, list_features
        upsert_feature({
            "featureId": "hermes-devsync",
            "title": "fix: add write permissions and pull-before-push",
            "status": "implemented",
        })
        assert list_features()[0]["title"] == "Hermes DevSync"

    def test_list_merges_canonical_feature_ids(self, tmp_path):
        from app.services.hermes_devsync_service import list_features
        p = tmp_path / "hermes" / "registry" / "features.yaml"
        p.write_text(
            "features:\n"
            "- featureId: feature.presence_websocket\n"
            "  title: Presence WebSocket\n"
            "  linkedEventIds: [manual]\n"
            "  endpoints: []\n"
            "- featureId: presence-websocket\n"
            "  title: Presence WebSocket\n"
            "  linkedEventIds: [git]\n"
            "  endpoints: [GET /online]\n"
        )

        features = list_features()
        assert len(features) == 1
        assert features[0]["featureId"] == "feature.presence_websocket"
        assert features[0]["linkedEventIds"] == ["manual", "git"]
        assert features[0]["endpoints"] == ["GET /online"]

    def test_upsert_matches_canonical_feature_ids(self, tmp_path):
        from app.services.hermes_devsync_service import list_features, upsert_feature
        upsert_feature({
            "featureId": "feature.presence_websocket",
            "title": "Presence WebSocket",
            "linkedEventIds": ["manual"],
        })
        upserted = upsert_feature({
            "featureId": "presence-websocket",
            "title": "feat: presence websocket polish",
            "linkedEventIds": ["git"],
            "endpoints": ["GET /online"],
        })

        features = list_features()
        assert len(features) == 1
        assert upserted["featureId"] == "feature.presence_websocket"
        assert upserted["title"] == "Presence WebSocket"
        assert features[0]["linkedEventIds"] == ["manual", "git"]

    def test_alias_feature_ids_merge_into_canonical_feature(self, tmp_path):
        from app.services.hermes_devsync_service import list_features, upsert_feature
        upsert_feature({
            "featureId": "hermes-chat-gateway",
            "title": "Hermes Chat Gateway",
            "linkedEventIds": ["chat"],
        })
        upsert_feature({
            "featureId": "hermes-command-gateway",
            "title": "Hermes Command Gateway",
            "linkedEventIds": ["command"],
            "endpoints": ["POST /hermes/commands/execute"],
        })

        features = list_features()
        assert len(features) == 1
        assert features[0]["featureId"] == "hermes-chat-gateway"
        assert "hermes-command-gateway" in features[0]["aliases"]
        assert features[0]["linkedEventIds"] == ["chat", "command"]

    def test_hermes_scripts_aliases_to_devsync(self, tmp_path):
        from app.services.hermes_devsync_service import list_features, upsert_feature
        upsert_feature({"featureId": "hermes-devsync", "title": "Hermes DevSync"})
        upsert_feature({"featureId": "hermes-scripts", "title": "Hermes Scripts"})

        features = list_features()
        assert len(features) == 1
        assert features[0]["featureId"] == "hermes-devsync"
        assert "hermes-scripts" in features[0]["aliases"]

    def test_noisy_git_commit_backend_frontend_features_are_filtered(self, tmp_path):
        from app.services.hermes_devsync_service import list_features
        p = tmp_path / "hermes" / "registry" / "features.yaml"
        p.write_text(
            "features:\n"
            "- featureId: backend\n"
            "  title: 'feat: generic backend noise'\n"
            "  source: git_commit\n"
            "- featureId: frontend\n"
            "  title: 'feat: generic frontend noise'\n"
            "  source: git_commit\n"
            "- featureId: hermes-devsync\n"
            "  title: Hermes DevSync\n"
        )

        assert [f["featureId"] for f in list_features()] == ["hermes-devsync"]

    def test_list_features_hydrates_missing_tests_from_kanban(self, tmp_path):
        from app.services.hermes_devsync_service import list_features

        (tmp_path / "hermes" / "registry" / "features.yaml").write_text(
            "features:\n"
            "- featureId: feature.review_workbench\n"
            "  title: Review Workbench\n"
            "  tests: {}\n"
            "  docs: []\n"
        )
        (tmp_path / "hermes" / "feature_registry.yaml").write_text(
            "features:\n"
            "- featureId: feature.review_workbench\n"
            "  name: Review Workbench\n"
            "  tests:\n"
            "  - 'Backend: test_review_workbench_service.py'\n"
            "  docs:\n"
            "  - MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md\n"
        )

        feature = list_features()[0]

        assert feature["tests"] == {
            "Backend: test_review_workbench_service.py": "recorded in feature_registry.yaml"
        }
        assert feature["docs"] == [
            "MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md"
        ]


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

    def test_gap_resolves_when_docs_or_tests_are_recorded(self, tmp_path):
        from app.services.hermes_devsync_service import (
            create_missing_docs_gap,
            create_missing_tests_gap,
            reconcile_feature_gaps,
        )

        feature = {"featureId": "hermes-devsync", "title": "Hermes DevSync"}
        create_missing_docs_gap({**feature, "docs": []})
        create_missing_tests_gap({**feature, "tests": {}})

        resolved = reconcile_feature_gaps({
            **feature,
            "docs": ["Markdown_Readme/features/hermes-devsync.md"],
            "tests": {"backend": "passed"},
        })

        import yaml
        gaps = yaml.safe_load((tmp_path / "hermes" / "governance_gaps.yaml").read_text())["gaps"]
        assert resolved == 2
        assert {g["status"] for g in gaps} == {"resolved"}

    def test_noisy_commit_gaps_are_retired(self, tmp_path):
        from app.services.hermes_devsync_service import retire_noisy_devsync_gaps

        (tmp_path / "hermes" / "governance_gaps.yaml").write_text(
            "gaps:\n"
            "- gapId: gap.devsync.fix-health-check-15-retries-x-5s.missing_tests\n"
            "  title: noisy\n"
            "  status: open\n"
            "- gapId: gap.devsync.hermes-auto-dev-event-from-push-a255d000.missing_tests\n"
            "  title: noisy hook event\n"
            "  status: open\n"
            "- gapId: gap.devsync.hermes-record-ops-runner-dev-event.missing_tests\n"
            "  title: noisy record event\n"
            "  status: open\n"
        )

        assert retire_noisy_devsync_gaps() == 3

        import yaml
        gaps = yaml.safe_load((tmp_path / "hermes" / "governance_gaps.yaml").read_text())["gaps"]
        assert {gap["status"] for gap in gaps} == {"resolved"}


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
        assert feats[0]["docs"] == ["Markdown_Readme/features/test-feature.md"]
        ev = tmp_path / "hermes" / "evidence_ledger.jsonl"
        assert ev.is_file()

    def test_sync_prefers_specific_inferred_feature_over_generic_links(self, tmp_path):
        from app.services.hermes_devsync_service import append_dev_event, list_features, sync_dev_events
        append_dev_event(_make_event(
            "e1",
            title="feat: JATO monthly update guardrails",
            linkedFeatureIds=["frontend", "backend"],
            changedFiles=[
                "06_AppPlatform/backend/app/services/jato_monthly_update_service.py",
                "06_AppPlatform/frontend/src/pages/JatoMonthlyUpdatePage.tsx",
            ],
            frontendChanges=["06_AppPlatform/frontend/src/pages/JatoMonthlyUpdatePage.tsx"],
            backendChanges=["06_AppPlatform/backend/app/services/jato_monthly_update_service.py"],
            tests={},
        ))

        sync_dev_events()

        features = list_features()
        assert [f["featureId"] for f in features] == ["feature.jato_monthly_update"]

    def test_sync_skips_noisy_git_commit_without_specific_feature(self, tmp_path):
        from app.services.hermes_devsync_service import append_dev_event, list_features, sync_dev_events
        append_dev_event(_make_event(
            "e1",
            source="git_commit",
            title="fix: health check 15 retries x 5s",
            linkedFeatureIds=["fix-health-check-15-retries-x-5s"],
            changedFiles=["03_Scripts/ops/deploy_fullstack_server.sh"],
            tests={},
        ))

        result = sync_dev_events()

        assert result["featuresCreated"] == []
        assert list_features() == []

    def test_sync_skips_auto_generated_dev_event_commits(self, tmp_path):
        from app.services.hermes_devsync_service import append_dev_event, list_features, sync_dev_events

        append_dev_event(_make_event(
            "e1",
            source="git_commit",
            title="hermes: auto dev event from push a255d000",
            linkedFeatureIds=["hermes-auto-dev-event-from-push-a255d000"],
            changedFiles=["hermes/dev_events/dev_events.jsonl"],
            tests={},
        ))
        append_dev_event(_make_event(
            "e2",
            source="git_commit",
            title="hermes: record ops runner dev event",
            linkedFeatureIds=["hermes-record-ops-runner-dev-event"],
            changedFiles=["hermes/dev_events/dev_events.jsonl"],
            tests={},
        ))

        result = sync_dev_events()

        assert result["featuresCreated"] == []
        assert list_features() == []

    def test_sync_to_kanban_preserves_curated_docs_and_tests(self, tmp_path):
        import yaml
        from app.services.hermes_devsync_service import _sync_to_kanban, upsert_feature

        (tmp_path / "hermes" / "feature_registry.yaml").write_text(
            "features:\n"
            "- featureId: feature.current_price\n"
            "  name: Current Price\n"
            "  backendApis:\n"
            "  - GET /v1/msrp/current-prices\n"
            "  docs:\n"
            "  - MSRP/03_Implementation/MSRP_OVERRIDE_AND_PRICE_HISTORY_2026-04-11.md\n"
            "  tests:\n"
            "  - 'Frontend: msrpCurrentPrice.test.ts'\n"
        )
        upsert_feature({
            "featureId": "feature.current_price",
            "title": "feat: current price touch-up",
            "status": "implemented",
            "docs": [],
            "tests": {},
        })

        assert _sync_to_kanban(["feature.current_price"]) == 1

        features = yaml.safe_load(
            (tmp_path / "hermes" / "feature_registry.yaml").read_text(),
        )["features"]
        current_price = features[0]
        assert current_price["docs"] == [
            "MSRP/03_Implementation/MSRP_OVERRIDE_AND_PRICE_HISTORY_2026-04-11.md"
        ]
        assert current_price["tests"] == ["Frontend: msrpCurrentPrice.test.ts"]
        assert current_price["backendApis"] == ["GET /v1/msrp/current-prices"]
