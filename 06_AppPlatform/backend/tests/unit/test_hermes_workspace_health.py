"""Tests for hermes_workspace_health_service."""

from __future__ import annotations

import pytest


# ── helpers ──────────────────────────────────────────────────────────


def _mock_git_lines_factory(mapping: dict | None = None):
    """Return a callable that returns *mapping* keyed by git args tuple."""

    def _git_lines(*args: str, **kw):
        return mapping.get(args, []) if mapping else []

    return _git_lines


# ── tests ───────────────────────────────────────────────────────────


class TestGetWorkspaceHealth:
    def test_clean_workspace(self, monkeypatch):
        from app.services import hermes_workspace_health_service as whs

        def _mock_git(*args, **kw):
            if args[0] == "rev-parse":
                return [".git"]
            return []

        monkeypatch.setattr(whs, "_git_lines", _mock_git)
        health = whs.get_workspace_health()
        assert health["riskLevel"] == "low"
        assert health["unlinkedChanges"] == 0
        assert health["changedFiles"] == []
        assert health["stagedFiles"] == []
        assert health["committedUnpushed"] == []
        assert health["warnings"] == []
        assert health["gitAvailable"] is True

    def test_unlinked_changes_medium(self, monkeypatch):
        from app.services import hermes_workspace_health_service as whs

        mapping = {
            ("diff", "--name-only"): ["a.py", "b.ts", "c.tsx", "d.yaml", "e.css"],
            ("diff", "--cached", "--name-only"): [],
            ("log", "origin/main..HEAD", "--oneline"): [],
        }
        monkeypatch.setattr(whs, "_git_lines", _mock_git_lines_factory(mapping))
        health = whs.get_workspace_health()
        assert health["unlinkedChanges"] == 5
        assert health["riskLevel"] == "medium"
        assert "5 code files changed" in health["warnings"][0]

    def test_unlinked_changes_high(self, monkeypatch):
        from app.services import hermes_workspace_health_service as whs

        files = [f"{c}.py" for c in "abcdefghijk"]  # 11 files
        mapping = {
            ("diff", "--name-only"): files,
            ("diff", "--cached", "--name-only"): [],
            ("log", "origin/main..HEAD", "--oneline"): [],
        }
        monkeypatch.setattr(whs, "_git_lines", _mock_git_lines_factory(mapping))
        health = whs.get_workspace_health()
        assert health["unlinkedChanges"] == 11
        assert health["riskLevel"] == "high"

    def test_dev_events_present_no_unlinked(self, monkeypatch):
        from app.services import hermes_workspace_health_service as whs

        mapping = {
            ("diff", "--name-only"): ["a.py", "dev_events.jsonl"],
            ("diff", "--cached", "--name-only"): [],
            ("log", "origin/main..HEAD", "--oneline"): [],
        }
        monkeypatch.setattr(whs, "_git_lines", _mock_git_lines_factory(mapping))
        health = whs.get_workspace_health()
        assert health["unlinkedChanges"] == 0

    def test_unpushed_commits_warning(self, monkeypatch):
        from app.services import hermes_workspace_health_service as whs

        mapping = {
            ("diff", "--name-only"): [],
            ("diff", "--cached", "--name-only"): [],
            ("log", "origin/main..HEAD", "--oneline"): [
                "abc123 feat: one",
                "def456 fix: two",
                "ghi789 chore: three",
                "jkl012 test: four",
                "mno345 docs: five",
            ],
        }
        monkeypatch.setattr(whs, "_git_lines", _mock_git_lines_factory(mapping))
        health = whs.get_workspace_health()
        assert len(health["committedUnpushed"]) == 5
        assert "5 unpushed commits" in str(health["warnings"])

    def test_git_unavailable(self, monkeypatch):
        from app.services import hermes_workspace_health_service as whs

        def _raise(*a, **kw):
            raise Exception("boom")

        monkeypatch.setattr(whs, "_git_lines", _raise)
        health = whs.get_workspace_health()
        assert health["gitAvailable"] is False
        assert any("git unavailable" in w for w in health["warnings"])

    def test_staged_files_tracked(self, monkeypatch):
        from app.services import hermes_workspace_health_service as whs

        mapping = {
            ("diff", "--name-only"): [],
            ("diff", "--cached", "--name-only"): ["staged.py", "also.yaml"],
            ("log", "origin/main..HEAD", "--oneline"): [],
        }
        monkeypatch.setattr(whs, "_git_lines", _mock_git_lines_factory(mapping))
        health = whs.get_workspace_health()
        assert health["unlinkedChanges"] == 2  # staged code but no dev_events
        assert "staged.py" in health["stagedFiles"]

    def test_non_code_files_ignored(self, monkeypatch):
        from app.services import hermes_workspace_health_service as whs

        mapping = {
            ("diff", "--name-only"): ["image.png", "readme.md", "data.csv"],
            ("diff", "--cached", "--name-only"): [],
            ("log", "origin/main..HEAD", "--oneline"): [],
        }
        monkeypatch.setattr(whs, "_git_lines", _mock_git_lines_factory(mapping))
        health = whs.get_workspace_health()
        assert health["unlinkedChanges"] == 0  # no code files changed

    def test_node_modules_ignored(self, monkeypatch):
        from app.services import hermes_workspace_health_service as whs

        mapping = {
            ("diff", "--name-only"): ["node_modules/pkg/foo.ts", "src/bar.py"],
            ("diff", "--cached", "--name-only"): [],
            ("log", "origin/main..HEAD", "--oneline"): [],
        }
        monkeypatch.setattr(whs, "_git_lines", _mock_git_lines_factory(mapping))
        health = whs.get_workspace_health()
        assert health["unlinkedChanges"] == 1  # only bar.py
