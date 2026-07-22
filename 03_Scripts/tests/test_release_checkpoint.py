from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "03_Scripts" / "deploy"))

import release_checkpoint as checkpoint  # noqa: E402


COMMIT = "a" * 40
ARCHIVE_SHA256 = "b" * 64
FRONTEND_CHECKSUM = "c" * 64


class ReleaseCheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.checkpoint_path = self.root / "release-checkpoint.json"
        self.journal_path = self.root / "release-checkpoint.jsonl"
        self.identity = checkpoint.ReleaseIdentity.create(
            repository="example/JATO_Analysis_System",
            commit=COMMIT,
            archive_sha256=ARCHIVE_SHA256,
            archive_bytes=22_000_000,
            run_id=123456,
            run_attempt=2,
            frontend_identity="gha://example/JATO_Analysis_System/artifacts/frontend",
            frontend_checksum=FRONTEND_CHECKSUM,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def write(
        self,
        phase: str,
        *,
        status: str = "completed",
        retry_class: str = "automatic",
        identity: checkpoint.ReleaseIdentity | None = None,
    ) -> dict[str, object]:
        return checkpoint.write_checkpoint(
            checkpoint_path=self.checkpoint_path,
            journal_path=self.journal_path,
            identity=identity or self.identity,
            phase=phase,
            status=status,
            retry_class=retry_class,
            now="2026-07-22T01:02:03.000Z",
        )

    def namespaced_identity(
        self,
        *,
        commit: str = COMMIT,
        archive_sha256: str = ARCHIVE_SHA256,
        run_id: int = 123456,
    ) -> checkpoint.ReleaseIdentity:
        return checkpoint.ReleaseIdentity.create(
            repository=self.identity.repository,
            commit=commit,
            archive_sha256=archive_sha256,
            archive_bytes=self.identity.archiveBytes,
            run_id=run_id,
            run_attempt=self.identity.runAttempt,
            frontend_identity=self.identity.frontendIdentity,
            frontend_checksum=self.identity.frontendChecksum,
        )

    def write_namespaced(
        self,
        checkpoints_root: Path,
        identity: checkpoint.ReleaseIdentity,
        phase: str,
        *,
        status: str = "completed",
        retry_class: str = "automatic",
    ) -> Path:
        checkpoint_path = (
            checkpoints_root
            / identity.commit
            / f"{identity.archiveSha256}.json"
        )
        journal_path = (
            checkpoints_root.parent
            / "journals"
            / identity.commit
            / f"{identity.archiveSha256}.jsonl"
        )
        checkpoint.write_checkpoint(
            checkpoint_path=checkpoint_path,
            journal_path=journal_path,
            identity=identity,
            phase=phase,
            status=status,
            retry_class=retry_class,
            now="2026-07-22T01:02:03.000Z",
        )
        return checkpoint_path

    def test_write_is_private_and_atomically_overwrites_checkpoint(self) -> None:
        first = self.write("packaged")
        second = self.write("transport_verified")

        persisted = checkpoint.load_checkpoint(self.checkpoint_path)
        self.assertEqual(persisted, second)
        self.assertEqual(first["sequence"], 1)
        self.assertEqual(second["sequence"], 2)
        self.assertEqual(
            stat.S_IMODE(self.checkpoint_path.stat().st_mode),
            0o600,
        )
        self.assertEqual(stat.S_IMODE(self.journal_path.stat().st_mode), 0o600)
        self.assertEqual(
            list(self.root.glob(f".{self.checkpoint_path.name}.*.tmp")),
            [],
        )

    def test_atomic_replace_failure_preserves_previous_document(self) -> None:
        original = {"value": "old"}
        checkpoint.atomic_write_json(self.checkpoint_path, original)
        original_bytes = self.checkpoint_path.read_bytes()

        with mock.patch.object(checkpoint.os, "replace", side_effect=OSError("boom")):
            with self.assertRaisesRegex(OSError, "boom"):
                checkpoint.atomic_write_json(self.checkpoint_path, {"value": "new"})

        self.assertEqual(self.checkpoint_path.read_bytes(), original_bytes)
        self.assertEqual(
            list(self.root.glob(f".{self.checkpoint_path.name}.*.tmp")),
            [],
        )

    def test_existing_checkpoint_identity_mismatch_fails_closed(self) -> None:
        self.write("packaged")
        different = checkpoint.ReleaseIdentity.create(
            repository=self.identity.repository,
            commit="d" * 40,
            archive_sha256=self.identity.archiveSha256,
            archive_bytes=self.identity.archiveBytes,
            run_id=self.identity.runId,
            run_attempt=self.identity.runAttempt,
            frontend_identity=self.identity.frontendIdentity,
            frontend_checksum=self.identity.frontendChecksum,
        )

        with self.assertRaisesRegex(checkpoint.CheckpointError, "identity mismatch"):
            self.write("transport_verified", identity=different)

        self.assertEqual(
            checkpoint.load_checkpoint(self.checkpoint_path)["phase"],
            "packaged",
        )
        self.assertEqual(len(self.journal_path.read_text().splitlines()), 1)

    def test_identity_validation_is_strict(self) -> None:
        invalid_values = (
            {"repository": "missing-owner"},
            {"commit": "a" * 39},
            {"archive_sha256": "not-a-checksum"},
            {"archive_bytes": 0},
            {"archive_bytes": "22000000"},
            {"run_id": 0},
            {"run_attempt": -1},
            {"frontend_identity": "contains whitespace"},
            {"frontend_checksum": "c" * 63},
        )
        defaults = {
            "repository": self.identity.repository,
            "commit": self.identity.commit,
            "archive_sha256": self.identity.archiveSha256,
            "archive_bytes": self.identity.archiveBytes,
            "run_id": self.identity.runId,
            "run_attempt": self.identity.runAttempt,
            "frontend_identity": self.identity.frontendIdentity,
            "frontend_checksum": self.identity.frontendChecksum,
        }
        for override in invalid_values:
            with self.subTest(override=override):
                with self.assertRaises(checkpoint.CheckpointError):
                    checkpoint.ReleaseIdentity.create(**(defaults | override))

    def test_checkpoint_and_journal_must_be_distinct(self) -> None:
        with self.assertRaisesRegex(checkpoint.CheckpointError, "must be different"):
            checkpoint.write_checkpoint(
                checkpoint_path=self.checkpoint_path,
                journal_path=self.checkpoint_path,
                identity=self.identity,
                phase="packaged",
                status="completed",
                retry_class="automatic",
            )

        self.assertFalse(self.checkpoint_path.exists())

    def test_phase_regression_is_rejected_without_journal_entry(self) -> None:
        self.write("prepared")

        with self.assertRaisesRegex(checkpoint.CheckpointError, "phase regression"):
            self.write("transport_verified")

        self.assertEqual(
            checkpoint.load_checkpoint(self.checkpoint_path)["phase"],
            "prepared",
        )
        self.assertEqual(len(self.journal_path.read_text().splitlines()), 1)

    def test_unknown_migration_outcome_is_never_resumable(self) -> None:
        for status in ("in_progress", "failed"):
            with self.subTest(status=status):
                with tempfile.TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    checkpoint.write_checkpoint(
                        checkpoint_path=root / "checkpoint.json",
                        journal_path=root / "journal.jsonl",
                        identity=self.identity,
                        phase="migration_started",
                        status=status,
                        retry_class="manual_db_recovery",
                        now="2026-07-22T01:02:03.000Z",
                    )
                    with self.assertRaisesRegex(
                        checkpoint.CheckpointError,
                        "migration outcome is unknown",
                    ):
                        checkpoint.assert_resumable(
                            checkpoint_path=root / "checkpoint.json",
                            expected_identity=self.identity,
                        )

    def test_interrupted_source_install_requires_rollback(self) -> None:
        self.write(
            "source_install_started",
            status="in_progress",
            retry_class="rollback_required",
        )

        with self.assertRaisesRegex(checkpoint.CheckpointError, "requires rollback"):
            checkpoint.assert_resumable(
                checkpoint_path=self.checkpoint_path,
                expected_identity=self.identity,
            )

        self.assertLess(
            checkpoint.PHASE_INDEX["prepared"],
            checkpoint.PHASE_INDEX["source_install_started"],
        )
        self.assertLess(
            checkpoint.PHASE_INDEX["source_installed"],
            checkpoint.PHASE_INDEX["backup_verified"],
        )

    def test_same_artifact_can_resume_after_migrated(self) -> None:
        self.write(
            "migrated",
            status="completed",
            retry_class="inspect_then_resume",
        )

        result = checkpoint.assert_resumable(
            checkpoint_path=self.checkpoint_path,
            expected_identity=self.identity,
        )

        self.assertEqual(result["decision"], "resumable")
        self.assertEqual(result["phase"], "migrated")

    def test_complete_returns_already_complete_and_is_immutable(self) -> None:
        self.write("complete", retry_class="complete")

        result = checkpoint.assert_resumable(
            checkpoint_path=self.checkpoint_path,
            expected_identity=self.identity,
        )

        self.assertEqual(result["decision"], "already-complete")
        with self.assertRaisesRegex(checkpoint.CheckpointError, "immutable"):
            self.write("complete", retry_class="complete")

    def test_manual_and_rollback_retry_classes_are_not_resumable(self) -> None:
        for retry_class, expected_message in (
            ("manual_db_recovery", "manual database recovery"),
            ("rollback_required", "requires rollback"),
        ):
            with self.subTest(retry_class=retry_class):
                with tempfile.TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    checkpoint.write_checkpoint(
                        checkpoint_path=root / "checkpoint.json",
                        journal_path=root / "journal.jsonl",
                        identity=self.identity,
                        phase="switched",
                        status="failed",
                        retry_class=retry_class,
                        now="2026-07-22T01:02:03.000Z",
                    )
                    with self.assertRaisesRegex(
                        checkpoint.CheckpointError,
                        expected_message,
                    ):
                        checkpoint.assert_resumable(
                            checkpoint_path=root / "checkpoint.json",
                            expected_identity=self.identity,
                        )

    def test_journal_is_append_only_and_contains_full_transitions(self) -> None:
        self.write("packaged")
        self.write("transport_verified", status="in_progress")
        self.write("transport_verified", status="completed")

        events = [
            json.loads(line)
            for line in self.journal_path.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual([event["sequence"] for event in events], [1, 2, 3])
        self.assertEqual(
            [event["phase"] for event in events],
            ["packaged", "transport_verified", "transport_verified"],
        )
        self.assertTrue(all(event["event"] == "checkpoint_transition" for event in events))
        self.assertTrue(all(event["identity"] == self.identity.to_dict() for event in events))

    def test_cli_show_and_assert_resumable_emit_machine_readable_json(self) -> None:
        self.write("migrated")
        identity_arguments = [
            "--repository",
            self.identity.repository,
            "--commit",
            self.identity.commit,
            "--archive-sha256",
            self.identity.archiveSha256,
            "--archive-bytes",
            str(self.identity.archiveBytes),
            "--run-id",
            str(self.identity.runId),
            "--run-attempt",
            str(self.identity.runAttempt),
            "--frontend-identity",
            self.identity.frontendIdentity,
            "--frontend-checksum",
            self.identity.frontendChecksum,
        ]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exit_code = checkpoint.main(
                [
                    "assert-resumable",
                    "--checkpoint",
                    str(self.checkpoint_path),
                    *identity_arguments,
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(output.getvalue())["decision"], "resumable")

    def test_cross_release_gate_blocks_mutated_release_from_other_commit(self) -> None:
        checkpoints_root = self.root / "checkpoints"
        current_path = (
            checkpoints_root / self.identity.commit / f"{self.identity.archiveSha256}.json"
        )
        current_path.parent.mkdir(parents=True)
        old_identity = self.namespaced_identity(
            commit="d" * 40,
            archive_sha256="e" * 64,
            run_id=234567,
        )
        self.write_namespaced(
            checkpoints_root,
            old_identity,
            "source_installed",
            retry_class="inspect_then_resume",
        )

        with self.assertRaisesRegex(
            checkpoint.CheckpointError,
            "another release may have mutated production",
        ):
            checkpoint.assert_cross_release_safe(
                checkpoints_root=checkpoints_root,
                current_checkpoint=current_path,
                expected_identity=self.identity,
            )

    def test_cross_release_gate_blocks_other_archive_for_same_commit(self) -> None:
        checkpoints_root = self.root / "checkpoints"
        current_path = (
            checkpoints_root / self.identity.commit / f"{self.identity.archiveSha256}.json"
        )
        current_path.parent.mkdir(parents=True)
        old_identity = self.namespaced_identity(
            archive_sha256="d" * 64,
            run_id=234567,
        )
        self.write_namespaced(
            checkpoints_root,
            old_identity,
            "migrated",
            retry_class="inspect_then_resume",
        )

        with self.assertRaisesRegex(checkpoint.CheckpointError, "resume that exact release"):
            checkpoint.assert_cross_release_safe(
                checkpoints_root=checkpoints_root,
                current_checkpoint=current_path,
                expected_identity=self.identity,
            )

    def test_cross_release_gate_fails_closed_on_untrusted_namespace(self) -> None:
        scenarios = ("malformed", "identity_mismatch", "symlink")
        for scenario in scenarios:
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                checkpoints_root = root / "checkpoints"
                current_path = (
                    checkpoints_root
                    / self.identity.commit
                    / f"{self.identity.archiveSha256}.json"
                )
                current_path.parent.mkdir(parents=True)
                old_identity = self.namespaced_identity(
                    commit="d" * 40,
                    archive_sha256="e" * 64,
                    run_id=234567,
                )
                old_path = (
                    checkpoints_root
                    / old_identity.commit
                    / f"{old_identity.archiveSha256}.json"
                )
                old_path.parent.mkdir(parents=True)
                if scenario == "malformed":
                    old_path.write_text("{not-json", encoding="utf-8")
                elif scenario == "identity_mismatch":
                    payload = checkpoint.write_checkpoint(
                        checkpoint_path=root / "valid.json",
                        journal_path=root / "valid.jsonl",
                        identity=old_identity,
                        phase="prepared",
                        status="completed",
                        retry_class="automatic",
                        now="2026-07-22T01:02:03.000Z",
                    )
                    payload["identity"]["commit"] = "f" * 40
                    old_path.write_text(json.dumps(payload), encoding="utf-8")
                else:
                    target = root / "target.json"
                    target.write_text("{}", encoding="utf-8")
                    old_path.symlink_to(target)

                with self.assertRaises(checkpoint.CheckpointError):
                    checkpoint.assert_cross_release_safe(
                        checkpoints_root=checkpoints_root,
                        current_checkpoint=current_path,
                        expected_identity=self.identity,
                    )

    def test_cross_release_gate_allows_settled_old_and_exact_current_resume(self) -> None:
        checkpoints_root = self.root / "checkpoints"
        old_identity = self.namespaced_identity(
            commit="d" * 40,
            archive_sha256="e" * 64,
            run_id=234567,
        )
        self.write_namespaced(
            checkpoints_root,
            old_identity,
            "complete",
            retry_class="complete",
        )
        healthy_identity = self.namespaced_identity(
            commit="f" * 40,
            archive_sha256="1" * 64,
            run_id=345678,
        )
        self.write_namespaced(
            checkpoints_root,
            healthy_identity,
            "backend_healthy",
        )
        prepared_identity = self.namespaced_identity(
            commit="2" * 40,
            archive_sha256="3" * 64,
            run_id=456789,
        )
        self.write_namespaced(
            checkpoints_root,
            prepared_identity,
            "prepared",
        )
        current_path = self.write_namespaced(
            checkpoints_root,
            self.identity,
            "source_installed",
            retry_class="inspect_then_resume",
        )
        evidence_path = current_path.with_name(
            f"{self.identity.archiveSha256}.evidence.json"
        )
        evidence_path.write_text("not checkpoint JSON", encoding="utf-8")

        result = checkpoint.assert_cross_release_safe(
            checkpoints_root=checkpoints_root,
            current_checkpoint=current_path,
            expected_identity=self.identity,
        )
        resume = checkpoint.assert_resumable(
            checkpoint_path=current_path,
            expected_identity=self.identity,
        )

        self.assertEqual(result["decision"], "cross-release-safe")
        self.assertEqual(result["evidenceFilesExcluded"], 1)
        self.assertTrue(result["currentCheckpointPresent"])
        self.assertEqual(resume["decision"], "resumable")

    def test_cross_release_gate_blocks_manual_recovery_even_before_mutation(self) -> None:
        checkpoints_root = self.root / "checkpoints"
        current_path = (
            checkpoints_root / self.identity.commit / f"{self.identity.archiveSha256}.json"
        )
        current_path.parent.mkdir(parents=True)
        old_identity = self.namespaced_identity(
            commit="d" * 40,
            archive_sha256="e" * 64,
            run_id=234567,
        )
        self.write_namespaced(
            checkpoints_root,
            old_identity,
            "prepared",
            status="failed",
            retry_class="manual_db_recovery",
        )

        with self.assertRaisesRegex(checkpoint.CheckpointError, "operator recovery"):
            checkpoint.assert_cross_release_safe(
                checkpoints_root=checkpoints_root,
                current_checkpoint=current_path,
                expected_identity=self.identity,
            )


if __name__ == "__main__":
    unittest.main()
