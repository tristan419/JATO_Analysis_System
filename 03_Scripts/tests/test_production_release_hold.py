from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / ".github/scripts/production_release_hold.py"
SPEC = importlib.util.spec_from_file_location("production_release_hold", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
hold_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = hold_module
SPEC.loader.exec_module(hold_module)


class ProductionReleaseHoldTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.repo = Path(self.tempdir.name)
        self.hold_path = self.repo / hold_module.HOLD_PATH
        self.retirement_path = self.repo / hold_module.RETIREMENT_PATH
        self.plan_path = self.repo / hold_module.RECOVERY_PLAN_PATH
        self.hold_path.parent.mkdir(parents=True)
        self.hold_path.write_text(
            json.dumps(
                hold_module.EXPECTED_HOLD_DOCUMENT,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        shutil.copy2(ROOT / hold_module.RECOVERY_PLAN_PATH, self.plan_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _write_retirement(self) -> None:
        self.retirement_path.write_text(
            json.dumps(
                hold_module.EXPECTED_RETIREMENT_DOCUMENT,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def test_reviewed_hold_resolves_to_hold(self) -> None:
        self.assertEqual(hold_module.resolve_release_action(self.repo), "hold")
        hold_module.validate_active_hold(self.repo)

    def test_checked_in_hold_or_reviewed_retirement_is_valid(self) -> None:
        action = hold_module.resolve_release_action(ROOT)
        self.assertIn(action, {"hold", "deploy"})
        if (ROOT / hold_module.HOLD_PATH).exists():
            self.assertEqual(action, "hold")
            self.assertFalse((ROOT / hold_module.RETIREMENT_PATH).exists())
        else:
            self.assertEqual(action, "deploy")
            self.assertTrue((ROOT / hold_module.RETIREMENT_PATH).exists())

    def test_missing_hold_and_retirement_fails_closed(self) -> None:
        self.hold_path.unlink()
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "requires either the active hold or its reviewed retirement record",
        ):
            hold_module.resolve_release_action(self.repo)
        output = self.repo / "github-output"
        self.assertEqual(
            hold_module.main(
                ["resolve", "--github-output", str(output)],
                repo_root=self.repo,
            ),
            1,
        )
        self.assertFalse(output.exists())

    def test_exact_retirement_resolves_to_deploy(self) -> None:
        self.hold_path.unlink()
        self._write_retirement()
        self.assertEqual(hold_module.resolve_release_action(self.repo), "deploy")
        hold_module.validate_retirement(self.repo)

    def test_hold_and_retirement_together_fail_closed(self) -> None:
        self._write_retirement()
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "must not exist simultaneously",
        ):
            hold_module.resolve_release_action(self.repo)
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "must not exist simultaneously",
        ):
            hold_module.validate_active_hold(self.repo)

    def test_malformed_retirement_fails_closed(self) -> None:
        self.hold_path.unlink()
        self.retirement_path.write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "retirement record does not match the reviewed incident contract",
        ):
            hold_module.resolve_release_action(self.repo)

    def test_noncanonical_retirement_fails_closed(self) -> None:
        self.hold_path.unlink()
        self.retirement_path.write_text(
            json.dumps(hold_module.EXPECTED_RETIREMENT_DOCUMENT),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "retirement record is not the reviewed canonical serialization",
        ):
            hold_module.resolve_release_action(self.repo)

    def test_retirement_symlink_fails_closed(self) -> None:
        self.hold_path.unlink()
        original = self.retirement_path.with_name(
            self.retirement_path.name + ".original"
        )
        original.write_text(
            json.dumps(
                hold_module.EXPECTED_RETIREMENT_DOCUMENT,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        self.retirement_path.symlink_to(original.name)
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "retirement record must be a regular file",
        ):
            hold_module.resolve_release_action(self.repo)

    def test_malformed_hold_fails_closed(self) -> None:
        self.hold_path.write_text("{", encoding="utf-8")
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "strict UTF-8 JSON",
        ):
            hold_module.resolve_release_action(self.repo)

    def test_duplicate_key_fails_closed(self) -> None:
        self.hold_path.write_text(
            '{"status":"active","status":"active"}\n',
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "duplicate JSON key",
        ):
            hold_module.resolve_release_action(self.repo)

    def test_extra_or_changed_contract_field_fails_closed(self) -> None:
        payload = json.loads(self.hold_path.read_text(encoding="utf-8"))
        payload["incidentId"] = "different-incident"
        payload["unreviewed"] = True
        self.hold_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "does not match the reviewed incident contract",
        ):
            hold_module.resolve_release_action(self.repo)

    def test_noncanonical_hold_fails_closed(self) -> None:
        payload = json.loads(self.hold_path.read_text(encoding="utf-8"))
        self.hold_path.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "canonical serialization",
        ):
            hold_module.resolve_release_action(self.repo)

    def test_plan_digest_drift_fails_closed(self) -> None:
        with self.plan_path.open("ab") as plan:
            plan.write(b"\n")
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "recovery plan SHA-256 does not match",
        ):
            hold_module.resolve_release_action(self.repo)

    def test_hold_or_plan_symlink_fails_closed(self) -> None:
        for path, label in (
            (self.hold_path, "production release hold"),
            (self.plan_path, "recovery plan"),
        ):
            with self.subTest(path=path):
                original = path.with_name(path.name + ".original")
                path.rename(original)
                path.symlink_to(original.name)
                with self.assertRaisesRegex(
                    hold_module.HoldContractError,
                    f"{label} must be a regular file",
                ):
                    hold_module.resolve_release_action(self.repo)
                path.unlink()
                original.rename(path)

    def test_hold_or_plan_hard_link_fails_closed(self) -> None:
        for path, label in (
            (self.hold_path, "production release hold"),
            (self.plan_path, "recovery plan"),
        ):
            with self.subTest(path=path):
                extra_link = path.with_name(path.name + ".hardlink")
                extra_link.hardlink_to(path)
                with self.assertRaisesRegex(
                    hold_module.HoldContractError,
                    f"{label} must have exactly one hard link",
                ):
                    hold_module.resolve_release_action(self.repo)
                extra_link.unlink()

    def test_oversized_hold_fails_closed(self) -> None:
        self.hold_path.write_bytes(b"x" * (hold_module.MAX_HOLD_BYTES + 1))
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "production release hold size must be between",
        ):
            hold_module.resolve_release_action(self.repo)

    def test_resolve_cli_emits_exact_action_output(self) -> None:
        output = self.repo / "github-output"
        self.assertEqual(
            hold_module.main(
                ["resolve", "--github-output", str(output)],
                repo_root=self.repo,
            ),
            0,
        )
        self.assertEqual(output.read_text(encoding="utf-8"), "release-action=hold\n")

    def test_resolve_cli_emits_exact_deploy_output_after_reviewed_removal(self) -> None:
        self.hold_path.unlink()
        self._write_retirement()
        output = self.repo / "github-output"
        self.assertEqual(
            hold_module.main(
                ["resolve", "--github-output", str(output)],
                repo_root=self.repo,
            ),
            0,
        )
        self.assertEqual(
            output.read_text(encoding="utf-8"),
            "release-action=deploy\n",
        )

    def test_resolve_cli_emits_nothing_for_malformed_hold(self) -> None:
        self.hold_path.write_text("{}\n", encoding="utf-8")
        output = self.repo / "github-output"
        self.assertEqual(
            hold_module.main(
                ["resolve", "--github-output", str(output)],
                repo_root=self.repo,
            ),
            1,
        )
        self.assertFalse(output.exists())

    def test_require_active_cli_rejects_removed_hold(self) -> None:
        self.hold_path.unlink()
        self._write_retirement()
        with self.assertRaisesRegex(
            hold_module.HoldContractError,
            "retirement record is already present",
        ):
            hold_module.validate_active_hold(self.repo)
        self.assertEqual(
            hold_module.main(["require-active"], repo_root=self.repo),
            1,
        )


if __name__ == "__main__":
    unittest.main()
