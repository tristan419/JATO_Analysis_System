from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / ".github/scripts/release_coordination_guard.py"
SPEC = importlib.util.spec_from_file_location("release_coordination_guard", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
guard_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = guard_module
SPEC.loader.exec_module(guard_module)

GuardError = guard_module.GuardError
GitHubRestClient = guard_module.GitHubRestClient
ReleaseCoordinationGuard = guard_module.ReleaseCoordinationGuard
CONTRACT_PATH_TEMPLATE = guard_module.CONTRACT_PATH_TEMPLATE
parse_immutable_release_contract = guard_module.parse_immutable_release_contract
parse_pull_request_metadata = guard_module.parse_pull_request_metadata
parse_release_group_manifest = guard_module.parse_release_group_manifest
strict_json_mapping = guard_module._strict_json_mapping


REPOSITORY = "tristan419/JATO_Analysis_System"
MAIN_SHA = "f" * 40
BASELINE_SHA = "e" * 40
TARGET_ARCHIVE_SHA256 = "a" * 64
TARGET_MANIFEST_SHA256 = "b" * 64


def pull_request(
    number: int,
    *,
    body: str = "",
    state: str = "open",
    merged: bool = False,
    draft: bool = False,
    merge_sha: str | None = None,
    base_ref: str = "main",
    base_sha: str = MAIN_SHA,
    repository: str = REPOSITORY,
    author_association: str = "OWNER",
    labels: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    if labels is None:
        labels = (
            [{"name": "release-group"}]
            if "Release-Group: #" in body
            else []
        )
    return {
        "number": number,
        "body": body,
        "state": state,
        "draft": draft,
        "merged_at": "2026-07-23T00:00:00Z" if merged else None,
        "merge_commit_sha": merge_sha or f"{number:040x}",
        "author_association": author_association,
        "labels": labels,
        "head": {"sha": f"{number + 1000:040x}"},
        "base": {
            "ref": base_ref,
            "sha": base_sha,
            "repo": {"full_name": repository},
        },
    }


def release_group_issue(
    number: int,
    *,
    anchor: int,
    members: tuple[int, ...],
    label: str = "release-group",
) -> dict[str, Any]:
    return {
        "number": number,
        "body": (
            f"Release-Group-Anchor: #{anchor}\n"
            "Release-Group-Members: "
            + ", ".join(f"#{member}" for member in members)
        ),
        "labels": [{"name": label}],
        "author_association": "OWNER",
        "state": "open",
    }


def immutable_contract(
    number: int,
    *,
    dependencies: tuple[int, ...] = (),
    group_issue: int | None = None,
    group_anchor: int | None = None,
    group_members: tuple[int, ...] = (),
    repository: str = REPOSITORY,
) -> dict[str, Any]:
    release_group = None
    if group_issue is not None:
        release_group = {
            "issue": group_issue,
            "anchor": group_anchor,
            "members": list(group_members),
        }
    return {
        "schema": 1,
        "repository": repository,
        "pullRequest": number,
        "releaseGroup": release_group,
        "dependsOn": list(dependencies),
    }


class FakeGitHub:
    def __init__(
        self,
        *,
        pulls: list[Mapping[str, Any]] | None = None,
        issues: list[Mapping[str, Any]] | None = None,
        main_sha: str = MAIN_SHA,
        baseline_sha: str = BASELINE_SHA,
        range_numbers: set[int] | None = None,
        contracts: Mapping[int, Mapping[str, Any] | None] | None = None,
        contract_changes: Mapping[int, list[Mapping[str, Any]]] | None = None,
    ) -> None:
        self.pulls = {
            int(pull["number"]): dict(pull)
            for pull in (pulls or [])
        }
        self.issues = {
            int(issue["number"]): dict(issue)
            for issue in (issues or [])
        }
        self.main_sha = main_sha
        self.baseline_sha = baseline_sha
        self.range_numbers = range_numbers
        self.ancestor_pairs: set[tuple[str, str]] = {
            (baseline_sha, main_sha),
        }
        self.range_calls: list[tuple[str, str]] = []
        self.statuses: list[tuple[str, str, str]] = []
        self.current_statuses: dict[str, tuple[str, str]] = {}
        self.contracts: dict[
            tuple[int, str],
            tuple[Mapping[str, Any], str] | None,
        ] = {}
        self.contract_changes: dict[int, list[Mapping[str, Any]]] = {
            int(number): [dict(item) for item in changed_files]
            for number, changed_files in (contract_changes or {}).items()
        }
        contract_overrides = dict(contracts or {})
        for number, pull in self.pulls.items():
            if number in contract_overrides:
                contract_payload = contract_overrides[number]
            else:
                contract_payload = self._contract_from_live_metadata(pull)
            head_sha = str(pull["head"]["sha"])
            merge_sha = str(pull["merge_commit_sha"])
            stored = (
                None
                if contract_payload is None
                else (
                    dict(contract_payload),
                    f"{number + 2000:040x}",
                )
            )
            self.contracts[(number, head_sha)] = stored
            self.contracts[(number, merge_sha)] = stored
            if number not in self.contract_changes and contract_payload is not None:
                self.contract_changes[number] = [
                    {
                        "filename": CONTRACT_PATH_TEMPLATE.format(
                            pull_request=number
                        ),
                        "status": "added",
                    }
                ]

    def _contract_from_live_metadata(
        self,
        pull: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        number = int(pull["number"])
        try:
            metadata = parse_pull_request_metadata(str(pull.get("body") or ""))
        except GuardError:
            return None
        if metadata.release_group is None and not metadata.dependencies:
            return None
        group_anchor = None
        group_members: tuple[int, ...] = ()
        if metadata.release_group is not None:
            issue = self.issues.get(metadata.release_group)
            if issue is None:
                return None
            try:
                manifest = parse_release_group_manifest(
                    str(issue.get("body") or "")
                )
            except GuardError:
                return None
            group_anchor = manifest.anchor
            group_members = manifest.members
        return immutable_contract(
            number,
            dependencies=metadata.dependencies,
            group_issue=metadata.release_group,
            group_anchor=group_anchor,
            group_members=group_members,
        )

    def mark_ancestor(self, ancestor: str, descendant: str = MAIN_SHA) -> None:
        self.ancestor_pairs.add((ancestor, descendant))

    def get_issue(self, number: int) -> Mapping[str, Any]:
        if number not in self.issues:
            raise GuardError(f"GitHub API 404 for issue #{number}")
        return self.issues[number]

    def get_pull_request(self, number: int) -> Mapping[str, Any]:
        if number not in self.pulls:
            raise GuardError(f"GitHub API 404 for PR #{number}")
        return self.pulls[number]

    def list_pull_requests_between(
        self,
        baseline_sha: str,
        target_sha: str,
    ) -> list[Mapping[str, Any]]:
        if baseline_sha != self.baseline_sha:
            raise GuardError("unexpected production baseline")
        self.range_calls.append((baseline_sha, target_sha))
        return [
            pull
            for pull in self.pulls.values()
            if pull.get("merged_at")
            and pull.get("base", {}).get("ref") == "main"
            and (
                self.range_numbers is None
                or int(pull["number"]) in self.range_numbers
            )
        ]

    def is_ancestor(self, ancestor_sha: str, descendant_sha: str) -> bool:
        return (
            ancestor_sha == descendant_sha
            or (ancestor_sha, descendant_sha) in self.ancestor_pairs
        )

    def get_main_sha(self) -> str:
        return self.main_sha

    def get_current_production_sha(self) -> str:
        return self.baseline_sha

    def list_open_main_pull_requests(self) -> list[Mapping[str, Any]]:
        return [
            pull
            for pull in self.pulls.values()
            if pull.get("state") == "open"
            and pull.get("base", {}).get("ref") == "main"
        ]

    def set_commit_status(self, sha: str, state: str, description: str) -> None:
        self.statuses.append((sha, state, description))
        self.current_statuses[sha] = (state, description)

    def get_commit_status(self, sha: str) -> tuple[str, str] | None:
        return self.current_statuses.get(sha)

    def get_release_contract(
        self,
        pull_request_number: int,
        ref_sha: str,
    ) -> tuple[Mapping[str, Any], str] | None:
        return self.contracts.get((pull_request_number, ref_sha))

    def list_pull_request_files(
        self,
        pull_request_number: int,
    ) -> list[Mapping[str, Any]]:
        return self.contract_changes.get(pull_request_number, [])


class MetadataParsingTests(unittest.TestCase):
    def test_missing_trailers_default_to_independent(self) -> None:
        metadata = parse_pull_request_metadata("Normal PR body")
        self.assertIsNone(metadata.release_group)
        self.assertEqual(metadata.dependencies, ())

    def test_explicit_independent_and_none_are_allowed(self) -> None:
        metadata = parse_pull_request_metadata(
            "Release-Group: independent\nDepends-On: none"
        )
        self.assertIsNone(metadata.release_group)
        self.assertEqual(metadata.dependencies, ())

    def test_group_and_multiple_dependencies_are_parsed(self) -> None:
        metadata = parse_pull_request_metadata(
            "Release-Group: #200\nDepends-On: #174, #175"
        )
        self.assertEqual(metadata.release_group, 200)
        self.assertEqual(metadata.dependencies, (174, 175))

    def test_fenced_examples_and_comments_are_ignored(self) -> None:
        metadata = parse_pull_request_metadata(
            "```\nRelease-Group: #1\n```\n"
            "<!--\nDepends-On: #2\n-->\n"
            "Release-Group: independent\nDepends-On: none"
        )
        self.assertIsNone(metadata.release_group)
        self.assertEqual(metadata.dependencies, ())

    def test_indented_code_and_missing_commas_are_rejected_or_ignored(self) -> None:
        metadata = parse_pull_request_metadata(
            "    Release-Group: #200\n"
            "\tDepends-On: #174\n"
            "Release-Group: independent\nDepends-On: none"
        )
        self.assertIsNone(metadata.release_group)
        self.assertEqual(metadata.dependencies, ())
        with self.assertRaisesRegex(GuardError, "comma-separated"):
            parse_pull_request_metadata("Depends-On: #174#175")

    def test_duplicate_or_malformed_trailers_fail_closed(self) -> None:
        with self.assertRaisesRegex(GuardError, "duplicate Release-Group"):
            parse_pull_request_metadata(
                "Release-Group: #200\nRelease-Group: #201"
            )
        with self.assertRaisesRegex(GuardError, "comma-separated"):
            parse_pull_request_metadata("Depends-On: PR 174")
        with self.assertRaisesRegex(GuardError, "exceeds the supported range"):
            parse_pull_request_metadata("Depends-On: #" + ("9" * 5000))

    def test_group_manifest_requires_anchor_and_two_unique_members(self) -> None:
        manifest = parse_release_group_manifest(
            "Release-Group-Anchor: #175\n"
            "Release-Group-Members: #174, #175"
        )
        self.assertEqual(manifest.anchor, 175)
        self.assertEqual(manifest.members, (174, 175))
        with self.assertRaisesRegex(GuardError, "at least two"):
            parse_release_group_manifest(
                "Release-Group-Anchor: #175\n"
                "Release-Group-Members: #175"
            )
        with self.assertRaisesRegex(GuardError, "duplicate"):
            parse_release_group_manifest(
                "Release-Group-Anchor: #175\n"
                "Release-Group-Members: #174, #174, #175"
            )


class ImmutableContractParsingTests(unittest.TestCase):
    def test_valid_dependency_and_group_contracts_are_typed(self) -> None:
        dependency = parse_immutable_release_contract(
            immutable_contract(175, dependencies=(174,)),
            expected_repository=REPOSITORY,
            expected_pull_request=175,
            blob_sha="a" * 40,
        )
        self.assertEqual(dependency.dependencies, (174,))
        self.assertIsNone(dependency.release_group)

        grouped = parse_immutable_release_contract(
            immutable_contract(
                175,
                dependencies=(174,),
                group_issue=200,
                group_anchor=175,
                group_members=(174, 175),
            ),
            expected_repository=REPOSITORY,
            expected_pull_request=175,
            blob_sha="b" * 40,
        )
        self.assertEqual(grouped.release_group.issue, 200)
        self.assertEqual(grouped.release_group.members, (174, 175))

    def test_contract_rejects_empty_independent_or_wrong_identity(self) -> None:
        with self.assertRaisesRegex(GuardError, "omit an empty independent"):
            parse_immutable_release_contract(
                immutable_contract(10),
                expected_repository=REPOSITORY,
                expected_pull_request=10,
                blob_sha="a" * 40,
            )
        with self.assertRaisesRegex(GuardError, "repository does not match"):
            parse_immutable_release_contract(
                immutable_contract(10, dependencies=(9,), repository="other/repo"),
                expected_repository=REPOSITORY,
                expected_pull_request=10,
                blob_sha="a" * 40,
            )
        with self.assertRaisesRegex(GuardError, "identifies PR #11"):
            parse_immutable_release_contract(
                immutable_contract(11, dependencies=(9,)),
                expected_repository=REPOSITORY,
                expected_pull_request=10,
                blob_sha="a" * 40,
            )

    def test_contract_numbers_must_be_json_positive_integers(self) -> None:
        payload = immutable_contract(10, dependencies=(9,))
        payload["schema"] = True
        with self.assertRaisesRegex(GuardError, "version is unsupported"):
            parse_immutable_release_contract(
                payload,
                expected_repository=REPOSITORY,
                expected_pull_request=10,
                blob_sha="a" * 40,
            )
        payload = immutable_contract(10, dependencies=(9,))
        payload["pullRequest"] = 10.0
        with self.assertRaisesRegex(GuardError, "invalid contract pullRequest"):
            parse_immutable_release_contract(
                payload,
                expected_repository=REPOSITORY,
                expected_pull_request=10,
                blob_sha="a" * 40,
            )
        payload = immutable_contract(10, dependencies=(9,))
        payload["dependsOn"] = [9.5]
        with self.assertRaisesRegex(GuardError, "invalid contract dependsOn"):
            parse_immutable_release_contract(
                payload,
                expected_repository=REPOSITORY,
                expected_pull_request=10,
                blob_sha="a" * 40,
            )

    def test_deep_or_nonstandard_json_is_normalized_to_guard_error(self) -> None:
        deeply_nested = '{"value":' + ("[" * 2000) + "0" + ("]" * 2000) + "}"
        with self.assertRaisesRegex(GuardError, "is not valid JSON"):
            strict_json_mapping(deeply_nested, "test contract")
        with self.assertRaisesRegex(GuardError, "unsupported JSON constant 'NaN'"):
            strict_json_mapping('{"value": NaN}', "test contract")
        with self.assertRaisesRegex(GuardError, "integer outside the safe range"):
            strict_json_mapping(
                '{"pullRequest":' + ("9" * 5000) + "}",
                "test contract",
            )


class PullRequestGuardTests(unittest.TestCase):
    def test_independent_pull_request_passes_without_group_state(self) -> None:
        api = FakeGitHub(pulls=[pull_request(10)])
        result = ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)
        self.assertEqual(result, "PR #10 is independent")

    def test_merged_dependency_must_be_current_main_ancestor(self) -> None:
        dependency = pull_request(9, state="closed", merged=True)
        current = pull_request(10, body="Depends-On: #9")
        api = FakeGitHub(pulls=[dependency, current])
        api.mark_ancestor(str(dependency["merge_commit_sha"]))
        result = ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)
        self.assertIn("release coordination is valid", result)

        api.ancestor_pairs.clear()
        with self.assertRaisesRegex(GuardError, "not an ancestor"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)

    def test_open_draft_and_closed_unmerged_dependencies_fail_closed(self) -> None:
        cases = (
            (pull_request(9), "still open"),
            (pull_request(9, draft=True), "still a draft"),
            (pull_request(9, state="closed"), "closed without merging"),
        )
        for dependency, expected in cases:
            with self.subTest(expected=expected):
                current = pull_request(10, body="Depends-On: #9")
                api = FakeGitHub(pulls=[dependency, current])
                with self.assertRaisesRegex(GuardError, expected):
                    ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)

    def test_dependency_must_be_same_repository_and_target_main(self) -> None:
        current = pull_request(10, body="Depends-On: #9")
        wrong_base = pull_request(
            9,
            state="closed",
            merged=True,
            base_ref="preview",
        )
        api = FakeGitHub(pulls=[wrong_base, current])
        with self.assertRaisesRegex(GuardError, "must target main"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)

        wrong_repository = pull_request(
            9,
            state="closed",
            merged=True,
            repository="someone/fork",
        )
        api = FakeGitHub(pulls=[wrong_repository, current])
        with self.assertRaisesRegex(GuardError, "must belong"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)

    def test_dependency_cycle_fails_closed(self) -> None:
        first = pull_request(10, body="Depends-On: #9")
        second = pull_request(
            9,
            body="Depends-On: #10",
            state="closed",
            merged=True,
        )
        api = FakeGitHub(pulls=[first, second])
        api.mark_ancestor(str(second["merge_commit_sha"]))
        with self.assertRaisesRegex(GuardError, "#10 -> #9 -> #10"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)

    def test_coordinated_body_requires_matching_owned_contract(self) -> None:
        dependency = pull_request(9, state="closed", merged=True)
        current = pull_request(10, body="Depends-On: #9")
        api = FakeGitHub(
            pulls=[dependency, current],
            contracts={10: None},
        )
        with self.assertRaisesRegex(GuardError, "must add immutable contract"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)

        mismatch = FakeGitHub(
            pulls=[dependency, current],
            contracts={10: immutable_contract(10, dependencies=(8,))},
        )
        with self.assertRaisesRegex(GuardError, "does not match Depends-On"):
            ReleaseCoordinationGuard(mismatch, REPOSITORY).validate_pull_request(10)

    def test_independent_body_cannot_smuggle_a_contract(self) -> None:
        current = pull_request(10)
        api = FakeGitHub(
            pulls=[current],
            contracts={10: immutable_contract(10, dependencies=(9,))},
        )
        with self.assertRaisesRegex(GuardError, "body is independent"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)

    def test_pr_cannot_modify_or_rename_another_contract(self) -> None:
        current = pull_request(10)
        foreign_path = CONTRACT_PATH_TEMPLATE.format(pull_request=9)
        api = FakeGitHub(
            pulls=[current],
            contract_changes={
                10: [
                    {
                        "filename": "docs/retired-contract.json",
                        "previous_filename": foreign_path,
                        "status": "renamed",
                    }
                ]
            },
        )
        with self.assertRaisesRegex(GuardError, "cannot change another"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)

    def test_contract_must_be_newly_added_by_its_own_pr(self) -> None:
        current = pull_request(10, body="Depends-On: #9")
        dependency = pull_request(9, state="closed", merged=True)
        api = FakeGitHub(
            pulls=[dependency, current],
            contract_changes={
                10: [
                    {
                        "filename": CONTRACT_PATH_TEMPLATE.format(
                            pull_request=10
                        ),
                        "status": "modified",
                    }
                ]
            },
        )
        with self.assertRaisesRegex(GuardError, "must be newly added"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)

    def test_group_anchor_cannot_pass_until_all_members_are_merged(self) -> None:
        group = release_group_issue(200, anchor=175, members=(174, 175))
        member = pull_request(174, body="Release-Group: #200\nDepends-On: none")
        anchor = pull_request(
            175,
            body="Release-Group: #200\nDepends-On: #174",
        )
        api = FakeGitHub(pulls=[member, anchor], issues=[group])
        member_result = ReleaseCoordinationGuard(
            api,
            REPOSITORY,
        ).validate_pull_request(174)
        self.assertIn("release coordination is valid", member_result)
        with self.assertRaisesRegex(GuardError, "dependency PR #174 is still open"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(175)

    def test_group_manifest_and_member_metadata_fail_closed(self) -> None:
        group = release_group_issue(200, anchor=175, members=(174, 175))
        member = pull_request(174, body="Release-Group: independent")
        anchor = pull_request(
            175,
            body="Release-Group: #200\nDepends-On: none",
        )
        api = FakeGitHub(pulls=[member, anchor], issues=[group])
        with self.assertRaisesRegex(GuardError, "member PR #174 points to independent"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(175)

    def test_coordinated_pr_requires_trusted_author_and_group_label(self) -> None:
        dependency = pull_request(9, state="closed", merged=True)
        untrusted = pull_request(
            10,
            body="Depends-On: #9",
            author_association="CONTRIBUTOR",
        )
        api = FakeGitHub(pulls=[dependency, untrusted])
        with self.assertRaisesRegex(GuardError, "must be authored"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(10)

        group = release_group_issue(200, anchor=175, members=(174, 175))
        member = pull_request(
            174,
            body="Release-Group: #200",
            labels=[],
        )
        anchor = pull_request(
            175,
            body="Release-Group: #200\nDepends-On: #174",
        )
        api = FakeGitHub(pulls=[member, anchor], issues=[group])
        with self.assertRaisesRegex(GuardError, "missing the 'release-group' label"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(174)

    def test_release_group_issue_requires_trusted_author(self) -> None:
        group = release_group_issue(200, anchor=175, members=(174, 175))
        group["author_association"] = "CONTRIBUTOR"
        member = pull_request(174, body="Release-Group: #200")
        anchor = pull_request(
            175,
            body="Release-Group: #200\nDepends-On: #174",
        )
        api = FakeGitHub(pulls=[member, anchor], issues=[group])
        with self.assertRaisesRegex(GuardError, "must be owned"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(174)

    def test_release_group_issue_must_stay_open(self) -> None:
        group = release_group_issue(200, anchor=175, members=(174, 175))
        group["state"] = "closed"
        member = pull_request(174, body="Release-Group: #200")
        anchor = pull_request(
            175,
            body="Release-Group: #200\nDepends-On: #174",
        )
        api = FakeGitHub(pulls=[member, anchor], issues=[group])
        with self.assertRaisesRegex(GuardError, "must stay open"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_pull_request(174)

    def test_sweeper_sets_explicit_head_statuses_and_fails_invalid_prs(self) -> None:
        independent = pull_request(10)
        blocked = pull_request(11, body="Depends-On: #9")
        dependency = pull_request(9)
        api = FakeGitHub(pulls=[independent, blocked, dependency])
        with self.assertRaisesRegex(GuardError, "1 open PR"):
            ReleaseCoordinationGuard(api, REPOSITORY).sweep_open_pull_requests()
        states_by_sha: dict[str, list[str]] = {}
        for sha, state, _ in api.statuses:
            states_by_sha.setdefault(sha, []).append(state)
        self.assertEqual(
            states_by_sha[independent["head"]["sha"]],
            ["pending", "success"],
        )
        self.assertEqual(
            states_by_sha[blocked["head"]["sha"]],
            ["pending", "failure"],
        )
        status_count = len(api.statuses)
        with self.assertRaisesRegex(GuardError, "1 open PR"):
            ReleaseCoordinationGuard(api, REPOSITORY).sweep_open_pull_requests()
        self.assertEqual(len(api.statuses), status_count)

    def test_sweeper_fails_all_open_prs_that_share_a_head_sha(self) -> None:
        first = pull_request(10)
        second = pull_request(11)
        second["head"]["sha"] = first["head"]["sha"]
        api = FakeGitHub(pulls=[first, second])
        with self.assertRaisesRegex(GuardError, "2 open PR"):
            ReleaseCoordinationGuard(api, REPOSITORY).sweep_open_pull_requests()
        self.assertEqual(
            api.current_statuses[str(first["head"]["sha"])],
            (
                "failure",
                "Blocked: head SHA is shared by open PRs #10, #11",
            ),
        )
        self.assertEqual(len(api.statuses), 2)

    def test_pr_event_can_revoke_old_success_with_pending_before_sweep(self) -> None:
        pull = pull_request(10)
        api = FakeGitHub(pulls=[pull])
        api.current_statuses[pull["head"]["sha"]] = (
            "success",
            "Independent release contract is valid",
        )
        ReleaseCoordinationGuard(api, REPOSITORY).mark_pull_request_pending(10)
        self.assertEqual(
            api.current_statuses[pull["head"]["sha"]],
            ("pending", "Validating release coordination metadata"),
        )


class GitHubRestClientTests(unittest.TestCase):
    @staticmethod
    def _public_client(responses: Mapping[str, Any]) -> GitHubRestClient:
        class StubClient(GitHubRestClient):
            def _public_request(self, path: str) -> Any:
                if path not in responses:
                    raise AssertionError(f"unexpected public path {path}")
                response = responses[path]
                if isinstance(response, Exception):
                    raise response
                return response

            def _request(self, *_: Any, **__: Any) -> Any:
                raise AssertionError("production baseline must not read Actions history")

        return StubClient(REPOSITORY, "token")

    def test_current_production_sha_uses_live_deploy_commit(self) -> None:
        deploy_sha = "1" * 40
        client = self._public_client(
            {
                "/healthz": {"status": "ok"},
                "/build-meta.json": {
                    "deployCommit": deploy_sha,
                    "githubSha": deploy_sha,
                    "commit": "2" * 40,
                    "commitMode": "application",
                },
            }
        )
        self.assertEqual(client.get_current_production_sha(), deploy_sha)

    def test_current_production_sha_accepts_legacy_commit_only(self) -> None:
        legacy_sha = "3" * 40
        client = self._public_client(
            {
                "/healthz": {"status": "ok"},
                "/build-meta.json": {"commit": legacy_sha},
            }
        )
        self.assertEqual(client.get_current_production_sha(), legacy_sha)

    def test_current_production_sha_rejects_identity_conflict(self) -> None:
        client = self._public_client(
            {
                "/healthz": {"status": "ok"},
                "/build-meta.json": {
                    "deployCommit": "4" * 40,
                    "githubSha": "5" * 40,
                },
            }
        )
        with self.assertRaisesRegex(GuardError, "deployCommit/githubSha mismatch"):
            client.get_current_production_sha()

    def test_current_production_sha_rejects_unhealthy_or_ambiguous_legacy(self) -> None:
        unhealthy = self._public_client(
            {
                "/healthz": {"status": "failed"},
                "/build-meta.json": {"deployCommit": "6" * 40},
            }
        )
        with self.assertRaisesRegex(GuardError, "status=ok"):
            unhealthy.get_current_production_sha()

        ambiguous = self._public_client(
            {
                "/healthz": {"status": "ok"},
                "/build-meta.json": {
                    "commit": "7" * 40,
                    "commitMode": "application",
                },
            }
        )
        with self.assertRaisesRegex(GuardError, "no unambiguous deploy SHA"):
            ambiguous.get_current_production_sha()

    def test_associated_pr_pagination_excludes_old_merge_contracts(self) -> None:
        commit_sha = "3" * 40
        old_pulls = [
            pull_request(
                number,
                state="closed",
                merged=True,
                merge_sha=f"{number + 3000:040x}",
            )
            for number in range(1, 101)
        ]
        current = pull_request(
            101,
            state="closed",
            merged=True,
            merge_sha=commit_sha,
        )

        class StubClient(GitHubRestClient):
            def __init__(self) -> None:
                super().__init__(REPOSITORY, "token")
                self._pull_requests = {
                    int(pull["number"]): pull
                    for pull in (*old_pulls, current)
                }

            def is_ancestor(
                self,
                ancestor_sha: str,
                descendant_sha: str,
            ) -> bool:
                return True

            def _request(
                self,
                path: str,
                query: Mapping[str, str] | None = None,
                **_: Any,
            ) -> Any:
                page = int((query or {}).get("page", "1"))
                if "/compare/" in path:
                    return {
                        "total_commits": 1,
                        "commits": [{"sha": commit_sha}],
                    }
                if path.endswith(f"/commits/{commit_sha}/pulls"):
                    if page == 1:
                        return [
                            {"number": int(pull["number"])}
                            for pull in old_pulls
                        ]
                    if page == 2:
                        return [{"number": 101}]
                    return []
                raise AssertionError(f"unexpected API path {path}")

        result = StubClient().list_pull_requests_between(
            BASELINE_SHA,
            MAIN_SHA,
        )
        self.assertEqual([pull["number"] for pull in result], [101])

    def test_pull_request_file_visibility_limit_fails_closed(self) -> None:
        class StubClient(GitHubRestClient):
            def _request(
                self,
                path: str,
                query: Mapping[str, str] | None = None,
                **_: Any,
            ) -> Any:
                page = int((query or {}).get("page", "1"))
                return [
                    {
                        "filename": f"generated/{page}/{item}.txt",
                        "status": "added",
                    }
                    for item in range(100)
                ]

        with self.assertRaisesRegex(GuardError, "3000-file visibility limit"):
            StubClient(REPOSITORY, "token").list_pull_request_files(10)


class ProductionGuardTests(unittest.TestCase):
    def _group_api(
        self,
        *,
        member_merged: bool,
        anchor_merged: bool,
        include_unrelated: bool = False,
    ) -> FakeGitHub:
        group = release_group_issue(200, anchor=175, members=(174, 175))
        member = pull_request(
            174,
            body="Release-Group: #200\nDepends-On: none",
            state="closed" if member_merged else "open",
            merged=member_merged,
        )
        anchor = pull_request(
            175,
            body="Release-Group: #200\nDepends-On: #174",
            state="closed" if anchor_merged else "open",
            merged=anchor_merged,
        )
        pulls = [member, anchor]
        if include_unrelated:
            pulls.append(pull_request(176, state="closed", merged=True))
        api = FakeGitHub(pulls=pulls, issues=[group])
        for pull in pulls:
            if pull.get("merged_at"):
                api.mark_ancestor(str(pull["merge_commit_sha"]))
        return api

    def test_zero_merged_group_does_not_block_unrelated_release(self) -> None:
        api = self._group_api(
            member_merged=False,
            anchor_merged=False,
            include_unrelated=True,
        )
        result, _ = ReleaseCoordinationGuard(
            api,
            REPOSITORY,
        ).validate_production(
            MAIN_SHA,
            operation="prepare-candidate",
            run_id="100",
            run_attempt="1",
        )
        self.assertIn("0 completed groups", result)

    def test_174_partial_merge_blocks_even_after_unrelated_176_merge(self) -> None:
        api = self._group_api(
            member_merged=True,
            anchor_merged=False,
            include_unrelated=True,
        )
        with self.assertRaisesRegex(
            GuardError,
            r"partially merged: merged #174; pending #175",
        ):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_production(
                MAIN_SHA,
                operation="prepare-candidate",
                run_id="100",
                run_attempt="1",
            )

    def test_partial_group_contract_survives_mutable_metadata_deletion(self) -> None:
        api = self._group_api(
            member_merged=True,
            anchor_merged=False,
            include_unrelated=True,
        )
        api.pulls[174]["body"] = "Release-Group: independent\nDepends-On: none"
        api.pulls[174]["labels"] = []
        api.issues[200]["body"] = (
            "Release-Group-Anchor: #175\n"
            "Release-Group-Members: #175, #176"
        )
        api.issues[200]["labels"] = []
        api.issues[200]["state"] = "closed"
        with self.assertRaisesRegex(
            GuardError,
            r"partially merged: merged #174; pending #175",
        ):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_production(
                MAIN_SHA,
                operation="prepare-candidate",
                run_id="100",
                run_attempt="1",
            )

    def test_later_contract_deletion_does_not_erase_merge_time_hold(self) -> None:
        api = self._group_api(
            member_merged=True,
            anchor_merged=False,
            include_unrelated=True,
        )
        api.contracts[(174, MAIN_SHA)] = None
        api.contract_changes[176] = [
            {
                "filename": "docs/deleted-contract.json",
                "previous_filename": CONTRACT_PATH_TEMPLATE.format(
                    pull_request=174
                ),
                "status": "renamed",
            }
        ]
        with self.assertRaisesRegex(GuardError, "cannot change another"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_production(
                MAIN_SHA,
                operation="prepare-candidate",
                run_id="100",
                run_attempt="1",
            )

    def test_same_head_foreign_contract_cannot_merge_as_independent_pr(self) -> None:
        group = release_group_issue(200, anchor=175, members=(174, 175))
        coordinated = pull_request(
            174,
            body="Release-Group: #200\nDepends-On: none",
        )
        independent = pull_request(
            176,
            state="closed",
            merged=True,
        )
        independent["head"]["sha"] = coordinated["head"]["sha"]
        api = FakeGitHub(
            pulls=[coordinated, independent],
            issues=[group],
            range_numbers={176},
            contract_changes={
                176: [
                    {
                        "filename": CONTRACT_PATH_TEMPLATE.format(
                            pull_request=174
                        ),
                        "status": "added",
                    }
                ]
            },
        )
        api.mark_ancestor(str(independent["merge_commit_sha"]))
        with self.assertRaisesRegex(
            GuardError,
            r"PR #176 cannot change another release contract: .*pr-174.json",
        ):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_production(
                MAIN_SHA,
                operation="prepare-candidate",
                run_id="100",
                run_attempt="1",
            )

    def test_merge_time_group_snapshots_must_be_identical(self) -> None:
        api = self._group_api(
            member_merged=True,
            anchor_merged=True,
            include_unrelated=True,
        )
        anchor = api.pulls[175]
        api.contracts[(175, str(anchor["merge_commit_sha"]))] = (
            immutable_contract(
                175,
                dependencies=(174, 176),
                group_issue=200,
                group_anchor=175,
                group_members=(174, 175, 176),
            ),
            "c" * 40,
        )
        with self.assertRaisesRegex(GuardError, "immutable manifests disagree"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_production(
                MAIN_SHA,
                operation="prepare-candidate",
                run_id="100",
                run_attempt="1",
            )

    def test_175_anchor_merge_completes_and_releases_the_global_hold(self) -> None:
        api = self._group_api(
            member_merged=True,
            anchor_merged=True,
            include_unrelated=True,
        )
        result, plan = ReleaseCoordinationGuard(
            api,
            REPOSITORY,
        ).validate_production(
            MAIN_SHA,
            operation="prepare-candidate",
            run_id="100",
            run_attempt="1",
        )
        self.assertIn("1 completed groups", result)
        self.assertIn("1 dependency contracts", result)
        self.assertEqual(plan["schema"], 3)
        self.assertEqual(plan["operation"], "prepare-candidate")
        self.assertEqual(plan["workflowSha"], MAIN_SHA)
        self.assertEqual(plan["targetSha"], MAIN_SHA)
        self.assertEqual(plan["baselineSha"], BASELINE_SHA)
        self.assertEqual(plan["releaseGroups"][0]["issue"], 200)

    def test_completed_group_recovers_after_a_bypassed_anchor_order(self) -> None:
        api = self._group_api(
            member_merged=True,
            anchor_merged=True,
        )
        result, _ = ReleaseCoordinationGuard(
            api,
            REPOSITORY,
        ).validate_production(
            MAIN_SHA,
            operation="prepare-candidate",
            run_id="100",
            run_attempt="1",
        )
        self.assertIn("1 completed groups", result)

    def test_stale_queued_or_rerun_sha_fails_before_release(self) -> None:
        stale_sha = "a" * 40
        api = FakeGitHub(main_sha=MAIN_SHA)
        with self.assertRaisesRegex(
            GuardError,
            r"stale production run: workflow a{12} is not current main f{12}",
        ):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_production(
                stale_sha,
                operation="prepare-candidate",
                run_id="100",
                run_attempt="1",
            )

    def test_update_active_can_target_an_older_main_ancestor(self) -> None:
        target_sha = "d" * 40
        api = FakeGitHub()
        api.mark_ancestor(BASELINE_SHA, target_sha)
        api.mark_ancestor(target_sha, MAIN_SHA)

        result, plan = ReleaseCoordinationGuard(
            api,
            REPOSITORY,
        ).validate_production(
            MAIN_SHA,
            operation="update-active",
            target_sha=target_sha,
            target_archive_sha256=TARGET_ARCHIVE_SHA256,
            target_manifest_sha256=TARGET_MANIFEST_SHA256,
            run_id="100",
            run_attempt="1",
        )

        self.assertIn("update-active", result)
        self.assertEqual(plan["operation"], "update-active")
        self.assertEqual(plan["workflowSha"], MAIN_SHA)
        self.assertEqual(plan["baselineSha"], BASELINE_SHA)
        self.assertEqual(plan["targetSha"], target_sha)
        self.assertEqual(plan["targetArchiveSha256"], TARGET_ARCHIVE_SHA256)
        self.assertEqual(plan["targetManifestSha256"], TARGET_MANIFEST_SHA256)
        self.assertEqual(api.range_calls, [(BASELINE_SHA, target_sha)])

    def test_update_active_rejects_non_ancestor_or_regressed_target(self) -> None:
        target_sha = "d" * 40
        api = FakeGitHub()
        with self.assertRaisesRegex(GuardError, "not an ancestor of workflow main"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_production(
                MAIN_SHA,
                operation="update-active",
                target_sha=target_sha,
                target_archive_sha256=TARGET_ARCHIVE_SHA256,
                target_manifest_sha256=TARGET_MANIFEST_SHA256,
                run_id="100",
                run_attempt="1",
            )

        api.mark_ancestor(target_sha, MAIN_SHA)
        with self.assertRaisesRegex(GuardError, "current www Active.*not an ancestor"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_production(
                MAIN_SHA,
                operation="update-active",
                target_sha=target_sha,
                target_archive_sha256=TARGET_ARCHIVE_SHA256,
                target_manifest_sha256=TARGET_MANIFEST_SHA256,
                run_id="100",
                run_attempt="1",
            )

    def test_non_publishing_controls_do_not_scan_later_main(self) -> None:
        discard_api = FakeGitHub()
        _, discard_plan = ReleaseCoordinationGuard(
            discard_api,
            REPOSITORY,
        ).validate_production(
            MAIN_SHA,
            operation="discard-candidate",
            run_id="100",
            run_attempt="1",
        )
        self.assertEqual(discard_plan["targetSha"], BASELINE_SHA)
        self.assertEqual(discard_plan["unpublishedPullRequests"], [])
        self.assertEqual(discard_api.range_calls, [])

        rollback_sha = "d" * 40
        rollback_api = FakeGitHub()
        rollback_api.mark_ancestor(rollback_sha, BASELINE_SHA)
        rollback_api.mark_ancestor(rollback_sha, MAIN_SHA)
        _, rollback_plan = ReleaseCoordinationGuard(
            rollback_api,
            REPOSITORY,
        ).validate_production(
            MAIN_SHA,
            operation="rollback-active",
            target_sha=rollback_sha,
            target_archive_sha256=TARGET_ARCHIVE_SHA256,
            target_manifest_sha256=TARGET_MANIFEST_SHA256,
            run_id="100",
            run_attempt="1",
        )
        self.assertEqual(rollback_plan["targetSha"], rollback_sha)
        self.assertEqual(
            rollback_plan["targetArchiveSha256"],
            TARGET_ARCHIVE_SHA256,
        )
        self.assertEqual(
            rollback_plan["targetManifestSha256"],
            TARGET_MANIFEST_SHA256,
        )
        self.assertEqual(rollback_plan["unpublishedPullRequests"], [])
        self.assertEqual(rollback_api.range_calls, [])

    def test_rollback_rejects_forward_or_partial_identity(self) -> None:
        rollback_sha = "d" * 40
        api = FakeGitHub()
        api.mark_ancestor(rollback_sha, MAIN_SHA)
        with self.assertRaisesRegex(GuardError, "not an ancestor of current www"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_production(
                MAIN_SHA,
                operation="rollback-active",
                target_sha=rollback_sha,
                target_archive_sha256=TARGET_ARCHIVE_SHA256,
                target_manifest_sha256=TARGET_MANIFEST_SHA256,
                run_id="100",
                run_attempt="1",
            )
        with self.assertRaisesRegex(GuardError, "target archive SHA-256"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_production(
                MAIN_SHA,
                operation="rollback-active",
                target_sha=rollback_sha,
                run_id="100",
                run_attempt="1",
            )

    def test_release_target_input_is_bound_to_its_operation(self) -> None:
        guard = ReleaseCoordinationGuard(FakeGitHub(), REPOSITORY)
        with self.assertRaisesRegex(GuardError, "cannot specify a target identity"):
            guard.validate_production(
                MAIN_SHA,
                operation="prepare-candidate",
                target_sha="d" * 40,
                run_id="100",
                run_attempt="1",
            )
        with self.assertRaisesRegex(GuardError, "invalid update-active target SHA"):
            guard.validate_production(
                MAIN_SHA,
                operation="update-active",
                run_id="100",
                run_attempt="1",
            )

    def test_merged_dependency_contract_is_a_persistent_global_hold(self) -> None:
        dependency = pull_request(174)
        dependent = pull_request(
            175,
            body="Depends-On: #174",
            state="closed",
            merged=True,
        )
        unrelated = pull_request(176, state="closed", merged=True)
        api = FakeGitHub(pulls=[dependency, dependent, unrelated])
        api.mark_ancestor(str(dependent["merge_commit_sha"]))
        api.mark_ancestor(str(unrelated["merge_commit_sha"]))
        with self.assertRaisesRegex(GuardError, "dependency PR #174 is still open"):
            ReleaseCoordinationGuard(api, REPOSITORY).validate_production(
                MAIN_SHA,
                operation="prepare-candidate",
                run_id="100",
                run_attempt="1",
            )

    def test_mutable_historical_pr_bodies_before_baseline_are_not_scanned(self) -> None:
        historical = pull_request(
            100,
            body="Depends-On: malformed historical prose",
            state="closed",
            merged=True,
            author_association="CONTRIBUTOR",
        )
        unpublished = pull_request(176, state="closed", merged=True)
        api = FakeGitHub(
            pulls=[historical, unpublished],
            range_numbers={176},
        )
        api.mark_ancestor(str(unpublished["merge_commit_sha"]))
        result, plan = ReleaseCoordinationGuard(
            api,
            REPOSITORY,
        ).validate_production(
            MAIN_SHA,
            operation="prepare-candidate",
            run_id="100",
            run_attempt="1",
        )
        self.assertIn("1 unpublished PRs", result)
        self.assertEqual(
            [item["number"] for item in plan["unpublishedPullRequests"]],
            [176],
        )

    def test_api_failure_fails_closed(self) -> None:
        class FailingGitHub(FakeGitHub):
            def get_main_sha(self) -> str:
                raise GuardError("GitHub API unavailable")

        with self.assertRaisesRegex(GuardError, "GitHub API unavailable"):
            ReleaseCoordinationGuard(
                FailingGitHub(),
                REPOSITORY,
            ).validate_production(
                MAIN_SHA,
                operation="prepare-candidate",
                run_id="100",
                run_attempt="1",
            )

    def test_frozen_plan_is_bound_to_run_and_rechecks_only_current_main(self) -> None:
        api = self._group_api(
            member_merged=True,
            anchor_merged=True,
        )
        guard = ReleaseCoordinationGuard(api, REPOSITORY)
        _, plan = guard.validate_production(
            MAIN_SHA,
            operation="prepare-candidate",
            run_id="100",
            run_attempt="2",
        )
        result = guard.verify_frozen_plan(
            plan,
            main_sha=MAIN_SHA,
            operation="prepare-candidate",
            run_id="100",
            run_attempt="2",
        )
        self.assertIn("frozen release coordination plan is valid", result)

        api.issues[200]["body"] = "malformed after approval"
        api.pulls[175]["body"] = "Depends-On: malformed after approval"
        result = guard.verify_frozen_plan(
            plan,
            main_sha=MAIN_SHA,
            operation="prepare-candidate",
            run_id="100",
            run_attempt="2",
        )
        self.assertIn("frozen release coordination plan is valid", result)

        changed = dict(plan)
        changed["runAttempt"] = "1"
        with self.assertRaisesRegex(GuardError, "run attempt does not match"):
            guard.verify_frozen_plan(
                changed,
                main_sha=MAIN_SHA,
                operation="prepare-candidate",
                run_id="100",
                run_attempt="2",
            )

    def test_frozen_plan_rejects_main_advancing_after_approval(self) -> None:
        api = FakeGitHub()
        guard = ReleaseCoordinationGuard(api, REPOSITORY)
        _, plan = guard.validate_production(
            MAIN_SHA,
            operation="prepare-candidate",
            run_id="100",
            run_attempt="1",
        )
        api.main_sha = "d" * 40
        with self.assertRaisesRegex(GuardError, "stale production run after approval"):
            guard.verify_frozen_plan(
                plan,
                main_sha=MAIN_SHA,
                operation="prepare-candidate",
                run_id="100",
                run_attempt="1",
            )

    def test_frozen_update_plan_binds_operation_and_older_target(self) -> None:
        target_sha = "d" * 40
        api = FakeGitHub()
        api.mark_ancestor(BASELINE_SHA, target_sha)
        api.mark_ancestor(target_sha, MAIN_SHA)
        guard = ReleaseCoordinationGuard(api, REPOSITORY)
        _, plan = guard.validate_production(
            MAIN_SHA,
            operation="update-active",
            target_sha=target_sha,
            target_archive_sha256=TARGET_ARCHIVE_SHA256,
            target_manifest_sha256=TARGET_MANIFEST_SHA256,
            run_id="100",
            run_attempt="1",
        )
        result = guard.verify_frozen_plan(
            plan,
            main_sha=MAIN_SHA,
            operation="update-active",
            target_sha=target_sha,
            target_archive_sha256=TARGET_ARCHIVE_SHA256,
            target_manifest_sha256=TARGET_MANIFEST_SHA256,
            run_id="100",
            run_attempt="1",
        )
        self.assertIn(target_sha[:12], result)

        with self.assertRaisesRegex(GuardError, "operation does not match"):
            guard.verify_frozen_plan(
                plan,
                main_sha=MAIN_SHA,
                operation="prepare-candidate",
                run_id="100",
                run_attempt="1",
            )
        with self.assertRaisesRegex(GuardError, "target SHA does not match"):
            guard.verify_frozen_plan(
                plan,
                main_sha=MAIN_SHA,
                operation="update-active",
                target_sha="c" * 40,
                target_archive_sha256=TARGET_ARCHIVE_SHA256,
                target_manifest_sha256=TARGET_MANIFEST_SHA256,
                run_id="100",
                run_attempt="1",
            )
        with self.assertRaisesRegex(GuardError, "target archive SHA-256"):
            guard.verify_frozen_plan(
                plan,
                main_sha=MAIN_SHA,
                operation="update-active",
                target_sha=target_sha,
                target_archive_sha256="c" * 64,
                target_manifest_sha256=TARGET_MANIFEST_SHA256,
                run_id="100",
                run_attempt="1",
            )

    def test_frozen_rollback_plan_binds_the_reviewed_target_triple(self) -> None:
        target_sha = "d" * 40
        api = FakeGitHub()
        api.mark_ancestor(target_sha, BASELINE_SHA)
        api.mark_ancestor(target_sha, MAIN_SHA)
        guard = ReleaseCoordinationGuard(api, REPOSITORY)
        _, plan = guard.validate_production(
            MAIN_SHA,
            operation="rollback-active",
            target_sha=target_sha,
            target_archive_sha256=TARGET_ARCHIVE_SHA256,
            target_manifest_sha256=TARGET_MANIFEST_SHA256,
            run_id="100",
            run_attempt="1",
        )
        result = guard.verify_frozen_plan(
            plan,
            main_sha=MAIN_SHA,
            operation="rollback-active",
            target_sha=target_sha,
            target_archive_sha256=TARGET_ARCHIVE_SHA256,
            target_manifest_sha256=TARGET_MANIFEST_SHA256,
            run_id="100",
            run_attempt="1",
        )
        self.assertIn(target_sha[:12], result)
        self.assertEqual(api.range_calls, [])

        changed = dict(plan)
        changed["targetManifestSha256"] = "c" * 64
        with self.assertRaisesRegex(GuardError, "target manifest SHA-256"):
            guard.verify_frozen_plan(
                changed,
                main_sha=MAIN_SHA,
                operation="rollback-active",
                target_sha=target_sha,
                target_archive_sha256=TARGET_ARCHIVE_SHA256,
                target_manifest_sha256=TARGET_MANIFEST_SHA256,
                run_id="100",
                run_attempt="1",
            )


if __name__ == "__main__":
    unittest.main()
