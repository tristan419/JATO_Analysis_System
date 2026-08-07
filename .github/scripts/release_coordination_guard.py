#!/usr/bin/env python3
"""Fail-closed release coordination checks for pull requests and production.

The coordination contract is intentionally opt-in:

* PRs without ``Release-Group`` or ``Depends-On`` trailers are independent.
* ``Release-Group: #<issue>`` points to a same-repository issue carrying the
  ``release-group`` label and an explicit anchor/member manifest.
* ``Depends-On: #<pr>[, #<pr>...]`` requires merged, non-draft main PRs whose
  merge commits are ancestors of the main revision being evaluated.
* Coordinated PRs add one per-PR JSON contract. PR checks bind it to the exact
  head SHA; production checks bind it to the exact merge SHA and blob SHA.

Only the GitHub REST API and the Python standard library are used so the same
trusted script can run before a production environment or build is entered.
"""

from __future__ import annotations

import argparse
import base64
import binascii
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Iterable, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


MAIN_BRANCH = "main"
RELEASE_GROUP_LABEL = "release-group"
STATUS_CONTEXT = "release-coordination-guard"
TRUSTED_ISSUE_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}
PLAN_SCHEMA = 3
CONTRACT_SCHEMA = 1
MAX_GITHUB_NUMBER = 9_223_372_036_854_775_807
CONTRACT_PATH_TEMPLATE = (
    ".github/release-coordination/contracts/pr-{pull_request}.json"
)
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
PUBLIC_ORIGIN_PATTERN = re.compile(
    r"^https://[A-Za-z0-9.-]+(?::[1-9][0-9]{0,4})?$"
)
RELEASE_OPERATIONS = {
    "prepare-candidate",
    "discard-candidate",
    "update-active",
    "rollback-active",
}
TRAILER_PATTERN = re.compile(
    r"^\s*(Release-Group|Depends-On):\s*(.*?)\s*$",
    re.IGNORECASE,
)
GROUP_FIELD_PATTERN = re.compile(
    r"^\s*(Release-Group-Anchor|Release-Group-Members):\s*(.*?)\s*$",
    re.IGNORECASE,
)
REFERENCE_PATTERN = re.compile(r"#([1-9][0-9]*)")


class GuardError(RuntimeError):
    """A release contract violation or an unavailable validation dependency."""


@dataclass(frozen=True)
class PullRequestMetadata:
    release_group: int | None
    dependencies: tuple[int, ...]


@dataclass(frozen=True)
class ReleaseGroupManifest:
    anchor: int
    members: tuple[int, ...]


@dataclass(frozen=True)
class ImmutableReleaseGroup:
    issue: int
    anchor: int
    members: tuple[int, ...]


@dataclass(frozen=True)
class ImmutableReleaseContract:
    repository: str
    pull_request: int
    release_group: ImmutableReleaseGroup | None
    dependencies: tuple[int, ...]
    blob_sha: str


class GitHubGateway(Protocol):
    def get_issue(self, number: int) -> Mapping[str, Any]: ...

    def get_pull_request(self, number: int) -> Mapping[str, Any]: ...

    def is_ancestor(self, ancestor_sha: str, descendant_sha: str) -> bool: ...

    def get_main_sha(self) -> str: ...

    def get_current_production_sha(self) -> str: ...

    def list_pull_requests_between(
        self,
        baseline_sha: str,
        target_sha: str,
    ) -> list[Mapping[str, Any]]: ...

    def list_open_main_pull_requests(self) -> list[Mapping[str, Any]]: ...

    def get_commit_status(self, sha: str) -> tuple[str, str] | None: ...

    def set_commit_status(self, sha: str, state: str, description: str) -> None: ...

    def get_release_contract(
        self,
        pull_request_number: int,
        ref_sha: str,
    ) -> tuple[Mapping[str, Any], str] | None: ...

    def list_pull_request_files(
        self,
        pull_request_number: int,
    ) -> list[Mapping[str, Any]]: ...


def _visible_markdown_lines(body: str | None) -> Iterable[str]:
    """Yield metadata-capable lines, excluding fenced examples and comments."""

    in_fence = False
    in_comment = False
    for line in (body or "").splitlines():
        if line.startswith("    ") or line.startswith("\t"):
            continue
        stripped = line.strip()
        if stripped.startswith("```") or stripped.startswith("~~~"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if in_comment:
            if "-->" in stripped:
                in_comment = False
            continue
        if stripped.startswith("<!--"):
            if "-->" not in stripped:
                in_comment = True
            continue
        yield line


def _parse_references(value: str, field_name: str, *, allow_none: bool) -> tuple[int, ...]:
    normalized = value.strip()
    if allow_none and normalized.lower() in {"", "none"}:
        return ()
    if not re.fullmatch(r"#[1-9][0-9]*(?:\s*,\s*#[1-9][0-9]*)*", normalized):
        raise GuardError(
            f"{field_name} must be {'none or ' if allow_none else ''}"
            "a comma-separated list such as #174, #175"
        )
    references = []
    for match in REFERENCE_PATTERN.findall(normalized):
        if len(match) > 19:
            raise GuardError(f"{field_name} reference exceeds the supported range")
        try:
            reference = int(match)
        except ValueError as exc:
            raise GuardError(f"{field_name} contains an invalid PR reference") from exc
        if reference > MAX_GITHUB_NUMBER:
            raise GuardError(f"{field_name} reference exceeds the supported range")
        references.append(reference)
    if len(references) != len(set(references)):
        raise GuardError(f"{field_name} contains duplicate PR references")
    return tuple(references)


def parse_pull_request_metadata(body: str | None) -> PullRequestMetadata:
    values: dict[str, str] = {}
    for line in _visible_markdown_lines(body):
        match = TRAILER_PATTERN.match(line)
        if not match:
            continue
        key = match.group(1).lower()
        if key in values:
            raise GuardError(f"duplicate {match.group(1)} trailer")
        values[key] = match.group(2)

    release_group_value = values.get("release-group", "").strip()
    if release_group_value.lower() in {"", "none", "independent"}:
        release_group = None
    else:
        references = _parse_references(
            release_group_value,
            "Release-Group",
            allow_none=False,
        )
        if len(references) != 1:
            raise GuardError("Release-Group must reference exactly one issue")
        release_group = references[0]

    dependencies = _parse_references(
        values.get("depends-on", "none"),
        "Depends-On",
        allow_none=True,
    )
    return PullRequestMetadata(
        release_group=release_group,
        dependencies=dependencies,
    )


def parse_release_group_manifest(body: str | None) -> ReleaseGroupManifest:
    values: dict[str, str] = {}
    for line in _visible_markdown_lines(body):
        match = GROUP_FIELD_PATTERN.match(line)
        if not match:
            continue
        key = match.group(1).lower()
        if key in values:
            raise GuardError(f"duplicate {match.group(1)} field")
        values[key] = match.group(2)

    missing = {
        "release-group-anchor",
        "release-group-members",
    } - set(values)
    if missing:
        raise GuardError(
            "release-group issue is missing fields: " + ", ".join(sorted(missing))
        )
    anchor_values = _parse_references(
        values["release-group-anchor"],
        "Release-Group-Anchor",
        allow_none=False,
    )
    if len(anchor_values) != 1:
        raise GuardError("Release-Group-Anchor must reference exactly one PR")
    members = _parse_references(
        values["release-group-members"],
        "Release-Group-Members",
        allow_none=False,
    )
    if len(members) < 2:
        raise GuardError("a release group must list at least two member PRs")
    anchor = anchor_values[0]
    if anchor not in members:
        raise GuardError("release-group anchor must also be listed as a member")
    return ReleaseGroupManifest(anchor=anchor, members=members)


def _mapping(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise GuardError(f"GitHub API returned invalid {context}")
    return value


def _strict_json_mapping(text: str, context: str) -> Mapping[str, Any]:
    depth = 0
    in_string = False
    escaped = False
    for character in text:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > 100:
                raise GuardError(
                    f"{context} is not valid JSON: nesting exceeds the safe limit"
                )
        elif character in "]}":
            depth -= 1

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise GuardError(f"{context} contains duplicate key {key!r}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise GuardError(f"{context} contains unsupported JSON constant {value!r}")

    def parse_integer(value: str) -> int:
        digits = value.removeprefix("-")
        if len(digits) > 19:
            raise GuardError(f"{context} contains an integer outside the safe range")
        parsed = int(value)
        if abs(parsed) > MAX_GITHUB_NUMBER:
            raise GuardError(f"{context} contains an integer outside the safe range")
        return parsed

    try:
        payload = json.loads(
            text,
            object_pairs_hook=reject_duplicates,
            parse_int=parse_integer,
            parse_constant=reject_constant,
        )
    except (ValueError, RecursionError) as exc:
        raise GuardError(f"{context} is not valid JSON") from exc
    return _mapping(payload, context)


def _number(value: Any, context: str) -> int:
    if (
        type(value) is not int
        or value < 1
        or value > MAX_GITHUB_NUMBER
    ):
        raise GuardError(f"GitHub API returned invalid {context}")
    return value


def _sha(value: Any, context: str) -> str:
    candidate = str(value or "").lower()
    if not SHA_PATTERN.fullmatch(candidate):
        raise GuardError(f"GitHub API returned invalid {context}")
    return candidate


def _sha256(value: Any, context: str) -> str:
    candidate = str(value or "").lower()
    if not SHA256_PATTERN.fullmatch(candidate):
        raise GuardError(f"GitHub API returned invalid {context}")
    return candidate


def _release_operation(value: Any) -> str:
    operation = str(value or "")
    if operation not in RELEASE_OPERATIONS:
        raise GuardError("production release operation is unsupported")
    return operation


def _target_identity_inputs(
    operation: str,
    target_sha: str,
    target_archive_sha256: str,
    target_manifest_sha256: str,
) -> tuple[str, str, str]:
    raw_identity = (
        str(target_sha or ""),
        str(target_archive_sha256 or ""),
        str(target_manifest_sha256 or ""),
    )
    if operation in {"update-active", "rollback-active"}:
        return (
            _sha(raw_identity[0], f"{operation} target SHA"),
            _sha256(raw_identity[1], f"{operation} target archive SHA-256"),
            _sha256(raw_identity[2], f"{operation} target manifest SHA-256"),
        )
    if any(raw_identity):
        raise GuardError(f"{operation} cannot specify a target identity")
    return "", "", ""


def _number_list(value: Any, context: str) -> tuple[int, ...]:
    if not isinstance(value, list):
        raise GuardError(f"{context} must be a list")
    numbers = tuple(_number(item, context) for item in value)
    if len(numbers) != len(set(numbers)):
        raise GuardError(f"{context} contains duplicate PR references")
    return numbers


def parse_immutable_release_contract(
    payload: Mapping[str, Any],
    *,
    expected_repository: str,
    expected_pull_request: int,
    blob_sha: str,
) -> ImmutableReleaseContract:
    if set(payload) != {
        "schema",
        "repository",
        "pullRequest",
        "releaseGroup",
        "dependsOn",
    }:
        raise GuardError(
            f"PR #{expected_pull_request} immutable release contract schema is invalid"
        )
    if (
        type(payload.get("schema")) is not int
        or payload.get("schema") != CONTRACT_SCHEMA
    ):
        raise GuardError(
            f"PR #{expected_pull_request} immutable release contract version is unsupported"
        )
    if payload.get("repository") != expected_repository:
        raise GuardError(
            f"PR #{expected_pull_request} immutable release contract repository "
            "does not match"
        )
    pull_request = _number(payload.get("pullRequest"), "contract pullRequest")
    if pull_request != expected_pull_request:
        raise GuardError(
            f"PR #{expected_pull_request} immutable contract identifies PR #{pull_request}"
        )
    dependencies = _number_list(payload.get("dependsOn"), "contract dependsOn")
    if pull_request in dependencies:
        raise GuardError(f"PR #{pull_request} immutable contract cannot depend on itself")

    release_group_payload = payload.get("releaseGroup")
    release_group = None
    if release_group_payload is not None:
        group = _mapping(release_group_payload, "contract releaseGroup")
        if set(group) != {"issue", "anchor", "members"}:
            raise GuardError(
                f"PR #{pull_request} immutable release-group schema is invalid"
            )
        issue = _number(group.get("issue"), "contract release-group issue")
        anchor = _number(group.get("anchor"), "contract release-group anchor")
        members = _number_list(
            group.get("members"),
            "contract release-group members",
        )
        if len(members) < 2:
            raise GuardError("immutable release group must contain at least two members")
        if anchor not in members:
            raise GuardError("immutable release-group anchor must be a member")
        if pull_request not in members:
            raise GuardError(
                f"immutable release group does not list PR #{pull_request}"
            )
        release_group = ImmutableReleaseGroup(
            issue=issue,
            anchor=anchor,
            members=members,
        )

    if release_group is None and not dependencies:
        raise GuardError(
            f"PR #{pull_request} must omit an empty independent release contract"
        )
    return ImmutableReleaseContract(
        repository=expected_repository,
        pull_request=pull_request,
        release_group=release_group,
        dependencies=dependencies,
        blob_sha=_sha(blob_sha, "release contract blob SHA"),
    )


class GitHubRestClient:
    """Minimal read-only GitHub REST client with fail-closed pagination."""

    def __init__(
        self,
        repository: str,
        token: str,
        *,
        api_url: str = "https://api.github.com",
        public_origin: str = "https://www.ojeur.cloud",
    ) -> None:
        if not REPOSITORY_PATTERN.fullmatch(repository):
            raise GuardError("GITHUB_REPOSITORY must use owner/repository form")
        if not token:
            raise GuardError("GITHUB_TOKEN is required")
        if not PUBLIC_ORIGIN_PATTERN.fullmatch(public_origin):
            raise GuardError("JATO_PUBLIC_ORIGIN must be one HTTPS origin")
        self.repository = repository
        self.token = token
        self.api_url = api_url.rstrip("/")
        self.public_origin = public_origin
        self._issues: dict[int, Mapping[str, Any]] = {}
        self._pull_requests: dict[int, Mapping[str, Any]] = {}
        self._ancestor_results: dict[tuple[str, str], bool] = {}

    def _request(
        self,
        path: str,
        query: Mapping[str, str] | None = None,
        *,
        method: str = "GET",
        payload: Mapping[str, Any] | None = None,
        allow_not_found: bool = False,
    ) -> Any:
        url = f"{self.api_url}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"
        data = None
        if payload is not None:
            data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request = Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "jato-release-coordination-guard",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if allow_not_found and exc.code == 404:
                return None
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            raise GuardError(
                f"GitHub API {exc.code} for {path}: {detail or exc.reason}"
            ) from exc
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise GuardError(f"GitHub API unavailable for {path}: {exc}") from exc
        return payload

    def _public_request(self, path: str) -> Any:
        separator = "&" if "?" in path else "?"
        url = (
            f"{self.public_origin}{path}{separator}"
            f"release_coordination_nonce={os.urandom(8).hex()}"
        )
        request = Request(
            url,
            method="GET",
            headers={
                "Accept": "application/json",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "User-Agent": "jato-release-coordination-guard",
            },
        )
        try:
            with urlopen(request, timeout=20) as response:
                status = int(getattr(response, "status", 0))
                body = response.read(256 * 1024 + 1)
        except HTTPError as exc:
            raise GuardError(
                f"www Active endpoint returned HTTP {exc.code}: {path}"
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise GuardError(f"www Active endpoint is unavailable: {path}") from exc
        if status != 200 or len(body) > 256 * 1024:
            raise GuardError(f"www Active endpoint response is invalid: {path}")
        try:
            text = body.decode("utf-8")
        except UnicodeError as exc:
            raise GuardError(f"www Active endpoint is not UTF-8: {path}") from exc
        return _strict_json_mapping(text, f"www Active endpoint {path}")

    def _paginate(
        self,
        path: str,
        query: Mapping[str, str],
    ) -> list[Mapping[str, Any]]:
        results: list[Mapping[str, Any]] = []
        for page in range(1, 101):
            page_query = dict(query)
            page_query.update({"per_page": "100", "page": str(page)})
            payload = self._request(path, page_query)
            if not isinstance(payload, list):
                raise GuardError(f"GitHub API returned invalid list for {path}")
            for item in payload:
                results.append(_mapping(item, f"{path} item"))
            if len(payload) < 100:
                return results
        raise GuardError(f"GitHub API pagination exceeded the safe limit for {path}")

    def get_issue(self, number: int) -> Mapping[str, Any]:
        if number not in self._issues:
            payload = self._request(f"/repos/{self.repository}/issues/{number}")
            self._issues[number] = _mapping(payload, f"issue #{number}")
        return self._issues[number]

    def get_pull_request(self, number: int) -> Mapping[str, Any]:
        if number not in self._pull_requests:
            payload = self._request(f"/repos/{self.repository}/pulls/{number}")
            self._pull_requests[number] = _mapping(payload, f"PR #{number}")
        return self._pull_requests[number]

    def is_ancestor(self, ancestor_sha: str, descendant_sha: str) -> bool:
        ancestor = _sha(ancestor_sha, "ancestor SHA")
        descendant = _sha(descendant_sha, "descendant SHA")
        key = (ancestor, descendant)
        if key not in self._ancestor_results:
            payload = _mapping(
                self._request(
                    "/repos/"
                    f"{self.repository}/compare/{quote(ancestor)}...{quote(descendant)}"
                ),
                "compare result",
            )
            merge_base = _mapping(payload.get("merge_base_commit"), "merge base")
            status = str(payload.get("status") or "")
            merge_base_sha = _sha(merge_base.get("sha"), "merge-base SHA")
            self._ancestor_results[key] = (
                status in {"ahead", "identical"} and merge_base_sha == ancestor
            )
        return self._ancestor_results[key]

    def get_main_sha(self) -> str:
        payload = _mapping(
            self._request(f"/repos/{self.repository}/branches/{MAIN_BRANCH}"),
            f"{MAIN_BRANCH} branch",
        )
        commit = _mapping(payload.get("commit"), f"{MAIN_BRANCH} branch commit")
        return _sha(commit.get("sha"), f"{MAIN_BRANCH} branch SHA")

    def get_current_production_sha(self) -> str:
        health = _mapping(
            self._public_request("/healthz"),
            "www Active health response",
        )
        if health.get("status") != "ok":
            raise GuardError("www Active health check did not report status=ok")

        metadata = _mapping(
            self._public_request("/build-meta.json"),
            "www Active build metadata",
        )
        deploy_value = metadata.get("deployCommit")
        github_value = metadata.get("githubSha")
        commit_value = metadata.get("commit")
        commit_mode = metadata.get("commitMode")

        if deploy_value is not None:
            deploy_sha = _sha(deploy_value, "www Active deployCommit")
            if github_value is not None:
                github_sha = _sha(github_value, "www Active githubSha")
                if github_sha != deploy_sha:
                    raise GuardError(
                        "www Active build metadata deployCommit/githubSha mismatch"
                    )
            if commit_mode == "deploy" and commit_value is not None:
                commit_sha = _sha(commit_value, "www Active commit")
                if commit_sha != deploy_sha:
                    raise GuardError(
                        "www Active build metadata deploy-mode commit mismatch"
                    )
            return deploy_sha

        if github_value is not None:
            github_sha = _sha(github_value, "legacy www Active githubSha")
            if commit_mode == "deploy" and commit_value is not None:
                commit_sha = _sha(commit_value, "legacy www Active commit")
                if commit_sha != github_sha:
                    raise GuardError(
                        "legacy www Active build metadata commit/githubSha mismatch"
                    )
            return github_sha

        if commit_value is not None and commit_mode in {None, "", "deploy"}:
            return _sha(commit_value, "legacy www Active commit")
        raise GuardError("www Active build metadata has no unambiguous deploy SHA")

    def list_pull_requests_between(
        self,
        baseline_sha: str,
        target_sha: str,
    ) -> list[Mapping[str, Any]]:
        baseline = _sha(baseline_sha, "production baseline SHA")
        target = _sha(target_sha, "production target SHA")
        if baseline == target:
            return []
        if not self.is_ancestor(baseline, target):
            raise GuardError(
                f"verified production baseline {baseline[:12]} is not an "
                f"ancestor of target {target[:12]}"
            )

        commits: dict[str, Mapping[str, Any]] = {}
        compare_path = (
            f"/repos/{self.repository}/compare/"
            f"{quote(baseline)}...{quote(target)}"
        )
        for page in range(1, 101):
            payload = _mapping(
                self._request(
                    compare_path,
                    {"per_page": "100", "page": str(page)},
                ),
                "compare result",
            )
            page_commits = payload.get("commits")
            if not isinstance(page_commits, list):
                raise GuardError("GitHub API returned invalid compare commits")
            for commit in page_commits:
                commit_payload = _mapping(commit, "compare commit")
                commit_sha = _sha(commit_payload.get("sha"), "compare commit SHA")
                commits[commit_sha] = commit_payload
            total_commits = _number(payload.get("total_commits"), "compare total commits")
            if len(commits) >= total_commits:
                break
            if not page_commits:
                raise GuardError("GitHub compare pagination ended before all commits")
        else:
            raise GuardError("GitHub compare pagination exceeded the safe limit")
        if not commits:
            raise GuardError(
                "production baseline and target differ but compare returned no commits"
            )

        pull_requests: dict[int, Mapping[str, Any]] = {}
        for commit_sha in commits:
            associated_pull_requests = self._paginate(
                f"/repos/{self.repository}/commits/{commit_sha}/pulls",
                {},
            )
            commit_has_merged_main_pr = False
            for pull_request in associated_pull_requests:
                pull_payload = _mapping(pull_request, "associated PR")
                number = _number(pull_payload.get("number"), "associated PR number")
                full_pull_request = self.get_pull_request(number)
                merge_sha_value = full_pull_request.get("merge_commit_sha")
                merge_sha = (
                    _sha(merge_sha_value, f"PR #{number} merge SHA")
                    if merge_sha_value
                    else None
                )
                if (
                    full_pull_request.get("merged_at")
                    and merge_sha in commits
                    and str(
                        _mapping(
                            full_pull_request.get("base"),
                            f"PR #{number} base",
                        ).get("ref")
                        or ""
                    )
                    == MAIN_BRANCH
                ):
                    commit_has_merged_main_pr = True
                    pull_requests[number] = full_pull_request
            if not commit_has_merged_main_pr:
                raise GuardError(
                    f"unpublished main commit {commit_sha[:12]} has no associated "
                    "merged pull request"
                )
        return [pull_requests[number] for number in sorted(pull_requests)]

    def list_open_main_pull_requests(self) -> list[Mapping[str, Any]]:
        pulls = self._paginate(
            f"/repos/{self.repository}/pulls",
            {"state": "open", "base": MAIN_BRANCH},
        )
        result = []
        for pull_request in pulls:
            number = _number(pull_request.get("number"), "open PR number")
            full_pull_request = self.get_pull_request(number)
            result.append(full_pull_request)
        return result

    def set_commit_status(self, sha: str, state: str, description: str) -> None:
        target_sha = _sha(sha, "status target SHA")
        if state not in {"pending", "success", "failure"}:
            raise GuardError(f"invalid commit status state {state!r}")
        if not description or len(description) > 140:
            raise GuardError("commit status description must contain 1-140 characters")
        self._request(
            f"/repos/{self.repository}/statuses/{target_sha}",
            method="POST",
            payload={
                "state": state,
                "context": STATUS_CONTEXT,
                "description": description,
            },
        )

    def get_commit_status(self, sha: str) -> tuple[str, str] | None:
        target_sha = _sha(sha, "status target SHA")
        path = f"/repos/{self.repository}/commits/{target_sha}/status"
        for page in range(1, 11):
            payload = _mapping(
                self._request(
                    path,
                    {"per_page": "100", "page": str(page)},
                ),
                "combined commit status",
            )
            statuses = payload.get("statuses")
            if not isinstance(statuses, list):
                raise GuardError("GitHub API returned invalid commit statuses")
            for status in statuses:
                if not isinstance(status, Mapping):
                    raise GuardError("GitHub API returned invalid commit status")
                if str(status.get("context") or "") == STATUS_CONTEXT:
                    state = str(status.get("state") or "")
                    description = str(status.get("description") or "")
                    if state not in {"pending", "success", "failure", "error"}:
                        raise GuardError(
                            "GitHub API returned invalid commit status state"
                        )
                    return state, description
            if len(statuses) < 100:
                return None
        raise GuardError("GitHub commit statuses pagination exceeded the safe limit")

    def get_release_contract(
        self,
        pull_request_number: int,
        ref_sha: str,
    ) -> tuple[Mapping[str, Any], str] | None:
        number = _number(pull_request_number, "contract PR number")
        ref = _sha(ref_sha, "contract ref SHA")
        path = CONTRACT_PATH_TEMPLATE.format(pull_request=number)
        payload = self._request(
            f"/repos/{self.repository}/contents/{quote(path, safe='/')}",
            {"ref": ref},
            allow_not_found=True,
        )
        if payload is None:
            return None
        content_payload = _mapping(payload, f"PR #{number} release contract file")
        if content_payload.get("type") != "file":
            raise GuardError(f"PR #{number} release contract path is not a file")
        if content_payload.get("encoding") != "base64":
            raise GuardError(f"PR #{number} release contract must use base64 API encoding")
        encoded = str(content_payload.get("content") or "").replace("\n", "")
        try:
            raw = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise GuardError(f"PR #{number} release contract is not valid base64") from exc
        if not raw or len(raw) > 16_384:
            raise GuardError(f"PR #{number} release contract size is invalid")
        try:
            decoded = raw.decode("utf-8")
            contract = _strict_json_mapping(decoded, f"PR #{number} release contract")
        except UnicodeDecodeError as exc:
            raise GuardError(f"PR #{number} release contract is not UTF-8") from exc
        blob_sha = _sha(
            content_payload.get("sha"),
            f"PR #{number} release contract blob SHA",
        )
        return contract, blob_sha

    def list_pull_request_files(
        self,
        pull_request_number: int,
    ) -> list[Mapping[str, Any]]:
        number = _number(pull_request_number, "PR number")
        path = f"/repos/{self.repository}/pulls/{number}/files"
        results: list[Mapping[str, Any]] = []
        for page in range(1, 31):
            payload = self._request(
                path,
                {"per_page": "100", "page": str(page)},
            )
            if not isinstance(payload, list):
                raise GuardError(f"GitHub API returned invalid list for {path}")
            results.extend(
                _mapping(item, f"{path} item")
                for item in payload
            )
            if len(payload) < 100:
                return results
        raise GuardError(
            f"PR #{number} reaches GitHub's 3000-file visibility limit; "
            "release-contract ownership cannot be proven"
        )


class ReleaseCoordinationGuard:
    def __init__(self, api: GitHubGateway, repository: str) -> None:
        self.api = api
        self.repository = repository
        self._metadata: dict[int, PullRequestMetadata] = {}
        self._contracts: dict[
            tuple[int, str],
            ImmutableReleaseContract | None,
        ] = {}
        self._live_contracts: dict[int, ImmutableReleaseContract | None] = {}
        self._contract_ownership: dict[int, bool] = {}

    def _pr_number(self, pull_request: Mapping[str, Any]) -> int:
        return _number(pull_request.get("number"), "PR number")

    def _metadata_for(self, pull_request: Mapping[str, Any]) -> PullRequestMetadata:
        number = self._pr_number(pull_request)
        if number not in self._metadata:
            metadata = parse_pull_request_metadata(
                str(pull_request.get("body") or "")
            )
            if metadata.release_group is not None or metadata.dependencies:
                association = str(
                    pull_request.get("author_association") or ""
                ).upper()
                if association not in TRUSTED_ISSUE_ASSOCIATIONS:
                    raise GuardError(
                        f"coordinated PR #{number} must be authored by a repository "
                        "owner, member, or collaborator"
                    )
            if metadata.release_group is not None:
                labels = pull_request.get("labels")
                if not isinstance(labels, list):
                    raise GuardError(f"release-group PR #{number} has invalid labels")
                label_names = {
                    str(label.get("name") or "").lower()
                    for label in labels
                    if isinstance(label, Mapping)
                }
                if RELEASE_GROUP_LABEL not in label_names:
                    raise GuardError(
                        f"release-group PR #{number} is missing the "
                        f"{RELEASE_GROUP_LABEL!r} label"
                    )
            self._metadata[number] = metadata
        return self._metadata[number]

    def _contract_for(
        self,
        pull_request: Mapping[str, Any],
        ref_sha: str,
    ) -> ImmutableReleaseContract | None:
        number = self._pr_number(pull_request)
        ref = _sha(ref_sha, f"PR #{number} contract ref SHA")
        key = (number, ref)
        if key not in self._contracts:
            raw_contract = self.api.get_release_contract(number, ref)
            if raw_contract is None:
                self._contracts[key] = None
            else:
                payload, blob_sha = raw_contract
                self._contracts[key] = parse_immutable_release_contract(
                    payload,
                    expected_repository=self.repository,
                    expected_pull_request=number,
                    blob_sha=blob_sha,
                )
        return self._contracts[key]

    def _validate_contract_file_ownership(
        self,
        pull_request: Mapping[str, Any],
    ) -> bool:
        number = self._pr_number(pull_request)
        if number in self._contract_ownership:
            return self._contract_ownership[number]
        expected_path = CONTRACT_PATH_TEMPLATE.format(pull_request=number)
        contract_changes = []
        for changed_file in self.api.list_pull_request_files(number):
            filename = str(changed_file.get("filename") or "")
            previous_filename = str(changed_file.get("previous_filename") or "")
            contract_paths = {
                path
                for path in (filename, previous_filename)
                if path.startswith(".github/release-coordination/contracts/")
            }
            if contract_paths:
                contract_changes.append(changed_file)
                foreign_paths = contract_paths - {expected_path}
                if foreign_paths:
                    raise GuardError(
                        f"PR #{number} cannot change another release contract: "
                        f"{sorted(foreign_paths)[0]}"
                    )
        if len(contract_changes) > 1:
            raise GuardError(f"PR #{number} changes its release contract more than once")
        if contract_changes:
            status = str(contract_changes[0].get("status") or "")
            if status != "added":
                raise GuardError(
                    f"PR #{number} release contract must be newly added, found {status!r}"
                )
        owns_contract = bool(contract_changes)
        self._contract_ownership[number] = owns_contract
        return owns_contract

    def _validate_pull_request_contract(
        self,
        pull_request: Mapping[str, Any],
        metadata: PullRequestMetadata,
    ) -> ImmutableReleaseContract | None:
        number = self._pr_number(pull_request)
        if number in self._live_contracts:
            return self._live_contracts[number]
        expected_path = CONTRACT_PATH_TEMPLATE.format(pull_request=number)
        owns_contract = self._validate_contract_file_ownership(pull_request)
        head_sha = self._head_sha(pull_request)
        contract = self._contract_for(pull_request, head_sha)
        coordinated = metadata.release_group is not None or bool(metadata.dependencies)
        if coordinated and contract is None:
            raise GuardError(
                f"PR #{number} must add immutable contract {expected_path}"
            )
        if not coordinated and contract is not None:
            raise GuardError(
                f"PR #{number} has an immutable contract but its body is independent"
            )
        if contract is None:
            if owns_contract:
                raise GuardError(
                    f"PR #{number} changed {expected_path} but it is absent from its head"
                )
            self._live_contracts[number] = None
            return None
        if not owns_contract:
            raise GuardError(
                f"PR #{number} immutable contract is not owned by this PR diff"
            )
        if contract.dependencies != metadata.dependencies:
            raise GuardError(
                f"PR #{number} immutable dependsOn does not match Depends-On trailer"
            )
        contract_group = (
            contract.release_group.issue
            if contract.release_group is not None
            else None
        )
        if contract_group != metadata.release_group:
            raise GuardError(
                f"PR #{number} immutable releaseGroup does not match "
                "Release-Group trailer"
            )
        if contract.release_group is not None:
            issue = self._release_group_issue(contract.release_group.issue)
            manifest = parse_release_group_manifest(str(issue.get("body") or ""))
            if (
                manifest.anchor != contract.release_group.anchor
                or manifest.members != contract.release_group.members
            ):
                raise GuardError(
                    f"PR #{number} immutable group manifest does not match issue "
                    f"#{contract.release_group.issue}"
                )
        self._live_contracts[number] = contract
        return contract

    def _assert_same_repository_main(
        self,
        pull_request: Mapping[str, Any],
    ) -> None:
        number = self._pr_number(pull_request)
        base = _mapping(pull_request.get("base"), f"PR #{number} base")
        base_repository = _mapping(base.get("repo"), f"PR #{number} base repository")
        if str(base_repository.get("full_name") or "") != self.repository:
            raise GuardError(f"PR #{number} must belong to {self.repository}")
        if str(base.get("ref") or "") != MAIN_BRANCH:
            raise GuardError(f"PR #{number} must target {MAIN_BRANCH}")

    def _merge_sha(self, pull_request: Mapping[str, Any]) -> str:
        number = self._pr_number(pull_request)
        if bool(pull_request.get("draft")):
            raise GuardError(f"dependency PR #{number} is still a draft")
        state = str(pull_request.get("state") or "")
        merged_at = pull_request.get("merged_at")
        if state != "closed" or not merged_at:
            if state == "closed":
                raise GuardError(f"dependency PR #{number} was closed without merging")
            raise GuardError(f"dependency PR #{number} is still open")
        return _sha(pull_request.get("merge_commit_sha"), f"PR #{number} merge SHA")

    def _assert_merged_ancestor(
        self,
        pull_request: Mapping[str, Any],
        main_sha: str,
    ) -> str:
        self._assert_same_repository_main(pull_request)
        merge_sha = self._merge_sha(pull_request)
        if not self.api.is_ancestor(merge_sha, main_sha):
            number = self._pr_number(pull_request)
            raise GuardError(
                f"dependency PR #{number} merge {merge_sha[:12]} is not an "
                f"ancestor of main {main_sha[:12]}"
            )
        return merge_sha

    def _validate_dependency_tree(
        self,
        root_pull_request: Mapping[str, Any],
        main_sha: str,
    ) -> None:
        root_number = self._pr_number(root_pull_request)
        active = [root_number]
        validated: set[int] = set()

        def visit(number: int) -> None:
            if number in active:
                cycle = " -> ".join(f"#{item}" for item in (*active, number))
                raise GuardError(f"Depends-On cycle detected: {cycle}")
            if number in validated:
                return
            active.append(number)
            dependency = self.api.get_pull_request(number)
            self._assert_same_repository_main(dependency)
            merge_sha = self._assert_merged_ancestor(dependency, main_sha)
            contract = self._contract_for(dependency, merge_sha)
            nested_dependencies = (
                contract.dependencies if contract is not None else ()
            )
            for nested in nested_dependencies:
                visit(nested)
            active.pop()
            validated.add(number)

        metadata = self._metadata_for(root_pull_request)
        if root_number in metadata.dependencies:
            raise GuardError(f"PR #{root_number} cannot depend on itself")
        for dependency_number in metadata.dependencies:
            visit(dependency_number)

    def _release_group_issue(self, number: int) -> Mapping[str, Any]:
        issue = self.api.get_issue(number)
        if "pull_request" in issue:
            raise GuardError(f"Release-Group #{number} must reference an issue, not a PR")
        if str(issue.get("state") or "") != "open":
            raise GuardError(
                f"release-group issue #{number} must stay open until production succeeds"
            )
        association = str(issue.get("author_association") or "").upper()
        if association not in TRUSTED_ISSUE_ASSOCIATIONS:
            raise GuardError(
                f"release-group issue #{number} must be owned by a repository "
                "owner, member, or collaborator"
            )
        labels = issue.get("labels")
        if not isinstance(labels, list):
            raise GuardError(f"release-group issue #{number} has invalid labels")
        label_names = {
            str(label.get("name") or "").lower()
            for label in labels
            if isinstance(label, Mapping)
        }
        if RELEASE_GROUP_LABEL not in label_names:
            raise GuardError(
                f"Release-Group issue #{number} is missing the "
                f"{RELEASE_GROUP_LABEL!r} label"
            )
        return issue

    def _load_group(
        self,
        group_number: int,
    ) -> tuple[ReleaseGroupManifest, dict[int, Mapping[str, Any]]]:
        issue = self._release_group_issue(group_number)
        manifest = parse_release_group_manifest(str(issue.get("body") or ""))
        expected_group = ImmutableReleaseGroup(
            issue=group_number,
            anchor=manifest.anchor,
            members=manifest.members,
        )
        members: dict[int, Mapping[str, Any]] = {}
        for number in manifest.members:
            pull_request = self.api.get_pull_request(number)
            self._assert_same_repository_main(pull_request)
            metadata = self._metadata_for(pull_request)
            if metadata.release_group != group_number:
                found = (
                    "independent"
                    if metadata.release_group is None
                    else f"#{metadata.release_group}"
                )
                raise GuardError(
                    f"release-group #{group_number} member PR #{number} points to {found}"
                )
            contract = self._validate_pull_request_contract(
                pull_request,
                metadata,
            )
            if contract is None or contract.release_group != expected_group:
                raise GuardError(
                    f"release-group #{group_number} member PR #{number} "
                    "does not carry the identical immutable group manifest"
                )
            members[number] = pull_request

        anchor_metadata = self._metadata_for(members[manifest.anchor])
        required_dependencies = set(manifest.members) - {manifest.anchor}
        missing_dependencies = required_dependencies - set(anchor_metadata.dependencies)
        if missing_dependencies:
            missing = ", ".join(f"#{number}" for number in sorted(missing_dependencies))
            raise GuardError(
                f"release-group #{group_number} anchor PR #{manifest.anchor} "
                f"must Depends-On every other member; missing {missing}"
            )
        return manifest, members

    def validate_pull_request(self, pull_request_number: int) -> str:
        pull_request = self.api.get_pull_request(pull_request_number)
        self._assert_same_repository_main(pull_request)
        metadata = self._metadata_for(pull_request)
        self._validate_pull_request_contract(pull_request, metadata)
        base = _mapping(pull_request.get("base"), f"PR #{pull_request_number} base")
        main_sha = _sha(base.get("sha"), f"PR #{pull_request_number} base SHA")

        if metadata.release_group is not None:
            manifest, members = self._load_group(metadata.release_group)
            if pull_request_number not in members:
                raise GuardError(
                    f"PR #{pull_request_number} is not listed in release-group "
                    f"#{metadata.release_group}"
                )
            anchor = members[manifest.anchor]
            if anchor.get("merged_at") and any(
                not member.get("merged_at")
                for number, member in members.items()
                if number != manifest.anchor
            ):
                raise GuardError(
                    f"release-group #{metadata.release_group} anchor "
                    f"PR #{manifest.anchor} merged before all members"
                )

        self._validate_dependency_tree(pull_request, main_sha)
        if metadata.release_group is None and not metadata.dependencies:
            return f"PR #{pull_request_number} is independent"
        return (
            f"PR #{pull_request_number} release coordination is valid "
            f"at main {main_sha[:12]}"
        )

    def _head_sha(self, pull_request: Mapping[str, Any]) -> str:
        number = self._pr_number(pull_request)
        head = _mapping(pull_request.get("head"), f"PR #{number} head")
        return _sha(head.get("sha"), f"PR #{number} head SHA")

    def mark_pull_request_pending(self, pull_request_number: int) -> str:
        pull_request = self.api.get_pull_request(pull_request_number)
        head_sha = self._head_sha(pull_request)
        current = self.api.get_commit_status(head_sha)
        if current is None or current[0] != "pending":
            self.api.set_commit_status(
                head_sha,
                "pending",
                "Validating release coordination metadata",
            )
        return f"PR #{pull_request_number} release coordination is pending"

    def sweep_open_pull_requests(self) -> str:
        pull_requests = self.api.list_open_main_pull_requests()
        pull_requests_by_head: dict[str, list[int]] = {}
        for pull_request in pull_requests:
            pull_requests_by_head.setdefault(
                self._head_sha(pull_request),
                [],
            ).append(self._pr_number(pull_request))
        failures: list[str] = []
        for pull_request in pull_requests:
            number = self._pr_number(pull_request)
            head_sha = self._head_sha(pull_request)
            duplicate_numbers = pull_requests_by_head[head_sha]
            try:
                if len(duplicate_numbers) > 1:
                    duplicates = ", ".join(
                        f"#{item}" for item in sorted(duplicate_numbers)
                    )
                    raise GuardError(
                        f"head SHA is shared by open PRs {duplicates}"
                    )
                self.validate_pull_request(number)
            except GuardError as exc:
                state = "failure"
                description = f"Blocked: {exc}"[:140]
                failures.append(f"PR #{number}: {exc}")
            else:
                state = "success"
                metadata = self._metadata_for(pull_request)
                description = (
                    "Independent release contract is valid"
                    if metadata.release_group is None and not metadata.dependencies
                    else "Release coordination contract is valid"
                )
            current = self.api.get_commit_status(head_sha)
            if current == (state, description):
                continue
            if current is None or current[0] != "pending":
                self.api.set_commit_status(
                    head_sha,
                    "pending",
                    "Validating release coordination metadata",
                )
            self.api.set_commit_status(
                head_sha,
                state,
                description,
            )
        if failures:
            raise GuardError(
                f"{len(failures)} open PR release coordination checks failed; "
                + " | ".join(failures[:5])
            )
        return f"release coordination status refreshed for {len(pull_requests)} open PRs"

    def _validate_immutable_dependency_tree(
        self,
        root_contract: ImmutableReleaseContract,
        main_sha: str,
        range_contracts: Mapping[int, ImmutableReleaseContract | None],
    ) -> None:
        active = [root_contract.pull_request]
        validated: set[int] = set()

        def visit(number: int) -> None:
            if number in active:
                cycle = " -> ".join(f"#{item}" for item in (*active, number))
                raise GuardError(f"Depends-On cycle detected: {cycle}")
            if number in validated:
                return
            active.append(number)
            dependency = self.api.get_pull_request(number)
            merge_sha = self._assert_merged_ancestor(dependency, main_sha)
            contract = (
                range_contracts[number]
                if number in range_contracts
                else self._contract_for(dependency, merge_sha)
            )
            for nested in contract.dependencies if contract is not None else ():
                visit(nested)
            active.pop()
            validated.add(number)

        for dependency_number in root_contract.dependencies:
            visit(dependency_number)

    def _validate_immutable_release_groups(
        self,
        main_sha: str,
        range_records: Mapping[
            int,
            tuple[Mapping[str, Any], str, ImmutableReleaseContract | None],
        ],
    ) -> list[dict[str, Any]]:
        groups: dict[int, ImmutableReleaseGroup] = {}
        sources: dict[int, set[int]] = {}
        range_contracts = {
            number: contract
            for number, (_, _, contract) in range_records.items()
        }
        for number, (_, _, contract) in range_records.items():
            if contract is None or contract.release_group is None:
                continue
            group = contract.release_group
            existing = groups.get(group.issue)
            if existing is not None and existing != group:
                raise GuardError(
                    f"release-group #{group.issue} immutable manifests disagree"
                )
            groups[group.issue] = group
            sources.setdefault(group.issue, set()).add(number)

        plans: list[dict[str, Any]] = []
        for issue in sorted(groups):
            group = groups[issue]
            missing_members = set(group.members) - set(range_records)
            if missing_members:
                merged = ", ".join(
                    f"#{number}" for number in sorted(sources[issue])
                )
                pending = ", ".join(
                    f"#{number}" for number in sorted(missing_members)
                )
                raise GuardError(
                    f"release-group #{issue} is partially merged: "
                    f"merged {merged}; pending {pending}"
                )

            member_plan = []
            for number in group.members:
                pull_request, merge_sha, contract = range_records[number]
                if contract is None or contract.release_group != group:
                    raise GuardError(
                        f"release-group #{issue} member PR #{number} does not "
                        "carry the identical merge-time contract"
                    )
                self._assert_merged_ancestor(pull_request, main_sha)
                member_plan.append(
                    {
                        "number": number,
                        "mergeSha": merge_sha,
                        "contractBlobSha": contract.blob_sha,
                        "dependencies": list(contract.dependencies),
                    }
                )

            anchor_contract = range_records[group.anchor][2]
            if anchor_contract is None:
                raise GuardError(
                    f"release-group #{issue} anchor contract is missing"
                )
            required_dependencies = set(group.members) - {group.anchor}
            missing_dependencies = required_dependencies - set(
                anchor_contract.dependencies
            )
            if missing_dependencies:
                missing = ", ".join(
                    f"#{number}" for number in sorted(missing_dependencies)
                )
                raise GuardError(
                    f"release-group #{issue} anchor PR #{group.anchor} immutable "
                    f"contract must depend on every other member; missing {missing}"
                )
            for number in group.members:
                contract = range_records[number][2]
                if contract is not None:
                    self._validate_immutable_dependency_tree(
                        contract,
                        main_sha,
                        range_contracts,
                    )
            plans.append(
                {
                    "issue": issue,
                    "anchor": group.anchor,
                    "members": member_plan,
                }
            )
        return plans

    def _validate_immutable_range_dependencies(
        self,
        main_sha: str,
        range_records: Mapping[
            int,
            tuple[Mapping[str, Any], str, ImmutableReleaseContract | None],
        ],
    ) -> list[dict[str, Any]]:
        range_contracts = {
            number: contract
            for number, (_, _, contract) in range_records.items()
        }
        plans: list[dict[str, Any]] = []
        for number in sorted(range_records):
            _, merge_sha, contract = range_records[number]
            if contract is None or not contract.dependencies:
                continue
            self._validate_immutable_dependency_tree(
                contract,
                main_sha,
                range_contracts,
            )
            plans.append(
                {
                    "pullRequest": number,
                    "mergeSha": merge_sha,
                    "contractBlobSha": contract.blob_sha,
                    "dependencies": list(contract.dependencies),
                }
            )
        return plans

    def build_production_plan(
        self,
        main_sha: str,
        *,
        operation: str,
        target_sha: str = "",
        target_archive_sha256: str = "",
        target_manifest_sha256: str = "",
        run_id: str,
        run_attempt: str,
    ) -> dict[str, Any]:
        workflow_sha = _sha(main_sha, "main SHA")
        release_operation = _release_operation(operation)
        if not re.fullmatch(r"[1-9][0-9]*", run_id):
            raise GuardError("production run id must be a positive integer")
        if not re.fullmatch(r"[1-9][0-9]*", run_attempt):
            raise GuardError("production run attempt must be a positive integer")
        remote_main = self.api.get_main_sha()
        if workflow_sha != remote_main:
            raise GuardError(
                f"stale production run: workflow {workflow_sha[:12]} is not current "
                f"main {remote_main[:12]}"
            )
        baseline_sha = self.api.get_current_production_sha()
        requested_target, requested_archive, requested_manifest = (
            _target_identity_inputs(
                release_operation,
                target_sha,
                target_archive_sha256,
                target_manifest_sha256,
            )
        )
        if release_operation == "prepare-candidate":
            release_target = workflow_sha
        elif release_operation in {"update-active", "rollback-active"}:
            release_target = requested_target
        else:
            release_target = baseline_sha
        if not self.api.is_ancestor(release_target, workflow_sha):
            raise GuardError(
                f"release target {release_target[:12]} is not an ancestor of "
                f"workflow main {workflow_sha[:12]}"
            )
        if release_operation == "rollback-active":
            if not self.api.is_ancestor(release_target, baseline_sha):
                raise GuardError(
                    f"rollback target {release_target[:12]} is not an ancestor of "
                    f"current www Active {baseline_sha[:12]}"
                )
            range_pull_requests: list[Mapping[str, Any]] = []
        elif release_operation == "discard-candidate":
            range_pull_requests = []
        else:
            if not self.api.is_ancestor(baseline_sha, release_target):
                raise GuardError(
                    f"current www Active {baseline_sha[:12]} is not an ancestor of "
                    f"release target {release_target[:12]}"
                )
            range_pull_requests = self.api.list_pull_requests_between(
                baseline_sha,
                release_target,
            )
        range_records: dict[
            int,
            tuple[Mapping[str, Any], str, ImmutableReleaseContract | None],
        ] = {}
        for pull_request in range_pull_requests:
            number = self._pr_number(pull_request)
            merge_sha = self._assert_merged_ancestor(
                pull_request,
                release_target,
            )
            contract = self._contract_for(pull_request, merge_sha)
            owns_contract = self._validate_contract_file_ownership(pull_request)
            expected_path = CONTRACT_PATH_TEMPLATE.format(pull_request=number)
            if contract is not None and not owns_contract:
                raise GuardError(
                    f"merged PR #{number} contract {expected_path} is not owned "
                    "by that PR diff"
                )
            if contract is None and owns_contract:
                raise GuardError(
                    f"merged PR #{number} added {expected_path} but it is absent "
                    "from the exact merge SHA"
                )
            range_records[number] = (pull_request, merge_sha, contract)

        range_plan = []
        for number in sorted(range_records):
            _, merge_sha, contract = range_records[number]
            contract_plan = None
            if contract is not None:
                contract_plan = {
                    "blobSha": contract.blob_sha,
                    "releaseGroup": (
                        {
                            "issue": contract.release_group.issue,
                            "anchor": contract.release_group.anchor,
                            "members": list(contract.release_group.members),
                        }
                        if contract.release_group is not None
                        else None
                    ),
                    "dependencies": list(contract.dependencies),
                }
            range_plan.append(
                {
                    "number": number,
                    "mergeSha": merge_sha,
                    "contract": contract_plan,
                }
            )
        group_plans = self._validate_immutable_release_groups(
            release_target,
            range_records,
        )
        dependency_plans = self._validate_immutable_range_dependencies(
            release_target,
            range_records,
        )
        return {
            "schema": PLAN_SCHEMA,
            "repository": self.repository,
            "runId": run_id,
            "runAttempt": run_attempt,
            "operation": release_operation,
            "workflowSha": workflow_sha,
            "baselineSha": baseline_sha,
            "targetSha": release_target,
            "targetArchiveSha256": requested_archive,
            "targetManifestSha256": requested_manifest,
            "unpublishedPullRequests": range_plan,
            "releaseGroups": group_plans,
            "mergedDependencyContracts": dependency_plans,
        }

    def validate_production(
        self,
        main_sha: str,
        *,
        operation: str,
        target_sha: str = "",
        target_archive_sha256: str = "",
        target_manifest_sha256: str = "",
        run_id: str,
        run_attempt: str,
    ) -> tuple[str, dict[str, Any]]:
        plan = self.build_production_plan(
            main_sha,
            operation=operation,
            target_sha=target_sha,
            target_archive_sha256=target_archive_sha256,
            target_manifest_sha256=target_manifest_sha256,
            run_id=run_id,
            run_attempt=run_attempt,
        )
        groups_checked = len(plan["releaseGroups"])
        dependencies_checked = len(plan["mergedDependencyContracts"])
        return (
            f"production release coordination is valid for {plan['operation']} "
            f"at {plan['targetSha'][:12]}: "
            f"{len(plan['unpublishedPullRequests'])} unpublished PRs, "
            f"{groups_checked} completed groups, "
            f"{dependencies_checked} dependency contracts",
            plan,
        )

    def verify_frozen_plan(
        self,
        plan: Mapping[str, Any],
        *,
        main_sha: str,
        operation: str,
        target_sha: str = "",
        target_archive_sha256: str = "",
        target_manifest_sha256: str = "",
        run_id: str,
        run_attempt: str,
    ) -> str:
        expected_keys = {
            "schema",
            "repository",
            "runId",
            "runAttempt",
            "operation",
            "workflowSha",
            "baselineSha",
            "targetSha",
            "targetArchiveSha256",
            "targetManifestSha256",
            "unpublishedPullRequests",
            "releaseGroups",
            "mergedDependencyContracts",
        }
        if set(plan) != expected_keys:
            raise GuardError("frozen coordination plan schema is invalid")
        if (
            type(plan.get("schema")) is not int
            or plan.get("schema") != PLAN_SCHEMA
        ):
            raise GuardError("frozen coordination plan version is unsupported")
        if plan.get("repository") != self.repository:
            raise GuardError("frozen coordination plan repository does not match")
        if str(plan.get("runId") or "") != run_id:
            raise GuardError("frozen coordination plan run id does not match")
        if str(plan.get("runAttempt") or "") != run_attempt:
            raise GuardError("frozen coordination plan run attempt does not match")
        release_operation = _release_operation(operation)
        if plan.get("operation") != release_operation:
            raise GuardError("frozen coordination plan operation does not match")
        frozen_workflow_sha = _sha(
            plan.get("workflowSha"),
            "frozen plan workflow SHA",
        )
        expected_workflow_sha = _sha(main_sha, "main SHA")
        if frozen_workflow_sha != expected_workflow_sha:
            raise GuardError("frozen coordination plan workflow SHA does not match")
        frozen_target_sha = _sha(plan.get("targetSha"), "frozen plan target SHA")
        baseline_sha = _sha(
            plan.get("baselineSha"),
            "frozen plan baseline SHA",
        )
        requested_target, requested_archive, requested_manifest = (
            _target_identity_inputs(
                release_operation,
                target_sha,
                target_archive_sha256,
                target_manifest_sha256,
            )
        )
        if release_operation == "prepare-candidate":
            expected_target_sha = expected_workflow_sha
        elif release_operation in {"update-active", "rollback-active"}:
            expected_target_sha = requested_target
        else:
            expected_target_sha = baseline_sha
        if frozen_target_sha != expected_target_sha:
            raise GuardError("frozen coordination plan target SHA does not match")
        if plan.get("targetArchiveSha256") != requested_archive:
            raise GuardError(
                "frozen coordination plan target archive SHA-256 does not match"
            )
        if plan.get("targetManifestSha256") != requested_manifest:
            raise GuardError(
                "frozen coordination plan target manifest SHA-256 does not match"
            )
        for key in (
            "unpublishedPullRequests",
            "releaseGroups",
            "mergedDependencyContracts",
        ):
            if not isinstance(plan.get(key), list):
                raise GuardError(f"frozen coordination plan {key} must be a list")
        remote_main = self.api.get_main_sha()
        if expected_workflow_sha != remote_main:
            raise GuardError(
                f"stale production run after approval: workflow "
                f"{expected_workflow_sha[:12]} is not current main {remote_main[:12]}"
            )
        if not self.api.is_ancestor(frozen_target_sha, frozen_workflow_sha):
            raise GuardError("frozen coordination target is not a workflow ancestor")
        if release_operation == "rollback-active":
            if not self.api.is_ancestor(frozen_target_sha, baseline_sha):
                raise GuardError(
                    "frozen coordination rollback target is not a baseline ancestor"
                )
        elif not self.api.is_ancestor(baseline_sha, frozen_target_sha):
            raise GuardError("frozen coordination plan baseline is not a target ancestor")
        return (
            f"frozen release coordination plan is valid for run "
            f"{run_id}/{run_attempt} at {frozen_target_sha[:12]}"
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repository",
        default=os.environ.get("GITHUB_REPOSITORY", ""),
        help="GitHub owner/repository (defaults to GITHUB_REPOSITORY)",
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)
    pull_request = subparsers.add_parser("pull-request")
    pull_request.add_argument("--pull-request", required=True, type=int)
    mark_pending = subparsers.add_parser("mark-pending")
    mark_pending.add_argument("--pull-request", required=True, type=int)
    subparsers.add_parser("sweep")
    production = subparsers.add_parser("production")
    production.add_argument("--main-sha", required=True)
    production.add_argument(
        "--operation",
        required=True,
        choices=sorted(RELEASE_OPERATIONS),
    )
    production.add_argument("--target-sha", default="")
    production.add_argument("--target-archive-sha256", default="")
    production.add_argument("--target-manifest-sha256", default="")
    production.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID", ""))
    production.add_argument(
        "--run-attempt",
        default=os.environ.get("GITHUB_RUN_ATTEMPT", ""),
    )
    production.add_argument("--plan-output", required=True)
    verify_plan = subparsers.add_parser("verify-plan")
    verify_plan.add_argument("--main-sha", required=True)
    verify_plan.add_argument(
        "--operation",
        required=True,
        choices=sorted(RELEASE_OPERATIONS),
    )
    verify_plan.add_argument("--target-sha", default="")
    verify_plan.add_argument("--target-archive-sha256", default="")
    verify_plan.add_argument("--target-manifest-sha256", default="")
    verify_plan.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID", ""))
    verify_plan.add_argument(
        "--run-attempt",
        default=os.environ.get("GITHUB_RUN_ATTEMPT", ""),
    )
    verify_plan.add_argument("--plan", required=True)
    return parser


def _canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _load_expected_plan(path: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GuardError(f"cannot read expected coordination plan: {exc}") from exc
    return _mapping(payload, "expected coordination plan")


def _write_plan(path: str, plan: Mapping[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(_canonical_json(plan) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        client = GitHubRestClient(
            repository=args.repository,
            token=os.environ.get("GITHUB_TOKEN", ""),
            api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
            public_origin=os.environ.get(
                "JATO_PUBLIC_ORIGIN",
                "https://www.ojeur.cloud",
            ),
        )
        guard = ReleaseCoordinationGuard(client, args.repository)
        if args.mode == "pull-request":
            result = guard.validate_pull_request(args.pull_request)
        elif args.mode == "mark-pending":
            result = guard.mark_pull_request_pending(args.pull_request)
        elif args.mode == "sweep":
            result = guard.sweep_open_pull_requests()
        elif args.mode == "production":
            result, plan = guard.validate_production(
                args.main_sha,
                operation=args.operation,
                target_sha=args.target_sha,
                target_archive_sha256=args.target_archive_sha256,
                target_manifest_sha256=args.target_manifest_sha256,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
            )
            _write_plan(args.plan_output, plan)
        else:
            result = guard.verify_frozen_plan(
                _load_expected_plan(args.plan),
                main_sha=args.main_sha,
                operation=args.operation,
                target_sha=args.target_sha,
                target_archive_sha256=args.target_archive_sha256,
                target_manifest_sha256=args.target_manifest_sha256,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
            )
    except GuardError as exc:
        message = str(exc).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print(
                f"::error title=Release coordination blocked::{message}",
                file=sys.stderr,
            )
        else:
            print(f"release coordination blocked: {exc}", file=sys.stderr)
        return 1
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
