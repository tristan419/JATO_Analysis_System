from __future__ import annotations

import hashlib
import hmac
import os
import re
import stat
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any


REPLAYABLE_EVIDENCE_TYPES = frozenset(
    {"uploaded_pdf", "downloaded_pdf", "html_snapshot", "api_snapshot"}
)
SUPPORTING_OBJECT_EVIDENCE_TYPES = frozenset({"screenshot"})
OBJECT_BACKED_EVIDENCE_TYPES = (
    REPLAYABLE_EVIDENCE_TYPES | SUPPORTING_OBJECT_EVIDENCE_TYPES
)
NON_REPLAYABLE_REFERENCE_TYPES = frozenset({"official_url"})
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _row_value(row: object, field: str) -> Any:
    if isinstance(row, dict):
        return row.get(field)
    return getattr(row, field, None)


def _asset_id(row: object) -> str:
    return str(_row_value(row, "evidence_asset_id") or "")


def _safe_object_path(root: Path, storage_key: str) -> tuple[Path | None, str | None]:
    if (
        not storage_key
        or any(character in storage_key for character in ("\x00", "\r", "\n", "\\"))
    ):
        return None, "invalid_storage_key"
    key_path = PurePosixPath(storage_key)
    if (
        key_path.is_absolute()
        or not key_path.parts
        or key_path.parts[0] != "assets"
        or any(part in {"", ".", ".."} for part in key_path.parts)
        or storage_key.startswith("-")
    ):
        return None, "invalid_storage_key"
    candidate = (root / Path(*key_path.parts)).resolve(strict=False)
    try:
        candidate.relative_to(root)
    except ValueError:
        return None, "path_escape"
    return candidate, None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _content_address_matches(storage_key: str, expected_sha256: str) -> bool:
    parts = PurePosixPath(storage_key).parts
    if len(parts) != 3 or not SHA256_PATTERN.fullmatch(expected_sha256):
        return False
    filename_hash = parts[2].split(".", 1)[0]
    return parts[1] == expected_sha256[:2] and filename_hash == expected_sha256


def _audit_object_group(
    root: Path,
    storage_key: str,
    grouped: list[object],
) -> dict[str, object]:
    expected_hashes = {
        str(_row_value(row, "sha256") or "").casefold() for row in grouped
    }
    expected_sizes = {_row_value(row, "size_bytes") for row in grouped}
    expected_sha256 = next(iter(expected_hashes)) if len(expected_hashes) == 1 else ""
    expected_size = next(iter(expected_sizes)) if len(expected_sizes) == 1 else None
    issues: list[str] = []
    object_path, path_issue = _safe_object_path(root, storage_key)
    if len(expected_hashes) != 1 or not SHA256_PATTERN.fullmatch(expected_sha256):
        issues.append("invalid_sha256_metadata")
    if not path_issue and not _content_address_matches(storage_key, expected_sha256):
        issues.append("content_address_mismatch")
    if len(expected_sizes) != 1 or not isinstance(expected_size, int) or expected_size < 0:
        issues.append("invalid_size_metadata")

    actual_size: int | None = None
    actual_sha256: str | None = None
    if path_issue:
        issues.append(path_issue)
    elif object_path is None:
        issues.append("invalid_storage_key")
    else:
        try:
            file_stat = object_path.lstat()
        except FileNotFoundError:
            issues.append("missing")
        except OSError:
            issues.append("unreadable")
        else:
            actual_size = file_stat.st_size
            if stat.S_ISLNK(file_stat.st_mode) or not stat.S_ISREG(file_stat.st_mode):
                issues.append("not_regular_file")
            elif not os.access(object_path, os.R_OK):
                issues.append("unreadable")
            else:
                try:
                    actual_sha256 = _sha256_file(object_path)
                except OSError:
                    issues.append("unreadable")
            if isinstance(expected_size, int) and actual_size != expected_size:
                issues.append("size_mismatch")
            if (
                actual_sha256
                and expected_sha256
                and not hmac.compare_digest(actual_sha256, expected_sha256)
            ):
                issues.append("sha256_mismatch")

    return {
        "storageKey": storage_key,
        "evidenceAssetIds": sorted(_asset_id(row) for row in grouped),
        "replayable": any(
            str(_row_value(row, "evidence_type") or "").strip()
            in REPLAYABLE_EVIDENCE_TYPES
            for row in grouped
        ),
        "expectedSizeBytes": expected_size,
        "actualSizeBytes": actual_size,
        "expectedSha256": expected_sha256 or None,
        "actualSha256": actual_sha256,
        "status": "healthy" if not issues else "unhealthy",
        "issues": issues,
    }


def audit_evidence_object(row: object, evidence_root: Path) -> dict[str, object]:
    root = evidence_root.expanduser().resolve()
    evidence_type = str(_row_value(row, "evidence_type") or "").strip()
    storage_key = str(_row_value(row, "storage_key") or "").strip()
    if evidence_type not in OBJECT_BACKED_EVIDENCE_TYPES:
        return {
            "storageKey": storage_key or None,
            "evidenceAssetIds": [_asset_id(row)],
            "replayable": False,
            "expectedSizeBytes": _row_value(row, "size_bytes"),
            "actualSizeBytes": None,
            "expectedSha256": str(_row_value(row, "sha256") or "").casefold(),
            "actualSha256": None,
            "status": "unhealthy",
            "issues": [
                "non_replayable_reference"
                if evidence_type in NON_REPLAYABLE_REFERENCE_TYPES
                else "unsupported_evidence_type"
            ],
        }
    if not storage_key:
        return {
            "storageKey": None,
            "evidenceAssetIds": [_asset_id(row)],
            "replayable": evidence_type in REPLAYABLE_EVIDENCE_TYPES,
            "expectedSizeBytes": _row_value(row, "size_bytes"),
            "actualSizeBytes": None,
            "expectedSha256": str(_row_value(row, "sha256") or "").casefold(),
            "actualSha256": None,
            "status": "unhealthy",
            "issues": ["missing_storage_key"],
        }
    return _audit_object_group(root, storage_key, [row])


def _orphan_payload(root: Path, path: Path) -> dict[str, object]:
    relative_path = path.relative_to(root).as_posix()
    payload: dict[str, object] = {
        "storageKey": relative_path,
        "sizeBytes": None,
        "sha256": None,
        "issues": ["orphan"],
    }
    try:
        file_stat = path.lstat()
    except OSError as exc:
        payload["issues"] = ["orphan", "unreadable"]
        payload["error"] = str(exc)
        return payload
    payload["sizeBytes"] = file_stat.st_size
    if stat.S_ISLNK(file_stat.st_mode) or not stat.S_ISREG(file_stat.st_mode):
        payload["issues"] = ["orphan", "not_regular_file"]
        return payload
    try:
        payload["sha256"] = _sha256_file(path)
    except OSError as exc:
        payload["issues"] = ["orphan", "unreadable"]
        payload["error"] = str(exc)
    return payload


def audit_msrp_evidence_integrity(
    rows: Iterable[object],
    evidence_root: Path,
    *,
    checked_at: datetime | None = None,
) -> dict[str, object]:
    root = evidence_root.expanduser().resolve()
    checked_at_utc = checked_at or datetime.now(timezone.utc)
    row_list = list(rows)
    grouped_rows: dict[str, list[object]] = defaultdict(list)
    ignored_assets: list[dict[str, object]] = []
    object_results: list[dict[str, object]] = []
    replayable_row_count = 0
    supporting_object_row_count = 0

    for row in row_list:
        evidence_type = str(_row_value(row, "evidence_type") or "").strip()
        storage_key = str(_row_value(row, "storage_key") or "").strip()
        if evidence_type not in OBJECT_BACKED_EVIDENCE_TYPES:
            ignored_assets.append(
                {
                    "evidenceAssetId": _asset_id(row),
                    "evidenceType": evidence_type,
                    "storageKey": storage_key or None,
                    "reason": (
                        "non_replayable_reference"
                        if evidence_type in NON_REPLAYABLE_REFERENCE_TYPES
                        else "unsupported_evidence_type"
                    ),
                }
            )
            continue
        if evidence_type in REPLAYABLE_EVIDENCE_TYPES:
            replayable_row_count += 1
        else:
            supporting_object_row_count += 1
        if not storage_key:
            object_results.append(
                {
                    "storageKey": None,
                    "evidenceAssetIds": [_asset_id(row)],
                    "replayable": evidence_type in REPLAYABLE_EVIDENCE_TYPES,
                    "expectedSizeBytes": _row_value(row, "size_bytes"),
                    "actualSizeBytes": None,
                    "expectedSha256": str(_row_value(row, "sha256") or "").casefold(),
                    "actualSha256": None,
                    "status": "unhealthy",
                    "issues": ["missing_storage_key"],
                }
            )
            continue
        grouped_rows[storage_key].append(row)

    for storage_key in sorted(grouped_rows):
        object_results.append(
            _audit_object_group(root, storage_key, grouped_rows[storage_key])
        )

    expected_storage_keys = set(grouped_rows)
    orphan_objects: list[dict[str, object]] = []
    assets_root = root / "assets"
    root_issues: list[str] = []
    if root.exists() and not root.is_dir():
        root_issues.append("evidence_root_not_directory")
    elif assets_root.exists():
        try:
            candidates = sorted(assets_root.rglob("*"))
        except OSError:
            root_issues.append("evidence_root_unreadable")
            candidates = []
        for candidate in candidates:
            try:
                candidate_stat = candidate.lstat()
            except OSError:
                candidate_stat = None
            if candidate_stat is not None and stat.S_ISDIR(candidate_stat.st_mode):
                continue
            storage_key = candidate.relative_to(root).as_posix()
            if storage_key in expected_storage_keys:
                continue
            orphan_objects.append(_orphan_payload(root, candidate))

    issue_counts: Counter[str] = Counter(root_issues)
    for item in object_results:
        issue_counts.update(str(issue) for issue in item["issues"])
    for item in orphan_objects:
        issue_counts.update(str(issue) for issue in item["issues"])
    healthy_objects = [item for item in object_results if item["status"] == "healthy"]
    mismatched_keys = {
        str(item["storageKey"])
        for item in object_results
        if any(issue in {"size_mismatch", "sha256_mismatch"} for issue in item["issues"])
    }
    healthy = (
        not root_issues
        and not any(item["status"] != "healthy" for item in object_results)
        and not orphan_objects
    )

    return {
        "schemaVersion": "msrp_evidence_integrity_v1",
        "checkedAtUtc": checked_at_utc.astimezone(timezone.utc).isoformat(),
        "evidenceRoot": str(root),
        "evidenceRootExists": root.exists(),
        "status": "healthy" if healthy else "unhealthy",
        "summary": {
            "databaseAssetRowCount": len(row_list),
            "replayableAssetRowCount": replayable_row_count,
            "supportingObjectAssetRowCount": supporting_object_row_count,
            "ignoredNonReplayableRowCount": len(ignored_assets),
            "expectedObjectCount": len(grouped_rows),
            "healthyObjectCount": len(healthy_objects),
            "verifiedObjectBytes": sum(
                int(item["actualSizeBytes"] or 0) for item in healthy_objects
            ),
            "missingObjectCount": (
                issue_counts["missing"] + issue_counts["missing_storage_key"]
            ),
            "mismatchedObjectCount": len(mismatched_keys),
            "unreadableObjectCount": issue_counts["unreadable"],
            "notRegularFileCount": issue_counts["not_regular_file"],
            "invalidPathCount": (
                issue_counts["invalid_storage_key"] + issue_counts["path_escape"]
            ),
            "invalidMetadataCount": (
                issue_counts["invalid_sha256_metadata"]
                + issue_counts["invalid_size_metadata"]
                + issue_counts["content_address_mismatch"]
            ),
            "invalidContentAddressCount": issue_counts["content_address_mismatch"],
            "orphanObjectCount": len(orphan_objects),
        },
        "rootIssues": root_issues,
        "objects": object_results,
        "ignoredAssets": ignored_assets,
        "orphans": orphan_objects,
    }
