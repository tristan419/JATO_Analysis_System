#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from sqlalchemy import text  # noqa: E402

from app.core.config import (  # noqa: E402
    resolve_msrp_governance_evidence_root,
)
from app.db.session import get_session_factory  # noqa: E402
from app.infra import msrp_source_governance_repository as repo  # noqa: E402
from app.services.msrp_evidence_integrity_service import (  # noqa: E402
    audit_msrp_evidence_integrity,
)


EVIDENCE_TABLE_REGCLASS = "msrp.source_evidence_assets"


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only DB-to-object integrity audit for MSRP evidence assets."
    )
    parser.add_argument(
        "--evidence-root",
        type=Path,
        help="Override the configured MSRP evidence root for this audit.",
    )
    parser.add_argument("--output", type=Path, help="Optional JSON report path.")
    parser.add_argument(
        "--object-list-output",
        type=Path,
        help="Optional newline-delimited list of verified storage keys.",
    )
    parser.add_argument(
        "--allow-uninitialized",
        action="store_true",
        help=(
            "Allow an empty pre-0044 database to emit a not_initialized report. "
            "Any pre-existing evidence object still fails closed."
        ),
    )
    return parser.parse_args(argv)


def _load_evidence_assets() -> list[dict[str, object]] | None:
    session = get_session_factory()()
    try:
        session.execute(text("SET TRANSACTION READ ONLY"))
        table_name = session.execute(
            text("SELECT to_regclass(:qualified_name)"),
            {"qualified_name": EVIDENCE_TABLE_REGCLASS},
        ).scalar_one_or_none()
        if table_name is None:
            return None
        return [
            {
                "evidence_asset_id": str(row.evidence_asset_id),
                "evidence_type": row.evidence_type,
                "storage_key": row.storage_key,
                "size_bytes": row.size_bytes,
                "sha256": row.sha256,
            }
            for row in repo.list_all_evidence_assets(session)
        ]
    finally:
        session.rollback()
        session.close()


def _uninitialized_report(evidence_root: Path) -> tuple[dict[str, object], int]:
    root = evidence_root.expanduser().resolve()
    assets_root = root / "assets"
    if assets_root.is_symlink() or (
        assets_root.exists() and not assets_root.is_dir()
    ):
        object_paths = ["assets"]
    elif assets_root.exists():
        object_paths = sorted(
            path.relative_to(root).as_posix()
            for path in assets_root.rglob("*")
            if path.is_file() or path.is_symlink()
        )
    else:
        object_paths = []
    has_untracked_objects = bool(object_paths)
    report = {
        "schemaVersion": "msrp_evidence_integrity_v1",
        "checkedAtUtc": datetime.now(timezone.utc).isoformat(),
        "evidenceRoot": str(root),
        "evidenceRootExists": root.exists(),
        "status": "unhealthy" if has_untracked_objects else "not_initialized",
        "summary": {
            "databaseAssetRowCount": 0,
            "replayableAssetRowCount": 0,
            "supportingObjectAssetRowCount": 0,
            "ignoredNonReplayableRowCount": 0,
            "expectedObjectCount": 0,
            "healthyObjectCount": 0,
            "verifiedObjectBytes": 0,
            "missingObjectCount": 0,
            "mismatchedObjectCount": 0,
            "unreadableObjectCount": 0,
            "notRegularFileCount": 0,
            "invalidPathCount": 0,
            "invalidMetadataCount": 0,
            "invalidContentAddressCount": 0,
            "orphanObjectCount": len(object_paths),
        },
        "rootIssues": (
            ["governance_table_missing_with_untracked_objects"]
            if has_untracked_objects
            else ["governance_evidence_table_not_initialized"]
        ),
        "objects": [],
        "ignoredAssets": [],
        "orphans": [
            {"storageKey": path, "issues": ["orphan"]} for path in object_paths
        ],
    }
    return report, 1 if has_untracked_objects else 0


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.tmp")
    temporary_path.write_text(content, encoding="utf-8")
    temporary_path.replace(path)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    evidence_root = resolve_msrp_governance_evidence_root(args.evidence_root)
    try:
        rows = _load_evidence_assets()
        if rows is None:
            if not args.allow_uninitialized:
                raise RuntimeError(
                    f"required governance table is missing: {EVIDENCE_TABLE_REGCLASS}"
                )
            report, exit_code = _uninitialized_report(evidence_root)
        else:
            report = audit_msrp_evidence_integrity(rows, evidence_root)
            exit_code = 0 if report["status"] == "healthy" else 1
    except Exception as exc:  # operational failure is still structured output
        report = {
            "schemaVersion": "msrp_evidence_integrity_v1",
            "evidenceRoot": str(evidence_root),
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
        exit_code = 2

    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        _write_text(args.output, encoded)
    if args.object_list_output:
        storage_keys = [
            str(item["storageKey"])
            for item in report.get("objects", [])
            if item.get("status") == "healthy" and item.get("storageKey")
        ]
        _write_text(
            args.object_list_output,
            "".join(f"{storage_key}\n" for storage_key in storage_keys),
        )
    sys.stdout.write(encoded)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
