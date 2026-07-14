#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
    return parser.parse_args(argv)


def _load_evidence_assets() -> list[object]:
    session = get_session_factory()()
    try:
        session.execute(text("SET TRANSACTION READ ONLY"))
        return list(repo.list_all_evidence_assets(session))
    finally:
        session.rollback()
        session.close()


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
