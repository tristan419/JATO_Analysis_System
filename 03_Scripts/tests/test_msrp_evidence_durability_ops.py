from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from types import SimpleNamespace


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = PROJECT_ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.msrp_evidence_integrity_service import (  # noqa: E402
    audit_msrp_evidence_integrity,
)


BACKUP_SCRIPT = PROJECT_ROOT / "03_Scripts" / "ops" / "backup_production_data.sh"
INTEGRITY_SCRIPT = PROJECT_ROOT / "03_Scripts" / "ops" / "msrp_evidence_integrity.py"
RELEASE_PATHS_LIB = PROJECT_ROOT / "03_Scripts" / "deploy" / "lib" / "release_paths.sh"


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _backup_fixture(tmp_path: Path) -> tuple[dict[str, object], Path, dict[str, object]]:
    fixture_repo = tmp_path / "repo"
    evidence_root = (
        fixture_repo / "04_Processed_data" / "ops" / "msrp_source_evidence"
    )
    content = b"%PDF-1.4\nfixture replayable evidence\n%%EOF\n"
    digest = hashlib.sha256(content).hexdigest()
    storage_key = f"assets/{digest[:2]}/{digest}.pdf"
    object_path = evidence_root / storage_key
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(content)
    row = {
        "evidence_asset_id": "11111111-1111-1111-1111-111111111111",
        "evidence_type": "uploaded_pdf",
        "storage_key": storage_key,
        "size_bytes": len(content),
        "sha256": digest,
    }

    env_file = tmp_path / "backend.env"
    env_file.write_text(
        "\n".join(
            (
                "APP_DATABASE_ENABLED=true",
                "APP_DATABASE_URL=postgresql+psycopg://fixture.invalid/jato",
                f"APP_PROJECT_ROOT={fixture_repo}",
                f"MSRP_GOVERNANCE_EVIDENCE_ROOT={evidence_root}",
                "",
            )
        ),
        encoding="utf-8",
    )

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    pg_args = tmp_path / "pg-dump-args.txt"
    fixture_dump_payload = tmp_path / "fixture-dump.json"
    fixture_dump_payload.write_text(json.dumps({"msrpEvidenceAsset": row}))
    _write_executable(
        fake_bin / "pg_dump",
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "$PG_DUMP_ARGS_FILE"
out=''
while [[ "$#" -gt 0 ]]; do
  if [[ "$1" == '-f' ]]; then
    out="$2"
    shift 2
  else
    shift
  fi
done
cp "$FIXTURE_DUMP_PAYLOAD" "$out"
""",
    )

    integrity_generator = tmp_path / "fixture_integrity.py"
    integrity_generator.write_text(
        """import argparse
import hashlib
import json
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--evidence-root', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--object-list-output', type=Path, required=True)
args = parser.parse_args()
row = json.loads(os.environ['FIXTURE_EVIDENCE_ROW'])
path = args.evidence_root / row['storage_key']
actual = hashlib.sha256(path.read_bytes()).hexdigest()
healthy = path.is_file() and path.stat().st_size == row['size_bytes'] and actual == row['sha256']
report = {
    'schemaVersion': 'msrp_evidence_integrity_v1',
    'evidenceRoot': str(args.evidence_root.resolve()),
    'status': 'healthy' if healthy else 'unhealthy',
    'summary': {
        'databaseAssetRowCount': 1,
        'replayableAssetRowCount': 1,
        'ignoredNonReplayableRowCount': 0,
        'expectedObjectCount': 1,
        'healthyObjectCount': 1 if healthy else 0,
        'verifiedObjectBytes': row['size_bytes'] if healthy else 0,
        'missingObjectCount': 0 if path.exists() else 1,
        'mismatchedObjectCount': 0 if healthy or not path.exists() else 1,
        'unreadableObjectCount': 0,
        'notRegularFileCount': 0,
        'invalidPathCount': 0,
        'invalidMetadataCount': 0,
        'invalidContentAddressCount': 0,
        'orphanObjectCount': 0,
    },
    'rootIssues': [],
    'objects': [{
        'storageKey': row['storage_key'],
        'evidenceAssetIds': [row['evidence_asset_id']],
        'expectedSizeBytes': row['size_bytes'],
        'actualSizeBytes': path.stat().st_size if path.exists() else None,
        'expectedSha256': row['sha256'],
        'actualSha256': actual if path.exists() else None,
        'status': 'healthy' if healthy else 'unhealthy',
        'issues': [] if healthy else ['mismatch'],
    }],
    'ignoredAssets': [],
    'orphans': [],
}
encoded = json.dumps(report, indent=2, sort_keys=True) + '\\n'
args.output.write_text(encoded)
args.object_list_output.write_text(row['storage_key'] + '\\n' if healthy else '')
print(encoded, end='')
raise SystemExit(0 if healthy else 1)
""",
        encoding="utf-8",
    )
    python_wrapper = tmp_path / "fixture-python"
    _write_executable(
        python_wrapper,
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "$MSRP_EVIDENCE_INTEGRITY_SCRIPT" ]]; then
  shift
  exec "$REAL_PYTHON" "$FIXTURE_INTEGRITY_GENERATOR" "$@"
fi
exec "$REAL_PYTHON" "$@"
""",
    )

    backup_root = tmp_path / "backups"
    env = os.environ.copy()
    env.update(
        {
            "REPO_DIR": str(fixture_repo),
            "BACKEND_ENV_FILE": str(env_file),
            "BACKUP_ROOT": str(backup_root),
            "BACKUP_TIMESTAMP": "20260714-120000",
            "PYTHON_BIN": str(python_wrapper),
            "MSRP_EVIDENCE_INTEGRITY_SCRIPT": str(INTEGRITY_SCRIPT),
            "MSRP_RELEASE_PATHS_LIB": str(RELEASE_PATHS_LIB),
            "PG_DUMP_ARGS_FILE": str(pg_args),
            "FIXTURE_DUMP_PAYLOAD": str(fixture_dump_payload),
            "FIXTURE_EVIDENCE_ROW": json.dumps(row),
            "FIXTURE_INTEGRITY_GENERATOR": str(integrity_generator),
            "REAL_PYTHON": sys.executable,
            "PATH": f"{fake_bin}{os.pathsep}{env['PATH']}",
        }
    )
    result = subprocess.run(
        ["bash", str(BACKUP_SCRIPT)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    manifest_path = backup_root / "manifests" / "backup-20260714-120000.json"
    return json.loads(manifest_path.read_text()), pg_args, row


def test_backup_manifest_and_restore_drill_keep_database_and_objects_consistent(
    tmp_path: Path,
) -> None:
    manifest, pg_args_path, source_row = _backup_fixture(tmp_path)
    pg_args = pg_args_path.read_text().splitlines()

    assert manifest["backupTimestamp"] == "20260714-120000"
    assert manifest["database"]["schemas"] == ["auth", "ordering", "public", "msrp"]
    assert "--schema=msrp" in pg_args
    assert manifest["evidenceObjects"]["objectCount"] == 1
    assert manifest["evidenceObjects"]["totalObjectBytes"] == source_row["size_bytes"]

    for manifest_key in ("database", "evidenceObjects", "integrityReport"):
        artifact = manifest[manifest_key]
        artifact_path = Path(artifact["artifactPath"])
        assert artifact_path.is_file()
        assert artifact["sha256"] == _sha256(artifact_path)
        assert artifact["sizeBytes"] == artifact_path.stat().st_size

    # The fixture dump reconstructs the Evidence Asset row; the archive restores
    # the matching immutable object before backend writes are re-enabled.
    restored_dump = json.loads(Path(manifest["database"]["artifactPath"]).read_text())
    restored_row = restored_dump["msrpEvidenceAsset"]
    assert restored_row == source_row
    restore_staging = tmp_path / "restore-staging"
    restore_staging.mkdir()
    with tarfile.open(manifest["evidenceObjects"]["artifactPath"], "r:gz") as archive:
        archive.extractall(restore_staging, filter="data")
    restored_object = restore_staging / restored_row["storage_key"]
    assert restored_object.is_file()
    assert restored_object.stat().st_size == restored_row["size_bytes"]
    assert _sha256(restored_object) == restored_row["sha256"]

    restored_report = audit_msrp_evidence_integrity(
        [SimpleNamespace(**restored_row)],
        restore_staging,
    )
    assert restored_report["status"] == "healthy", restored_report
    assert restored_report["summary"]["healthyObjectCount"] == 1


def test_release_guard_rejects_evidence_root_inside_replaced_tree(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    unsafe_root = repo_root / "06_AppPlatform" / "backend" / "artifacts" / "evidence"
    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; assert_path_outside_release_roots "$2" "$3" 06_AppPlatform',
            "_",
            str(RELEASE_PATHS_LIB),
            str(repo_root),
            str(unsafe_root),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "inside release replacement root" in result.stderr
