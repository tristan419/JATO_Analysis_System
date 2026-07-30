from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tarfile
import subprocess
import stat

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOY_DIR = REPO_ROOT / "03_Scripts/deploy"
sys.path.insert(0, str(DEPLOY_DIR))

from validate_release_archive import (  # noqa: E402
    ArchiveValidationError,
    assert_materialization_headroom,
    evaluate_materialization_headroom,
    inspect_root_sealed_bundle,
    validate_archive,
)
import validate_release_archive as validator_module  # noqa: E402


VALIDATOR = DEPLOY_DIR / "validate_release_archive.py"
VALIDATOR_RELATIVE = "03_Scripts/deploy/validate_release_archive.py"
WORKBOOK = "01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx"
DIAGNOSTICS_DIR = (
    "03_Scripts/diagnostics/artifacts/msrp_backfill/"
    "sweden_swiss_top30_suv"
)
DIAGNOSTICS_FILE = (
    f"{DIAGNOSTICS_DIR}/top30_suv_price_movement_candidates.json"
)
OFFICIAL_EVIDENCE = f"{DIAGNOSTICS_DIR}/official_evidence_leads.json"


def _member(
    name: str,
    *,
    mode: int,
    payload: bytes | None = None,
    uid: int = 0,
    gid: int = 0,
) -> tuple[tarfile.TarInfo, bytes | None]:
    info = tarfile.TarInfo(name)
    info.mode = mode
    info.uid = uid
    info.gid = gid
    info.mtime = 1_700_000_000
    if payload is None:
        info.type = tarfile.DIRTYPE
        info.size = 0
    else:
        info.type = tarfile.REGTYPE
        info.size = len(payload)
    return info, payload


def _valid_members() -> list[tuple[tarfile.TarInfo, bytes | None]]:
    return [
        _member(".", mode=0o755),
        _member("public", mode=0o755),
        _member("public/app.py", mode=0o644, payload=b"print('ok')\n"),
        _member("01_RAW_DATA", mode=0o711),
        _member(WORKBOOK, mode=0o600, payload=b"private workbook"),
        _member("03_Scripts", mode=0o755),
        _member("03_Scripts/deploy", mode=0o755),
        _member("03_Scripts/diagnostics", mode=0o755),
        _member("03_Scripts/diagnostics/artifacts", mode=0o711),
        _member(
            "03_Scripts/diagnostics/artifacts/msrp_backfill",
            mode=0o711,
        ),
        _member(DIAGNOSTICS_DIR, mode=0o711),
        _member(DIAGNOSTICS_FILE, mode=0o600, payload=b"{}"),
        _member(OFFICIAL_EVIDENCE, mode=0o600, payload=b'{"leads": []}'),
        _member(
            VALIDATOR_RELATIVE,
            mode=0o644,
            payload=VALIDATOR.read_bytes(),
        ),
    ]


def _write_archive(
    path: Path,
    members: list[tuple[tarfile.TarInfo, bytes | None]],
    *,
    tar_format: int = tarfile.GNU_FORMAT,
) -> tuple[str, int]:
    with tarfile.open(path, "w:gz", format=tar_format) as archive:
        for info, payload in members:
            archive.addfile(
                info,
                io.BytesIO(payload) if payload is not None else None,
            )
    raw = path.read_bytes()
    return hashlib.sha256(raw).hexdigest(), len(raw)


def _validate(path: Path) -> dict[str, object]:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return validate_archive(
        path.resolve(),
        expected_sha256=digest,
        expected_bytes=path.stat().st_size,
        trusted_controls={VALIDATOR_RELATIVE: VALIDATOR},
    )


def test_valid_archive_records_modes_identity_and_control_provenance(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "release.tar.gz"
    digest, byte_count = _write_archive(archive, _valid_members())

    receipt = validate_archive(
        archive.resolve(),
        expected_sha256=digest,
        expected_bytes=byte_count,
        trusted_controls={VALIDATOR_RELATIVE: VALIDATOR},
    )

    assert receipt["schemaVersion"] == 2
    assert receipt["archiveSha256"] == digest
    assert receipt["archiveBytes"] == byte_count
    assert receipt["privateModeEvidence"] == {
        "requiredWorkbook": {
            "path": WORKBOOK,
            "type": "file",
            "mode": "0600",
        },
        "diagnosticsArtifacts": {
            "prefix": "03_Scripts/diagnostics/artifacts/",
            "fileModes": ["0600"],
            "directoryModes": ["0711"],
        },
    }
    assert receipt["trustedControls"] == {
        VALIDATOR_RELATIVE: hashlib.sha256(VALIDATOR.read_bytes()).hexdigest(),
    }
    private_entries = receipt["privateEntries"]
    assert isinstance(private_entries, dict)
    private_files = private_entries["files"]
    assert {
        entry["path"]: {
            key: value
            for key, value in entry.items()
            if key != "path"
        }
        for entry in private_files
    } == {
        WORKBOOK: {
            "mode": "0600",
            "sha256": hashlib.sha256(b"private workbook").hexdigest(),
            "bytes": len(b"private workbook"),
        },
        DIAGNOSTICS_FILE: {
            "mode": "0600",
            "sha256": hashlib.sha256(b"{}").hexdigest(),
            "bytes": 2,
        },
        OFFICIAL_EVIDENCE: {
            "mode": "0600",
            "sha256": hashlib.sha256(b'{"leads": []}').hexdigest(),
            "bytes": len(b'{"leads": []}'),
        },
    }
    private_directories = private_entries["directories"]
    assert {
        (entry["path"], entry["mode"])
        for entry in private_directories
    } == {
        ("01_RAW_DATA", "0711"),
        ("03_Scripts/diagnostics/artifacts", "0711"),
        (
            "03_Scripts/diagnostics/artifacts/msrp_backfill",
            "0711",
        ),
        (DIAGNOSTICS_DIR, "0711"),
    }
    member_classes = receipt["memberClasses"]
    assert member_classes["privateFiles"] == len(private_files)
    assert member_classes["privateDirectories"] == len(private_directories)


@pytest.mark.parametrize(
    ("member_name", "mode"),
    (
        ("public/app.py", 0o600),
        (WORKBOOK, 0o644),
        (DIAGNOSTICS_DIR, 0o755),
        (DIAGNOSTICS_FILE, 0o644),
        (VALIDATOR_RELATIVE, 0o4755),
    ),
)
def test_rejects_wrong_public_private_or_special_modes(
    tmp_path: Path,
    member_name: str,
    mode: int,
) -> None:
    members = _valid_members()
    next(
        member
        for member, _payload in members
        if member.name == member_name
    ).mode = mode
    archive = tmp_path / "release.tar.gz"
    _write_archive(archive, members)

    with pytest.raises(ArchiveValidationError, match="mode|coverage"):
        _validate(archive)


def test_rejects_non_root_member_ownership(tmp_path: Path) -> None:
    members = _valid_members()
    members[2][0].uid = 1000
    archive = tmp_path / "release.tar.gz"
    _write_archive(archive, members)

    with pytest.raises(ArchiveValidationError, match="ownership"):
        _validate(archive)


@pytest.mark.parametrize("order", ("parent-first", "child-first"))
def test_rejects_file_parent_conflicts(tmp_path: Path, order: str) -> None:
    conflict = [
        _member("conflict", mode=0o644, payload=b"file"),
        _member("conflict/child", mode=0o644, payload=b"child"),
    ]
    if order == "child-first":
        conflict.reverse()
    archive = tmp_path / "release.tar.gz"
    _write_archive(archive, _valid_members() + conflict)

    with pytest.raises(ArchiveValidationError, match="explicit directory parent"):
        _validate(archive)


def test_rejects_duplicate_and_missing_root(tmp_path: Path) -> None:
    duplicate = tmp_path / "duplicate.tar.gz"
    members = _valid_members()
    members.append(_member("public/app.py", mode=0o644, payload=b"again"))
    _write_archive(duplicate, members)
    with pytest.raises(ArchiveValidationError, match="duplicate"):
        _validate(duplicate)

    missing = tmp_path / "missing-root.tar.gz"
    _write_archive(missing, _valid_members()[1:])
    with pytest.raises(ArchiveValidationError, match="lacks its normalized root"):
        _validate(missing)


def test_rejects_pax_headers_and_links(tmp_path: Path) -> None:
    pax_members = _valid_members()
    pax_members[2][0].pax_headers = {"comment": "unsupported"}
    pax_archive = tmp_path / "pax.tar.gz"
    _write_archive(pax_archive, pax_members, tar_format=tarfile.PAX_FORMAT)
    with pytest.raises(ArchiveValidationError, match="PAX|extension"):
        _validate(pax_archive)

    link_members = _valid_members()
    link = tarfile.TarInfo("public/link")
    link.type = tarfile.SYMTYPE
    link.linkname = "app.py"
    link.mode = 0o777
    link_members.append((link, None))
    link_archive = tmp_path / "link.tar.gz"
    _write_archive(link_archive, link_members)
    with pytest.raises(ArchiveValidationError, match="unsupported"):
        _validate(link_archive)


@pytest.mark.parametrize(
    ("parent_name", "replacement_mode", "replacement_uid"),
    (
        ("01_RAW_DATA", None, None),
        ("03_Scripts", None, None),
        ("03_Scripts/diagnostics", None, None),
        ("03_Scripts/diagnostics", 0o700, None),
        ("03_Scripts/diagnostics/artifacts", None, None),
        ("03_Scripts/diagnostics/artifacts", 0o700, None),
        ("03_Scripts/diagnostics/artifacts", None, 1000),
    ),
)
def test_requires_explicit_parent_directories_with_exact_mode_and_owner(
    tmp_path: Path,
    parent_name: str,
    replacement_mode: int | None,
    replacement_uid: int | None,
) -> None:
    members = _valid_members()
    parent_index = next(
        index
        for index, (member, _payload) in enumerate(members)
        if member.name == parent_name
    )
    if replacement_mode is None and replacement_uid is None:
        members.pop(parent_index)
    else:
        parent = members[parent_index][0]
        if replacement_mode is not None:
            parent.mode = replacement_mode
        if replacement_uid is not None:
            parent.uid = replacement_uid
    archive = tmp_path / "parent-chain.tar.gz"
    _write_archive(archive, members)
    with pytest.raises(
        ArchiveValidationError,
        match="explicit directory parent|mode|ownership",
    ):
        _validate(archive)


@pytest.mark.parametrize("required_file", (OFFICIAL_EVIDENCE, DIAGNOSTICS_FILE))
def test_rejects_missing_required_diagnostics_evidence(
    tmp_path: Path,
    required_file: str,
) -> None:
    members = [
        item
        for item in _valid_members()
        if item[0].name != required_file
    ]
    archive = tmp_path / "missing-evidence.tar.gz"
    _write_archive(archive, members)
    with pytest.raises(ArchiveValidationError, match="required diagnostics"):
        _validate(archive)


@pytest.mark.parametrize("unsafe_name", ("/absolute", "../escaped"))
def test_rejects_absolute_and_parent_traversal(
    tmp_path: Path,
    unsafe_name: str,
) -> None:
    archive = tmp_path / "unsafe-path.tar.gz"
    _write_archive(
        archive,
        _valid_members()
        + [_member(unsafe_name, mode=0o644, payload=b"unsafe")],
    )
    with pytest.raises(ArchiveValidationError, match="unsafe.*path"):
        _validate(archive)


@pytest.mark.parametrize(
    ("root_mode", "root_uid", "duplicate"),
    (
        (0o700, 0, False),
        (0o755, 1000, False),
        (0o755, 0, True),
    ),
)
def test_rejects_wrong_or_duplicate_root(
    tmp_path: Path,
    root_mode: int,
    root_uid: int,
    duplicate: bool,
) -> None:
    members = _valid_members()
    members[0][0].mode = root_mode
    members[0][0].uid = root_uid
    if duplicate:
        members.insert(1, _member("./", mode=0o755))
    archive = tmp_path / "root.tar.gz"
    _write_archive(archive, members)
    with pytest.raises(ArchiveValidationError, match="root"):
        _validate(archive)


@pytest.mark.parametrize(
    "entry_type",
    (
        tarfile.LNKTYPE,
        tarfile.FIFOTYPE,
        tarfile.CHRTYPE,
        tarfile.BLKTYPE,
    ),
)
def test_rejects_hardlink_fifo_and_devices(
    tmp_path: Path,
    entry_type: bytes,
) -> None:
    info = tarfile.TarInfo("public/unsupported")
    info.type = entry_type
    info.mode = 0o644
    if entry_type == tarfile.LNKTYPE:
        info.linkname = "public/app.py"
    archive = tmp_path / "unsupported.tar.gz"
    _write_archive(archive, _valid_members() + [(info, None)])
    with pytest.raises(ArchiveValidationError, match="unsupported"):
        _validate(archive)


def test_rejects_missing_or_changed_trusted_control(tmp_path: Path) -> None:
    archive = tmp_path / "release.tar.gz"
    _write_archive(archive, _valid_members())
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    byte_count = archive.stat().st_size
    changed = tmp_path / "changed.py"
    changed.write_text("changed\n", encoding="utf-8")

    with pytest.raises(ArchiveValidationError, match="changed"):
        validate_archive(
            archive.resolve(),
            expected_sha256=digest,
            expected_bytes=byte_count,
            trusted_controls={VALIDATOR_RELATIVE: changed.resolve()},
        )
    with pytest.raises(ArchiveValidationError, match="missing"):
        validate_archive(
            archive.resolve(),
            expected_sha256=digest,
            expected_bytes=byte_count,
            trusted_controls={
                "03_Scripts/deploy/not-in-archive.py": VALIDATOR,
            },
        )


def test_rejects_archive_symlink_wrong_identity_and_corrupt_gzip(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "release.tar.gz"
    digest, byte_count = _write_archive(archive, _valid_members())
    symlink = tmp_path / "linked.tar.gz"
    symlink.symlink_to(archive)
    with pytest.raises(ArchiveValidationError, match="non-symlink"):
        validate_archive(
            symlink,
            expected_sha256=digest,
            expected_bytes=byte_count,
        )
    with pytest.raises(ArchiveValidationError, match="byte count"):
        validate_archive(
            archive.resolve(),
            expected_sha256=digest,
            expected_bytes=byte_count + 1,
        )
    with pytest.raises(ArchiveValidationError, match="SHA-256 mismatch"):
        validate_archive(
            archive.resolve(),
            expected_sha256="0" * 64,
            expected_bytes=byte_count,
        )
    corrupt = tmp_path / "corrupt.tar.gz"
    corrupt.write_bytes(b"not a gzip archive")
    with pytest.raises(ArchiveValidationError, match="parsed safely"):
        validate_archive(
            corrupt.resolve(),
            expected_sha256=hashlib.sha256(corrupt.read_bytes()).hexdigest(),
            expected_bytes=corrupt.stat().st_size,
        )


@pytest.mark.parametrize(
    ("limit_name", "limit_value", "message"),
    (
        ("MAX_MEMBERS", 2, "too many members"),
        ("MAX_MEMBER_BYTES", 4, "member size"),
        ("MAX_EXPANDED_BYTES", 8, "expands beyond"),
    ),
)
def test_bounded_archive_limits_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit_name: str,
    limit_value: int,
    message: str,
) -> None:
    archive = tmp_path / "bounded.tar.gz"
    _write_archive(archive, _valid_members())
    if limit_name == "MAX_EXPANDED_BYTES":
        monkeypatch.setattr(validator_module, "MAX_MEMBER_BYTES", 1024 * 1024)
    monkeypatch.setattr(validator_module, limit_name, limit_value)
    with pytest.raises(ArchiveValidationError, match=message):
        _validate(archive)


def test_materialization_headroom_fails_closed_on_low_disk(
    tmp_path: Path,
) -> None:
    receipt = {"expandedBytes": 256}
    required = assert_materialization_headroom(
        receipt,
        target=tmp_path,
        materialization_copies=2,
        reserve_bytes=1024,
        available_bytes=1536,
    )
    assert required == 1536
    with pytest.raises(ArchiveValidationError, match="lacks.*headroom"):
        assert_materialization_headroom(
            receipt,
            target=tmp_path,
            materialization_copies=2,
            reserve_bytes=1024,
            available_bytes=1535,
        )


def test_multi_target_headroom_deduplicates_one_filesystem(
    tmp_path: Path,
) -> None:
    worktree = tmp_path / "worktree"
    releases = tmp_path / "releases"
    worktree.mkdir()
    releases.mkdir()
    checks = evaluate_materialization_headroom(
        {"expandedBytes": 256},
        requests=[
            (worktree.resolve(), 1, 100),
            (releases.resolve(), 1, 100),
        ],
        device_probe=lambda _path: 7,
        available_probe=lambda _path: 1_000,
    )
    assert checks == [
        {
            "target": str(releases.resolve()),
            "targets": sorted(
                (str(worktree.resolve()), str(releases.resolve()))
            ),
            "device": 7,
            "availableBytes": 1_000,
            "requiredBytes": 612,
            "materializationCopies": 2,
            "reserveBytes": 100,
        }
    ]


def test_multi_target_headroom_records_each_unique_filesystem(
    tmp_path: Path,
) -> None:
    worktree = (tmp_path / "worktree").resolve()
    releases = (tmp_path / "releases").resolve()
    worktree.mkdir()
    releases.mkdir()
    devices = {worktree: 11, releases: 22}
    available = {worktree: 900, releases: 800}
    checks = evaluate_materialization_headroom(
        {"expandedBytes": 256},
        requests=[(worktree, 1, 100), (releases, 1, 100)],
        device_probe=devices.__getitem__,
        available_probe=available.__getitem__,
    )
    assert checks == [
        {
            "target": str(worktree),
            "targets": [str(worktree)],
            "device": 11,
            "availableBytes": 900,
            "requiredBytes": 356,
            "materializationCopies": 1,
            "reserveBytes": 100,
        },
        {
            "target": str(releases),
            "targets": [str(releases)],
            "device": 22,
            "availableBytes": 800,
            "requiredBytes": 356,
            "materializationCopies": 1,
            "reserveBytes": 100,
        },
    ]


def test_cli_records_attempt_scoped_headroom_receipt(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "release.tar.gz"
    digest, byte_count = _write_archive(archive, _valid_members())
    receipt = tmp_path / "receipt.json"
    result = subprocess.run(
        [
            sys.executable,
            str(VALIDATOR),
            "--archive",
            str(archive.resolve()),
            "--expected-sha256",
            digest,
            "--expected-bytes",
            str(byte_count),
            "--validation-run-id",
            "12345",
            "--validation-run-attempt",
            "2",
            "--headroom-target",
            str(tmp_path.resolve()),
            "1",
            "0",
            "--output",
            str(receipt),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    assert payload["validationAttempt"] == {
        "runId": "12345",
        "runAttempt": 2,
    }
    assert payload["headroomChecks"][0]["targets"] == [
        str(tmp_path.resolve())
    ]


def test_root_sealed_bundle_evidence_detects_parent_replacement(
    tmp_path: Path,
) -> None:
    anchor = tmp_path / "var-lib"
    sealed_root = anchor / "jato-sealed-inputs"
    run_dir = sealed_root / "inputs/commit/archive/1-1"
    run_dir.mkdir(parents=True)
    for path in (
        anchor,
        sealed_root,
        sealed_root / "inputs",
        sealed_root / "inputs/commit",
        sealed_root / "inputs/commit/archive",
        run_dir,
    ):
        path.chmod(0o750)
    archive = run_dir / "release.tar.gz"
    helper = run_dir / "validate_release_archive.py"
    archive.write_bytes(b"sealed archive")
    helper.write_bytes(b"trusted helper")
    archive.chmod(0o440)
    helper.chmod(0o550)
    archive_digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    helper_digest = hashlib.sha256(helper.read_bytes()).hexdigest()
    arguments = {
        "archive_path": archive,
        "helper_path": helper,
        "sealed_root": sealed_root,
        "expected_archive_sha256": archive_digest,
        "expected_archive_bytes": archive.stat().st_size,
        "expected_helper_sha256": helper_digest,
        "expected_group_id": os.getgid(),
        "anchor": anchor,
        "expected_owner_uid": os.getuid(),
    }
    before = inspect_root_sealed_bundle(**arguments)

    displaced = tmp_path / "displaced"
    sealed_root.rename(displaced)
    run_dir.mkdir(parents=True)
    for path in (
        sealed_root,
        sealed_root / "inputs",
        sealed_root / "inputs/commit",
        sealed_root / "inputs/commit/archive",
        run_dir,
    ):
        path.chmod(0o750)
    archive.write_bytes(b"sealed archive")
    helper.write_bytes(b"trusted helper")
    archive.chmod(0o440)
    helper.chmod(0o550)
    after = inspect_root_sealed_bundle(**arguments)

    assert before["directories"] != after["directories"]
    assert before["archive"]["inode"] != after["archive"]["inode"]


def test_rejects_reserved_runtime_member(tmp_path: Path) -> None:
    archive = tmp_path / "reserved.tar.gz"
    _write_archive(
        archive,
        _valid_members()
        + [
            _member(
                ".jato-canary-archive-validation.json",
                mode=0o644,
                payload=b"forged",
            )
        ],
    )
    with pytest.raises(ArchiveValidationError, match="reserved"):
        _validate(archive)


def test_gnu_tar_preserves_pristine_candidate_and_private_modes(
    tmp_path: Path,
) -> None:
    version = subprocess.run(
        ["tar", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    if "GNU tar" not in version.stdout:
        pytest.skip("GNU tar integration executes on Ubuntu CI and Tencent")
    archive = tmp_path / "release.tar.gz"
    _write_archive(archive, _valid_members())
    reference = tmp_path / "reference"
    candidate = tmp_path / "candidate"
    reference.mkdir(mode=0o700)
    candidate.mkdir(mode=0o711)
    result = subprocess.run(
        [
            "bash",
            "-c",
            (
                "set -Eeuo pipefail; umask 077; "
                'tar --same-permissions --no-overwrite-dir -xzf "$1" -C "$2"; '
                'tar --same-permissions --no-overwrite-dir -xzf "$1" -C "$3"'
            ),
            "bash",
            str(archive),
            str(reference),
            str(candidate),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert stat.S_IMODE(reference.stat().st_mode) == 0o700
    assert stat.S_IMODE(candidate.stat().st_mode) == 0o711
    for root in (reference, candidate):
        assert stat.S_IMODE((root / "public").stat().st_mode) == 0o755
        assert stat.S_IMODE((root / "public/app.py").stat().st_mode) == 0o644
        assert stat.S_IMODE((root / WORKBOOK).stat().st_mode) == 0o600
        assert stat.S_IMODE((root / DIAGNOSTICS_DIR).stat().st_mode) == 0o711
        assert stat.S_IMODE((root / DIAGNOSTICS_FILE).stat().st_mode) == 0o600
