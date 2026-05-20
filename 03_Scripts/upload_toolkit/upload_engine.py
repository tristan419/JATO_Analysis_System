"""Generic chunked upload engine — session management, SHA-256 verification, resume."""

import shutil
import uuid
from pathlib import Path
from typing import Any

from upload_toolkit.file_utils import (
    normalize_sha256,
    read_json,
    sha256_hex_for_bytes,
    sha256_hex_for_path,
    write_json,
)

# ── helpers ──


def _chunk_count(size_bytes: int, chunk_size: int) -> int:
    return max((size_bytes + chunk_size - 1) // chunk_size, 1)


# ── session management ──


def create_upload_session(
    session_root: Path,
    *,
    filename: str,
    size_bytes: int,
    chunk_size: int,
    resume_key: str | None = None,
    triggered_by: str = "anonymous",
) -> dict[str, Any]:
    """Create a new chunked upload session with resume support."""
    upload_id = f"upload-{uuid.uuid4().hex[:12]}"
    total_chunks = _chunk_count(size_bytes, chunk_size)

    session_dir = _session_dir(session_root, upload_id)
    session_dir.mkdir(parents=True, exist_ok=True)
    chunk_dir = _chunk_dir(session_root, upload_id)
    chunk_dir.mkdir(parents=True, exist_ok=True)

    state: dict[str, Any] = {
        "uploadId": upload_id,
        "filename": filename,
        "sizeBytes": size_bytes,
        "chunkSize": chunk_size,
        "totalChunks": total_chunks,
        "receivedChunks": [],
        "chunkDigests": {},
        "uploadedBytes": 0,
        "status": "pending",
        "resumeKey": resume_key,
        "fileSha256": None,
        "triggeredBy": triggered_by,
        "createdAt": _now(),
        "updatedAt": _now(),
    }
    _persist(session_root, upload_id, state)
    return state


def get_upload_session(
    session_root: Path,
    upload_id: str,
) -> dict[str, Any]:
    """Get current upload session state."""
    return _load(session_root, upload_id)


def find_resumable_session(
    session_root: Path,
    *,
    resume_key: str,
    filename: str,
    size_bytes: int,
) -> dict[str, Any] | None:
    """Find a resumable upload session matching the given resume key."""
    if not resume_key:
        return None
    for state_path in session_root.glob(f"*/upload_state.json"):
        try:
            payload = read_json(state_path)
        except Exception:
            continue
        if str(payload.get("resumeKey", "")) != resume_key:
            continue
        if str(payload.get("filename", "")) != filename:
            continue
        if int(payload.get("sizeBytes", 0) or 0) != size_bytes:
            continue
        if str(payload.get("status", "")) not in {"pending", "uploading", "completed"}:
            continue
        return payload
    return None


def receive_chunk(
    session_root: Path,
    upload_id: str,
    part_number: int,
    content: bytes,
    chunk_sha256: str | None = None,
    *,
    expected_chunk_size: int | None = None,
) -> dict[str, Any]:
    """Receive and store a single upload chunk with optional verification."""
    state = _load(session_root, upload_id)
    if str(state.get("status", "")) in {"completed", "assembling"}:
        raise RuntimeError("Upload session already completed.")

    part_number = int(part_number)
    if part_number < 1 or part_number > int(state.get("totalChunks", 0)):
        raise RuntimeError(f"Invalid part number: {part_number}")

    actual_sha256 = sha256_hex_for_bytes(content)

    if chunk_sha256:
        expected = normalize_sha256(chunk_sha256, detail=f"X-Chunk-SHA256 header invalid for part {part_number}")
        if actual_sha256 != expected:
            raise RuntimeError(f"Part {part_number} SHA-256 mismatch.")

    if expected_chunk_size is not None and len(content) > expected_chunk_size:
        raise RuntimeError(f"Part {part_number} exceeds chunk size limit.")

    chunk_dir = _chunk_dir(session_root, upload_id)
    chunk_dir.mkdir(parents=True, exist_ok=True)
    chunk_path = chunk_dir / str(part_number)
    chunk_path.write_bytes(content)

    received: list[int] = list(state.get("receivedChunks", []))
    digests: dict[str, str] = dict(state.get("chunkDigests", {}))
    if part_number not in received:
        received.append(part_number)
        state["uploadedBytes"] = int(state.get("uploadedBytes", 0) or 0) + len(content)
    digests[str(part_number)] = actual_sha256

    state["receivedChunks"] = sorted(received)
    state["chunkDigests"] = digests
    state["status"] = "uploading"
    _persist(session_root, upload_id, state)
    return state


def complete_upload_session(
    session_root: Path,
    upload_id: str,
    *,
    assembled_filename: str | None = None,
) -> dict[str, Any]:
    """Assemble received chunks into the final file and verify SHA-256."""
    state = _load(session_root, upload_id)
    if str(state.get("status", "")) == "completed":
        return state

    received = sorted(state.get("receivedChunks", []))
    total = int(state.get("totalChunks", 0))
    if len(received) != total:
        missing = sorted(set(range(1, total + 1)) - set(received))
        raise RuntimeError(f"Missing chunks: {missing}")

    state["status"] = "assembling"
    _persist(session_root, upload_id, state)

    chunk_dir = _chunk_dir(session_root, upload_id)
    assembled_dir = _assembled_dir(session_root, upload_id)
    assembled_dir.mkdir(parents=True, exist_ok=True)
    assembled_path = assembled_dir / (assembled_filename or state.get("filename", "upload.bin"))

    with assembled_path.open("wb") as out:
        for part in received:
            chunk_path = chunk_dir / str(part)
            if not chunk_path.exists():
                raise RuntimeError(f"Chunk file missing: {part}")
            out.write(chunk_path.read_bytes())

    file_sha256 = sha256_hex_for_path(assembled_path)
    state["fileSha256"] = file_sha256
    state["status"] = "completed"
    _persist(session_root, upload_id, state)
    return state


def cleanup_upload_session(session_root: Path, upload_id: str) -> None:
    """Remove all files for an upload session."""
    path = session_root / upload_id
    if path.exists():
        shutil.rmtree(path)


# ── internal paths ──


def _session_dir(root: Path, upload_id: str) -> Path:
    return root / upload_id


def _chunk_dir(root: Path, upload_id: str) -> Path:
    return _session_dir(root, upload_id) / "chunks"


def _assembled_dir(root: Path, upload_id: str) -> Path:
    return _session_dir(root, upload_id) / "assembled"


def _state_path(root: Path, upload_id: str) -> Path:
    return _session_dir(root, upload_id) / "upload_state.json"


def _now() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


def _persist(root: Path, upload_id: str, state: dict[str, Any]) -> None:
    state["updatedAt"] = _now()
    write_json(_state_path(root, upload_id), state)


def _load(root: Path, upload_id: str) -> dict[str, Any]:
    path = _state_path(root, upload_id)
    if not path.exists():
        raise RuntimeError(f"Upload session not found: {upload_id}")
    return read_json(path)
