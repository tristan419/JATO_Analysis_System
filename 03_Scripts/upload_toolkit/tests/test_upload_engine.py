"""Tests for upload_toolkit.upload_engine."""

import hashlib
import shutil
import tempfile
import time
from pathlib import Path

import pytest

from upload_toolkit.upload_engine import (
    _chunk_count,
    cleanup_expired_sessions,
    cleanup_upload_session,
    complete_upload_session,
    create_upload_session,
    find_resumable_session,
    get_upload_session,
    receive_chunk,
)


@pytest.fixture
def session_root() -> Path:
    root = Path(tempfile.mkdtemp())
    yield root
    shutil.rmtree(root, ignore_errors=True)


# ── helpers ──


def _chunk(data: bytes, size: int) -> list[bytes]:
    return [data[i : i + size] for i in range(0, len(data), size)]


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ── _chunk_count ──


class TestChunkCount:
    def test_exact_multiple(self) -> None:
        assert _chunk_count(16, 8) == 2

    def test_with_remainder(self) -> None:
        assert _chunk_count(10, 8) == 2

    def test_smaller_than_chunk(self) -> None:
        assert _chunk_count(4, 8) == 1

    def test_zero_size(self) -> None:
        assert _chunk_count(0, 8) == 1


# ── create / get / find ──


class TestSessionLifecycle:
    def test_create_session(self, session_root: Path) -> None:
        state = create_upload_session(
            session_root,
            filename="test.xlsx",
            size_bytes=100,
            chunk_size=32,
            triggered_by="test-user",
        )
        assert state["uploadId"].startswith("upload-")
        assert state["filename"] == "test.xlsx"
        assert state["totalChunks"] == 4  # ceil(100/32)
        assert state["status"] == "pending"
        assert state["triggeredBy"] == "test-user"

    def test_get_session(self, session_root: Path) -> None:
        created = create_upload_session(
            session_root, filename="f.xlsx", size_bytes=50, chunk_size=20
        )
        loaded = get_upload_session(session_root, created["uploadId"])
        assert loaded["uploadId"] == created["uploadId"]

    def test_get_session_not_found(self, session_root: Path) -> None:
        with pytest.raises(RuntimeError, match="Upload session not found"):
            get_upload_session(session_root, "nonexistent")

    def test_find_resumable_session(self, session_root: Path) -> None:
        create_upload_session(
            session_root,
            filename="test.xlsx",
            size_bytes=100,
            chunk_size=32,
            resume_key="abc123",
        )
        found = find_resumable_session(
            session_root, resume_key="abc123", filename="test.xlsx", size_bytes=100
        )
        assert found is not None
        assert found["resumeKey"] == "abc123"

    def test_find_resumable_session_no_match(self, session_root: Path) -> None:
        assert (
            find_resumable_session(
                session_root, resume_key="nope", filename="x.xlsx", size_bytes=10
            )
            is None
        )

    def test_find_resumable_session_empty_key(self, session_root: Path) -> None:
        assert (
            find_resumable_session(session_root, resume_key="", filename="x.xlsx", size_bytes=10)
            is None
        )


# ── upload / receive / complete ──


class TestChunkedUpload:
    def _make_session(self, root: Path, data: bytes, chunk_size: int) -> tuple[str, int]:
        state = create_upload_session(
            root, filename="data.bin", size_bytes=len(data), chunk_size=chunk_size
        )
        return state["uploadId"], state["totalChunks"]

    def test_upload_all_chunks(self, session_root: Path) -> None:
        data = b"hello world this is test data"
        upload_id, total = self._make_session(session_root, data, 8)
        assert total == len(_chunk(data, 8))

        progress_events: list[int] = []

        def on_progress(state: dict) -> None:
            progress_events.append(len(state["receivedChunks"]))

        chunks = _chunk(data, 8)
        for i, chunk in enumerate(chunks, 1):
            state = receive_chunk(
                session_root,
                upload_id,
                part_number=i,
                content=chunk,
                on_progress=on_progress,
            )
            assert state["status"] == "uploading"

        assert len(progress_events) == len(chunks)
        assert progress_events == list(range(1, len(chunks) + 1))

        final = complete_upload_session(session_root, upload_id)
        assert final["status"] == "completed"
        assert final["fileSha256"] == _sha256(data)

    def test_duplicate_chunk(self, session_root: Path) -> None:
        data = b"test data for dedup"
        upload_id, _ = self._make_session(session_root, data, 16)

        receive_chunk(session_root, upload_id, part_number=1, content=data)
        bytes_after_first = get_upload_session(session_root, upload_id)["uploadedBytes"]

        # Same chunk again — should not increment uploadedBytes
        receive_chunk(session_root, upload_id, part_number=1, content=data)
        state = get_upload_session(session_root, upload_id)
        assert state["uploadedBytes"] == bytes_after_first
        assert state["receivedChunks"] == [1]

    def test_missing_chunks(self, session_root: Path) -> None:
        data = b"data for missing chunk test"
        upload_id, total = self._make_session(session_root, data, 4)
        chunks = _chunk(data, 4)

        # Upload only half
        for i in range(1, total // 2 + 1):
            receive_chunk(session_root, upload_id, part_number=i, content=chunks[i - 1])

        with pytest.raises(RuntimeError, match="Missing chunks"):
            complete_upload_session(session_root, upload_id)

    def test_chunk_sha256_verification(self, session_root: Path) -> None:
        data = b"verify this chunk"
        upload_id, _ = self._make_session(session_root, data, 16)

        good_hash = _sha256(data)
        state = receive_chunk(
            session_root, upload_id, part_number=1, content=data, chunk_sha256=good_hash
        )
        assert state["status"] == "uploading"

        wrong_hash = _sha256(b"wrong data")
        with pytest.raises(RuntimeError, match="SHA-256 mismatch"):
            receive_chunk(
                session_root, upload_id, part_number=1, content=data, chunk_sha256=wrong_hash
            )

    def test_chunk_size_limit(self, session_root: Path) -> None:
        data = b"this chunk is too large"
        upload_id, _ = self._make_session(session_root, data, 8)

        with pytest.raises(RuntimeError, match="exceeds chunk size"):
            receive_chunk(
                session_root,
                upload_id,
                part_number=1,
                content=data,
                expected_chunk_size=4,
            )

    def test_invalid_part_number(self, session_root: Path) -> None:
        data = b"small"
        upload_id, _ = self._make_session(session_root, data, 8)

        with pytest.raises(RuntimeError, match="Invalid part number"):
            receive_chunk(session_root, upload_id, part_number=99, content=data)

    def test_complete_twice(self, session_root: Path) -> None:
        data = b"complete twice test"
        upload_id, total = self._make_session(session_root, data, 8)
        chunks = _chunk(data, 8)
        for i in range(1, total + 1):
            receive_chunk(session_root, upload_id, part_number=i, content=chunks[i - 1])

        first = complete_upload_session(session_root, upload_id)
        assert first["status"] == "completed"

        second = complete_upload_session(session_root, upload_id)
        assert second["status"] == "completed"


# ── cleanup ──


class TestCleanup:
    def test_cleanup_single_session(self, session_root: Path) -> None:
        state = create_upload_session(
            session_root, filename="todel.xlsx", size_bytes=10, chunk_size=10
        )
        cleanup_upload_session(session_root, state["uploadId"])
        with pytest.raises(RuntimeError, match="Upload session not found"):
            get_upload_session(session_root, state["uploadId"])

    def test_cleanup_expired(self, session_root: Path) -> None:
        # Create a session with a stale timestamp
        now = time.time()
        one_day_seconds = 86400

        stale_upload_id = "stale-session"
        stale_dir = session_root / stale_upload_id
        stale_dir.mkdir(parents=True, exist_ok=True)
        import json

        stale_state = {
            "uploadId": stale_upload_id,
            "filename": "old.xlsx",
            "sizeBytes": 10,
            "totalChunks": 1,
            "status": "abandoned",
            "createdAt": time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.gmtime(now - 100 * one_day_seconds)
            ),
            "updatedAt": time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.gmtime(now - 100 * one_day_seconds)
            ),
        }
        (stale_dir / "upload_state.json").write_text(
            json.dumps(stale_state), encoding="utf-8"
        )

        fresh_id = "fresh-session"
        fresh_dir = session_root / fresh_id
        fresh_dir.mkdir(parents=True, exist_ok=True)
        (fresh_dir / "upload_state.json").write_text(
            json.dumps(
                {
                    "uploadId": fresh_id,
                    "filename": "new.xlsx",
                    "sizeBytes": 10,
                    "totalChunks": 1,
                    "status": "completed",
                    "createdAt": time.strftime(
                        "%Y-%m-%dT%H:%M:%S", time.gmtime(now)
                    ),
                    "updatedAt": time.strftime(
                        "%Y-%m-%dT%H:%M:%S", time.gmtime(now)
                    ),
                }
            ),
            encoding="utf-8",
        )

        result = cleanup_expired_sessions(session_root, max_age_days=90)

        assert result["removedSessionCount"] == 1
        assert stale_upload_id in result["removedSessionIds"]
        assert fresh_dir.exists()  # fresh session kept
