"""Generic job engine — state machine, persistence, background runner."""

import json
import threading
import traceback
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from upload_toolkit.file_utils import read_json, write_json


def utc_now() -> datetime:
    return datetime.now(UTC)


def append_log(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(message)
        if not message.endswith("\n"):
            handle.write("\n")


def tail_text(path: Path, *, max_lines: int = 160, max_chars: int = 20000) -> str | None:
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    tail = "\n".join(lines[-max_lines:])
    if len(tail) > max_chars:
        tail = tail[-max_chars:]
    return tail


def state_path(root: Path, job_id: str, filename: str = "state.json") -> Path:
    return root / job_id / filename


def log_path(root: Path, job_id: str, filename: str = "job.log") -> Path:
    return root / job_id / filename


def load_job_state(path: Path) -> dict[str, Any]:
    return read_json(path)


def persist_job_state(path: Path, payload: dict[str, Any]) -> None:
    timestamp = utc_now().isoformat()
    payload["updatedAt"] = timestamp
    write_json(path, payload)


def require_no_running_jobs(
    state_dir: Path,
    filename: str = "state.json",
    *,
    exclude_job_id: str | None = None,
) -> list[str]:
    running: list[str] = []
    for sp in state_dir.glob(f"*/{filename}"):
        try:
            payload = read_json(sp)
            if str(payload.get("status", "")) in {"queued", "running"}:
                jid = str(payload.get("jobId", ""))
                if jid != str(exclude_job_id or ""):
                    running.append(jid)
        except Exception:
            continue
    return running


def list_job_payloads(
    state_dir: Path,
    filename: str = "state.json",
) -> list[dict[str, Any]]:
    state_dir.mkdir(parents=True, exist_ok=True)
    payloads: list[dict[str, Any]] = []
    for sp in sorted(state_dir.glob(f"*/{filename}")):
        try:
            payloads.append(read_json(sp))
        except Exception:
            continue
    return payloads


class BaseJobRunner(ABC):
    """Base class for background job runners with state machine."""

    def __init__(
        self,
        job_id: str,
        state_dir: Path,
        state_filename: str = "state.json",
        log_filename: str = "job.log",
    ) -> None:
        self.job_id = job_id
        self.state_dir = state_dir
        self.state_filename = state_filename
        self.log_filename = log_filename
        self._thread: threading.Thread | None = None

    # ── paths ──

    def _state_path(self) -> Path:
        return state_path(self.state_dir, self.job_id, self.state_filename)

    def _log_path(self) -> Path:
        return log_path(self.state_dir, self.job_id, self.log_filename)

    # ── state helpers ──

    def load_state(self) -> dict[str, Any]:
        return load_job_state(self._state_path())

    def persist_state(self, payload: dict[str, Any]) -> None:
        persist_job_state(self._state_path(), payload)

    def log(self, message: str) -> None:
        append_log(self._log_path(), f"[{utc_now().isoformat()}] {message}")

    # ── lifecycle ──

    def start(self) -> None:
        worker = threading.Thread(
            target=self._run_wrapper,
            args=(),
            name=f"job-{self.job_id}",
            daemon=True,
        )
        self._thread = worker
        worker.start()

    def _run_wrapper(self) -> None:
        try:
            self.run()
        except Exception as exc:
            state = self.load_state()
            state["status"] = "failed"
            state["phase"] = "failed"
            state["finishedAt"] = utc_now().isoformat()
            state["error"] = str(exc)
            self.persist_state(state)
            append_log(self._log_path(), "\n=== Failed ===")
            append_log(self._log_path(), str(exc))
            append_log(self._log_path(), traceback.format_exc())

    @abstractmethod
    def run(self) -> None:
        """Override with the actual job logic."""
