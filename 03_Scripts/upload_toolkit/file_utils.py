"""File utilities for upload toolkit — hashing, validation, JSON I/O."""

import hashlib
import json
from pathlib import Path
from typing import Any


_STREAM_CHUNK_SIZE = 64 * 1024  # 64 KB


def sha256_hex_for_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_hex_for_path(path: Path, chunk_size: int = _STREAM_CHUNK_SIZE) -> str:
    """Streaming SHA-256 — reads file in chunks to avoid loading entire file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def normalize_sha256(value: Any, *, detail: str = "") -> str:
    raw = str(value).strip().lower()
    if raw and all(c in "0123456789abcdef" for c in raw) and len(raw) == 64:
        return raw
    raise ValueError(detail or "无效的 SHA-256。")


def allowed_extension(
    filename: str,
    *,
    allowed: set[str] | None = None,
) -> bool:
    ext = Path(filename).suffix.lower()
    return ext in (allowed or {".xlsx", ".xlsm", ".xls"})


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object: {path}")
    return payload


def read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return read_json(path)


def write_json(path: Path, payload: dict[str, Any], *, atomic: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if atomic:
        tmp = path.with_suffix(f"{path.suffix}.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    else:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
