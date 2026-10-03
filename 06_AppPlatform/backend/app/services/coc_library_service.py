"""Shared COC sources and VIN index; PDF bytes are never parsed.

SQLite follows the existing COC service. Source and derived PDF ZIP stay outside
job history. No PI/VIN/price writes; selected content never falls back to an old
different-content version when its source is deleted.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from uuid import uuid4
import zipfile

from fastapi import HTTPException
from upload_toolkit.job_engine import BaseJobRunner, load_job_state, persist_job_state, state_path
from app.services.coc_match_service import (
    _get_assembled_path, _rar_member_listing, list_archive_members,
    _state_age_seconds, _COC_MATCH_STALE_AFTER_SECONDS,
)

# Production must explicitly supply a persistent, environment-specific location.
LIBRARY_ROOT = Path(os.environ["APP_COC_LIBRARY_ROOT"]).resolve() if os.getenv("APP_COC_LIBRARY_ROOT") else None
VIN_RE = re.compile(r"[A-HJ-NPR-Z0-9]{17}")
_RUNNERS: dict[str, BaseJobRunner] = {}


def _root() -> Path:
    if LIBRARY_ROOT is None:
        raise HTTPException(503, "COC library storage not configured; contact admin / 在线库持久目录未配置，请联系管理员")
    return LIBRARY_ROOT


@contextmanager
def _db():
    root = _root()
    root.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(root / "index.sqlite3", timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript("""
          CREATE TABLE IF NOT EXISTS sources (
            id TEXT PRIMARY KEY, filename TEXT NOT NULL, status TEXT NOT NULL,
            owner TEXT NOT NULL, source_hash TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
          CREATE TABLE IF NOT EXISTS members (
            id INTEGER PRIMARY KEY, source_id TEXT NOT NULL, vin TEXT NOT NULL,
            path TEXT NOT NULL, cache_name TEXT NOT NULL, sha TEXT NOT NULL,
            bytes INTEGER NOT NULL, selected INTEGER NOT NULL DEFAULT 0);
          CREATE INDEX IF NOT EXISTS members_vin ON members(vin, selected);
          CREATE INDEX IF NOT EXISTS members_source ON members(source_id);
        """)
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _source(conn, source_id: str):
    row = conn.execute("SELECT * FROM sources WHERE id=?", (source_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "Source not found; re-read library / 来源不存在，请重新读取在线库")
    return row


def _snapshot(conn) -> str:
    digest = hashlib.sha256()
    for row in conn.execute("SELECT id,vin,sha FROM members WHERE selected=1 ORDER BY id"):
        digest.update(json.dumps(tuple(row)).encode())
    for row in conn.execute("SELECT id,status FROM sources ORDER BY id"):
        digest.update(json.dumps(tuple(row)).encode())
    return digest.hexdigest()


def library_sources() -> dict:
    if LIBRARY_ROOT is None:
        return {"configured": False, "items": [], "vinCount": 0}
    with _db() as conn:
        items = []
        for row in conn.execute("SELECT * FROM sources ORDER BY created_at DESC,id"):
            item = dict(row)
            state = load_job_state(state_path(_root(), row["id"]))
            # Reuse COC's existing inactivity window, not a new recovery workflow.
            worker = getattr(_RUNNERS.get(row["id"]), "_thread", None)
            age = _state_age_seconds(state)
            if row["status"] == "indexing" and state.get("status") in {"queued", "running"} and not (worker and worker.is_alive()) and age is not None and age >= _COC_MATCH_STALE_AFTER_SECONDS:
                state.update(status="failed", error="Indexing interrupted; delete source and upload again / 索引中断，请删除来源后重新上传")
                persist_job_state(state_path(_root(), row["id"]), state)
                conn.execute("UPDATE sources SET status='failed' WHERE id=?", (row["id"],))
                item["status"] = "failed"
            item["job"] = state
            item["pdfCount"] = conn.execute("SELECT COUNT(*) FROM members WHERE source_id=?", (row["id"],)).fetchone()[0]
            items.append(item)
        count = conn.execute("SELECT COUNT(DISTINCT vin) FROM members WHERE selected=1").fetchone()[0]
        return {"configured": True, "items": items, "vinCount": count}


def _hash_file(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def import_library_source(path: Path, filename: str, owner: str, *, background: bool = True) -> dict:
    """Also used for the user-approved initial local source; no hardcoded package."""
    suffix = Path(filename).suffix.lower()
    if suffix not in {".zip", ".rar"}:
        raise HTTPException(400, "Use ZIP/RAR / 请使用 ZIP/RAR")
    source_id = uuid4().hex
    root = _root()
    root.mkdir(parents=True, exist_ok=True)
    source_hash = _hash_file(path)
    with _db() as conn:
        existing = conn.execute("SELECT id FROM sources WHERE source_hash=?", (source_hash,)).fetchone()
        if existing:
            return {"sourceId": existing[0], "duplicate": True}
        directory = root / source_id
        directory.mkdir()
        try:
            shutil.copyfile(path, directory / f"source{suffix}")
            conn.execute("INSERT INTO sources(id,filename,status,owner,source_hash) VALUES(?,?,'indexing',?,?)",
                         (source_id, Path(filename.replace('\\', '/')).name, owner, source_hash))
        except Exception:
            shutil.rmtree(directory)
            raise
    persist_job_state(state_path(root, source_id), {"jobId": source_id, "status": "queued", "phase": "indexing", "pdfCount": 0})
    runner = CocLibraryIndexRunner(source_id, root)
    if background:
        _RUNNERS[source_id] = runner
        runner.start()
    else:
        runner._run_wrapper()
    return {"sourceId": source_id, "duplicate": False}


def import_library_upload(upload_id: str, owner: str) -> dict:
    path = _get_assembled_path(upload_id)
    # Same upload sessions as COC matching; ownership checked before importing.
    from app.services.coc_match_service import _COC_UPLOAD_SESSION_ROOT
    from upload_toolkit.upload_engine import get_upload_session
    state = get_upload_session(_COC_UPLOAD_SESSION_ROOT, upload_id)
    if state.get("triggeredBy") != owner:
        raise HTTPException(403, "Upload belongs to another account / 上传不属于当前账号")
    return import_library_source(path, path.name, owner)


class CocLibraryIndexRunner(BaseJobRunner):
    def _run_wrapper(self) -> None:
        try:
            super()._run_wrapper()
        finally:
            _RUNNERS.pop(self.job_id, None)

    def run(self) -> None:
        state = self.load_state()
        state.update(status="running", phase="indexing")
        self.persist_state(state)
        directory = self.state_dir / self.job_id
        source = next(directory.glob("source.*"))
        rows = []
        invalid = 0
        total_bytes = 0
        # Bounded writes: ZIP stored members are directly readable after RAR
        # normalization. A source is opened once per batch, never once per VIN.
        with zipfile.ZipFile(directory / "pdfs.zip", "w", allowZip64=True) as cache:
            def visit(path: Path, members: list[dict]) -> None:
                nonlocal invalid, total_bytes
                by_name = {m["memberPath"][-1]: m for m in members}

                def save(name: str, handle) -> None:
                    nonlocal invalid, total_bytes
                    metadata = by_name[name.replace("\\", "/")]
                    vin = metadata["stem"].upper()
                    if not VIN_RE.fullmatch(vin):
                        invalid += 1
                        return
                    cache_name = f"{len(rows)}.pdf"
                    digest = hashlib.sha256()
                    size = 0
                    with cache.open(cache_name, "w", force_zip64=True) as target:
                        while chunk := handle.read(1024 * 1024):
                            size += len(chunk)
                            total_bytes += len(chunk)
                            if total_bytes > 100 * 1024**3:
                                raise ValueError("PDF total exceeds 100 GiB; split source / PDF总量超100GiB，请拆分来源")
                            digest.update(chunk)
                            target.write(chunk)
                    rows.append((self.job_id, vin, json.dumps(metadata["memberPath"], ensure_ascii=False), cache_name, digest.hexdigest(), size))
                    if len(rows) % 250 == 0:
                        state["pdfCount"] = len(rows)
                        self.persist_state(state)

                if path.suffix.lower() == ".zip":
                    with zipfile.ZipFile(path) as archive:
                        for info in archive.infolist():
                            if info.filename.replace("\\", "/") in by_name:
                                with archive.open(info) as handle:
                                    save(info.filename, handle)
                else:
                    tool = shutil.which("7zz") or shutil.which("7z")
                    if not tool:
                        raise ValueError("RAR extraction unavailable; use ZIP / 无RAR解包工具，请改传ZIP")
                    listing = _rar_member_listing(path, tool)
                    # Extract named PDF members once, including solid RAR. Validate
                    # names before handing them to the existing deployed extractor.
                    names = [name for name, _ in listing if name.replace("\\", "/") in by_name]
                    if len(set(names)) != len(names):
                        raise ValueError("Duplicate RAR paths; repack as ZIP / RAR同路径重复，请重新打包ZIP")
                    for name in names:
                        normalized = name.replace("\\", "/")
                        if '\n' in name or '\r' in name or PurePosixPath(normalized).is_absolute() or '..' in PurePosixPath(normalized).parts or ':' in normalized:
                            raise ValueError("Unsafe RAR path; repack source / RAR路径异常，请重新打包")
                    name_set = set(names)
                    if sum(size for name, size in listing if name in name_set) + total_bytes > 100 * 1024**3:
                        raise ValueError("PDF total too large; split source / PDF总量过大，请拆分来源")
                    with tempfile.TemporaryDirectory(prefix="coc-rar-", dir=directory) as temporary:
                        listing_file = Path(temporary) / "members.txt"
                        listing_file.write_text('\n'.join(names), encoding="utf-8")
                        output = Path(temporary) / "pdfs"
                        result = subprocess.run([tool, "x", "-y", "-spd", "-scsUTF-8", f"-o{output}", str(path), f"@{listing_file}"],
                                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=1800)
                        if result.returncode:
                            self.log(result.stderr.decode("utf-8", errors="replace")[:2000])
                            raise ValueError("Cannot extract PDFs; use ZIP or ask admin to check RAR decoder / 无法提取PDF，请改传ZIP或联系管理员检查RAR解码器")
                        for name in names:
                            target = output / name.replace("\\", "/")
                            if target.is_symlink() or not target.is_file() or not target.resolve().is_relative_to(output.resolve()):
                                raise ValueError("Invalid extracted PDF / 提取的PDF无效，请重新打包")
                            with target.open("rb") as handle:
                                save(name, handle)

            try:
                list_archive_members(source, max_members=200_000, max_nested_bytes=10 * 1024**3, on_archive=visit)
                with _db() as conn:
                    conn.executemany("INSERT INTO members(source_id,vin,path,cache_name,sha,bytes) VALUES(?,?,?,?,?,?)", rows)
                    conn.execute("UPDATE sources SET status='review' WHERE id=?", (self.job_id,))
            except Exception:
                with _db() as conn:
                    conn.execute("UPDATE sources SET status='failed' WHERE id=?", (self.job_id,))
                raise
        state.update(status="success", phase="review", pdfCount=len(rows), invalidCount=invalid)
        self.persist_state(state)
        _RUNNERS.pop(self.job_id, None)


def preview_source(source_id: str, *, conn=None) -> dict:
    if conn is None:
        with _db() as connection:
            return preview_source(source_id, conn=connection)
    if conn is not None:
        source = _source(conn, source_id)
        if source["status"] != "review":
            raise HTTPException(409, "Wait for indexing / 请等待索引完成")
        candidates = conn.execute("SELECT vin,COUNT(DISTINCT sha) variants,MIN(sha) sha FROM members WHERE source_id=? GROUP BY vin ORDER BY vin", (source_id,)).fetchall()
        choices = lookup_vins([r["vin"] for r in candidates], conn=conn)
        items = []
        for row in candidates:
            old = choices.get(row["vin"])
            status = "invalid" if row["variants"] != 1 else "new" if not old else "duplicate" if old["sha"] == row["sha"] else "conflict"
            items.append({"vin": row["vin"], "status": status, "oldSha": old["sha"] if old else None, "newSha": row["sha"]})
        return {"sourceId": source_id, "fingerprint": _snapshot(conn), "items": items}


def activate_source(source_id: str, fingerprint: str, replace_vins: list[str]) -> dict:
    if not isinstance(replace_vins, list) or not all(isinstance(v, str) for v in replace_vins):
        raise HTTPException(400, "Select replacement VINs from preview / 请从预览勾选要替换的VIN")
    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        if _snapshot(conn) != fingerprint:
            raise HTTPException(409, "Library changed; preview again / 在线库已变化，请重新预览")
        preview = preview_source(source_id, conn=conn)
        replacements = set(replace_vins)
        conflicts = {r["vin"] for r in preview["items"] if r["status"] == "conflict"}
        if any(r["status"] == "invalid" for r in preview["items"]) or not replacements <= conflicts:
            raise HTTPException(409, "Ambiguous PDFs; repack source / 同VIN有多份不同PDF，请整理来源后重传")
        for row in preview["items"]:
            vin = row["vin"]
            if row["status"] == "conflict" and vin not in replacements:
                continue
            if vin in replacements:
                conn.execute("UPDATE members SET selected=0 WHERE vin=?", (vin,))
            conn.execute("UPDATE members SET selected=1 WHERE source_id=? AND vin=?", (source_id, vin))
        conn.execute("UPDATE sources SET status='active' WHERE id=?", (source_id,))
    return {"sourceId": source_id, "status": "active"}


def lookup_vins(vins: list[str], *, conn=None) -> dict:
    if conn is None:
        with _db() as connection:
            return lookup_vins(vins, conn=connection)
    result = {}
    unique = sorted({v.upper().strip() for v in vins if v})
    for start in range(0, len(unique), 500):
        batch = unique[start:start + 500]
        placeholders = ','.join('?' for _ in batch)
        for row in conn.execute(f"""SELECT m.* FROM members m JOIN
           (SELECT MIN(id) id FROM members WHERE selected=1 AND vin IN ({placeholders}) GROUP BY vin) ids
           ON m.id=ids.id""", batch):
            result[row["vin"]] = dict(row)
    return result


def delete_source(source_id: str, fingerprint: str | None = None) -> dict:
    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        source = _source(conn, source_id)
        job = load_job_state(state_path(_root(), source_id))
        if source["status"] == "indexing" and job.get("status") != "failed":
            raise HTTPException(409, "Indexing; wait before deleting / 正在索引，请完成后删除")
        lost = [r[0] for r in conn.execute("""SELECT DISTINCT a.vin FROM members a WHERE a.source_id=? AND a.selected=1
          AND NOT EXISTS(SELECT 1 FROM members b WHERE b.vin=a.vin AND b.sha=a.sha AND b.selected=1 AND b.source_id<>a.source_id)""", (source_id,))]
        token = _snapshot(conn)
        if fingerprint is None:
            return {"sourceId": source_id, "fingerprint": token, "lostVins": lost, "lostCount": len(lost)}
        if fingerprint != token:
            raise HTTPException(409, "Library changed; preview deletion again / 在线库已变化，请重新预览删除")
        conn.execute("DELETE FROM members WHERE source_id=?", (source_id,))
        conn.execute("DELETE FROM sources WHERE id=?", (source_id,))
    shutil.rmtree(_root() / source_id)
    return {"deleted": True, "lostCount": len(lost)}


def write_pdf_zip(vins: list[str], target: Path) -> dict:
    # Resolve first: no partial download disguised as a complete selection.
    vins = sorted({v.strip().upper() for v in vins})
    members = lookup_vins(vins)
    missing = sorted(set(vins) - members.keys())
    if missing:
        raise HTTPException(409, "Some PDFs missing; re-read and select available rows / 部分PDF缺失，请重新查库并勾选可用车辆")
    groups: dict[str, list[dict]] = {}
    for member in members.values():
        groups.setdefault(member["source_id"], []).append(member)
    with zipfile.ZipFile(target, "w", allowZip64=True) as output:
        for source_id, group in groups.items():
            with zipfile.ZipFile(_root() / source_id / "pdfs.zip") as cache:
                for member in group:
                    with cache.open(member["cache_name"]) as src, output.open(f"{member['vin']}.pdf", "w", force_zip64=True) as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)
    return {"count": len(members)}
