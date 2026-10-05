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
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4
import zipfile

from fastapi import HTTPException
from upload_toolkit.job_engine import BaseJobRunner, load_job_state, persist_job_state, state_path
from app.services.coc_match_service import (
    _get_assembled_path, list_archive_members, visit_archive_files,
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
            resource_path = _root() / row["id"] / "resources.json"
            resources = load_job_state(resource_path) if resource_path.exists() else {}
            # Reuse COC's existing inactivity window, not a new recovery workflow.
            worker = getattr(_RUNNERS.get(row["id"]), "_thread", None)
            age = _state_age_seconds(state)
            resource_age = _state_age_seconds(resources)
            if resources.get("status") == "running" and resource_age is not None:
                age = min(age, resource_age) if age is not None else resource_age
            if row["status"] == "indexing" and state.get("status") in {"queued", "running", "success"} and not (worker and worker.is_alive()) and age is not None and age >= _COC_MATCH_STALE_AFTER_SECONDS:
                state.update(status="failed", error="Indexing interrupted; delete source and upload again / 索引中断，请删除来源后重新上传")
                persist_job_state(state_path(_root(), row["id"]), state)
                conn.execute("UPDATE sources SET status='failed' WHERE id=?", (row["id"],))
                item["status"] = "failed"
                resources.update(status="finished", rssBytes=0)
            item["job"] = state
            item["resources"] = resources
            item["pdfCount"] = conn.execute("SELECT COUNT(*) FROM members WHERE source_id=?", (row["id"],)).fetchone()[0]
            items.append(item)
        count = conn.execute("SELECT COUNT(DISTINCT vin) FROM members WHERE selected=1").fetchone()[0]
        # Only a few files per source: never scan every PDF on each UI poll.
        library_bytes = sum(path.stat().st_size for item in items for path in (_root() / item["id"]).iterdir() if path.is_file())
        library_bytes += (_root() / "index.sqlite3").stat().st_size
        return {"configured": True, "items": items, "vinCount": count,
                "libraryBytes": library_bytes, "diskFreeBytes": shutil.disk_usage(_root()).free}


def _hash_file(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _check_disk(required_bytes: int = 0) -> None:
    reserve = int(os.getenv("APP_COC_MIN_FREE_DISK_BYTES", str(2 * 1024**3)))
    if shutil.disk_usage(_root()).free < reserve + required_bytes:
        raise ValueError("Insufficient disk space; remove unused sources or ask admin / 磁盘空间不足，请删除不用的来源包或联系管理员")


def _resource_worker():
    scripts = Path(__file__).resolve().parents[4] / "03_Scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    import jato_monthly_worker
    return jato_monthly_worker


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
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute("SELECT id,status FROM sources WHERE source_hash=?", (source_hash,)).fetchone()
        if existing:
            needs_review = existing["status"] == "active" and conn.execute("""SELECT 1 FROM members a WHERE a.source_id=?
              AND NOT EXISTS(SELECT 1 FROM members b WHERE b.source_id=a.source_id AND b.vin=a.vin AND b.selected=1) LIMIT 1""", (existing["id"],)).fetchone() is not None
            if needs_review:
                conn.execute("UPDATE sources SET status='review' WHERE id=?", (existing["id"],))
            return {"sourceId": existing["id"], "duplicate": True, "needsReview": needs_review}
        if conn.execute("SELECT 1 FROM sources WHERE status='indexing' LIMIT 1").fetchone():
            raise HTTPException(409, "One source is indexing; wait before uploading another / 已有来源正在索引，请完成后再导入")
        _check_disk(path.stat().st_size)
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
    runner = CocLibraryIndexRunner(source_id, root, isolated=background)
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
    from upload_toolkit.upload_engine import get_upload_session, cleanup_upload_session
    state = get_upload_session(_COC_UPLOAD_SESSION_ROOT, upload_id)
    if state.get("triggeredBy") != owner:
        raise HTTPException(403, "Upload belongs to another account / 上传不属于当前账号")
    result = import_library_source(path, path.name, owner)
    cleanup_upload_session(_COC_UPLOAD_SESSION_ROOT, upload_id)
    return result


class CocLibraryIndexRunner(BaseJobRunner):
    def __init__(self, job_id: str, state_dir: Path, *, isolated: bool = True, supervisor: bool = False):
        super().__init__(job_id, state_dir)
        self.isolated = isolated
        self.supervisor = supervisor

    def _run_wrapper(self) -> None:
        try:
            super()._run_wrapper()
        finally:
            if self.load_state().get("status") == "failed":
                with _db() as conn:
                    conn.execute("UPDATE sources SET status='failed' WHERE id=?", (self.job_id,))
                directory = self.state_dir / self.job_id
                (directory / "pdfs.zip").unlink(missing_ok=True)
                for path in directory.iterdir():
                    if path.is_dir() and path.name.startswith(("coc-rar-", "jato-coc-archive-")):
                        shutil.rmtree(path)
            _RUNNERS.pop(self.job_id, None)

    def run(self) -> None:
        if self.supervisor:
            self._run_isolated()
        elif self.isolated:
            self._launch_supervised()
        else:
            self._index()

    def _launch_supervised(self) -> None:
        # Keep the existing monthly watchdog alive if an API worker restarts.
        backend = Path(__file__).resolve().parents[2]
        env = {**os.environ, "APP_COC_LIBRARY_ROOT": str(self.state_dir),
               "PYTHONPATH": os.pathsep.join([str(backend), str(backend.parents[1] / "03_Scripts"), os.getenv("PYTHONPATH", "")])}
        code = "from app.services.coc_library_service import _index_main; _index_main(supervise=True)"
        with (self.state_dir / self.job_id / "supervisor.log").open("ab") as log:
            result = subprocess.run([sys.executable, "-c", code, self.job_id], env=env,
                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        if result.returncode != 0 and self.load_state().get("status") != "failed":
            raise ValueError("Index supervisor stopped; delete and retry / 索引监测进程停止，请删除后重试")

    def _run_isolated(self) -> None:
        worker = _resource_worker()
        directory = self.state_dir / self.job_id
        receipt = directory / "exit.json"
        limit = int(os.getenv("APP_COC_INDEX_RSS_LIMIT_BYTES", str(worker.DIGEST_RSS_LIMIT_BYTES)))
        if limit <= 0:
            raise ValueError("COC memory limit must be positive / COC 内存上限必须大于零")
        cgroup = worker._cgroup_memory_snapshot()
        if cgroup and str(cgroup.get("max", "")).isdigit() and str(cgroup.get("current", "")).isdigit():
            headroom = int(cgroup["max"]) - int(cgroup["current"])
            if headroom < 256 * 1024**2:
                raise ValueError("Insufficient runtime memory; retry later / 当前内存余量不足，请稍后重试")
            limit = min(limit, int(headroom * 0.8))

        def sample(resources: dict) -> None:
            resources["updatedAt"] = datetime.now(UTC).isoformat()
            worker._atomic_write_json(directory / "resources.json", resources)
            if resources.get("status") == "finished" or self.load_state().get("status") == "success":
                return
            _check_disk()
            current = worker._cgroup_memory_snapshot()
            if current and str(current.get("max", "")).isdigit() and str(current.get("current", "")).isdigit() and int(current["max"]) - int(current["current"]) < 64 * 1024**2:
                raise ValueError("Runtime memory exhausted / 运行环境内存余量不足")

        backend = Path(__file__).resolve().parents[2]
        code = "from app.services.coc_library_service import _index_main; _index_main()"
        worker._supervise_digest_upload(upload_id=self.job_id, attempt_id=self.job_id,
            log_path=directory / "worker.log", receipt_path=receipt,
            worker_command=[sys.executable, "-c", code, self.job_id],
            worker_env={"APP_COC_LIBRARY_ROOT": str(self.state_dir), "APP_COC_SUPERVISED_INDEX": "1",
                        "PYTHONPATH": os.pathsep.join([str(backend), str(backend.parents[1] / "03_Scripts"), os.getenv("PYTHONPATH", "")])},
            on_sample=sample, rss_limit_bytes=limit)
        evidence = load_job_state(receipt)
        resources = load_job_state(directory / "resources.json") if (directory / "resources.json").exists() else {}
        resources.update(status="finished", rssBytes=0, terminationReason=evidence.get("terminationReason"))
        worker._atomic_write_json(directory / "resources.json", resources)
        if evidence.get("returnCode") != 0 or evidence.get("supervisorError"):
            raise ValueError("Indexing stopped (memory, disk, or archive error); original source retained. Delete and retry a smaller ZIP / 索引已停止（内存、磁盘或压缩包异常），原包保留；请删除后改传较小 ZIP")
        with _db() as conn:
            conn.execute("UPDATE sources SET status='review' WHERE id=?", (self.job_id,))

    def _index(self) -> None:
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
                        _check_disk()
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

                visit_archive_files(path, set(by_name), save, temporary_root=directory, max_bytes=100 * 1024**3 - total_bytes)

            try:
                list_archive_members(source, max_members=200_000, max_nested_bytes=10 * 1024**3, on_archive=visit, temporary_root=directory)
            except Exception:
                with _db() as conn:
                    conn.execute("UPDATE sources SET status='failed' WHERE id=?", (self.job_id,))
                raise
        with _db() as conn:
            conn.executemany("INSERT INTO members(source_id,vin,path,cache_name,sha,bytes) VALUES(?,?,?,?,?,?)", rows)
            if os.getenv("APP_COC_SUPERVISED_INDEX") != "1":
                conn.execute("UPDATE sources SET status='review' WHERE id=?", (self.job_id,))
        state.update(status="success", phase="review", pdfCount=len(rows), invalidCount=invalid)
        self.persist_state(state)
        _RUNNERS.pop(self.job_id, None)


def _index_main(*, supervise: bool = False) -> None:
    runner = CocLibraryIndexRunner(sys.argv[1], _root(), isolated=False, supervisor=supervise)
    runner._run_wrapper()
    raise SystemExit(0 if runner.load_state().get("status") == "success" else 1)


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


def write_pdf_zip(vins: list[str], target: Path, *, mode: str = "w") -> dict:
    # Resolve first: no partial download disguised as a complete selection.
    vins = sorted({v.strip().upper() for v in vins})
    members = lookup_vins(vins)
    missing = sorted(set(vins) - members.keys())
    if missing:
        raise HTTPException(409, "Some PDFs missing; re-read and select available rows / 部分PDF缺失，请重新查库并勾选可用车辆")
    groups: dict[str, list[dict]] = {}
    for member in members.values():
        groups.setdefault(member["source_id"], []).append(member)
    with zipfile.ZipFile(target, mode, allowZip64=True) as output:
        for source_id, group in groups.items():
            with zipfile.ZipFile(_root() / source_id / "pdfs.zip") as cache:
                for member in group:
                    with cache.open(member["cache_name"]) as src, output.open(f"{member['vin']}.pdf", "w", force_zip64=True) as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)
    return {"count": len(members)}
