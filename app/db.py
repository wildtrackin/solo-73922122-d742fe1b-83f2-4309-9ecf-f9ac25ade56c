"""SQLite 持久层。

关键约束
--------
* ``records`` 按 OAI identifier 唯一；删除只是置 ``deleted=1``，并且永远保留，
  绝不以"本轮未出现"推断删除。
* 任意时刻最多有一个未解决的采集中/发布中轮次，由部分唯一索引兜底。
* 发布会写入唯一文件名 + 一条 publications 记录（水位与文件路径同一条记录），
  下载端只从该记录解析文件，因此进程在发布前后任意点退出，页面水位与下载内容
  要么都是旧的一整套、要么都是新的一整套。
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
import json
import os
import sqlite3
import threading
from typing import Any, Iterator

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS records (
    identifier    TEXT PRIMARY KEY,
    datestamp     TEXT,
    deleted       INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT,
    metadata_xml  TEXT,
    sets_json     TEXT NOT NULL DEFAULT '[]',
    first_seen_run INTEGER,
    updated_run   INTEGER,
    updated_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_deleted ON records(deleted);

CREATE TABLE IF NOT EXISTS runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mode            TEXT NOT NULL,              -- full | incremental
    window_from     TEXT,                       -- NULL = 全量(不带 from)
    window_until    TEXT NOT NULL,
    status          TEXT NOT NULL,              -- running|paused|publishing|publish_failed|completed|canceled
    resumption_token TEXT,
    pages_done      INTEGER NOT NULL DEFAULT 0,
    records_seen    INTEGER NOT NULL DEFAULT 0,
    complete_list_size INTEGER,
    cursor          INTEGER,
    token_resets    INTEGER NOT NULL DEFAULT 0,
    error_code      TEXT,
    error_message   TEXT,
    notice          TEXT,
    started_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    finished_at     TEXT
);

-- 同一时间只允许一轮未解决采集；paused/publish_failed 需先继续、重试或放弃。
CREATE UNIQUE INDEX IF NOT EXISTS uq_runs_unresolved
    ON runs((1)) WHERE status IN ('running','paused','publishing','publish_failed');

CREATE TABLE IF NOT EXISTS publications (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       INTEGER NOT NULL REFERENCES runs(id),
    watermark    TEXT NOT NULL,
    file_path    TEXT NOT NULL UNIQUE,
    file_name    TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    size_bytes   INTEGER NOT NULL,
    sha256       TEXT NOT NULL,
    published_at TEXT NOT NULL
);

"""

UNRESOLVED = ("running", "paused", "publishing", "publish_failed")


def utcnow() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class Run:
    id: int
    mode: str
    window_from: str | None
    window_until: str
    status: str
    resumption_token: str | None
    pages_done: int
    records_seen: int
    complete_list_size: int | None
    cursor: int | None
    token_resets: int
    error_code: str | None
    error_message: str | None
    notice: str | None
    started_at: str
    updated_at: str
    finished_at: str | None

    @property
    def is_unresolved(self) -> bool:
        return self.status in UNRESOLVED

    @property
    def is_resumable(self) -> bool:
        return self.status == "paused"

    @property
    def is_publish_retryable(self) -> bool:
        return self.status == "publish_failed"

    @property
    def has_resumption_token(self) -> bool:
        return bool(self.resumption_token)

    @property
    def progress_fraction(self) -> float | None:
        if self.complete_list_size and self.complete_list_size > 0:
            return min(1.0, self.records_seen / self.complete_list_size)
        return None


class Database:
    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent or ".", exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def query(self, sql: str, *params: Any) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, params))

    def one(self, sql: str, *params: Any) -> sqlite3.Row | None:
        rows = self.query(sql, *params)
        return rows[0] if rows else None

    # ------------------------------------------------------------------ meta
    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.one("SELECT value FROM meta WHERE key = ?", key)
        return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def watermark(self) -> str | None:
        """展示/续采水位只取自最近一次已提交的发布记录，保证与下载内容成对。"""
        latest = self.latest_publication()
        if latest is None:
            return None
        return latest["watermark"]

    def take_publish_fault(self) -> str | None:
        """一次性故障注入（演示用）；读后即清除，避免一直污染后续发布。"""
        with self.tx() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key = 'publish_fault'").fetchone()
            if row is None or row["value"] == "none":
                return None
            conn.execute("DELETE FROM meta WHERE key = 'publish_fault'")
            return row["value"]

    # --------------------------------------------------------------- records
    def upsert_record(self, rec: dict[str, Any], run_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                INSERT INTO records(identifier, datestamp, deleted, metadata_json, metadata_xml,
                                    sets_json, first_seen_run, updated_run, updated_at)
                VALUES(:identifier, :datestamp, :deleted, :metadata_json, :metadata_xml,
                       :sets_json, :run_id, :run_id, :updated_at)
                ON CONFLICT(identifier) DO UPDATE SET
                    datestamp     = excluded.datestamp,
                    deleted       = excluded.deleted,
                    metadata_json = excluded.metadata_json,
                    metadata_xml  = excluded.metadata_xml,
                    sets_json     = excluded.sets_json,
                    updated_run   = excluded.updated_run,
                    updated_at    = excluded.updated_at
                """,
                {
                    "identifier": rec["identifier"],
                    "datestamp": rec.get("datestamp"),
                    "deleted": 1 if rec.get("deleted") else 0,
                    "metadata_json": (
                        json.dumps(rec["metadata"], ensure_ascii=False)
                        if rec.get("metadata") is not None
                        else None
                    ),
                    "metadata_xml": rec.get("metadata_xml"),
                    "sets_json": json.dumps(rec.get("sets") or [], ensure_ascii=False),
                    "run_id": run_id,
                    "updated_at": utcnow(),
                },
            )

    def active_records(self) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM records WHERE deleted = 0 ORDER BY identifier")

    def record_stats(self) -> tuple[int, int]:
        row = self.one("SELECT COUNT(*) AS total, COALESCE(SUM(deleted), 0) AS deleted FROM records")
        total = int(row["total"]) if row else 0
        deleted = int(row["deleted"]) if row else 0
        return total - deleted, deleted

    def records_changed_in_run(self, run_id: int) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM records WHERE updated_run = ? ORDER BY updated_at DESC, identifier",
            run_id,
        )

    # ------------------------------------------------------------------ runs
    @staticmethod
    def _run(row: sqlite3.Row) -> Run:
        return Run(**{key: row[key] for key in row.keys()})

    def get_run(self, run_id: int) -> Run | None:
        row = self.one("SELECT * FROM runs WHERE id = ?", run_id)
        return self._run(row) if row else None

    def unresolved_run(self) -> Run | None:
        row = self.one(
            f"SELECT * FROM runs WHERE status IN ({','.join('?' * len(UNRESOLVED))}) ORDER BY id DESC LIMIT 1",
            *UNRESOLVED,
        )
        return self._run(row) if row else None

    def create_run(self, mode: str, window_from: date | None, window_until: date) -> Run:
        with self.tx() as conn:
            existing = conn.execute(
                f"SELECT id, status FROM runs WHERE status IN ({','.join('?' * len(UNRESOLVED))}) LIMIT 1",
                UNRESOLVED,
            ).fetchone()
            if existing is not None:
                raise RuntimeError(f"已有未结束的轮次 #{existing['id']}（{existing['status']}）")
            now = utcnow()
            cur = conn.execute(
                """
                INSERT INTO runs(mode, window_from, window_until, status, pages_done, records_seen,
                                 started_at, updated_at)
                VALUES(?, ?, ?, 'running', 0, 0, ?, ?)
                """,
                (mode, window_from.isoformat() if window_from else None, window_until.isoformat(), now, now),
            )
            run_id = int(cur.lastrowid)
        run = self.get_run(run_id)
        assert run is not None
        return run

    def update_progress(
        self,
        run_id: int,
        *,
        token: str | None,
        pages_done: int,
        records_seen: int,
        complete_list_size: int | None = None,
        cursor: int | None = None,
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET resumption_token = ?, pages_done = ?, records_seen = ?,
                       complete_list_size = COALESCE(?, complete_list_size),
                       cursor = COALESCE(?, cursor), updated_at = ?
                 WHERE id = ? AND status = 'running'
                """,
                (token, pages_done, records_seen, complete_list_size, cursor, utcnow(), run_id),
            )

    def add_notice(self, run_id: int, notice: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "UPDATE runs SET notice = ?, updated_at = ? WHERE id = ?",
                (notice, utcnow(), run_id),
            )

    def complete_harvest(self, run_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'publishing', resumption_token = NULL,
                       error_code = NULL, error_message = NULL, updated_at = ?
                 WHERE id = ? AND status = 'running'
                """,
                (utcnow(), run_id),
            )

    def pause_run(self, run_id: int, code: str, message: str, *, notice: str | None = None) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'paused', error_code = ?, error_message = ?,
                       notice = COALESCE(?, notice), updated_at = ?
                 WHERE id = ? AND status IN ('running','publishing')
                """,
                (code, message[:3000], notice, utcnow(), run_id),
            )

    def mark_publish_failed(self, run_id: int, message: str) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'publish_failed', error_code = 'publish_failed',
                       error_message = ?, updated_at = ?
                 WHERE id = ? AND status IN ('publishing','running')
                """,
                (message[:3000], utcnow(), run_id),
            )

    def begin_publish_retry(self, run_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'publishing', error_code = NULL, error_message = NULL, updated_at = ?
                 WHERE id = ? AND status = 'publish_failed'
                """,
                (utcnow(), run_id),
            )

    def mark_completed(self, run_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs SET status = 'completed', updated_at = ?, finished_at = ?
                 WHERE id = ? AND status = 'publishing'
                """,
                (utcnow(), utcnow(), run_id),
            )

    def cancel_run(self, run_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'canceled', resumption_token = NULL, updated_at = ?, finished_at = ?
                 WHERE id = ? AND status IN ('running','paused','publishing','publish_failed')
                """,
                (utcnow(), utcnow(), run_id),
            )

    def reset_for_relist(self, run_id: int, reason: str) -> None:
        """badResumptionToken：保留原窗口，从第一页重采，不改进度水位。"""
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET resumption_token = NULL, pages_done = 0, records_seen = 0,
                       cursor = NULL, token_resets = token_resets + 1,
                       notice = ?, error_code = NULL, error_message = NULL,
                       status = 'running', updated_at = ?
                 WHERE id = ? AND status IN ('running','paused')
                """,
                (reason, utcnow(), run_id),
            )

    def recent_runs(self, limit: int = 20) -> list[Run]:
        rows = self.query("SELECT * FROM runs ORDER BY id DESC LIMIT ?", limit)
        return [self._run(r) for r in rows]

    def recover_after_restart(self) -> list[Run]:
        """进程启动时处理上次遗留状态。

        running -> paused（保留 token，可续采）
        publishing -> 保持，由启动流程根据 publications 判断补完成或重新发布
        """
        with self.tx() as conn:
            conn.execute(
                """
                UPDATE runs
                   SET status = 'paused', error_code = 'interrupted',
                       error_message = '进程在采集途中退出',
                       notice = '服务重启，本轮已中断；分页进度已保存，可继续采集。',
                       updated_at = ?
                 WHERE status = 'running'
                """,
                (utcnow(),),
            )
        return self.recent_runs(50)

    # ---------------------------------------------------------- publications
    def latest_publication(self) -> sqlite3.Row | None:
        return self.one("SELECT * FROM publications ORDER BY id DESC LIMIT 1")

    def publication_for_run(self, run_id: int) -> sqlite3.Row | None:
        return self.one("SELECT * FROM publications WHERE run_id = ? ORDER BY id DESC LIMIT 1", run_id)

    def record_publication(
        self,
        *,
        run_id: int,
        watermark: str,
        file_path: str,
        file_name: str,
        record_count: int,
        size_bytes: int,
        sha256: str,
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                INSERT INTO publications(run_id, watermark, file_path, file_name, record_count,
                                         size_bytes, sha256, published_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, watermark, file_path, file_name, record_count, size_bytes, sha256, utcnow()),
            )

    def publications(self, limit: int = 20) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM publications ORDER BY id DESC LIMIT ?", limit)
