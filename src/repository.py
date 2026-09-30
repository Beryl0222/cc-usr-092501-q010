"""SQLite 事件存储。

只依赖标准库。所有事实以事件形式只可追加地写入 ``events`` 表；上传批次、
访问令牌等少量簿记表用于实现：

* **上传幂等**：同一 upload_id 重传且内容指纹一致 → 安全返回成功，不重复落库。
* **冲突隔离**：同一 upload_id 内容不一致，或批次中任一事件不合法 → 整个站点
  批次原子隔离（BATCH_QUARANTINED），批次内事件一条都不写入，其他地区不受影响。
* **并发锁库**：冻结/发布等中央命令以 ``BEGIN IMMEDIATE`` 抢占写锁，抢不到时
  抛 :class:`LibraryLocked`，调用方稍后重试。
* **崩溃恢复**：事件与上传登记在同一事务提交；进程崩溃后重放事件流即可重建
  全部状态，不存在半写入批次。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from .envelope import validate_event
from .events import BATCH_QUARANTINED, canonical_json, make_event, now_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL UNIQUE,
    event_type     TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    site_id        TEXT NOT NULL,
    occurred_at    TEXT NOT NULL,
    version        INTEGER NOT NULL,
    summary        TEXT NOT NULL,
    payload        TEXT NOT NULL,
    upload_id      TEXT,
    UNIQUE (aggregate_id, version)
);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_site ON events(site_id);
CREATE INDEX IF NOT EXISTS idx_events_upload ON events(upload_id);

CREATE TABLE IF NOT EXISTS uploads (
    upload_id    TEXT PRIMARY KEY,
    site_id      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('accepted', 'quarantined')),
    reason       TEXT,
    event_count  INTEGER NOT NULL DEFAULT 0,
    first_seq    INTEGER,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tokens (
    token_hash TEXT PRIMARY KEY,
    role       TEXT NOT NULL CHECK (role IN ('coordinator', 'researcher', 'clinician', 'site_user')),
    site_id    TEXT,
    issued_at  TEXT NOT NULL
);
"""


class StoreError(Exception):
    """存储层错误基类。"""


class LibraryLocked(StoreError):
    """写锁被其他连接占用（并发锁库）。"""


class ConflictQuarantine(StoreError):
    """上传批次内容冲突，已被隔离。"""

    def __init__(self, reason: str, upload_id: str, site_id: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.upload_id = upload_id
        self.site_id = site_id


def content_fingerprint(events: list[dict[str, Any]]) -> str:
    return hashlib.sha256(canonical_json(events).encode("utf-8")).hexdigest()


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class EventStore:
    def __init__(self, path: str = ":memory:", *, lock_timeout_ms: int = 2000) -> None:
        self.path = path
        self._lock_timeout_ms = lock_timeout_ms
        # 单一互斥门串行化本进程对连接的访问，消除单连接上读写交错导致的游标错乱。
        # 跨进程争用仍由 SQLite busy_timeout 与 BEGIN IMMEDIATE 处理。
        self._gate = threading.Lock()
        self._write_owner: int | None = None
        self._conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(f"PRAGMA busy_timeout = {int(lock_timeout_ms)}")
        self._conn.execute("PRAGMA foreign_keys = ON")
        if path != ":memory:":
            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA synchronous = FULL")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "EventStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---- 事务与写锁 -----------------------------------------------------

    @contextmanager
    def _read_gate(self) -> Iterator[None]:
        """读访问门：写事务所在线程直接放行（写事务内可读），其余互斥等待。"""
        if self._write_owner == threading.get_ident():
            yield
            return
        with self._gate:
            yield

    @contextmanager
    def write_lock(self) -> Iterator[sqlite3.Connection]:
        """立即获取写锁的事务上下文；争用失败抛 LibraryLocked。"""
        thread_id = threading.get_ident()
        if self._write_owner == thread_id:
            raise LibraryLocked("本线程内已有写事务在执行")
        if not self._gate.acquire(blocking=False):
            raise LibraryLocked("本进程内已有写事务在执行")
        self._write_owner = thread_id
        try:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as error:
                if "locked" in str(error).lower() or "busy" in str(error).lower():
                    raise LibraryLocked(f"事件库正被其他进程锁定：{error}") from error
                raise
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")
        finally:
            self._write_owner = None
            self._gate.release()

    # ---- 读取 -----------------------------------------------------------

    def replay(self) -> list[dict[str, Any]]:
        with self._read_gate():
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY seq"
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def events_for(self, aggregate_id: str) -> list[dict[str, Any]]:
        with self._read_gate():
            rows = self._conn.execute(
                "SELECT * FROM events WHERE aggregate_id = ? ORDER BY seq", (aggregate_id,)
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def get_upload(self, upload_id: str) -> dict[str, Any] | None:
        with self._read_gate():
            row = self._conn.execute(
                "SELECT * FROM uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_uploads(self, site_id: str | None = None) -> list[dict[str, Any]]:
        with self._read_gate():
            if site_id:
                rows = self._conn.execute(
                    "SELECT * FROM uploads WHERE site_id = ? ORDER BY rowid", (site_id,)
                ).fetchall()
            else:
                rows = self._conn.execute("SELECT * FROM uploads ORDER BY rowid").fetchall()
        return [dict(r) for r in rows]

    def next_version(self, conn: sqlite3.Connection, aggregate_id: str) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) AS v FROM events WHERE aggregate_id = ?",
            (aggregate_id,),
        ).fetchone()
        return int(row["v"]) + 1

    # ---- 写入 -----------------------------------------------------------

    def _insert_event(self, conn: sqlite3.Connection, event: dict[str, Any]) -> None:
        conn.execute(
            """INSERT INTO events
               (event_id, event_type, aggregate_type, aggregate_id, site_id,
                occurred_at, version, summary, payload, upload_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                event["event_id"],
                event["event_type"],
                event["aggregate_type"],
                event["aggregate_id"],
                event["site_id"],
                event["occurred_at"],
                event["version"],
                event["summary"],
                canonical_json(event["payload"]),
                event.get("upload_id"),
            ),
        )

    def accept_upload(
        self,
        site_id: str,
        upload_id: str,
        events: list[dict[str, Any]],
        *,
        content_hash: str | None = None,
    ) -> dict[str, Any]:
        """登记一个站点上传批次。返回 {'status': ..., 'reason': ..., 'count': n}。

        content_hash 为调用方（治理层）对原始记录算出的指纹；缺省时退化为
        对事件数组的指纹。
        """
        if not events:
            raise StoreError("空批次不予接收")
        fingerprint = content_hash or content_fingerprint(events)
        with self.write_lock() as conn:
            existing = conn.execute(
                "SELECT * FROM uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
            if existing is not None:
                if existing["site_id"] != site_id:
                    raise ConflictQuarantine(
                        "上传凭证曾被另一站点使用", upload_id, site_id
                    )
                if existing["status"] == "quarantined":
                    # 凭证一旦带毒就不再接受，站点必须更换凭证重传
                    raise ConflictQuarantine(
                        f"该上传凭证此前已隔离（{existing['reason']}），请使用新凭证重传",
                        upload_id,
                        site_id,
                    )
                if existing["content_hash"] == fingerprint:
                    return {"status": "duplicate", "reason": "内容一致，安全重传", "count": existing["event_count"]}
                # 已接受凭证出现不同内容：新来的整批隔离，原批次事实保持不动
                self._record_batch_quarantine(
                    conn, site_id, upload_id,
                    f"内容冲突：已存指纹 {existing['content_hash'][:12]}，新来 {fingerprint[:12]}",
                )
                raise ConflictQuarantine("同一上传凭证内容不一致，整批已隔离", upload_id, site_id)

            errors: list[str] = []
            for index, event in enumerate(events):
                event_errors = validate_event(event)
                if event.get("site_id") != site_id:
                    event_errors.append(f"事件 {index} 的 site_id 与上传站点不一致")
                if event.get("upload_id") and event["upload_id"] != upload_id:
                    event_errors.append(f"事件 {index} 的 upload_id 与凭证不一致")
                errors.extend(event_errors)
            if errors:
                conn.execute(
                    """INSERT INTO uploads (upload_id, site_id, content_hash, status, reason, event_count, created_at)
                       VALUES (?, ?, ?, 'quarantined', ?, 0, ?)""",
                    (upload_id, site_id, fingerprint, "；".join(errors[:8]), now_iso()),
                )
                self._record_batch_quarantine(
                    conn, site_id, upload_id, f"批次质量校验失败：{errors[0]}"
                )
                raise ConflictQuarantine(f"批次含不合法事件，整批已隔离：{errors[0]}", upload_id, site_id)

            first_seq: int | None = None
            for event in events:
                event.setdefault("upload_id", upload_id)
                self._insert_event(conn, event)
                if first_seq is None:
                    first_seq = conn.execute("SELECT last_insert_rowid() AS s").fetchone()["s"]
            try:
                conn.execute(
                    """INSERT INTO uploads (upload_id, site_id, content_hash, status, reason, event_count, first_seq, created_at)
                       VALUES (?, ?, ?, 'accepted', NULL, ?, ?, ?)""",
                    (upload_id, site_id, fingerprint, len(events), first_seq, now_iso()),
                )
            except sqlite3.IntegrityError as error:
                # 分类与接受之间凭证被并发占用：with 退出时回滚全部事件
                raise ConflictQuarantine(
                    f"上传凭证在处理期间被并发占用，整批已隔离：{error}", upload_id, site_id
                ) from error
        return {"status": "accepted", "reason": None, "count": len(events)}

    def flag_content_conflict(
        self, site_id: str, upload_id: str, new_fingerprint: str
    ) -> None:
        """已接受凭证出现不同内容：追加隔离事实，原批次事实不动。"""
        with self.write_lock() as conn:
            existing = conn.execute(
                "SELECT content_hash FROM uploads WHERE upload_id = ? AND status = 'accepted'",
                (upload_id,),
            ).fetchone()
            if existing is None:
                return
            reason = (
                f"内容冲突：已存指纹 {existing['content_hash'][:12]}，"
                f"新来 {new_fingerprint[:12]}"
            )
            self._record_batch_quarantine(conn, site_id, upload_id, reason)

    def classify_upload(
        self, site_id: str, upload_id: str, fingerprint: str
    ) -> tuple[str, dict[str, Any] | None]:
        """按原始内容指纹判定凭证状态：new / duplicate / conflict / poisoned。"""
        with self.write_lock() as conn:
            row = conn.execute(
                "SELECT * FROM uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
        if row is None:
            return "new", None
        existing = dict(row)
        if existing["site_id"] != site_id:
            return "conflict", existing
        if existing["status"] == "quarantined":
            return "poisoned", existing
        return ("duplicate" if existing["content_hash"] == fingerprint else "conflict"), existing

    def quarantine_upload(
        self,
        site_id: str,
        upload_id: str,
        fingerprint: str,
        reason: str,
    ) -> None:
        """语义校验失败时登记隔离：批次原始内容不落业务事件表，仅留隔离事实。"""
        with self.write_lock() as conn:
            existing = conn.execute(
                "SELECT status FROM uploads WHERE upload_id = ?", (upload_id,)
            ).fetchone()
            if existing is not None:
                # 幂等：同一失败凭证重复提交不再追加隔离事件
                return
            conn.execute(
                """INSERT INTO uploads (upload_id, site_id, content_hash, status, reason, event_count, created_at)
                   VALUES (?, ?, ?, 'quarantined', ?, 0, ?)""",
                (upload_id, site_id, fingerprint, reason, now_iso()),
            )
            self._record_batch_quarantine(conn, site_id, upload_id, reason)

    def _record_batch_quarantine(
        self,
        conn: sqlite3.Connection,
        site_id: str,
        upload_id: str,
        reason: str,
    ) -> None:
        """隔离事实本身是一条系统事件（不含批次原始内容）。"""
        aggregate_id = f"upload:{upload_id}"
        version = self.next_version(conn, aggregate_id)
        marker = make_event(
            event_id=f"quarantine-{upload_id}-{version}",
            event_type=BATCH_QUARANTINED,
            aggregate_type="site_upload",
            aggregate_id=aggregate_id,
            site_id=site_id,
            version=version,
            summary=f"站点批次隔离：{reason}",
            payload={"upload_id": upload_id, "site_id": site_id, "reason": reason},
        )
        self._insert_event(conn, marker)

    def append_system(self, events: list[dict[str, Any]]) -> int:
        """中央命令追加事件（冻结、发布、撤回传播等），返回首条 seq。

        版本冲突（另一写者已提交同一聚合的新版本）映射为 LibraryLocked，
        调用方按最新事件流重建后重试即可。
        """
        try:
            with self.write_lock() as conn:
                first_seq = conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM events").fetchone()["s"]
                for event in events:
                    errors = validate_event(event)
                    if errors:
                        raise StoreError(f"系统事件不合法：{errors[0]}")
                    self._insert_event(conn, event)
        except sqlite3.IntegrityError as error:
            raise LibraryLocked(f"并发写入冲突，事件库已更新，请重建后重试：{error}") from error
        return int(first_seq)

    # ---- 访问令牌 -------------------------------------------------------

    def mint_token(self, role: str, site_id: str | None = None) -> str:
        import secrets

        token = secrets.token_urlsafe(24)
        with self.write_lock() as conn:
            conn.execute(
                "INSERT INTO tokens (token_hash, role, site_id, issued_at) VALUES (?, ?, ?, ?)",
                (hash_token(token), role, site_id, now_iso()),
            )
        return token

    def authorize(self, token: str) -> dict[str, Any] | None:
        with self._read_gate():
            row = self._conn.execute(
                "SELECT role, site_id FROM tokens WHERE token_hash = ?", (hash_token(token),)
            ).fetchone()
        if not row:
            return None
        return {"role": row["role"], "site_id": row["site_id"]}

    # ---- 恢复与自检 -----------------------------------------------------

    def verify(self) -> dict[str, Any]:
        """重放前的完整性自检。"""
        with self.write_lock() as conn:
            total = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]
            distinct_ids = conn.execute("SELECT COUNT(DISTINCT event_id) AS n FROM events").fetchone()["n"]
            bad_versions = conn.execute(
                """SELECT COUNT(*) AS n FROM (
                       SELECT aggregate_id, version FROM events
                       GROUP BY aggregate_id, version HAVING COUNT(*) > 1
                   )""",
            ).fetchone()["n"]
            quarantined = conn.execute(
                "SELECT COUNT(*) AS n FROM uploads WHERE status = 'quarantined'"
            ).fetchone()["n"]
        if distinct_ids != total or bad_versions:
            raise StoreError("事件库完整性校验失败：存在重复 event_id 或重复聚合版本")
        return {"events": total, "quarantined_uploads": quarantined}

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> dict[str, Any]:
        event = {
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "aggregate_type": row["aggregate_type"],
            "aggregate_id": row["aggregate_id"],
            "site_id": row["site_id"],
            "occurred_at": row["occurred_at"],
            "version": row["version"],
            "summary": row["summary"],
            "payload": json.loads(row["payload"]),
        }
        if row["upload_id"]:
            event["upload_id"] = row["upload_id"]
        event["_seq"] = row["seq"]
        return event
