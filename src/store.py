"""SQLite 事件存储与运行时表。

事件表只追加：唯一约束保证
- event_id 不复用；
- 同一聚合上的版本号不冲突（乐观并发）。

上传批次、凭证、冻结锁是运行时状态，放在独立表里；其中冻结锁带持有者和
过期时间，服务崩溃重启后可依据过期时间恢复（ANALYSIS_LOCKED/UNLOCKED
事件仍记录在事件流中用于审计）。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL UNIQUE,
    event_type     TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    version        INTEGER NOT NULL,
    occurred_at    TEXT NOT NULL,
    summary        TEXT NOT NULL,
    site_id        TEXT,
    upload_id      TEXT,
    payload        TEXT NOT NULL DEFAULT '{}',
    UNIQUE(aggregate_type, aggregate_id, version)
);

CREATE TABLE IF NOT EXISTS sites (
    site_id    TEXT PRIMARY KEY,
    province   TEXT NOT NULL,
    name       TEXT NOT NULL,
    salt       TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tokens (
    token_id   TEXT PRIMARY KEY,
    site_id    TEXT NOT NULL REFERENCES sites(site_id),
    active     INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS upload_batches (
    upload_id    TEXT PRIMARY KEY,
    token_id     TEXT NOT NULL,
    site_id      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    status       TEXT NOT NULL CHECK(status IN ('accepted', 'quarantined')),
    reason       TEXT,
    received_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS token_first_use (
    token_id      TEXT PRIMARY KEY,
    content_hash  TEXT NOT NULL,
    upload_id     TEXT NOT NULL,
    status        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS upload_rejections (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    upload_id    TEXT NOT NULL,
    token_id     TEXT NOT NULL,
    site_id      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    reason       TEXT NOT NULL,
    received_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS freeze_locks (
    scope       TEXT PRIMARY KEY,
    holder      TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    user_key TEXT PRIMARY KEY,
    role     TEXT NOT NULL,
    site_id  TEXT REFERENCES sites(site_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS linkage_codes (
    code          TEXT PRIMARY KEY,
    pid           TEXT NOT NULL,
    site_id       TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    consumed_at   TEXT,
    consumer_pid  TEXT,
    consumer_site TEXT
);
"""


class StoreError(Exception):
    """存储层错误基类。"""


class VersionConflict(StoreError):
    """事件 ID 或聚合版本冲突（并发追加/事件复用）。"""


class LockBusy(StoreError):
    """冻结锁被其他持有者占用。"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class EventStore:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- 事务 ----------
    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """串行化写事务：同一进程内用 RLock，进程内/外靠 BEGIN IMMEDIATE。"""
        conn = self._conn
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    # ---------- 事件追加 ----------
    @staticmethod
    def _row_payload(event: dict[str, Any]) -> tuple:
        return (
            event["event_id"],
            event["event_type"],
            event["aggregate_type"],
            event["aggregate_id"],
            event["version"],
            event["occurred_at"],
            event["summary"],
            event.get("site_id"),
            event.get("upload_id"),
            json.dumps(event.get("payload", {}), ensure_ascii=False, sort_keys=True),
        )

    def append(self, event: dict[str, Any]) -> int:
        with self.transaction() as conn:
            return self._insert(conn, event)

    def append_many(self, events: list[dict[str, Any]]) -> list[int]:
        if not events:
            return []
        with self.transaction() as conn:
            return [self._insert(conn, event) for event in events]

    def _insert(self, conn: sqlite3.Connection, event: dict[str, Any]) -> int:
        cur = conn.execute(
            """INSERT INTO events(event_id, event_type, aggregate_type, aggregate_id,
                                  version, occurred_at, summary, site_id, upload_id, payload)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            self._row_payload(event),
        )
        return int(cur.lastrowid)

    # ---------- 读取 ----------
    def events(self, after_seq: int = 0) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE seq > ? ORDER BY seq", (after_seq,)
            ).fetchall()
        return [self._decode(row) for row in rows]

    def events_for(self, aggregate_type: str, aggregate_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE aggregate_type=? AND aggregate_id=? ORDER BY seq",
                (aggregate_type, aggregate_id),
            ).fetchall()
        return [self._decode(row) for row in rows]

    @staticmethod
    def _decode(row: sqlite3.Row) -> dict[str, Any]:
        event = {
            "seq": row["seq"],
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "aggregate_type": row["aggregate_type"],
            "aggregate_id": row["aggregate_id"],
            "version": row["version"],
            "occurred_at": row["occurred_at"],
            "summary": row["summary"],
            "payload": json.loads(row["payload"] or "{}"),
        }
        if row["site_id"] is not None:
            event["site_id"] = row["site_id"]
        if row["upload_id"] is not None:
            event["upload_id"] = row["upload_id"]
        return event

    def latest_seq(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COALESCE(MAX(seq),0) AS s FROM events").fetchone()
        return int(row["s"])

    # ---------- 站点 / 凭证 ----------
    def create_site(self, site_id: str, province: str, name: str, salt: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO sites(site_id, province, name, salt, created_at) VALUES (?,?,?,?,?)",
                (site_id, province, name, salt, now_iso()),
            )

    def get_site(self, site_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM sites WHERE site_id=?", (site_id,)
            ).fetchone()

    def site_salt(self, site_id: str) -> str:
        row = self.get_site(site_id)
        if row is None:
            raise StoreError(f"未知站点：{site_id}")
        return row["salt"]

    def list_sites(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute("SELECT * FROM sites ORDER BY site_id").fetchall()

    def create_token(self, token_id: str, site_id: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO tokens(token_id, site_id, active, created_at) VALUES (?, ?, 1, ?)",
                (token_id, site_id, now_iso()),
            )

    def deactivate_token(self, token_id: str) -> None:
        with self.transaction() as conn:
            conn.execute("UPDATE tokens SET active=0 WHERE token_id=?", (token_id,))

    def get_token(self, token_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM tokens WHERE token_id=?", (token_id,)
            ).fetchone()

    # ---------- 上传批次 / 幂等 ----------
    def record_batch(
        self,
        upload_id: str,
        token_id: str,
        site_id: str,
        content_hash: str,
        status: str,
        reason: str | None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO upload_batches(upload_id, token_id, site_id, content_hash,
                                              status, reason, received_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (upload_id, token_id, site_id, content_hash, status, reason, now_iso()),
            )

    def get_batch(self, upload_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM upload_batches WHERE upload_id=?", (upload_id,)
            ).fetchone()

    def list_batches(self, site_id: str | None = None) -> list[sqlite3.Row]:
        with self._lock:
            if site_id:
                rows = self._conn.execute(
                    "SELECT * FROM upload_batches WHERE site_id=? ORDER BY rowid", (site_id,)
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM upload_batches ORDER BY rowid"
                ).fetchall()
        return rows

    def first_use(self, token_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM token_first_use WHERE token_id=?", (token_id,)
            ).fetchone()

    def remember_first_use(
        self, token_id: str, content_hash: str, upload_id: str, status: str
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO token_first_use(token_id, content_hash, upload_id, status)
                   VALUES (?,?,?,?)
                   ON CONFLICT(token_id) DO NOTHING""",
                (token_id, content_hash, upload_id, status),
            )

    # ---------- 冻结锁 ----------
    def acquire_freeze_lock(
        self, scope: str, holder: str, ttl_seconds: float, now: datetime | None = None
    ) -> bool:
        """尝试获取冻结锁。已被他人持有且未过期返回 False；过期锁可接管（恢复）。"""
        moment = (now or datetime.now(timezone.utc))
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM freeze_locks WHERE scope=?", (scope,)
            ).fetchone()
            if row is not None:
                expires = datetime.fromisoformat(row["expires_at"])
                if row["holder"] == holder:
                    conn.execute(
                        "UPDATE freeze_locks SET acquired_at=?, expires_at=? WHERE scope=?",
                        (moment.isoformat(), _plus(moment, ttl_seconds).isoformat(), scope),
                    )
                    return True
                if expires > moment:
                    return False
                # 过期锁：崩溃恢复，接管
            conn.execute(
                """INSERT INTO freeze_locks(scope, holder, acquired_at, expires_at)
                   VALUES (?,?,?,?)
                   ON CONFLICT(scope) DO UPDATE SET holder=excluded.holder,
                        acquired_at=excluded.acquired_at, expires_at=excluded.expires_at""",
                (scope, holder, moment.isoformat(), _plus(moment, ttl_seconds).isoformat()),
            )
            return True

    def refresh_freeze_lock(self, scope: str, holder: str, ttl_seconds: float) -> None:
        moment = datetime.now(timezone.utc)
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM freeze_locks WHERE scope=?", (scope,)
            ).fetchone()
            if row is None or row["holder"] != holder:
                raise LockBusy("锁已丢失或被接管")
            conn.execute(
                "UPDATE freeze_locks SET expires_at=? WHERE scope=?",
                (_plus(moment, ttl_seconds).isoformat(), scope),
            )

    def release_freeze_lock(self, scope: str, holder: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "DELETE FROM freeze_locks WHERE scope=? AND holder=?", (scope, holder)
            )

    def lock_holder(self, scope: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM freeze_locks WHERE scope=?", (scope,)
            ).fetchone()
        if row is None:
            return None
        expires = datetime.fromisoformat(row["expires_at"])
        if expires <= datetime.now(timezone.utc):
            return None
        return row["holder"]

    # ---------- 用户 ----------
    def create_user(self, user_key: str, role: str, site_id: str | None = None) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO users(user_key, role, site_id, created_at) VALUES (?,?,?,?)",
                (user_key, role, site_id, now_iso()),
            )

    def get_user(self, user_key: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM users WHERE user_key=?", (user_key,)
            ).fetchone()

    # ---------- 跨区关联码 ----------
    def issue_linkage_code(self, code: str, pid: str, site_id: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO linkage_codes(code, pid, site_id, created_at)
                   VALUES (?,?,?,?)""",
                (code, pid, site_id, now_iso()),
            )

    def get_linkage_code(self, code: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM linkage_codes WHERE code=?", (code,)
            ).fetchone()

    def consume_linkage_code(
        self, code: str, consumer_pid: str, consumer_site: str
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """UPDATE linkage_codes
                   SET consumed_at=?, consumer_pid=?, consumer_site=?
                   WHERE code=? AND consumed_at IS NULL""",
                (now_iso(), consumer_pid, consumer_site, code),
            )

    def append_event_and_consume_linkage(
        self, event: dict[str, Any], code: str,
        consumer_pid: str, consumer_site: str,
    ) -> int:
        """原子地落关联事件并消费一次性码。"""
        with self.transaction() as conn:
            seq = self._insert(conn, event)
            cur = conn.execute(
                """UPDATE linkage_codes
                   SET consumed_at=?, consumer_pid=?, consumer_site=?
                   WHERE code=? AND consumed_at IS NULL""",
                (now_iso(), consumer_pid, consumer_site, code),
            )
            if cur.rowcount == 0:
                raise StoreError("关联码已被使用或无效")
        return seq


def _plus(moment: datetime, seconds: float) -> datetime:
    from datetime import timedelta

    return moment + timedelta(seconds=seconds)
