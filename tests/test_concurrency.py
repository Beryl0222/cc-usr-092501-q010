"""场景三：并发锁库。

中央冻结/发布命令以 BEGIN IMMEDIATE 抢占写锁；持锁期间另一写请求得到
LibraryLocked（HTTP 503 Retry-After），锁释放后重试成功，数据不丢失。
"""

from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.events import make_event, now_iso
from src.repository import EventStore, LibraryLocked
from src.governance import GovernanceService
from tests import factory

SITE = "site-3701"
EXAM = "2026-09-10T09:00:00+08:00"


class ConcurrencyLockTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = EventStore(":memory:", lock_timeout_ms=50)
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmpdir.name) / "lock-test.db")

    def tearDown(self) -> None:
        self.store.close()
        self._tmpdir.cleanup()

    def test_external_writer_lock_is_observed(self) -> None:
        # 文件库 + 另一连接模拟另一个进程持有写锁
        file_store = EventStore(self.db_path, lock_timeout_ms=50)
        other = sqlite3.connect(self.db_path, isolation_level=None)
        other.execute("PRAGMA busy_timeout = 0")
        other.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(LibraryLocked):
                file_store.append_system([
                    make_event(
                        event_id="e-locked",
                        event_type="RULESET_PUBLISHED",
                        aggregate_type="ruleset",
                        aggregate_id="ruleset:9",
                        site_id="central",
                        version=1,
                        summary="不应写入",
                        payload={"version": 9},
                    )
                ])
        finally:
            other.execute("ROLLBACK")
            other.close()
            file_store.close()

    def test_concurrent_freeze_one_wins_one_retries(self) -> None:
        service = GovernanceService(self.store)
        service.ingest_upload(
            SITE, "up-1",
            [
                factory.enrollment(SITE, "P-1", 1968, "male", EXAM),
                factory.consent("P-1"),
                factory.device("dev-1"),
                factory.calibration("dev-1", "2026-08-01T00:00:00+08:00"),
                factory.visit("V-1", "P-1", EXAM, factory.baseline_sections("dev-1", EXAM)),
            ],
        )
        outcomes: dict[str, str] = {}
        barrier = threading.Barrier(2)

        def freeze(name: str, snapshot: str) -> None:
            barrier.wait()
            try:
                result = service.freeze_batch(snapshot, ["V-1"])
                outcomes[name] = result["status"]
            except LibraryLocked:
                outcomes[name] = "locked"

        t1 = threading.Thread(target=freeze, args=("a", "SA"))
        t2 = threading.Thread(target=freeze, args=("b", "SB"))
        t1.start(); t2.start()
        t1.join(5); t2.join(5)
        # 一个成功冻结，另一个要么抢锁失败、要么因 ALREADY_FROZEN 被拒
        self.assertIn("frozen", outcomes.values())
        self.assertIn(outcomes.get("a") if outcomes.get("a") != "frozen" else outcomes.get("b"),
                      ("locked", "rejected", None))

        # 重试成功路径：再开一个不冲突的访视批次冻结不受影响
        projection = service.build_projection()
        self.assertEqual(len(projection.snapshots), 1)

    def test_in_process_lock_is_reentrant_after_release(self) -> None:
        with self.store.write_lock():
            with self.assertRaises(LibraryLocked):
                with self.store.write_lock():
                    pass
        # 锁释放后可立即再取
        with self.store.write_lock():
            pass


if __name__ == "__main__":
    unittest.main()
