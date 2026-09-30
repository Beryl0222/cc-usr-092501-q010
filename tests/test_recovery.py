"""服务恢复：关闭后重开事件库，重放事件流完整重建投影。

模拟进程崩溃：事件与上传登记在同一事务提交，未提交的批次在磁盘上不留痕；
重新打开服务后状态一致，verify() 自检通过。
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.governance import GovernanceService
from src.repository import EventStore
from tests import factory

SITE = "site-3701"
EXAM = "2026-09-10T09:00:00+08:00"


def _seed(service: GovernanceService) -> None:
    records = [
        factory.enrollment(SITE, "P1", 1968, "male", EXAM),
        factory.consent("P1"),
        factory.device("dev-1"),
        factory.calibration("dev-1", "2026-08-01T00:00:00+08:00"),
        factory.visit("V1", "P1", EXAM, factory.baseline_sections("dev-1", EXAM)),
        {"record_type": "referral", "referral_id": "R1", "study_id": "P1",
         "visit_id": "V1", "band": "high"},
    ]
    result = service.ingest_upload(SITE, "up-1", records)
    assert result.status == "accepted", result.reason


class RecoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp.name) / "cohort.db")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_replay_rebuilds_state_after_restart(self) -> None:
        store = EventStore(self.db_path)
        service = GovernanceService(store)
        _seed(service)
        frozen = service.freeze_batch("S1", ["V1"])
        self.assertEqual(frozen["status"], "frozen")
        report_before = service.recompute("S1")
        store.close()

        # 重新打开：状态完全由事件流重建
        store2 = EventStore(self.db_path)
        service2 = GovernanceService(store2)
        self.assertEqual(store2.verify()["events"] >= 8, True)
        projection = service2.build_projection()
        self.assertIn("P1", projection.participants)
        self.assertIn("V1", projection.visits)
        self.assertEqual(projection.visits["V1"].frozen_snapshot, "S1")
        self.assertEqual(projection.referrals["R1"]["band"], "high")
        report_after = service2.recompute("S1")
        self.assertEqual(report_after["strata"], report_before["strata"])
        store2.close()

    def test_uncommitted_batch_leaves_no_trace(self) -> None:
        store = EventStore(self.db_path)
        _seed(GovernanceService(store))
        events_before = store.replay()
        # 直接开一个未提交事务，模拟进程在提交前崩溃后回滚
        conn = store._conn
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO events (event_id,event_type,aggregate_type,aggregate_id,site_id,"
            "occurred_at,version,summary,payload) VALUES "
            "('ghost','WITHDRAWAL_APPLIED','participant_link','participant:GHOST','x',"
            "'2026-09-10T00:00:00+08:00',1,'x','{}')"
        )
        conn.execute("ROLLBACK")
        events_after = store.replay()
        self.assertEqual(len(events_after), len(events_before))
        self.assertNotIn("ghost", [e["event_id"] for e in events_after])
        store.close()

        # 重启后幽灵记录不存在，服务正常
        store2 = EventStore(self.db_path)
        projection = GovernanceService(store2).build_projection()
        self.assertNotIn("participant:GHOST", projection.aggregate_versions)
        store2.close()

    def test_quarantine_marker_survives_restart(self) -> None:
        store = EventStore(self.db_path)
        service = GovernanceService(store)
        service.ingest_upload(
            SITE, "up-bad",
            [factory.enrollment(SITE, "P1", 1968, "male", EXAM),
             {"record_type": "visit", "visit_id": "V?", "study_id": "NOBODY",
              "planned_at": EXAM, "sections": {}}],
        )
        store.close()
        store2 = EventStore(self.db_path)
        upload = store2.get_upload("up-bad")
        self.assertEqual(upload["status"], "quarantined")
        markers = [e for e in store2.replay() if e["event_type"] == "BATCH_QUARANTINED"]
        self.assertEqual(len(markers), 1)
        # 隔离原因可序列化
        json.dumps(markers[0], ensure_ascii=False)
        store2.close()


if __name__ == "__main__":
    unittest.main()
