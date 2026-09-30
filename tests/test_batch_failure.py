"""场景四：部分批次失败与上传凭证语义。

* 同一上传凭证内容一致 → 安全重传，事件不重复。
* 同一凭证内容冲突 → 整个站点批次隔离，原批次事实不动。
* 批次内任一记录语义非法 → 整批原子拒绝，不产生半个访视。
* 一个站点的隔离绝不影响其他地区批次。
"""

from __future__ import annotations

import unittest

from src.governance import GovernanceService
from src.repository import ConflictQuarantine, EventStore
from tests import factory

SITE_A = "site-3701"
SITE_B = "site-4401"
EXAM = "2026-09-10T09:00:00+08:00"


def _complete_batch(site: str, pid: str, vid: str, dev: str) -> list[dict]:
    return [
        factory.enrollment(site, pid, 1968, "male", EXAM),
        factory.consent(pid),
        factory.device(dev),
        factory.calibration(dev, "2026-08-01T00:00:00+08:00"),
        factory.visit(vid, pid, EXAM, factory.baseline_sections(dev, EXAM)),
    ]


class UploadBatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = EventStore(":memory:")
        self.service = GovernanceService(self.store)

    def tearDown(self) -> None:
        self.store.close()

    def test_idempotent_safe_retransmit(self) -> None:
        records = _complete_batch(SITE_A, "P-A", "V-A", "dev-A")
        first = self.service.ingest_upload(SITE_A, "up-same", records)
        self.assertEqual(first.status, "accepted")
        # 5 条站点记录展开为 10 条领域事件（访视含 5 个组成部分）
        self.assertEqual(first.count, 10)
        again = self.service.ingest_upload(SITE_A, "up-same", [dict(r) for r in records])
        self.assertEqual(again.status, "duplicate")
        self.assertEqual(again.count, first.count)
        # 事件没有重复
        participants = [
            e for e in self.store.replay() if e["event_type"] == "PARTICIPANT_ENROLLED"
        ]
        self.assertEqual(len(participants), 1)

    def test_conflicting_content_quarantines_whole_site_batch(self) -> None:
        good = _complete_batch(SITE_A, "P-A", "V-A", "dev-A")
        first = self.service.ingest_upload(SITE_A, "up-1", good)
        self.assertEqual(first.status, "accepted")

        conflict = [dict(r) for r in good]
        conflict[0] = factory.enrollment(SITE_A, "P-A", 1972, "female", EXAM)  # 同一凭证不同内容
        result = self.service.ingest_upload(SITE_A, "up-1", conflict)
        self.assertEqual(result.status, "quarantined")
        self.assertIn("内容不一致", result.reason)

        # 原始批次事实保持：出生年份仍是 1968
        projection = self.service.build_projection()
        self.assertEqual(projection.participants["P-A"].birth_year, 1968)
        # 隔离留痕
        upload = self.store.get_upload("up-1")
        self.assertEqual(upload["status"], "accepted")
        quarantine_events = [
            e for e in self.store.replay() if e["event_type"] == "BATCH_QUARANTINED"
        ]
        self.assertEqual(len(quarantine_events), 1)

        # 毒凭证不能复活
        again = self.service.ingest_upload(SITE_A, "up-1", conflict)
        self.assertEqual(again.status, "quarantined")

    def test_one_bad_record_aborts_entire_batch(self) -> None:
        records = _complete_batch(SITE_A, "P-A", "V-A", "dev-A")
        records[2] = {"record_type": "visit", "visit_id": "GHOST",
                      "study_id": "NOBODY", "planned_at": EXAM, "sections": {}}
        result = self.service.ingest_upload(SITE_A, "up-bad", records)
        self.assertEqual(result.status, "quarantined")
        projection = self.service.build_projection()
        # 招募与设备也不能部分落库
        self.assertNotIn("P-A", projection.participants)
        self.assertNotIn("dev-A", projection.devices)
        self.assertEqual(len(projection.visits), 0)

    def test_quarantine_does_not_pollute_other_sites(self) -> None:
        bad = _complete_batch(SITE_A, "P-A", "V-A", "dev-A")
        bad.append({"record_type": "withdrawal", "study_id": "GHOST"})
        result = self.service.ingest_upload(SITE_A, "up-a-bad", bad)
        self.assertEqual(result.status, "quarantined")

        good_b = self.service.ingest_upload(SITE_B, "up-b-good", _complete_batch(SITE_B, "P-B", "V-B", "dev-B"))
        self.assertEqual(good_b.status, "accepted", good_b.reason)
        frozen_b = self.service.freeze_batch("S-B", ["V-B"])
        self.assertEqual(frozen_b["status"], "frozen", frozen_b)

        # A 站点批次修复后用新凭证可正常提交
        fixed = self.service.ingest_upload(SITE_A, "up-a-fixed", _complete_batch(SITE_A, "P-A", "V-A", "dev-A"))
        self.assertEqual(fixed.status, "accepted", fixed.reason)

    def test_upload_id_reuse_by_other_site_rejected(self) -> None:
        self.service.ingest_upload(SITE_A, "up-shared", _complete_batch(SITE_A, "P-A", "V-A", "dev-A"))
        result = self.service.ingest_upload(SITE_B, "up-shared", _complete_batch(SITE_B, "P-B", "V-B", "dev-B"))
        self.assertEqual(result.status, "quarantined")
        self.assertIn("另一站点", result.reason)


if __name__ == "__main__":
    unittest.main()
