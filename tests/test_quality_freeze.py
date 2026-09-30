"""场景二/五相关：设备校准失效、质量门禁、迟到更正与已发布口径锁定。

* 校准过期/设备停用的无创结果不能通过质量门禁，访视不得冻结。
* 冻结后、发布前的迟到更正（新版本）影响复算结果。
* 发布后患病率快照永久保留原分母与口径，更正被拒绝写入已发布访视。
"""

from __future__ import annotations

import unittest

from src.governance import GovernanceError, GovernanceService
from src.repository import EventStore
from tests import factory

SITE = "site-3701"
EXAM = "2026-09-10T09:00:00+08:00"
SID = "P-001"
VID = "V-1"


def _good_batch(noninv: dict | None = None) -> list[dict]:
    return [
        factory.enrollment(SITE, SID, 1968, "male", EXAM),
        factory.consent(SID),
        factory.device("dev-1"),
        factory.visit(VID, SID, EXAM, factory.baseline_sections("dev-1", EXAM, noninv=noninv)),
    ]


class CalibrationAndQualityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = EventStore(":memory:")
        self.service = GovernanceService(self.store)

    def tearDown(self) -> None:
        self.store.close()

    def _calibrate(self, at: str, *, active: bool = True, upload: str = "up-cal") -> None:
        result = self.service.ingest_upload(
            SITE, upload, [factory.calibration("dev-1", at, active=active)]
        )
        self.assertEqual(result.status, "accepted", result.reason)

    def test_expired_calibration_blocks_freeze(self) -> None:
        result = self.service.ingest_upload(SITE, "up-1", _good_batch())
        self.assertEqual(result.status, "accepted", result.reason)
        # 校准于检查前 200 天，超过规则 v1 的 180 天有效期
        self._calibrate("2026-02-01T00:00:00+08:00")
        frozen = self.service.freeze_batch("S-1", [VID])
        self.assertEqual(frozen["status"], "rejected")
        codes = {r["code"] for r in frozen["exclusions"][0]["reasons"]}
        self.assertIn("CALIBRATION_INVALID", codes)

    def test_deactivated_device_blocks_freeze(self) -> None:
        result = self.service.ingest_upload(SITE, "up-1", _good_batch())
        self.assertEqual(result.status, "accepted", result.reason)
        self._calibrate("2026-08-01T00:00:00+08:00", active=False)
        frozen = self.service.freeze_batch("S-1", [VID])
        self.assertEqual(frozen["status"], "rejected")
        codes = {r["code"] for r in frozen["exclusions"][0]["reasons"]}
        self.assertIn("CALIBRATION_INVALID", codes)

    def test_valid_calibration_passes(self) -> None:
        result = self.service.ingest_upload(SITE, "up-1", _good_batch())
        self.assertEqual(result.status, "accepted", result.reason)
        self._calibrate("2026-08-01T00:00:00+08:00")
        frozen = self.service.freeze_batch("S-1", [VID])
        self.assertEqual(frozen["status"], "frozen", frozen)

    def test_quality_reasons_are_itemized(self) -> None:
        bad_noninv = factory.valid_noninv("dev-1", EXAM, lsm=8.0)
        bad_noninv.update(valid_shots=6, iqr_median_ratio=0.55)
        result = self.service.ingest_upload(SITE, "up-1", _good_batch(noninv=bad_noninv))
        self._calibrate("2026-08-01T00:00:00+08:00")
        frozen = self.service.freeze_batch("S-1", [VID])
        codes = {r["code"] for r in frozen["exclusions"][0]["reasons"]}
        self.assertIn("INSUFFICIENT_SHOTS", codes)
        self.assertIn("LOW_RELIABILITY", codes)


class LateCorrectionAndPublishedCaliberTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = EventStore(":memory:")
        self.service = GovernanceService(self.store)
        # LSM 7.5 + 低 FIB-4 → 灰区（indeterminate）
        noninv = factory.valid_noninv("dev-1", EXAM, lsm=7.5, ast=25.0, alt=30.0, platelets=220)
        self.service.ingest_upload(SITE, "up-1", _good_batch(noninv))
        self.service.ingest_upload(SITE, "up-cal", [factory.calibration("dev-1", "2026-08-01T00:00:00+08:00")])
        frozen = self.service.freeze_batch("S-1", [VID])
        self.assertEqual(frozen["status"], "frozen", frozen)

    def tearDown(self) -> None:
        self.store.close()

    def test_correction_before_publish_changes_recompute(self) -> None:
        before = self.service.recompute("S-1")
        self.assertEqual(before["strata"]["indeterminate"]["count"], 1)

        # 迟到更正：LSM 升到 12.0 → 高风险
        corrected = factory.valid_noninv("dev-1", EXAM, lsm=12.0, ast=25.0, alt=30.0, platelets=220)
        result = self.service.ingest_upload(
            SITE,
            "up-corr",
            [{"record_type": "section_correction", "visit_id": VID,
              "section": "noninvasive", "data": corrected}],
        )
        self.assertEqual(result.status, "accepted", result.reason)

        after = self.service.recompute("S-1")
        self.assertEqual(after["strata"]["high"]["count"], 1)
        self.assertEqual(after["strata"]["indeterminate"]["count"], 0)

        published = self.service.publish_snapshot("S-1")
        self.assertEqual(published["report"]["strata"]["high"]["count"], 1)

        # 发布后再更正同一访视：拒绝
        corrected2 = factory.valid_noninv("dev-1", EXAM, lsm=4.0)
        result = self.service.ingest_upload(
            SITE,
            "up-corr2",
            [{"record_type": "section_correction", "visit_id": VID,
              "section": "noninvasive", "data": corrected2}],
        )
        self.assertEqual(result.status, "quarantined")
        self.assertIn("已发布快照", result.reason)

        # 已发布快照仍按原分母与口径返回
        immutable = self.service.recompute("S-1")
        self.assertTrue(immutable["immutable"])
        self.assertEqual(immutable["strata"]["high"]["count"], 1)
        self.assertEqual(immutable["denominator"], 1)
        # 发布事件中封存的口径可追溯
        self.assertIn("members_counted", immutable["caliber"])

    def test_double_freeze_rejected(self) -> None:
        frozen = self.service.freeze_batch("S-2", [VID])
        self.assertEqual(frozen["status"], "rejected")
        self.assertIn("ALREADY_FROZEN", {r["code"] for r in frozen["exclusions"][0]["reasons"]})


if __name__ == "__main__":
    unittest.main()
