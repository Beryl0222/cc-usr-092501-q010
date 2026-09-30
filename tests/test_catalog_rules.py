"""版本化规则目录：新问卷/新规则只能追加发布，访视按其时点规则接受。"""

from __future__ import annotations

import dataclasses
import unittest

from src.catalog import INSTRUMENT_BASELINE, QuestionnaireVersion
from src.governance import GovernanceService
from src.repository import EventStore
from tests import factory

SITE = "site-3701"
EXAM = "2026-09-10T09:00:00+08:00"


class CatalogPublishingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = EventStore(":memory:")
        self.service = GovernanceService(self.store)
        result = self.service.ingest_upload(
            SITE, "up-pre",
            [
                factory.enrollment(SITE, "P1", 1968, "male", EXAM),
                factory.consent("P1"),
                factory.device("dev-1"),
                factory.calibration("dev-1", "2026-08-01T00:00:00+08:00"),
            ],
        )
        self.assertEqual(result.status, "accepted", result.reason)

    def tearDown(self) -> None:
        self.store.close()

    def _questionnaire_visit(self, visit_id: str, upload: str, *, version: int, extra_items=None) -> None:
        sections = factory.baseline_sections("dev-1", EXAM)
        sections["questionnaire"]["version"] = version
        if extra_items:
            sections["questionnaire"]["items"].update(extra_items)
        result = self.service.ingest_upload(
            SITE, upload, [factory.visit(visit_id, "P1", EXAM, sections)]
        )
        self.assertEqual(result.status, "accepted", result.reason)

    def test_unknown_questionnaire_version_blocks_freeze(self) -> None:
        self._questionnaire_visit("V1", "up-v1", version=2)
        frozen = self.service.freeze_batch("S1", ["V1"])
        self.assertEqual(frozen["status"], "rejected")
        codes = {r["code"] for r in frozen["exclusions"][0]["reasons"]}
        self.assertIn("RULESET_MISMATCH", codes)

    def test_publish_new_questionnaire_and_ruleset(self) -> None:
        self._questionnaire_visit("V1", "up-v1", version=2)
        self.assertEqual(self.service.freeze_batch("S1", ["V1"])["status"], "rejected")

        # 发布问卷 v2
        q2 = QuestionnaireVersion(
            instrument_id=INSTRUMENT_BASELINE, version=2,
            required_items=("diabetes_history", "drinker_status", "alcohol_grams_per_day",
                            "prior_liver_disease", "hbv_vaccination"),
            published_at="2026-06-01T00:00:00+08:00",
        )
        self.service.publish_questionnaire(q2)

        # 发布规则 v2：接受问卷 v1/v2，校准有效期收紧为 90 天
        v2 = dataclasses.replace(
            self.service.catalog.get_ruleset(1),
            version=2,
            label="社区肝病队列基线规则 v2",
            effective_from="2026-06-01T00:00:00+08:00",
            questionnaire_versions={INSTRUMENT_BASELINE: frozenset({1, 2})},
            calibration_validity_days=90,
        )
        self.service.publish_ruleset(v2)

        # 条目补齐后同一条访视可以冻结
        result = self.service.ingest_upload(
            SITE, "up-corr",
            [{
                "record_type": "section_correction", "visit_id": "V1",
                "section": "questionnaire",
                "data": {
                    "instrument_id": INSTRUMENT_BASELINE, "version": 2,
                    "items": {
                        "diabetes_history": "none", "drinker_status": "never",
                        "alcohol_grams_per_day": 0, "prior_liver_disease": "no",
                        "hbv_vaccination": "yes",
                    },
                },
            }],
        )
        self.assertEqual(result.status, "accepted", result.reason)
        frozen = self.service.freeze_batch("S1", ["V1"])
        self.assertEqual(frozen["status"], "frozen", frozen)

        # v2 规则下校准 90 天：2026-01-01 的校准对 2026-09-20 的检查失效
        self.service.ingest_upload(
            SITE, "up-cal-old", [factory.calibration("dev-1", "2026-01-01T00:00:00+08:00")]
        )
        late = "2026-09-20T09:00:00+08:00"
        sections = factory.baseline_sections("dev-1", late, noninv=factory.valid_noninv("dev-1", late))
        sections["questionnaire"]["version"] = 2
        sections["questionnaire"]["items"]["hbv_vaccination"] = "yes"
        sections["noninvasive"]["exam_at"] = late
        self.service.ingest_upload(
            SITE, "up-v2", [factory.visit("V2", "P1", late, sections)]
        )
        frozen = self.service.freeze_batch("S2", ["V2"])
        self.assertEqual(frozen["status"], "rejected")
        codes = {r["code"] for r in frozen["exclusions"][0]["reasons"]}
        self.assertIn("CALIBRATION_INVALID", codes)

    def test_duplicate_ruleset_version_rejected(self) -> None:
        with self.assertRaises(Exception):
            self.service.publish_ruleset(self.service.catalog.get_ruleset(1))


if __name__ == "__main__":
    unittest.main()
