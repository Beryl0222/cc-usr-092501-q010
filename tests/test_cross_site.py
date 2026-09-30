"""场景一：跨区复查。

同一人在山东招募、广东复查：研究标识一致被识别为同一参与者，但两次访视
各自留痕、互不合并；中央产生跨区联动事件，站点间视图隔离。
"""

from __future__ import annotations

import unittest

from src.governance import GovernanceService, NotAuthorized
from src.repository import EventStore
from tests import factory

EXAM = "2026-09-10T09:00:00+08:00"
EXAM2 = "2026-11-02T10:00:00+08:00"
SD = "site-3701"
GD = "site-4401"


class CrossSiteReviewTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = EventStore(":memory:")
        self.service = GovernanceService(self.store)
        id_number = factory.make_id_number("370102", "19680512", "321")
        self.study_id = factory.study_id_of(id_number)

    def tearDown(self) -> None:
        self.store.close()

    def _shandong_baseline(self) -> None:
        result = self.service.ingest_upload(
            SD,
            "up-sd-1",
            [
                factory.enrollment(SD, self.study_id, 1968, "male", EXAM),
                factory.consent(self.study_id),
                factory.device("dev-SD"),
                factory.calibration("dev-SD", "2026-06-01T00:00:00+08:00"),
                factory.visit(
                    "V-SD-1", self.study_id, EXAM,
                    factory.baseline_sections("dev-SD", EXAM),
                ),
            ],
        )
        self.assertEqual(result.status, "accepted", result.reason)

    def _guangdong_devices(self) -> None:
        result = self.service.ingest_upload(
            GD,
            "up-gd-dev",
            [
                factory.device("dev-GD"),
                factory.calibration("dev-GD", "2026-08-01T00:00:00+08:00"),
            ],
        )
        self.assertEqual(result.status, "accepted", result.reason)

    def test_review_is_new_visit_not_merge(self) -> None:
        self._shandong_baseline()
        self._guangdong_devices()

        result = self.service.ingest_upload(
            GD,
            "up-gd-1",
            [
                factory.enrollment(GD, self.study_id, 1968, "male", EXAM2),
                factory.visit(
                    "V-GD-1", self.study_id, EXAM2,
                    factory.followup_sections("dev-GD", EXAM2),
                    visit_type="followup",
                ),
            ],
        )
        self.assertEqual(result.status, "accepted", result.reason)
        self.assertEqual(len(result.linkages), 1)
        self.assertEqual(result.linkages[0]["home_site_id"], SD)
        self.assertEqual(result.linkages[0]["reviewing_site_id"], GD)

        projection = self.service.build_projection()
        participant = projection.participants[self.study_id]
        self.assertEqual(participant.home_site_id, SD)
        self.assertEqual(set(participant.seen_sites), {SD, GD})
        self.assertEqual({"V-SD-1", "V-GD-1"}, set(projection.visits))
        # 两次访视属于不同站点、不同类型，互不合并
        visits = projection.visits.values()
        self.assertEqual({v.site_id for v in visits}, {SD, GD})

        linkage_events = [
            e for e in self.store.replay() if e["event_type"] == "CROSS_SITE_LINK_RESOLVED"
        ]
        self.assertEqual(len(linkage_events), 1)

    def test_site_visibility_isolation(self) -> None:
        self._shandong_baseline()
        with self.assertRaises(NotAuthorized):
            self.service.participant_view(self.study_id, {"role": "site_user", "site_id": GD})
        view = self.service.participant_view(self.study_id, {"role": "site_user", "site_id": SD})
        self.assertEqual(len(view["visits"]), 1)
        self.assertEqual(view["visits"][0]["site_id"], SD)


if __name__ == "__main__":
    unittest.main()
