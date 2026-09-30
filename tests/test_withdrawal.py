"""场景六：撤回传播。

撤回后：
* 未来分析停止——撤回者不能再进入新的冻结批次；
* 未发布快照的复算按许可处理：未许可保留聚合者从分母剔除，许可保留者保留；
* 已发布快照的原分母与口径不受撤回影响；
* 临床转诊记录在独立视图中仍可被临床角色访问，研究角色不可见。
"""

from __future__ import annotations

import unittest

from src.governance import GovernanceService, NotAuthorized
from src.repository import EventStore
from tests import factory

SITE = "site-3701"
EXAM = "2026-09-10T09:00:00+08:00"
PIDS = ("P1", "P2", "P3")
VIDS = ("V1", "V2", "V3")


def _participant_batch(pid: str, vid: str, *, lsm: float = 12.0, consent_retention: bool = True) -> list[dict]:
    return [
        factory.enrollment(SITE, pid, 1968, "male", EXAM),
        {
            "record_type": "consent",
            "study_id": pid,
            "permissions": {"future_analysis": True, "retain_aggregates": consent_retention},
        },
        factory.device(f"dev-{pid}"),
        factory.calibration(f"dev-{pid}", "2026-08-01T00:00:00+08:00"),
        factory.visit(
            vid, pid, EXAM,
            factory.baseline_sections(
                f"dev-{pid}", EXAM,
                noninv=factory.valid_noninv(f"dev-{pid}", EXAM, lsm=lsm),
            ),
        ),
        factory.referral(f"REF-{pid}", pid, vid, band="high"),
    ]


def _bootstrap(publish: bool) -> tuple[EventStore, GovernanceService]:
    store = EventStore(":memory:")
    service = GovernanceService(store)
    for pid, vid, retention in zip(PIDS, VIDS, (False, True, True)):
        result = service.ingest_upload(
            SITE, f"up-{pid}", _participant_batch(pid, vid, consent_retention=retention)
        )
        assert result.status == "accepted", result.reason
    frozen = service.freeze_batch("SNAP", list(VIDS))
    assert frozen["status"] == "frozen", frozen
    if publish:
        service.publish_snapshot("SNAP")
    return store, service


def _withdraw(service: GovernanceService, pid: str) -> None:
    result = service.ingest_upload(
        SITE, f"up-w-{pid}",
        [{"record_type": "withdrawal", "study_id": pid, "reason": "本人申请",
          "retain_aggregates": pid == "P2"}],
    )
    assert result.status == "accepted", result.reason


class WithdrawalPropagationTest(unittest.TestCase):
    def test_unpublished_snapshot_excludes_by_permission(self) -> None:
        store, service = _bootstrap(publish=False)
        _withdraw(service, "P1")  # 不许可保留 → 剔除
        _withdraw(service, "P2")  # 许可保留 → 计入

        report = service.recompute("SNAP")
        self.assertFalse(report["immutable"])
        self.assertEqual(report["denominator"], 2)
        excluded = {item["visit_id"]: item["stage"] for item in report["excluded"]}
        self.assertEqual(excluded, {"V1": "withdrawal_propagation"})
        self.assertEqual(
            report["excluded"][0]["reasons"][0]["code"], "WITHDRAWN_NO_RETENTION"
        )
        # 发布后该口径封存
        published = service.publish_snapshot("SNAP")
        self.assertEqual(published["report"]["denominator"], 2)
        again = service.recompute("SNAP")
        self.assertTrue(again["immutable"])
        self.assertEqual(again["denominator"], 2)
        store.close()

    def test_published_snapshot_is_immune(self) -> None:
        store, service = _bootstrap(publish=True)
        _withdraw(service, "P1")
        _withdraw(service, "P2")
        report = service.recompute("SNAP")
        self.assertTrue(report["immutable"])
        self.assertEqual(report["denominator"], 3)
        store.close()

    def test_withdrawn_participant_cannot_enter_new_batch(self) -> None:
        store, service = _bootstrap(publish=False)
        _withdraw(service, "P1")
        # V1 已在未发布批次中且参与者已撤回：新批次冻结被拒，原因同时留痕
        frozen = service.freeze_batch("S2", ["V1"])
        self.assertEqual(frozen["status"], "rejected")
        codes = {r["code"] for r in frozen["exclusions"][0]["reasons"]}
        self.assertIn("WITHDRAWN", codes)
        self.assertIn("ALREADY_FROZEN", codes)
        store.close()

    def test_clinical_referral_separated_from_research_view(self) -> None:
        store, service = _bootstrap(publish=True)
        _withdraw(service, "P1")
        with self.assertRaises(NotAuthorized):
            service.participant_view("P1", {"role": "researcher"})
        referral = service.referral_view("REF-P1", {"role": "clinician"})
        self.assertEqual(referral["study_id"], "P1")
        with self.assertRaises(NotAuthorized):
            service.referral_view("REF-P1", {"role": "researcher"})
        # 转诊结局更新继续留痕
        result = service.ingest_upload(
            SITE, "up-ref-out",
            [{"record_type": "referral_outcome", "referral_id": "REF-P1", "status": "attended"}],
        )
        self.assertEqual(result.status, "accepted", result.reason)
        referral = service.referral_view("REF-P1", {"role": "clinician"})
        self.assertEqual(referral["outcome"]["status"], "attended")
        store.close()

    def test_followup_loss_recorded_after_withdrawal(self) -> None:
        store, service = _bootstrap(publish=False)
        _withdraw(service, "P2")
        result = service.ingest_upload(
            SITE, "up-fu",
            [{"record_type": "followup", "study_id": "P2", "status": "lost_to_followup"}],
        )
        self.assertEqual(result.status, "accepted")
        projection = service.build_projection()
        self.assertEqual(projection.participants["P2"].followup_status, "lost_to_followup")
        self.assertTrue(projection.participants["P2"].withdrawal["retain_aggregates"])
        store.close()


if __name__ == "__main__":
    unittest.main()
