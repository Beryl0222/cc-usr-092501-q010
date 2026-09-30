"""HTTP API 端到端：角色分权、站点隔离、结果溯源、隔离 409 与锁库 503。"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from src.api import serve_in_thread
from src.repository import EventStore
from tests import factory

SITE = "site-3701"
EXAM = "2026-09-10T09:00:00+08:00"


def _seed(service) -> None:
    records = [
        factory.enrollment(SITE, "P1", 1968, "male", EXAM),
        factory.consent("P1"),
        factory.device("dev-1"),
        factory.calibration("dev-1", "2026-08-01T00:00:00+08:00"),
        factory.visit(
            "V1", "P1", EXAM,
            factory.baseline_sections(
                "dev-1", EXAM,
                noninv=factory.valid_noninv("dev-1", EXAM, lsm=12.0),
            ),
        ),
        {"record_type": "referral", "referral_id": "R1", "study_id": "P1",
         "visit_id": "V1", "band": "high"},
    ]
    result = service.ingest_upload(SITE, "up-1", records)
    assert result.status == "accepted", result.reason
    assert service.freeze_batch("S1", ["V1"])["status"] == "frozen"


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp.name) / "api.db")
        self.store = EventStore(self.db_path)
        self.server, self.service, self.thread = serve_in_thread(self.store)
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        _seed(self.service)
        self.tokens = {
            "coordinator": self.store.mint_token("coordinator"),
            "researcher": self.store.mint_token("researcher"),
            "clinician": self.store.mint_token("clinician"),
            "site": self.store.mint_token("site_user", SITE),
            "site_other": self.store.mint_token("site_user", "site-4401"),
        }

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.store.close()
        self._tmp.cleanup()

    def _request(self, method: str, path: str, token: str | None = None, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read().decode())

    def test_health_is_open(self) -> None:
        status, body = self._request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")

    def test_missing_and_bad_token_rejected(self) -> None:
        self.assertEqual(self._request("GET", "/participants/P1")[0], 403)
        self.assertEqual(self._request("GET", "/participants/P1", "not-a-token")[0], 403)

    def test_site_isolation(self) -> None:
        status, _ = self._request("GET", "/participants/P1", self.tokens["site_other"])
        self.assertEqual(status, 403)
        status, body = self._request("GET", "/participants/P1", self.tokens["site"])
        self.assertEqual(status, 200)
        self.assertEqual(body["study_id"], "P1")

    def test_site_cannot_upload_for_other_site(self) -> None:
        status, body = self._request(
            "POST", "/uploads/site-4401/up-x", self.tokens["site"],
            [factory.enrollment("site-4401", "PX", 1970, "female", EXAM)],
        )
        self.assertEqual(status, 403)

    def test_clinician_referral_separation(self) -> None:
        # 临床角色看不到研究参与者视图中的无创结果
        status, _ = self._request("GET", "/participants/P1", self.tokens["clinician"])
        self.assertEqual(status, 200)
        status, body = self._request("GET", "/referrals/R1", self.tokens["clinician"])
        self.assertEqual(status, 200)
        self.assertEqual(body["band"], "high")
        # 研究角色不能访问临床转诊
        self.assertEqual(self._request("GET", "/referrals/R1", self.tokens["researcher"])[0], 403)

    def test_result_traceability(self) -> None:
        status, trace = self._request("GET", "/visits/V1/trace", self.tokens["coordinator"])
        self.assertEqual(status, 200)
        noninv = trace["sections"]["noninvasive"]
        self.assertEqual(noninv["upload_id"], "up-1")
        self.assertEqual(noninv["current_version"], 1)
        self.assertTrue(noninv["event_id"].startswith("up-1#4"))
        self.assertEqual(trace["frozen"]["snapshot_id"], "S1")
        self.assertEqual(trace["ruleset_version_at_visit"], 1)

    def test_coordinator_freeze_publish_recompute_flow(self) -> None:
        status, body = self._request(
            "POST", "/admin/freeze", self.tokens["coordinator"],
            {"snapshot_id": "S2", "visit_ids": [], "exclude_failures": True},
        )
        self.assertIn(status, (200, 400))
        status, report = self._request("GET", "/snapshots/S1/report", self.tokens["coordinator"])
        self.assertEqual(status, 200)
        self.assertEqual(report["denominator"], 1)
        self.assertEqual(report["strata"]["high"]["count"], 1)
        status, _ = self._request("POST", "/admin/publish/S1", self.tokens["coordinator"])
        self.assertEqual(status, 200)
        # 研究员可看报告，但不能发布
        self.assertEqual(self._request("GET", "/snapshots/S1/report", self.tokens["researcher"])[0], 200)
        self.assertEqual(self._request("POST", "/admin/publish/S1", self.tokens["researcher"])[0], 403)

    def test_quarantine_returns_409(self) -> None:
        status, body = self._request(
            "POST", f"/uploads/{SITE}/up-1", self.tokens["site"],
            [factory.enrollment(SITE, "P1", 1999, "female", EXAM)],
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["status"], "quarantined")
        self.assertIn("内容不一致", body["reason"])

    def test_idempotent_retransmit_via_api(self) -> None:
        records = [
            factory.enrollment(SITE, "P9", 1980, "female", EXAM),
            factory.consent("P9"),
        ]
        s1, b1 = self._request("POST", f"/uploads/{SITE}/up-9", self.tokens["site"], records)
        s2, b2 = self._request("POST", f"/uploads/{SITE}/up-9", self.tokens["site"], records)
        self.assertEqual((s1, b1["status"]), (200, "accepted"))
        self.assertEqual((s2, b2["status"]), (200, "duplicate"))

    def test_locked_library_returns_503_retry_after(self) -> None:
        other = sqlite3.connect(self.db_path, isolation_level=None)
        other.execute("PRAGMA busy_timeout = 0")
        other.execute("BEGIN IMMEDIATE")
        try:
            status, body = self._request(
                "POST", f"/uploads/{SITE}/up-locked", self.tokens["site"],
                [factory.enrollment(SITE, "PL", 1970, "male", EXAM)],
            )
            self.assertEqual(status, 503)
            self.assertEqual(body["error"], "library_locked")
        finally:
            other.execute("ROLLBACK")
            other.close()
        # 锁释放后重试成功（服务恢复）
        status, body = self._request(
            "POST", f"/uploads/{SITE}/up-locked", self.tokens["site"],
            [factory.enrollment(SITE, "PL", 1970, "male", EXAM)],
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "accepted")


if __name__ == "__main__":
    unittest.main()
