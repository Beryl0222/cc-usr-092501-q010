"""HTTP API 端到端测试：鉴权、分权、溯源、上传状态码与锁库。"""

from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from src.api import build_server
from tests.world import (
    batch, enroll_consent, p, setup_world_with_master, upload, visit,
    make_world,
)


def _request(server, method: str, path: str, user_key: str | None = None,
             body: dict | bytes | None = None) -> tuple[int, dict]:
    port = server.server_address[1]
    url = f"http://127.0.0.1:{port}{path}"
    headers = {}
    data = None
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(
            body, ensure_ascii=False
        ).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if user_key:
        headers["Authorization"] = f"Bearer {user_key}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_world(":memory:")
        setup_world_with_master(self.svc)
        self.server = build_server(self.svc, "127.0.0.1", 0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.svc.close()

    def test_auth_required(self) -> None:
        code, body = _request(self.server, "GET", "/participants")
        self.assertEqual(code, 403)
        self.assertEqual(body["error"], "access_denied")

    def test_research_and_referral_channels_separated(self) -> None:
        # 临床医生可读转诊，但研究端点 403
        code, _ = _request(self.server, "GET", "/referrals", "doctor01")
        self.assertEqual(code, 200)
        code, body = _request(self.server, "GET", "/participants", "doctor01")
        self.assertEqual(code, 403)
        # 站点协调员相反
        code, _ = _request(self.server, "GET", "/participants", "coord01")
        self.assertEqual(code, 200)
        code, _ = _request(self.server, "GET", "/referrals", "coord01")
        self.assertEqual(code, 403)

    def test_site_isolation_over_api(self) -> None:
        pid = p(self.svc, 1, "110101199001011234")
        upload(self.svc, "coord01", 1, "U-enr", enroll_consent(pid))
        code, _ = _request(self.server, "GET", f"/participants/{pid}", "coord02")
        self.assertEqual(code, 403)
        code, body = _request(self.server, "GET", f"/participants/{pid}", "coord01")
        self.assertEqual(code, 200)
        self.assertEqual(body["pid"], pid)

    def test_upload_quarantine_and_trace(self) -> None:
        pid = p(self.svc, 1, "11010119900202223X")
        admin = self.svc.principal("admin")
        token = self.svc.issue_token(admin, "S01")

        # 合格访视（含转诊）
        good = json.loads(batch("U-api",
            enroll_consent(pid) + [visit("V-api", pid, "2026-03-02",
                                         referral="LSM升高")]))
        good["token"] = token
        code, body = _request(self.server, "POST", "/uploads", "coord01", good)
        self.assertEqual(code, 200, body)
        self.assertEqual(body["status"], "accepted")

        # 同凭证重放 -> 200 幂等
        code, body = _request(self.server, "POST", "/uploads", "coord01", good)
        self.assertEqual(code, 200)
        self.assertTrue(body["idempotent"])

        # 溯源：研究通道能查到访视/规则版本/上传批次
        code, trace = _request(self.server, "GET", "/visits/V-api/trace",
                               "analyst")
        self.assertEqual(code, 200)
        self.assertEqual(trace["components"]["risk"]["rule_version"], 1)
        self.assertIn("U-api", trace["upload_ids"])
        self.assertTrue(trace["quality"]["components"]["risk"]["ok"])

        # 转诊在临床通道
        code, refs = _request(self.server, "GET", "/referrals", "doctor01")
        self.assertEqual(code, 200)
        self.assertTrue(any(r["referral_id"] == "R-V-api" for r in refs))

    def test_freeze_lock_returns_409_and_locked_upload_202(self) -> None:
        self.svc.store.acquire_freeze_lock("analysis:freeze", "other", 60)
        code, body = _request(self.server, "POST", "/freezes", "analyst",
                              {"holder": "me"})
        self.assertEqual(code, 409)
        self.assertEqual(body["error"], "lock_busy")

        pid = p(self.svc, 1, "110101199003033333")
        admin = self.svc.principal("admin")
        token = self.svc.issue_token(admin, "S01")
        payload = json.loads(batch("U-lk", enroll_consent(pid)))
        payload["token"] = token
        code, body = _request(self.server, "POST", "/uploads", "coord01", payload)
        self.assertEqual(code, 202)
        self.assertTrue(body["retryable"])

    def test_freeze_recompute_publish_flow(self) -> None:
        pid = p(self.svc, 1, "110101199004044444")
        upload(self.svc, "coord01", 1, "U-e", enroll_consent(pid))
        upload(self.svc, "coord01", 1, "U-v",
               [visit("V1", pid, "2026-03-02", level="high")])

        code, body = _request(self.server, "POST", "/freezes", "analyst", {})
        self.assertEqual(code, 201)
        sid = body["snapshot_id"]
        self.assertEqual(body["denominator"], 1)

        code, stats = _request(self.server, "POST",
                               f"/snapshots/{sid}/recompute", "analyst", {})
        self.assertEqual(code, 200)
        self.assertEqual(stats["denominator"], 1)

        code, _ = _request(self.server, "POST",
                           f"/snapshots/{sid}/publish", "analyst", {})
        self.assertEqual(code, 201)
        code, published = _request(
            self.server, "GET", f"/snapshots/{sid}/stats", "analyst"
        )
        self.assertEqual(code, 200)
        self.assertEqual(published["stats"]["denominator"], 1)

        # 协调员不能冻结
        code, _ = _request(self.server, "POST", "/freezes", "coord01", {})
        self.assertEqual(code, 403)


if __name__ == "__main__":
    unittest.main()
