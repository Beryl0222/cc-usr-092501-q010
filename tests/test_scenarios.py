"""六个规定场景的自动化验证：

1. 跨区复查（化名不同、关联码一次性、基线去重）
2. 设备校准失效（校准标识不匹配 / 超出有效期）
3. 并发锁库（第二冻结被拒、锁库期上传可重试）
4. 部分批次失败（整批隔离、不污染其他地区与后续批次）
5. 撤回传播（retain/remove、停止未来分析、已发布快照不变）
6. 服务恢复（重开重建投影、过期锁接管、迟到更正影响未发布统计）
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.contracts import Reasons
from src.service import CohortService
from src.store import LockBusy
from tests.world import (
    batch, enroll_consent, master_commands, nitx, p, setup_world_with_master,
    upload, visit, make_world,
)


class ScenarioTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_world(":memory:")
        setup_world_with_master(self.svc)

    def tearDown(self) -> None:
        self.svc.close()

    def _freeze_stats(self, holder="analyst"):
        analyst = self.svc.principal("analyst")
        event = self.svc.freeze(analyst, holder)
        sid = event["aggregate_id"]
        return sid, self.svc.recompute(analyst, sid)

    # ---------- 1. 跨区复查 ----------
    def test_cross_region_revisit(self) -> None:
        svc = self.svc
        bj, sh = svc.principal("coord01"), svc.principal("coord02")
        pid_bj = p(svc, 1, "110101199001011234")
        pid_sh = p(svc, 2, "110101199001011234")
        self.assertNotEqual(pid_bj, pid_sh, "跨站点化名必须不同，防止凭标识关联")

        upload(svc, "coord01", 1, "U-bj-enroll", enroll_consent(pid_bj))
        upload(svc, "coord01", 1, "U-bj-visit",
               [visit("V-bj", pid_bj, "2026-03-02", level="high")])

        # 上海复查登记为独立参与者
        upload(svc, "coord02", 2, "U-sh-enroll", enroll_consent(pid_sh, "2026-05-01"))
        code = svc.issue_linkage_code(bj, pid_bj)
        linked = svc.consume_linkage_code(sh, code, pid_sh)
        self.assertEqual(linked["canonical_pid"], pid_bj)
        self.assertIn(pid_sh, linked["aliases"])

        # 关联码一次性
        with self.assertRaises(Exception):
            svc.consume_linkage_code(sh, code, pid_sh)

        # 上海的合格基线不重复计数
        upload(svc, "coord02", 2, "U-sh-visit",
               [visit("V-sh", pid_sh, "2026-05-02", site_index=2,
                      diabetes="no", alcohol="none", level="medium")])
        _, stats = self._freeze_stats()
        self.assertEqual(stats["denominator"], 1)
        self.assertEqual(stats["exclusion_reasons"].get(Reasons.BASELINE_ALREADY_COUNTED), 1)

    # ---------- 2. 设备校准失效 ----------
    def test_device_calibration_failure(self) -> None:
        svc = self.svc
        pid = p(svc, 1, "110101199003033333")
        upload(svc, "coord01", 1, "U-enr", enroll_consent(pid))

        # 2a. 设备重新校准为 C01b；访视声称设备从未有过的 C99 -> 不匹配。
        #     （旧校准 C01 在其有效期内的历史访视仍应合格，不被新校准追溯否定）
        upload(svc, "coord01", 1, "U-cal2", [{
            "type": "calibration", "device_id": "D01", "calibration_id": "C01b",
            "calibrated_at": "2026-02-01T00:00:00+08:00",
        }])
        upload(svc, "coord01", 1, "U-v-mismatch", [
            visit("V-mismatch", pid, "2026-03-05", level="high",
                  device="D01", calibration="C99")
        ])

        # 2b. 声称 C01b，但测量日期晚于校准 + 180 天 -> 过期
        upload(svc, "coord01", 1, "U-v-expired", [
            visit("V-expired", pid, "2026-03-06", level="high",
                  device="D01", calibration="C01b")
        ])
        svc.upload(svc.principal("coord01"),
                   svc.issue_token(svc.principal("admin"), "S01"),
                   batch("U-v-expired2", [{
                       "type": "nitx_update", "visit_id": "V-expired",
                       **nitx("2026-09-10", device="D01", calibration="C01b"),
                   }]))

        # 另招一人，在旧校准 C01 有效期内测的访视，重新校准后仍应合格
        pid_old = p(svc, 1, "110101199003044440")
        upload(svc, "coord01", 1, "U-oldcal-enr", enroll_consent(pid_old))
        upload(svc, "coord01", 1, "U-oldcal-v",
               [visit("V-oldcal", pid_old, "2026-03-08", device="D01",
                      calibration="C01")])

        _, stats = self._freeze_stats()
        # 两条坏访视被排除；旧校准窗口内的访视仍纳入
        self.assertEqual(stats["denominator"], 1)
        reasons = stats["exclusion_reasons"]
        self.assertIn(Reasons.CALIBRATION_MISMATCH, reasons)
        self.assertIn(Reasons.CALIBRATION_EXPIRED, reasons)

        # 溯源能看到排除原因
        trace = svc.trace_result(svc.principal("analyst"), "V-expired")
        self.assertFalse(trace["quality"]["components"]["nitx"]["ok"])

    # ---------- 3. 并发锁库 ----------
    def test_concurrent_freeze_lock(self) -> None:
        svc = self.svc
        analyst = svc.principal("analyst")
        # 直接在存储层持锁，模拟另一个冻结进行中
        acquired = svc.store.acquire_freeze_lock(
            "analysis:freeze", "other-freeze", ttl_seconds=60
        )
        self.assertTrue(acquired)

        with self.assertRaises(LockBusy):
            svc.freeze(analyst, "my-freeze")

        # 锁库期间上传不被隔离，返回 locked 可重试
        pid = p(svc, 1, "110101199004044444")
        result = upload(svc, "coord01", 1, "U-locked", enroll_consent(pid))
        self.assertEqual(result["status"], "locked")
        self.assertTrue(result["retryable"])

        # 持锁者释放后，冻结恢复可用
        svc.store.release_freeze_lock("analysis:freeze", "other-freeze")
        event = svc.freeze(analyst, "my-freeze")
        self.assertIn("冻结", event["summary"])

    # ---------- 4. 部分批次失败（隔离与地区隔离） ----------
    def test_partial_batch_failure_isolation(self) -> None:
        svc = self.svc
        pid_ok = p(svc, 1, "110101199005055555")
        # 批次内既有合法招募，又有坏引用 -> 整批隔离，合法命令也不落库
        bad = enroll_consent(pid_ok) + [
            {"type": "consent", "pid": "P-GHOST", "scope": "full"}
        ]
        result = upload(svc, "coord01", 1, "U-bad", bad)
        self.assertEqual(result["status"], "quarantined")
        self.assertIsNone(svc.projection.participant(pid_ok))

        # 隔离台账存在，且只涉及本站点
        batches = svc.list_batches(svc.principal("admin"))
        quarantined = [b for b in batches if b["upload_id"] == "U-bad"][0]
        self.assertEqual(quarantined["site_id"], "S01")

        # 不污染其他地区：站点 2 的批次照常接收
        pid2 = p(svc, 2, "310101199006066666")
        ok2 = upload(svc, "coord02", 2, "U-other-region", enroll_consent(pid2))
        self.assertEqual(ok2["status"], "accepted")
        self.assertIsNotNone(svc.projection.participant(pid2))

        # 同站点后续批次也正常
        ok3 = upload(svc, "coord01", 1, "U-after", enroll_consent(pid_ok))
        self.assertEqual(ok3["status"], "accepted")

        # 同凭证内容一致安全重传，冲突被隔离
        raw = batch("U-replay", enroll_consent(p(svc, 1, "110101199007077777")))
        token = svc.issue_token(svc.principal("admin"), "S01")
        first = svc.upload(svc.principal("coord01"), token, raw)
        replay = svc.upload(svc.principal("coord01"), token, raw)
        self.assertEqual(first["status"], "accepted")
        self.assertTrue(replay["idempotent"])
        changed = json.loads(raw)
        changed["commands"][0]["pid"] = p(svc, 1, "110101199008088888")
        conflict = svc.upload(
            svc.principal("coord01"), token,
            json.dumps(changed, ensure_ascii=False).encode(),
        )
        self.assertEqual(conflict["status"], "quarantined")

    # ---------- 5. 撤回传播 ----------
    def test_withdrawal_propagation(self) -> None:
        svc = self.svc
        analyst = svc.principal("analyst")
        pid_remove = p(svc, 1, "110101199009099999")
        pid_retain = p(svc, 1, "110101199010101010")
        upload(svc, "coord01", 1, "U-enr",
               enroll_consent(pid_remove) + enroll_consent(pid_retain))
        upload(svc, "coord01", 1, "U-v1",
               [visit("V-rm", pid_remove, "2026-03-02", level="high"),
                visit("V-rt", pid_retain, "2026-03-03", diabetes="no",
                      alcohol="none", level="medium")])

        sid, before = self._freeze_stats()
        self.assertEqual(before["denominator"], 2)
        svc.publish(analyst, sid)
        published_denom = svc.published_stats(analyst, sid)["stats"]["denominator"]

        # 一人要求移除既有聚合，一人许可保留既有聚合
        upload(svc, "coord01", 1, "U-wd", [
            {"type": "withdrawal", "pid": pid_remove, "aggregates": "remove",
             "at": "2026-04-01T00:00:00+08:00"},
            {"type": "withdrawal", "pid": pid_retain, "aggregates": "retain",
             "at": "2026-04-01T00:00:00+08:00"},
        ])
        # 撤回后的新访视：即便 retain 也必须停止未来分析
        upload(svc, "coord01", 1, "U-post", [
            visit("V-rt-post", pid_retain, "2026-05-01", level="high")
        ])

        _, after = self._freeze_stats(holder="analyst-2")
        self.assertEqual(after["denominator"], 1, "retain 保留既有基线，remove 移除")
        retained_row = after["by_site"]["S01"]
        self.assertEqual(retained_row["n"], 1)
        reasons = after["exclusion_reasons"]
        self.assertIn(Reasons.WITHDRAWN_AGGREGATES_REVOKED, reasons)
        # 撤回后访视被排除（参与者撤回）
        excluded_post = [
            e for e in self.svc.projection.snapshots.values()
        ][-1].payload["excluded"]
        post = [e for e in excluded_post if e["visit_id"] == "V-rt-post"][0]
        self.assertIn(Reasons.PARTICIPANT_WITHDRAWN, post["reasons"])

        # 已发布快照原分母与口径不变
        self.assertEqual(
            svc.published_stats(analyst, sid)["stats"]["denominator"],
            published_denom,
        )
        self.assertEqual(published_denom, 2)

    # ---------- 6. 服务恢复 + 迟到更正 ----------
    def test_service_recovery_and_late_correction(self) -> None:
        svc = self.svc
        analyst = svc.principal("analyst")
        pid = p(svc, 1, "110101199111111111")
        upload(svc, "coord01", 1, "U-enr", enroll_consent(pid))
        # 初始为合格访视（high）
        upload(svc, "coord01", 1, "U-v",
               [visit("V-late", pid, "2026-03-02", level="high")])

        # 冻结但暂不发布
        event = svc.freeze(analyst, "pre")
        sid = event["aggregate_id"]
        pre = svc.recompute(analyst, sid)
        self.assertEqual(pre["denominator"], 1)
        self.assertEqual(pre["prevalence"]["risk_counts"]["high"], 1)

        # 迟到更正：有效针次不足，使质量失败
        svc.upload(svc.principal("coord01"),
                   svc.issue_token(svc.principal("admin"), "S01"),
                   batch("U-fix", [{
                       "type": "nitx_update", "visit_id": "V-late",
                       **nitx("2026-03-02", shots=6),
                   }]))
        # 尚未发布的统计：重新冻结即反映更正
        event2 = svc.freeze(analyst, "pre2")
        post = svc.recompute(analyst, event2["aggregate_id"])
        self.assertEqual(post["denominator"], 0)
        self.assertIn(Reasons.NITX_SHOTS_INSUFFICIENT, post["exclusion_reasons"])

        # 关闭后重开：投影从事件流重建，状态一致
        svc.close()
        path = tempfile.mktemp(suffix=".db")
        # :memory: 无法重开；此处改为对文件库再验证恢复路径
        svc_file = make_world(path)
        try:
            setup_world_with_master(svc_file)
            pidf = p(svc_file, 1, "110101199212121212")
            upload(svc_file, "coord01", 1, "F-enr", enroll_consent(pidf))
            upload(svc_file, "coord01", 1, "F-v",
                   [visit("FV", pidf, "2026-03-02", level="high")])
            f_event = svc_file.freeze(svc_file.principal("analyst"), "f")
            svc_file.publish(svc_file.principal("analyst"), f_event["aggregate_id"])
            svc_file.close()

            reopened = CohortService.open(path)
            self.assertEqual(len(reopened.projection.visits), 1)
            snaps = reopened.list_snapshots(reopened.principal("analyst"))
            self.assertEqual(len(snaps), 1)
            self.assertTrue(snaps[0]["published"])

            # 过期冻结锁可被接管（崩溃恢复）
            past = datetime.now(timezone.utc) - timedelta(seconds=1)
            ok = reopened.store.acquire_freeze_lock(
                "analysis:freeze", "zombie", ttl_seconds=-10, now=past
            )
            self.assertTrue(ok)
            taken = reopened.store.acquire_freeze_lock(
                "analysis:freeze", "new-holder", ttl_seconds=30
            )
            self.assertTrue(taken, "过期锁应允许接管")
            reopened.close()
        finally:
            Path(path).unlink(missing_ok=True)


    # ---------- 3b. 同凭证并发提交 ----------
    def test_concurrent_same_token_upload(self) -> None:
        import threading

        svc = self.svc
        coord = svc.principal("coord01")
        token = svc.issue_token(svc.principal("admin"), "S01")
        pid = p(svc, 1, "110101199012121212")
        raw = batch("U-conc", enroll_consent(pid))
        outcomes: list[dict] = []
        barrier = threading.Barrier(2)

        def fire() -> None:
            barrier.wait()
            outcomes.append(svc.upload(coord, token, raw))

        threads = [threading.Thread(target=fire) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        statuses = sorted("idempotent" if o["idempotent"] else o["status"]
                          for o in outcomes)
        self.assertEqual(statuses, ["accepted", "idempotent"],
                         "同凭证同内容并发：恰好一个接收，另一个幂等回放")
        # 参与者只被招募一次
        self.assertEqual(
            len([x for x in svc.list_participants(svc.principal("analyst"))
                 if x["pid"] == pid]),
            1,
        )

        # 同凭证并发提交不同内容：一个接收，另一个隔离（绝不两份都落库）
        token2 = svc.issue_token(svc.principal("admin"), "S01")
        pid2, pid3 = p(svc, 1, "110101199113131313"), p(svc, 1, "110101199114141414")
        raw_a = batch("U-conflict", enroll_consent(pid2))
        raw_b = batch("U-conflict", enroll_consent(pid3))
        results: list[dict] = []
        barrier2 = threading.Barrier(2)

        def fire_two(raw: bytes) -> None:
            barrier2.wait()
            results.append(svc.upload(coord, token2, raw))

        ta = threading.Thread(target=fire_two, args=(raw_a,))
        tb = threading.Thread(target=fire_two, args=(raw_b,))
        ta.start(); tb.start(); ta.join(); tb.join()
        finals = sorted(r["status"] for r in results)
        self.assertEqual(finals, ["accepted", "quarantined"], finals)
        existing = [pid for pid in (pid2, pid3)
                    if svc.projection.participant(pid) is not None]
        self.assertEqual(len(existing), 1,
                         "只有接收方的参与者落库，冲突方整批回滚")


if __name__ == "__main__":
    unittest.main()
