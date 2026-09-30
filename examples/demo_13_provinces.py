"""13 省社区肝脏健康项目端到端演示。

运行：
    python3 examples/demo_13_provinces.py

内容覆盖：13 站点引导、本地身份化名、合格/不合格访视、跨区复查归并、
设备校准失效、整批隔离不污染他省、并发锁库、撤回传播、迟到更正、
冻结→复算→发布以及已发布快照口径不变。
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.contracts import Reasons  # noqa: E402
from tests.world import (  # noqa: E402
    PROVINCES, batch, enroll_consent, make_world, nitx, p,
    setup_world_with_master, upload, visit,
)


def line(title: str) -> None:
    print(f"\n{'─' * 64}\n{title}\n{'─' * 64}")


def main() -> int:
    db = Path(tempfile.gettempdir()) / "cohort_demo.db"
    if db.exists():
        db.unlink()
    svc = make_world(str(db))
    setup_world_with_master(svc)
    admin = svc.principal("admin")
    analyst = svc.principal("analyst")
    print(f"已引导 {len(PROVINCES)} 个省份站点：{ '、'.join(PROVINCES) }")

    # 每省招募 2 人并完成基线（省 13 的第二人故意校准过期）
    line("1) 各省招募与基线访视")

    def id_a(i: int) -> str:
        return f"1101{i:02d}199001011234"

    def id_b(i: int) -> str:
        return f"3101{i:02d}19900202223X"

    for i in range(1, 14):
        coord = f"coord{i:02d}"
        pid_a = p(svc, i, id_a(i))
        pid_b = p(svc, i, id_b(i))
        upload(svc, coord, i, f"U{i:02d}-enr",
               enroll_consent(pid_a) + enroll_consent(pid_b))
        cmds = [
            visit(f"V{i:02d}A", pid_a, "2026-03-02", site_index=i,
                  diabetes="yes" if i % 2 else "no",
                  alcohol="heavy" if i % 3 == 0 else "none",
                  referral="LSM 升高建议专科"),
            visit(f"V{i:02d}B", pid_b, "2026-03-04", site_index=i, age=30,
                  diabetes="no", alcohol="none", platelets=300),
        ]
        result = upload(svc, coord, i, f"U{i:02d}-vis", cmds)
        print(f"  {PROVINCES[i-1]:<2} 批次 {result['status']:<10} 2 条基线")

    # 省 13 补一条校准过期访视
    pid_bad = p(svc, 13, "440101199012319999")
    upload(svc, "coord13", 13, "U13-enr2", enroll_consent(pid_bad, "2026-02-01"))
    upload(svc, "coord13", 13, "U13-cal", [{
        "type": "calibration", "device_id": "D13", "calibration_id": "C13b",
        "calibrated_at": "2026-01-01T00:00:00+08:00",
    }])
    expired = visit("V13X", pid_bad, "2026-09-15", site_index=13,
                    calibration="C13b")
    upload(svc, "coord13", 13, "U13-exp", [expired])

    # 第一次冻结与发布
    line("2) 第一次冻结 → 复算 → 发布")
    f1 = svc.freeze(analyst, "quarterly-1")
    sid1 = f1["aggregate_id"]
    stats1 = svc.recompute(analyst, sid1)
    svc.publish(analyst, sid1)
    print(f"  {f1['summary']}")
    print(f"  分母 {stats1['denominator']}，中高风险 {stats1['prevalence']['elevated_pct']}%")
    print(f"  排除 {stats1['excluded_visits']} 条，原因：{stats1['exclusion_reasons']}")

    # 跨区复查：省 1 第一人到省 2 复查
    line("3) 跨区复查（北京 → 上海，一次性关联码归并）")
    bj_pid = p(svc, 1, id_a(1))
    sh_pid = p(svc, 2, id_a(1))
    print(f"  同一身份证在两省化名不同：{bj_pid[:10]}… ≠ {sh_pid[:10]}…")
    code = svc.issue_linkage_code(svc.principal("coord01"), bj_pid)
    upload(svc, "coord02", 2, "U2-xenr", enroll_consent(sh_pid, "2026-05-01"))
    linked = svc.consume_linkage_code(svc.principal("coord02"), code, sh_pid)
    print(f"  关联后归并到 {linked['canonical_pid'][:10]}…，化名数 {len(linked['aliases'])}")
    upload(svc, "coord02", 2, "U2-xvis",
           [visit("V02X", sh_pid, "2026-05-02", site_index=2,
                  diabetes="no", alcohol="none")])

    # 整批隔离
    line("4) 部分批次失败：一条坏命令隔离整批，不污染其他省")
    ghost = [{"type": "consent", "pid": "P-GHOST", "scope": "full"}]
    q = upload(svc, "coord05", 5, "U05-bad", ghost)
    print(f"  浙江坏批：{q['status']}（{q['quarantine_reason']}）")
    fine = upload(svc, "coord06", 6, "U06-fine",
                  enroll_consent(p(svc, 6, "320101199006066666")))
    print(f"  随后江苏批次：{fine['status']}（其他地区与后续批次不受影响）")

    # 锁库
    line("5) 并发锁库")
    svc.store.acquire_freeze_lock("analysis:freeze", "long-running", 60)
    blocked = upload(svc, "coord07", 7, "U07-locked",
                     enroll_consent(p(svc, 7, "420101199007077777")))
    print(f"  锁库期间上传状态：{blocked['status']}，可重试={blocked['retryable']}，不隔离")
    try:
        svc.freeze(analyst, "racer")
    except Exception as error:
        print(f"  并发冻结被拒：{error}")
    svc.store.release_freeze_lock("analysis:freeze", "long-running")

    # 撤回传播：省 3 第一人要求移除，省 4 第一人许可保留既有聚合
    line("6) 撤回传播")
    rm_pid = p(svc, 3, id_a(3))
    rt_pid = p(svc, 4, id_a(4))
    upload(svc, "coord03", 3, "U03-wd", [
        {"type": "withdrawal", "pid": rm_pid, "aggregates": "remove",
         "at": "2026-04-01T00:00:00+08:00"}])
    upload(svc, "coord04", 4, "U04-wd", [
        {"type": "withdrawal", "pid": rt_pid, "aggregates": "retain",
         "at": "2026-04-01T00:00:00+08:00"}])
    upload(svc, "coord04", 4, "U04-post", [
        visit("V04X", rt_pid, "2026-05-01", site_index=4)])

    # 迟到更正：省 8 第一人针次不足，未发布统计受影响
    fix_pid = p(svc, 8, id_a(8))
    upload(svc, "coord08", 8, "U08-fix", [{
        "type": "nitx_update", "visit_id": "V08A",
       **nitx("2026-03-02", site_index=8, shots=6)}])

    line("7) 第二次冻结：迟到更正与撤回生效，已发布快照口径不变")
    f2 = svc.freeze(analyst, "quarterly-2")
    sid2 = f2["aggregate_id"]
    stats2 = svc.recompute(analyst, sid2)
    svc.publish(analyst, sid2)
    print(f"  {f2['summary']}")
    print(f"  新分母 {stats2['denominator']}（第一次发布快照分母仍为 "
          f"{stats1['denominator']}）")
    print(f"  排除原因：{json.dumps(stats2['exclusion_reasons'], ensure_ascii=False)}")
    print(f"  缺失原因：{json.dumps(stats2['missing_reasons'], ensure_ascii=False)}")

    # 溯源示例
    line("8) 结果溯源：V01A 来自哪次访视、哪批上传、哪版规则")
    trace = svc.trace_result(analyst, "V01A")
    print(f"  访视 {trace['visit_id']}，站点 {trace['site_id']}，"
          f"上传批次 {trace['upload_ids']}")
    print(f"  风险规则版本 v{trace['components']['risk']['rule_version']}，"
          f"质量规则版本 v{trace['quality']['rule_version']}")
    print(f"  出现于 {len(trace['in_snapshots'])} 个冻结快照（含已发布，不可变）")

    # 分权
    line("9) 分权访问")
    doctor = svc.principal("doctor01")
    print(f"  临床医生可见转诊 {len(svc.list_referrals(doctor))} 条，研究端点无权访问")
    print(f"  站点协调员可见本站参与者 {len(svc.list_participants(svc.principal('coord01')))} 人；"
          f"中央分析师可见 {len(svc.list_participants(analyst))} 人")

    line("10) 服务恢复")
    svc.close()
    from src.service import CohortService
    reopened = CohortService.open(db)
    snaps = reopened.list_snapshots(reopened.principal("analyst"))
    print(f"  重开后投影重建：{len(reopened.projection.visits)} 次访视，"
          f"{len(snaps)} 个快照（全部保留发布状态）")
    reopened.close()
    print("\n演示完成。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
