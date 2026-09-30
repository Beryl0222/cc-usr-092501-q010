"""冻结分析批次。

一次冻结：
1. 获取全局冻结锁（并发的第二个冻结请求得到 LockBusy；锁库期间上传被拒）；
2. 按指定（或最新发布的）quality 规则版本，对全部基线访视逐组成部分评估；
3. 基线去重：同一规范参与者只保留最早一条合格基线，后续基线记
   baseline_already_counted；
4. 冻结行（含分层结果、所用规则版本、站点/省份）随 ANALYSIS_FROZEN 事件
   原样落库——此后与线上投影解耦，迟到更正改不到它；
5. 发布时中央复算在冻结行上进行，结果随 SNAPSHOT_PUBLISHED 永久固定，
   已发布快照的分母和口径不再变化。

迟到更正若到达于“冻结之后、发布之前”，协调方应重新冻结得到新快照；
已发布快照不可变，这正是“保留原分母和口径”。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from . import quality as quality_engine
from .contracts import (
    Aggregates, Components, Events, Reasons, RuleKind, VisitKind,
)
from .identity import new_id
from .projection import Projection, parse_date
from .store import EventStore, LockBusy

FREEZE_SCOPE = "analysis:freeze"
FREEZE_TTL_SECONDS = 30.0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_date(value: str | None):
    if not value:
        return None
    try:
        return parse_date(value)
    except ValueError:
        return None


def resolve_quality_version(projection: Projection, version: int | None) -> int:
    if version is not None:
        if projection.rule_body(RuleKind.QUALITY, version) is None:
            raise ValueError(f"quality 规则版本未发布：{version}")
        return version
    latest, _ = projection.latest_rule(RuleKind.QUALITY)
    if latest == 0:
        raise ValueError("尚未发布任何 quality 规则，无法冻结")
    return latest


def build_frozen_rows(
    projection: Projection, quality_version: int,
    province_by_site: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """返回 (纳入行, 排除行)。纯读操作，不产生事件。"""
    quality_body = projection.rule_body(RuleKind.QUALITY, quality_version)
    province_by_site = province_by_site or {}
    baseline_visits = sorted(
        (v for v in projection.visits.values() if v.kind == VisitKind.BASELINE),
        key=lambda v: (v.visit_date, v.seq),
    )

    included: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    counted: set[str] = set()

    for visit in baseline_visits:
        pid = visit.pid
        participant = projection.participant(pid)
        site_id = visit.site_id
        province = province_by_site.get(site_id, "")
        reasons: list[str] = []

        results = quality_engine.evaluate_visit(visit, projection, quality_body)
        for name in Components.BASELINE_REQUIRED:
            reasons.extend(results[name].reasons)

        # 撤回传播：撤回停止未来分析；既有基线是否留在聚合中按许可处理。
        withdrawn_retained = False
        if participant is not None and participant.withdrawn:
            withdrawn_date = _safe_date(participant.withdrawn_at)
            visit_d = _safe_date(visit.visit_date)
            is_after_withdrawal = (
                withdrawn_date is not None and visit_d is not None
                and visit_d > withdrawn_date
            )
            if not participant.aggregates_retained:
                # 许可要求移除既有聚合：全部排除
                reasons.append(Reasons.PARTICIPANT_WITHDRAWN)
                reasons.append(Reasons.WITHDRAWN_AGGREGATES_REVOKED)
            elif is_after_withdrawal:
                # 保留既有聚合，但撤回后的访视不再进入分析
                reasons.append(Reasons.PARTICIPANT_WITHDRAWN)
            else:
                withdrawn_retained = True

        if reasons:
            excluded.append({
                "visit_id": visit.visit_id, "pid": pid,
                "site_id": site_id, "province": province,
                "visit_date": visit.visit_date,
                "reasons": sorted(set(reasons)),
            })
            continue

        if pid in counted:
            excluded.append({
                "visit_id": visit.visit_id, "pid": pid,
                "site_id": site_id, "province": province,
                "visit_date": visit.visit_date,
                "reasons": [Reasons.BASELINE_ALREADY_COUNTED],
            })
            continue

        counted.add(pid)
        risk = visit.risk or {}
        questionnaire = visit.questionnaire or {}
        included.append({
            "pid": pid,
            "visit_id": visit.visit_id,
            "site_id": site_id,
            "province": province,
            "visit_date": visit.visit_date,
            "risk_level": risk.get("level"),
            "risk_rule_version": risk.get("rule_version"),
            "diabetes": questionnaire.get("diabetes"),
            "alcohol": questionnaire.get("alcohol"),
            "withdrawn_aggregates_retained": withdrawn_retained,
            "quality_version": quality_version,
        })

    return included, excluded


def freeze(
    store: EventStore, projection: Projection, holder: str,
    quality_version: int | None = None,
    ttl_seconds: float = FREEZE_TTL_SECONDS,
) -> dict[str, Any]:
    """加锁、构建冻结行并追加 ANALYSIS_FROZEN。返回冻结事件。"""
    if not store.acquire_freeze_lock(FREEZE_SCOPE, holder, ttl_seconds):
        current = store.lock_holder(FREEZE_SCOPE)
        raise LockBusy(f"分析库正被 {current} 锁定")

    try:
        qv = resolve_quality_version(projection, quality_version)
        province_by_site = {
            row["site_id"]: row["province"] for row in store.list_sites()
        }
        included, excluded = build_frozen_rows(projection, qv, province_by_site)
    except BaseException:
        store.release_freeze_lock(FREEZE_SCOPE, holder)
        raise

    snapshot_id = new_id("S")
    as_of_seq = projection.seq
    payload = {
        "as_of_seq": as_of_seq,
        "quality_version": qv,
        "created_at": _now(),
        "included": included,
        "excluded": excluded,
        "denominator": len(included),
    }
    try:
        event = {
            "event_id": f"ev-{snapshot_id}-freeze",
            "event_type": Events.ANALYSIS_FROZEN,
            "aggregate_type": Aggregates.SNAPSHOT,
            "aggregate_id": snapshot_id,
            "occurred_at": _now(),
            "version": 1,
            "summary": (
                f"冻结分析批次：纳入 {len(included)}，排除 {len(excluded)}，"
                f"quality v{qv}，截至事件 {as_of_seq}"
            ),
            "payload": payload,
        }
        seq = store.append(event)
        event["seq"] = seq
        projection.apply(event)
        lock_event = {
            "event_id": f"ev-{snapshot_id}-locked",
            "event_type": Events.ANALYSIS_LOCKED,
            "aggregate_type": Aggregates.SNAPSHOT,
            "aggregate_id": snapshot_id,
            "occurred_at": _now(),
            "version": 2,
            "summary": f"冻结期锁库：{holder}",
            "payload": {"holder": holder},
        }
        seq = store.append(lock_event)
        lock_event["seq"] = seq
        projection.apply(lock_event)
        return event
    finally:
        store.release_freeze_lock(FREEZE_SCOPE, holder)
        unlock_event = {
            "event_id": f"ev-{snapshot_id}-unlocked",
            "event_type": Events.ANALYSIS_UNLOCKED,
            "aggregate_type": Aggregates.SNAPSHOT,
            "aggregate_id": snapshot_id,
            "occurred_at": _now(),
            "version": 3,
            "summary": f"冻结期结束解锁：{holder}",
            "payload": {"holder": holder},
        }
        seq = store.append(unlock_event)
        unlock_event["seq"] = seq
        projection.apply(unlock_event)
