"""中央复算：从冻结快照的冻结行生成分层统计。

输入永远是某条 ANALYSIS_FROZEN 快照里的行（而不是线上投影），因此：
- 已发布的患病率快照不受迟到更正影响，分母与口径保持原样；
- 想纳入更正，必须先冻结出新快照，再复算、发布。

stats-v1 输出：
- 总患病率（中/高风险占比）与各风险层人数；
- 按站点、按省份分层；
- 糖尿病、饮酒暴露分层；
- 排除原因计数与缺失组成部分计数（missing / exclusions）。
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from .contracts import (
    Aggregates, Events, Reasons, RiskLevel, RuleKind,
)
from .projection import Projection
from .store import EventStore

# 质量原因 -> 归类为“缺失”还是“排除”
MISSING_REASONS = {
    Reasons.COMPONENT_MISSING,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def recompute(frozen_payload: dict[str, Any], stats_version: int = 1) -> dict[str, Any]:
    """对一条冻结快照的 payload 复算，返回可发布的统计结果。"""
    included = frozen_payload.get("included", [])
    excluded = frozen_payload.get("excluded", [])
    denominator = len(included)

    def pct(n: int) -> float | None:
        return round(100.0 * n / denominator, 2) if denominator else None

    risk_counts = {level: 0 for level in RiskLevel.ORDER}
    site_counts: dict[str, Counter] = defaultdict(Counter)
    province_counts: dict[str, Counter] = defaultdict(Counter)
    diabetes_counts = Counter()
    alcohol_counts = Counter()

    for row in included:
        level = row.get("risk_level")
        if level in risk_counts:
            risk_counts[level] += 1
        site_counts[row.get("site_id", "")][level or "unknown"] += 1
        province_counts[row.get("province", "") or "未知"][level or "unknown"] += 1
        diabetes_counts[row.get("diabetes", "unknown")] += 1
        alcohol_counts[row.get("alcohol", "unknown")] += 1

    elevated = risk_counts[RiskLevel.MEDIUM] + risk_counts[RiskLevel.HIGH]

    exclusion_counts = Counter()
    missing_counts = Counter()
    for row in excluded:
        for reason in row.get("reasons", []):
            if reason in MISSING_REASONS:
                missing_counts[reason] += 1
            else:
                exclusion_counts[reason] += 1
    # component_missing 无法区分是哪个组成部分，结合冻结前评估不可得，
    # 这里按原因码计数即可（复算的输出口径稳定）。

    def strata(counts: dict[str, Counter]) -> dict[str, Any]:
        out = {}
        for key, counter in sorted(counts.items()):
            total = sum(counter.values())
            out[key] = {
                "n": total,
                "risk": {level: counter.get(level, 0) for level in RiskLevel.ORDER},
                "elevated_n": counter.get(RiskLevel.MEDIUM, 0)
                + counter.get(RiskLevel.HIGH, 0),
                "elevated_pct": round(
                    100.0 * (counter.get(RiskLevel.MEDIUM, 0)
                             + counter.get(RiskLevel.HIGH, 0)) / total, 2
                ) if total else None,
            }
        return out

    return {
        "stats_version": stats_version,
        "stats_rule": f"stats-v{stats_version}",
        "quality_version": frozen_payload.get("quality_version"),
        "as_of_seq": frozen_payload.get("as_of_seq"),
        "computed_at": _now(),
        "denominator": denominator,
        "prevalence": {
            "elevated_n": elevated,
            "elevated_pct": pct(elevated),
            "risk_counts": risk_counts,
        },
        "by_site": strata(site_counts),
        "by_province": strata(province_counts),
        "by_diabetes": dict(sorted(diabetes_counts.items())),
        "by_alcohol": dict(sorted(alcohol_counts.items())),
        "excluded_visits": len(excluded),
        "exclusion_reasons": dict(sorted(exclusion_counts.items())),
        "missing_reasons": dict(sorted(missing_counts.items())),
    }


def publish(
    store: EventStore, projection: Projection, snapshot_id: str,
    stats_version: int | None = None,
) -> dict[str, Any]:
    """对已冻结快照复算并追加 SNAPSHOT_PUBLISHED；结果不可变。"""
    snapshot = projection.snapshots.get(snapshot_id)
    if snapshot is None:
        raise ValueError(f"快照不存在或未冻结：{snapshot_id}")
    if snapshot.published is not None:
        raise ValueError(f"快照已发布，不可更改：{snapshot_id}")

    if stats_version is None:
        latest, _ = projection.latest_rule(RuleKind.STATS)
        stats_version = latest or 1

    stats = recompute(snapshot.payload, stats_version)
    payload = {"stats": stats, "published_at": _now()}
    # 发布挂在同一快照聚合的下一版本（冻结=1，锁定=2，解锁=3，发布=4…）
    existing = store.events_for(Aggregates.SNAPSHOT, snapshot_id)
    version = max((e["version"] for e in existing), default=0) + 1
    event = {
        "event_id": f"ev-{snapshot_id}-publish",
        "event_type": Events.SNAPSHOT_PUBLISHED,
        "aggregate_type": Aggregates.SNAPSHOT,
        "aggregate_id": snapshot_id,
        "occurred_at": _now(),
        "version": version,
        "summary": (
            f"发布患病率快照 {snapshot_id}：分母 {stats['denominator']}，"
            f"中高风险占比 {stats['prevalence']['elevated_pct']}%"
        ),
        "payload": payload,
    }
    seq = store.append(event)
    event["seq"] = seq
    projection.apply(event)
    return event
