"""中央复算：从冻结快照生成分层统计，并输出缺失与排除原因。

* 对**已冻结未发布**的快照，复算读取各组成部分的最新版本：迟到更正因此影响
  尚未发布的统计；按许可撤回的参与者从分母排除。每次复算同时给出排除清单与
  字段级缺失原因。
* 对**已发布**快照，直接返回发布事件中封存的统计、分母与口径（caliber），
  任何后来的更正与撤回都不再改变它。
"""

from __future__ import annotations

from typing import Any

from .catalog import SEC_ALCOHOL, SEC_DIABETES, SEC_NONINVASIVE, Ruleset
from .governance import GovernanceError, GovernanceService, Projection, VisitState
from .quality import stratify


def _current_ruleset(service: GovernanceService, visit: VisitState) -> Ruleset:
    # 冻结时固定的规则版本即该快照口径；缺省回退到访视时点规则
    version = visit.frozen_ruleset or service.catalog.ruleset_for(visit.planned_at).version
    return service.catalog.get_ruleset(version)


def _recompute_risk(
    service: GovernanceService, visit: VisitState, projection: Projection
) -> tuple[str | None, dict[str, Any] | None, list[str]]:
    """返回 (band, risk, missing_reasons)。"""
    noninv = visit.sections.get(SEC_NONINVASIVE)
    participant = projection.participants.get(visit.study_id)
    missing: list[str] = []
    if noninv is None:
        return None, None, ["缺少无创检查组成部分"]
    data = noninv.current["data"]
    for field_name in ("ast", "alt", "platelets", "lsm_kpa"):
        if not isinstance(data.get(field_name), (int, float)):
            missing.append(f"无创结果缺少 {field_name}")
    if not participant or not participant.birth_year:
        missing.append("缺少出生年份，无法计算年龄")
    if missing:
        return None, None, missing
    age = int(visit.planned_at[:4]) - participant.birth_year
    if age <= 0:
        return None, None, ["年龄计算异常"]
    rules = _current_ruleset(service, visit)
    risk = stratify(
        rules,
        age=float(age),
        ast=float(data["ast"]),
        alt=float(data["alt"]),
        platelets=float(data["platelets"]),
        lsm_kpa=float(data["lsm_kpa"]),
    )
    return risk["band"], risk, []


def recompute(service: GovernanceService, snapshot_id: str) -> dict[str, Any]:
    projection = service.build_projection()
    snapshot = projection.snapshots.get(snapshot_id)
    if snapshot is None:
        raise GovernanceError(f"未知分析快照：{snapshot_id}")

    if snapshot["published_at"]:
        return {
            "snapshot_id": snapshot_id,
            "published": True,
            "immutable": True,
            "published_at": snapshot["published_at"],
            "denominator": snapshot["stats"].get("denominator", len(snapshot["visit_ids"])),
            "strata": snapshot["stats"].get("strata", {}),
            "prevalence": snapshot["stats"].get("prevalence", {}),
            "missing": [],
            "excluded": snapshot["stats"].get("excluded_at_publish", []),
            "caliber": snapshot.get("caliber", {}),
        }

    exclusions: list[dict[str, Any]] = []
    missing_report: list[dict[str, Any]] = []
    bands: dict[str, int] = {"low": 0, "indeterminate": 0, "high": 0}
    diabetes_known = 0
    current_drinkers = 0
    members: list[str] = []

    for visit_id in snapshot["visit_ids"]:
        visit = projection.visits[visit_id]
        participant = projection.participants.get(visit.study_id)

        evaluation = service.evaluate_visit(visit, projection)
        # 撤回统一交给撤回传播阶段按许可处理；其余质量原因仍然排除
        reasons = evaluation["reasons"]
        if participant and participant.withdrawn:
            reasons = [r for r in reasons if r["code"] != "WITHDRAWN"]
        if reasons:
            exclusions.append({
                "visit_id": visit_id,
                "study_id": visit.study_id,
                "stage": "pre_publish_recompute",
                "reasons": reasons,
            })
            continue
        if participant and participant.withdrawn and not participant.withdrawal["retain_aggregates"]:
            exclusions.append({
                "visit_id": visit_id,
                "study_id": visit.study_id,
                "stage": "withdrawal_propagation",
                "reasons": [{"code": "WITHDRAWN_NO_RETENTION",
                             "detail": "撤回且未许可保留既有聚合"}],
            })
            continue

        members.append(visit_id)
        band, _risk, missing = _recompute_risk(service, visit, projection)
        if band is None:
            missing_report.append({"visit_id": visit_id, "study_id": visit.study_id, "reasons": missing})
        else:
            bands[band] += 1

        diabetes = visit.sections.get(SEC_DIABETES)
        if diabetes and diabetes.current["data"].get("status") in ("known", "newly_diagnosed"):
            diabetes_known += 1
        alcohol = visit.sections.get(SEC_ALCOHOL)
        if alcohol and alcohol.current["data"].get("drinker_status") == "current":
            current_drinkers += 1

    denominator = len(members)
    strata = {
        band: {"count": count, "proportion": round(count / denominator, 4) if denominator else None}
        for band, count in bands.items()
    }
    strata["unclassified"] = {
        "count": len(missing_report),
        "proportion": round(len(missing_report) / denominator, 4) if denominator else None,
    }

    return {
        "snapshot_id": snapshot_id,
        "published": False,
        "immutable": False,
        "ruleset_version": snapshot["ruleset_version"],
        "denominator": denominator,
        "strata": strata,
        "prevalence": {
            "diabetes": {
                "count": diabetes_known,
                "proportion": round(diabetes_known / denominator, 4) if denominator else None,
            },
            "current_drinkers": {
                "count": current_drinkers,
                "proportion": round(current_drinkers / denominator, 4) if denominator else None,
            },
        },
        "missing": missing_report,
        "excluded": exclusions,
        "caliber": {
            "ruleset_version": snapshot["ruleset_version"],
            "frozen_at": snapshot["frozen_at"],
            "members_intended": list(snapshot["visit_ids"]),
            "members_counted": members,
        },
    }
