"""版本化访视质量规则（quality 规则）。

一次访视按组成部分分别检查，只有基线必需组成部分全部通过，访视才能进入
冻结分析批次。每个失败给出稳定原因码（见 contracts.Reasons），中央复算
把这些原因码作为排除/缺失口径输出。

内置 quality-v1 覆盖：
- consent：知情存在、未撤回、知情日期不晚于访视日期；
- questionnaire：问卷版本已发布、糖尿病/饮酒枚举合法、年龄合理；
- nitx：设备已登记、校准版本匹配且在校准有效期内、有效针次足够、
        LSM/AST/ALT/血小板在合理区间、LSM 的 IQR/中位数不过大；
- risk：所用风险规则版本存在、按该版本复算分层与上传一致。
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from . import risk as risk_engine
from .contracts import (
    AlcoholStatus, Components, Diabetes, Reasons, RiskLevel,
)
from .projection import Visit, parse_date

QUALITY_RULE_V1: dict[str, Any] = {
    "kind": "quality",
    "version": 1,
    "name": "visit-components-v1",
    "calibration_valid_days": 180,
    "min_shots": 10,
    "lsm_iqr_ratio_max": 0.30,
    "ranges": {
        "lsm_kpa": [0.5, 75.0],
        "alt": [1.0, 10000.0],
        "ast": [1.0, 10000.0],
        "platelets": [1.0, 2000.0],
        "age": [0, 120],
    },
}


class ComponentResult:
    def __init__(self, name: str) -> None:
        self.name = name
        self.present = False
        self.reasons: list[str] = []

    @property
    def ok(self) -> bool:
        return self.present and not self.reasons

    def as_dict(self) -> dict[str, Any]:
        return {"present": self.present, "ok": self.ok, "reasons": self.reasons}


def evaluate_visit(
    visit: Visit, projection: Any, quality_body: dict[str, Any]
) -> dict[str, ComponentResult]:
    results = {name: ComponentResult(name) for name in Components.BASELINE_REQUIRED}
    _check_consent(visit, projection, results[Components.CONSENT])
    _check_questionnaire(visit, projection, quality_body, results[Components.QUESTIONNAIRE])
    _check_nitx(visit, projection, quality_body, results[Components.NITX])
    _check_risk(visit, projection, results[Components.RISK])
    return results


def _check_consent(visit: Visit, projection: Any, result: ComponentResult) -> None:
    participant = projection.participant(visit.pid)
    if participant is None or participant.consent is None:
        result.reasons.append(Reasons.COMPONENT_MISSING)
        return
    result.present = True
    consent = participant.consent
    if consent.get("scope") == "withdrawn":
        result.reasons.append(Reasons.PARTICIPANT_WITHDRAWN)
    consent_from = consent.get("from")
    if consent_from and parse_date(consent_from) > parse_date(visit.visit_date):
        result.reasons.append(Reasons.CONSENT_OUT_OF_RANGE)


def _check_questionnaire(
    visit: Visit, projection: Any, body: dict[str, Any], result: ComponentResult
) -> None:
    q = visit.questionnaire
    if q is None:
        result.reasons.append(Reasons.COMPONENT_MISSING)
        return
    result.present = True
    version = str(q.get("version", ""))
    if version not in projection.questionnaires:
        result.reasons.append(Reasons.QUESTIONNAIRE_VERSION_UNKNOWN)
    ranges = body.get("ranges", {})
    age = q.get("age")
    if not isinstance(age, int) or isinstance(age, bool) or not _in_range(age, ranges.get("age")):
        result.reasons.append(Reasons.QUESTIONNAIRE_VALUE_INVALID)
    if q.get("diabetes") not in Diabetes.VALUES:
        result.reasons.append(Reasons.QUESTIONNAIRE_VALUE_INVALID)
    if q.get("alcohol") not in AlcoholStatus.VALUES:
        result.reasons.append(Reasons.QUESTIONNAIRE_VALUE_INVALID)


def _check_nitx(
    visit: Visit, projection: Any, body: dict[str, Any], result: ComponentResult
) -> None:
    n = visit.nitx
    if n is None:
        result.reasons.append(Reasons.COMPONENT_MISSING)
        return
    result.present = True
    device = projection.devices.get(n.get("device_id", ""))
    claimed = n.get("calibration_id")
    if device is None:
        result.reasons.append(Reasons.DEVICE_UNKNOWN)
    else:
        calibration = device.calibrations.get(str(claimed)) if claimed else None
        if calibration is None:
            # 设备从未有过该校准版本
            result.reasons.append(Reasons.CALIBRATION_MISMATCH)
        else:
            measured_at = parse_date(n["measured_at"])
            cal_date = parse_date(calibration["calibrated_at"])
            valid_days = int(body["calibration_valid_days"])
            if measured_at < cal_date:
                result.reasons.append(Reasons.CALIBRATION_MISMATCH)
            elif measured_at > cal_date + timedelta(days=valid_days):
                result.reasons.append(Reasons.CALIBRATION_EXPIRED)

    ranges = body.get("ranges", {})
    for field_name in ("lsm_kpa", "alt", "ast", "platelets"):
        value = n.get(field_name)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            result.reasons.append(Reasons.NITX_OUT_OF_RANGE)
        elif not _in_range(value, ranges.get(field_name)):
            result.reasons.append(Reasons.NITX_OUT_OF_RANGE)

    shots = n.get("valid_shots")
    if not isinstance(shots, int) or isinstance(shots, bool) or shots < int(body["min_shots"]):
        result.reasons.append(Reasons.NITX_SHOTS_INSUFFICIENT)

    iqr = n.get("lsm_iqr")
    lsm = n.get("lsm_kpa")
    if isinstance(iqr, (int, float)) and isinstance(lsm, (int, float)) and lsm > 0:
        if iqr / lsm > float(body["lsm_iqr_ratio_max"]):
            result.reasons.append(Reasons.NITX_IQR_TOO_HIGH)


def _check_risk(visit: Visit, projection: Any, result: ComponentResult) -> None:
    r = visit.risk
    if r is None:
        result.reasons.append(Reasons.COMPONENT_MISSING)
        return
    result.present = True
    version = int(r.get("rule_version", 0))
    body = projection.rule_body("risk", version)
    if body is None:
        result.reasons.append(Reasons.RISK_RULE_UNKNOWN)
        return
    recomputed = risk_engine.classify(
        body, risk_engine.risk_inputs(visit.questionnaire, visit.nitx)
    )
    if recomputed["level"] is None:
        result.reasons.append(Reasons.RISK_LEVEL_MISMATCH)
    elif recomputed["level"] != r.get("level") or recomputed["level"] not in RiskLevel.ORDER:
        result.reasons.append(Reasons.RISK_LEVEL_MISMATCH)


def _in_range(value: float, bounds: list[float] | None) -> bool:
    if bounds is None:
        return True
    low, high = bounds
    return low <= value <= high


def failed_components(results: dict[str, ComponentResult]) -> list[str]:
    return [name for name, r in results.items() if not r.ok]
