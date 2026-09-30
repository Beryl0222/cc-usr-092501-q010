"""访视组成部分的质量规则与风险分层。

每次访视拆成基线、问卷、糖尿病、饮酒暴露、无创检查五个组成部分分别留痕；
只有全部必需组成部分通过对应规则版本的质量检查，访视才能冻结进入分析批次。

设备校准是否在有效期由设备注册与校准记录决定：校准失效（含从未校准、
校准过期、设备停用）的无创结果直接判不合格。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable

from .catalog import (
    INSTRUMENT_BASELINE,
    Ruleset,
    SEC_ALCOHOL,
    SEC_BASELINE,
    SEC_DIABETES,
    SEC_NONINVASIVE,
    SEC_QUESTIONNAIRE,
)
from .events import parse_time

DIABETES_STATUSES = ("none", "known", "newly_diagnosed", "unknown")
DRINKER_STATUSES = ("never", "former", "current")


def issue(code: str, detail: str) -> dict[str, str]:
    return {"code": code, "detail": detail}


def _require_keys(section: dict[str, Any], keys: tuple[str, ...]) -> list[dict[str, str]]:
    problems = []
    for key in keys:
        if key not in section or section[key] in (None, ""):
            problems.append(issue("MISSING_FIELD", f"缺少字段：{key}"))
    return problems


def _number_in_range(value: Any, low: float, high: float) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and low <= float(value) <= high


def check_baseline(section: dict[str, Any], rules: Ruleset) -> list[dict[str, str]]:
    problems = _require_keys(section, ("birth_year", "sex", "enrolled_at"))
    if isinstance(section.get("birth_year"), int) and not 1900 <= section["birth_year"] <= 2026:
        problems.append(issue("OUT_OF_RANGE", "出生年份不合理"))
    if "sex" in section and section["sex"] not in ("male", "female"):
        problems.append(issue("OUT_OF_RANGE", "sex 必须是 male/female"))
    return problems


def check_questionnaire(section: dict[str, Any], rules: Ruleset, required_items: tuple[str, ...]) -> list[dict[str, str]]:
    problems = _require_keys(section, ("instrument_id", "version", "items"))
    items = section.get("items")
    if problems:
        return problems
    if section["instrument_id"] != INSTRUMENT_BASELINE:
        problems.append(issue("UNKNOWN_INSTRUMENT", f"不支持的问卷：{section['instrument_id']}"))
    if not isinstance(section["version"], int) or isinstance(section["version"], bool):
        problems.append(issue("BAD_VERSION", "问卷版本必须是整数"))
    elif not rules.allows_questionnaire(section["instrument_id"], section["version"]):
        problems.append(issue("RULESET_MISMATCH", f"规则 v{rules.version} 不接受问卷 v{section['version']}"))
    if not isinstance(items, dict):
        problems.append(issue("MISSING_FIELD", "items 必须是对象"))
        return problems
    for name in required_items:
        if name not in items or items[name] in (None, ""):
            problems.append(issue("QUESTIONNAIRE_ITEM_MISSING", f"问卷条目缺失：{name}"))
    return problems


def check_diabetes(section: dict[str, Any], rules: Ruleset) -> list[dict[str, str]]:
    problems = _require_keys(section, ("status",))
    status = section.get("status")
    if status is not None and status not in DIABETES_STATUSES:
        problems.append(issue("OUT_OF_RANGE", f"糖尿病状态非法：{status}"))
    if "hba1c" in section and section["hba1c"] is not None and not _number_in_range(section["hba1c"], 3.0, 20.0):
        problems.append(issue("OUT_OF_RANGE", "HbA1c 超出 3.0-20.0%"))
    if "fasting_glucose" in section and section["fasting_glucose"] is not None:
        if not _number_in_range(section["fasting_glucose"], 2.0, 40.0):
            problems.append(issue("OUT_OF_RANGE", "空腹血糖超出 2.0-40.0 mmol/L"))
    return problems


def check_alcohol(section: dict[str, Any], rules: Ruleset) -> list[dict[str, str]]:
    problems = _require_keys(section, ("drinker_status",))
    status = section.get("drinker_status")
    if status is not None and status not in DRINKER_STATUSES:
        problems.append(issue("OUT_OF_RANGE", f"饮酒状态非法：{status}"))
    grams = section.get("alcohol_grams_per_day")
    if grams is not None and not _number_in_range(grams, 0.0, 500.0):
        problems.append(issue("OUT_OF_RANGE", "日均酒精克数超出 0-500"))
    if status == "current" and grams is None:
        problems.append(issue("MISSING_FIELD", "当前饮酒者必须填报日均酒精克数"))
    return problems


@dataclass(frozen=True)
class CalibrationStatus:
    valid: bool
    reason: str | None = None


def calibration_status_for(
    device: dict[str, Any] | None,
    exam_at: str,
    rules: Ruleset,
) -> CalibrationStatus:
    """device 为设备注册/校准事件折叠后的状态。"""
    if device is None:
        return CalibrationStatus(False, "设备未注册")
    if not device.get("active", True):
        return CalibrationStatus(False, "设备已停用")
    calibrated_at = device.get("calibrated_at")
    if not calibrated_at:
        return CalibrationStatus(False, "设备没有校准记录")
    valid_until = parse_time(calibrated_at) + timedelta(days=rules.calibration_validity_days)
    if parse_time(exam_at) > valid_until:
        return CalibrationStatus(False, f"校准已于 {valid_until.date().isoformat()} 失效")
    return CalibrationStatus(True)


def check_noninv(
    section: dict[str, Any],
    rules: Ruleset,
    device_lookup: Callable[[str], dict[str, Any] | None],
) -> list[dict[str, str]]:
    problems = _require_keys(
        section,
        ("device_id", "exam_at", "valid_shots", "lsm_kpa", "cap_dbm", "iqr_median_ratio"),
    )
    if problems:
        return problems
    exam_at = section["exam_at"]
    try:
        parse_time(exam_at)
    except ValueError:
        problems.append(issue("BAD_TIME", "检查时间必须是带时区的 ISO 8601"))
        return problems
    calibration = calibration_status_for(device_lookup(section["device_id"]), exam_at, rules)
    if not calibration.valid:
        problems.append(issue("CALIBRATION_INVALID", calibration.reason or "校准失效"))
    valid_shots = section["valid_shots"]
    if not isinstance(valid_shots, int) or isinstance(valid_shots, bool) or valid_shots < 0:
        problems.append(issue("OUT_OF_RANGE", "有效测量次数必须是非负整数"))
    elif valid_shots < rules.min_valid_shots:
        problems.append(issue("INSUFFICIENT_SHOTS", f"有效测量 {valid_shots} 次，少于 {rules.min_valid_shots} 次"))
    ratio = section["iqr_median_ratio"]
    if not _number_in_range(ratio, 0.0, 1.0):
        problems.append(issue("OUT_OF_RANGE", "IQR/中位数比值必须在 0-1 之间"))
    elif ratio > rules.iqr_median_max:
        problems.append(issue("LOW_RELIABILITY", f"IQR/中位数 {ratio:.2f} 超过 {rules.iqr_median_max}"))
    if not _number_in_range(section["lsm_kpa"], *rules.lsm_range_kpa):
        problems.append(issue("OUT_OF_RANGE", f"LSM 超出 {rules.lsm_range_kpa} kPa"))
    if not _number_in_range(section["cap_dbm"], *rules.cap_range_dbm):
        problems.append(issue("OUT_OF_RANGE", f"CAP 超出 {rules.cap_range_dbm} dB/m"))
    return problems


def evaluate_section(
    name: str,
    section: dict[str, Any],
    rules: Ruleset,
    questionnaire_items: tuple[str, ...],
    device_lookup: Callable[[str], dict[str, Any] | None],
) -> list[dict[str, str]]:
    if name == SEC_BASELINE:
        return check_baseline(section, rules)
    if name == SEC_QUESTIONNAIRE:
        return check_questionnaire(section, rules, questionnaire_items)
    if name == SEC_DIABETES:
        return check_diabetes(section, rules)
    if name == SEC_ALCOHOL:
        return check_alcohol(section, rules)
    if name == SEC_NONINVASIVE:
        return check_noninv(section, rules, device_lookup)
    return [issue("UNKNOWN_SECTION", f"未知访视组成部分：{name}")]


def fib4(age: float, ast: float, alt: float, platelets: float) -> float:
    """FIB-4 = 年龄 × AST /（血小板 × √ALT），血小板单位 10^9/L。"""
    if min(age, ast, alt, platelets) <= 0:
        raise ValueError("FIB-4 输入必须全部为正")
    return age * ast / (platelets * math.sqrt(alt))


def stratify(
    rules: Ruleset,
    *,
    age: float,
    ast: float,
    alt: float,
    platelets: float,
    lsm_kpa: float,
) -> dict[str, Any]:
    """按当前规则版本给出分层；FIB-4 与 LSM 均处于灰区时判为不确定。"""
    score = fib4(age, ast, alt, platelets)
    if score < rules.fib4_low and lsm_kpa < rules.lsm_low_kpa:
        band = "low"
    elif score >= rules.fib4_high or lsm_kpa >= rules.lsm_high_kpa:
        band = "high"
    else:
        band = "indeterminate"
    return {
        "band": band,
        "fib4": round(score, 3),
        "lsm_kpa": lsm_kpa,
        "ruleset_version": rules.version,
    }
