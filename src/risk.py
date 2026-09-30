"""版本化风险分层引擎。

规则以 RULE_PUBLISHED(kind=risk) 发布；引擎按规则版本复算，保证：
- 站点上传的分层结果可以被中央按同一版本复核；
- 以后发布 v2 阈值时，旧冻结批次仍按其记录的 v1 口径解释。

内置 risk-v1：以 FIB-4（需要年龄、AST、ALT、血小板）为主，
糖尿病与重度饮酒暴露按规则参数上调一级。
"""

from __future__ import annotations

import math
from typing import Any

from .contracts import Diabetes, AlcoholStatus, RiskLevel

RISK_RULE_V1: dict[str, Any] = {
    "kind": "risk",
    "version": 1,
    "name": "fib4-adj-v1",
    "low_max": 1.30,
    "high_min": 2.67,
    "diabetes_bump": True,
    "heavy_alcohol_bump": True,
}


def risk_inputs(questionnaire: dict[str, Any] | None,
                nitx: dict[str, Any] | None) -> dict[str, Any]:
    q = questionnaire or {}
    n = nitx or {}
    return {
        "age": q.get("age"),
        "diabetes": q.get("diabetes"),
        "alcohol": q.get("alcohol"),
        "alt": n.get("alt"),
        "ast": n.get("ast"),
        "platelets": n.get("platelets"),
        "lsm_kpa": n.get("lsm_kpa"),
    }


def _bump(level: str) -> str:
    if level == RiskLevel.LOW:
        return RiskLevel.MEDIUM
    if level == RiskLevel.MEDIUM:
        return RiskLevel.HIGH
    return level


def classify(body: dict[str, Any], inputs: dict[str, Any]) -> dict[str, Any]:
    """按规则体复算，返回 {level, score, score_name, adjustments}。

    缺少必要输入时 level 为 None（质量层据此判失败）。
    """
    age = _num(inputs.get("age"))
    ast = _num(inputs.get("ast"))
    alt = _num(inputs.get("alt"))
    platelets = _num(inputs.get("platelets"))
    adjustments: list[str] = []

    score: float | None = None
    level: str | None = None
    if None not in (age, ast, alt, platelets) and alt > 0 and platelets > 0:
        score = round(age * ast / (platelets * math.sqrt(alt)), 3)
        if score < float(body["low_max"]):
            level = RiskLevel.LOW
        elif score < float(body["high_min"]):
            level = RiskLevel.MEDIUM
        else:
            level = RiskLevel.HIGH

    if level is not None:
        before = level
        if body.get("diabetes_bump") and inputs.get("diabetes") == Diabetes.YES:
            level = _bump(level)
            adjustments.append("diabetes")
        if body.get("heavy_alcohol_bump") and inputs.get("alcohol") == AlcoholStatus.HEAVY:
            level = _bump(level)
            adjustments.append("heavy_alcohol")
        if level != before:
            adjustments.append("bump")

    return {
        "level": level,
        "score": score,
        "score_name": "fib4" if score is not None else None,
        "adjustments": sorted(set(adjustments)),
    }


def _num(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None
