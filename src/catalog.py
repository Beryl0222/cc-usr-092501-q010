"""版本化规则目录。

问卷版本、无创检查质量阈值、设备校准有效期与风险分层界值都属于**规则**，
规则只能发布新版本、不能就地改写。访视冻结时固定所用规则版本，事后即可回答
“这条结果按哪一版规则判定”。内置 v1 规则随服务启动写入；新版本可通过
QUESTIONNAIRE_VERSION_PUBLISHED / RULESET_PUBLISHED 事件追加发布。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

INSTRUMENT_BASELINE = "CLH_BASELINE"

# 访视组成部分（section）名称
SEC_BASELINE = "baseline"
SEC_QUESTIONNAIRE = "questionnaire"
SEC_DIABETES = "diabetes"
SEC_ALCOHOL = "alcohol"
SEC_NONINVASIVE = "noninvasive"

ALL_SECTIONS = (SEC_BASELINE, SEC_QUESTIONNAIRE, SEC_DIABETES, SEC_ALCOHOL, SEC_NONINVASIVE)


@dataclass(frozen=True)
class QuestionnaireVersion:
    instrument_id: str
    version: int
    required_items: tuple[str, ...]
    published_at: str


@dataclass(frozen=True)
class Ruleset:
    version: int
    label: str
    effective_from: str
    required_sections: dict[str, tuple[str, ...]]
    questionnaire_versions: dict[str, frozenset[int]]
    calibration_validity_days: int
    min_valid_shots: int
    iqr_median_max: float
    lsm_range_kpa: tuple[float, float]
    cap_range_dbm: tuple[float, float]
    fib4_low: float
    fib4_high: float
    lsm_low_kpa: float
    lsm_high_kpa: float

    def allows_questionnaire(self, instrument_id: str, version: int) -> bool:
        return version in self.questionnaire_versions.get(instrument_id, frozenset())


_BUILTIN_QUESTIONNAIRES = {
    (INSTRUMENT_BASELINE, 1): QuestionnaireVersion(
        instrument_id=INSTRUMENT_BASELINE,
        version=1,
        required_items=(
            "diabetes_history",
            "drinker_status",
            "alcohol_grams_per_day",
            "prior_liver_disease",
        ),
        published_at="2026-01-01T00:00:00+08:00",
    )
}

_BUILTIN_RULESETS = {
    1: Ruleset(
        version=1,
        label="社区肝病队列基线规则 v1",
        effective_from="2026-01-01T00:00:00+08:00",
        required_sections={
            "baseline": ALL_SECTIONS,
            "followup": (SEC_QUESTIONNAIRE, SEC_DIABETES, SEC_ALCOHOL, SEC_NONINVASIVE),
        },
        questionnaire_versions={INSTRUMENT_BASELINE: frozenset({1})},
        calibration_validity_days=180,
        min_valid_shots=10,
        iqr_median_max=0.30,
        lsm_range_kpa=(1.0, 75.0),
        cap_range_dbm=(100.0, 400.0),
        fib4_low=1.3,
        fib4_high=2.67,
        lsm_low_kpa=7.0,
        lsm_high_kpa=11.0,
    )
}


class Catalog:
    """规则目录：内置版本 + 事件发布的新版本。"""

    def __init__(self) -> None:
        self._questionnaires: dict[tuple[str, int], QuestionnaireVersion] = dict(_BUILTIN_QUESTIONNAIRES)
        self._rulesets: dict[int, Ruleset] = dict(_BUILTIN_RULESETS)

    def publish_questionnaire(self, q: QuestionnaireVersion) -> None:
        key = (q.instrument_id, q.version)
        if key in self._questionnaires:
            raise ValueError(f"问卷版本已存在：{q.instrument_id} v{q.version}")
        self._questionnaires[key] = q

    def publish_ruleset(self, rules: Ruleset) -> None:
        if rules.version in self._rulesets:
            raise ValueError(f"规则版本已存在：v{rules.version}")
        self._rulesets[rules.version] = rules

    def has_ruleset(self, version: int) -> bool:
        return version in self._rulesets

    def has_questionnaire(self, instrument_id: str, version: int) -> bool:
        return (instrument_id, version) in self._questionnaires

    def get_ruleset(self, version: int) -> Ruleset:
        try:
            return self._rulesets[version]
        except KeyError:
            raise ValueError(f"未知规则版本：v{version}") from None

    def get_questionnaire(self, instrument_id: str, version: int) -> QuestionnaireVersion:
        try:
            return self._questionnaires[(instrument_id, version)]
        except KeyError:
            raise ValueError(f"未知问卷版本：{instrument_id} v{version}") from None

    def ruleset_for(self, at_iso: str) -> Ruleset:
        """返回某一时点已生效的最新规则版本。"""
        candidates = [r for r in self._rulesets.values() if r.effective_from <= at_iso]
        if not candidates:
            raise ValueError("该时点没有已生效的规则版本")
        return max(candidates, key=lambda r: r.version)

    def latest_ruleset(self) -> Ruleset:
        return max(self._rulesets.values(), key=lambda r: r.version)

    def apply_events(self, events: list[dict[str, Any]]) -> None:
        """从 RULESET_PUBLISHED / QUESTIONNAIRE_VERSION_PUBLISHED 事件重建目录。

        重建是幂等的：内存目录已含的版本直接跳过（每次重放都会遇到历史事件）。
        """
        for event in events:
            p = event["payload"]
            if event["event_type"] == "RULESET_PUBLISHED":
                if p["version"] not in self._rulesets:
                    self.publish_ruleset(ruleset_from_payload(p))
            elif event["event_type"] == "QUESTIONNAIRE_VERSION_PUBLISHED":
                key = (p["instrument_id"], p["version"])
                if key not in self._questionnaires:
                    self.publish_questionnaire(
                        QuestionnaireVersion(
                            instrument_id=p["instrument_id"],
                            version=p["version"],
                            required_items=tuple(p["required_items"]),
                            published_at=event["occurred_at"],
                        )
                    )


def ruleset_from_payload(p: dict[str, Any]) -> Ruleset:
    return Ruleset(
        version=p["version"],
        label=p["label"],
        effective_from=p["effective_from"],
        required_sections={k: tuple(v) for k, v in p["required_sections"].items()},
        questionnaire_versions={k: frozenset(v) for k, v in p["questionnaire_versions"].items()},
        calibration_validity_days=p["calibration_validity_days"],
        min_valid_shots=p["min_valid_shots"],
        iqr_median_max=p["iqr_median_max"],
        lsm_range_kpa=tuple(p["lsm_range_kpa"]),
        cap_range_dbm=tuple(p["cap_range_dbm"]),
        fib4_low=p["fib4_low"],
        fib4_high=p["fib4_high"],
        lsm_low_kpa=p["lsm_low_kpa"],
        lsm_high_kpa=p["lsm_high_kpa"],
    )


def ruleset_payload(r: Ruleset) -> dict[str, Any]:
    return {
        "version": r.version,
        "label": r.label,
        "effective_from": r.effective_from,
        "required_sections": {k: list(v) for k, v in r.required_sections.items()},
        "questionnaire_versions": {k: sorted(v) for k, v in r.questionnaire_versions.items()},
        "calibration_validity_days": r.calibration_validity_days,
        "min_valid_shots": r.min_valid_shots,
        "iqr_median_max": r.iqr_median_max,
        "lsm_range_kpa": list(r.lsm_range_kpa),
        "cap_range_dbm": list(r.cap_range_dbm),
        "fib4_low": r.fib4_low,
        "fib4_high": r.fib4_high,
        "lsm_low_kpa": r.lsm_low_kpa,
        "lsm_high_kpa": r.lsm_high_kpa,
    }
