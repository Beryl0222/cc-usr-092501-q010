"""领域事件类型与构造。

事件是只可追加的事实信封。业务修订（迟到更正、撤回、转诊结局）一律通过
同一聚合上的新版本事件表达，任何事件都不在原地被覆盖。
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

# 事件名称
PARTICIPANT_ENROLLED = "PARTICIPANT_ENROLLED"
CROSS_SITE_LINK_RESOLVED = "CROSS_SITE_LINK_RESOLVED"
CONSENT_GRANTED = "CONSENT_GRANTED"
VISIT_RECORDED = "VISIT_RECORDED"
SECTION_RECORDED = "SECTION_RECORDED"
SECTION_CORRECTED = "SECTION_CORRECTED"
DEVICE_REGISTERED = "DEVICE_REGISTERED"
CALIBRATION_RECORDED = "CALIBRATION_RECORDED"
QUESTIONNAIRE_VERSION_PUBLISHED = "QUESTIONNAIRE_VERSION_PUBLISHED"
RULESET_PUBLISHED = "RULESET_PUBLISHED"
UPLOAD_ACCEPTED = "UPLOAD_ACCEPTED"
UPLOAD_QUARANTINED = "UPLOAD_QUARANTINED"
BATCH_QUARANTINED = "BATCH_QUARANTINED"
VISIT_FROZEN = "VISIT_FROZEN"
ANALYSIS_FROZEN = "ANALYSIS_FROZEN"
SNAPSHOT_PUBLISHED = "SNAPSHOT_PUBLISHED"
RISK_STRATIFIED = "RISK_STRATIFIED"
REFERRAL_RECORDED = "REFERRAL_RECORDED"
REFERRAL_OUTCOME_RECORDED = "REFERRAL_OUTCOME_RECORDED"
FOLLOWUP_STATUS_RECORDED = "FOLLOWUP_STATUS_RECORDED"
WITHDRAWAL_APPLIED = "WITHDRAWAL_APPLIED"

EVENT_TYPES: frozenset[str] = frozenset(
    {
        PARTICIPANT_ENROLLED,
        CROSS_SITE_LINK_RESOLVED,
        CONSENT_GRANTED,
        VISIT_RECORDED,
        SECTION_RECORDED,
        SECTION_CORRECTED,
        DEVICE_REGISTERED,
        CALIBRATION_RECORDED,
        QUESTIONNAIRE_VERSION_PUBLISHED,
        RULESET_PUBLISHED,
        UPLOAD_ACCEPTED,
        UPLOAD_QUARANTINED,
        BATCH_QUARANTINED,
        VISIT_FROZEN,
        ANALYSIS_FROZEN,
        SNAPSHOT_PUBLISHED,
        RISK_STRATIFIED,
        REFERRAL_RECORDED,
        REFERRAL_OUTCOME_RECORDED,
        FOLLOWUP_STATUS_RECORDED,
        WITHDRAWAL_APPLIED,
    }
)

AGGREGATE_TYPES: frozenset[str] = frozenset(
    {
        "participant_link",
        "consent",
        "study_visit",
        "visit_section",
        "device",
        "calibration",
        "questionnaire",
        "ruleset",
        "site_upload",
        "analysis_snapshot",
        "risk_assessment",
        "referral",
        "followup",
    }
)

CENTRAL_SITE = "central"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_event(
    event_id: str,
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    site_id: str,
    version: int,
    summary: str,
    payload: dict[str, Any],
    *,
    occurred_at: str | None = None,
    upload_id: str | None = None,
) -> dict[str, Any]:
    """构造一条完整事件。调用方负责 event_id 与 version 的唯一性。"""
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "site_id": site_id,
        "occurred_at": occurred_at or now_iso(),
        "version": version,
        "summary": summary,
        "payload": payload,
        **({"upload_id": upload_id} if upload_id else {}),
    }


def canonical_json(obj: Any) -> str:
    """稳定序列化：用于上传批次内容指纹，键序与空格变化不影响比对。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("时间必须包含时区")
    return parsed
