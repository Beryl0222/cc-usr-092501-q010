"""领域常量：事件类型、聚合类型、组件、角色、范围与规则版本。

事件只追加、不原地修改：任何业务修订都以同聚合上的新版本事件表达，
原始事件继续保留用于溯源。
"""

from __future__ import annotations


class Events:
    # 身份与知情
    PARTICIPANT_ENROLLED = "PARTICIPANT_ENROLLED"
    LINKAGE_RECORDED = "LINKAGE_RECORDED"
    CONSENT_RECORDED = "CONSENT_RECORDED"
    WITHDRAWAL_APPLIED = "WITHDRAWAL_APPLIED"
    # 主数据
    DEVICE_REGISTERED = "DEVICE_REGISTERED"
    DEVICE_CALIBRATED = "DEVICE_CALIBRATED"
    QUESTIONNAIRE_PUBLISHED = "QUESTIONNAIRE_PUBLISHED"
    RULE_PUBLISHED = "RULE_PUBLISHED"
    # 访视及其组成部分（分别留痕）
    VISIT_RECORDED = "VISIT_RECORDED"
    QUESTIONNAIRE_RESPONSE_RECORDED = "QUESTIONNAIRE_RESPONSE_RECORDED"
    NITX_RESULT_RECORDED = "NITX_RESULT_RECORDED"
    RISK_STRATIFIED = "RISK_STRATIFIED"
    REFERRAL_RECORDED = "REFERRAL_RECORDED"
    REFERRAL_UPDATED = "REFERRAL_UPDATED"
    FOLLOWUP_STATUS_RECORDED = "FOLLOWUP_STATUS_RECORDED"
    # 上传与分析
    UPLOAD_ACCEPTED = "UPLOAD_ACCEPTED"
    BATCH_QUARANTINED = "BATCH_QUARANTINED"
    ANALYSIS_LOCKED = "ANALYSIS_LOCKED"
    ANALYSIS_UNLOCKED = "ANALYSIS_UNLOCKED"
    ANALYSIS_FROZEN = "ANALYSIS_FROZEN"
    SNAPSHOT_PUBLISHED = "SNAPSHOT_PUBLISHED"


class Aggregates:
    PARTICIPANT = "participant_link"
    VISIT = "study_visit"
    UPLOAD = "site_upload"
    SNAPSHOT = "analysis_snapshot"
    DEVICE = "device"
    QUESTIONNAIRE = "questionnaire"
    RULE = "rule_definition"
    REFERRAL = "referral"


class Components:
    """一次访视的质量组成部分。"""

    CONSENT = "consent"
    QUESTIONNAIRE = "questionnaire"
    NITX = "nitx"
    RISK = "risk"
    REFERRAL = "referral"
    FOLLOWUP = "followup"

    BASELINE_REQUIRED = (CONSENT, QUESTIONNAIRE, NITX, RISK)


class VisitKind:
    BASELINE = "baseline"
    FOLLOWUP = "followup"


class ConsentScope:
    FULL = "full"                 # 研究分析 + 聚合
    AGGREGATES_ONLY = "aggregates_only"
    WITHDRAWN = "withdrawn"


class RuleKind:
    RISK = "risk"
    QUALITY = "quality"
    STATS = "stats"


class RiskLevel:
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

    ORDER = (LOW, MEDIUM, HIGH)


class Diabetes:
    YES = "yes"
    NO = "no"
    UNKNOWN = "unknown"

    VALUES = (YES, NO, UNKNOWN)


class AlcoholStatus:
    NONE = "none"          # 不饮酒
    WITHIN = "within"      # 限量内
    HEAVY = "heavy"        # 过量暴露

    VALUES = (NONE, WITHIN, HEAVY)


class FollowupStatus:
    RETAINED = "retained"
    LOST = "lost"

    VALUES = (RETAINED, LOST)


class Roles:
    ADMIN = "admin"
    CENTRAL_ANALYST = "central-analyst"
    SITE_COORDINATOR = "site-coordinator"
    CLINICIAN = "clinician"


class Scopes:
    UPLOAD_WRITE = "upload:write"
    RESEARCH_READ = "research:read"
    REFERRAL_READ = "referral:read"
    WITHDRAWAL_WRITE = "withdrawal:write"
    FREEZE_WRITE = "freeze:write"
    TOKEN_WRITE = "token:write"
    IDENTITY_MAP = "identity:map"


ROLE_SCOPES = {
    Roles.ADMIN: (
        Scopes.UPLOAD_WRITE, Scopes.RESEARCH_READ, Scopes.REFERRAL_READ,
        Scopes.WITHDRAWAL_WRITE, Scopes.FREEZE_WRITE, Scopes.TOKEN_WRITE,
        Scopes.IDENTITY_MAP,
    ),
    Roles.CENTRAL_ANALYST: (
        Scopes.RESEARCH_READ, Scopes.FREEZE_WRITE,
    ),
    Roles.SITE_COORDINATOR: (
        Scopes.UPLOAD_WRITE, Scopes.RESEARCH_READ, Scopes.WITHDRAWAL_WRITE,
        Scopes.IDENTITY_MAP,
    ),
    Roles.CLINICIAN: (
        Scopes.REFERRAL_READ,
    ),
}

# 排除 / 缺失原因码（中央复算输出按这些口径计数）
class Reasons:
    PARTICIPANT_WITHDRAWN = "participant_withdrawn"
    WITHDRAWN_AGGREGATES_REVOKED = "withdrawn_aggregates_revoked"
    COMPONENT_MISSING = "component_missing"
    CONSENT_OUT_OF_RANGE = "consent_out_of_range"
    QUESTIONNAIRE_VERSION_UNKNOWN = "questionnaire_version_unknown"
    QUESTIONNAIRE_VALUE_INVALID = "questionnaire_value_invalid"
    DEVICE_UNKNOWN = "device_unknown"
    CALIBRATION_MISMATCH = "calibration_mismatch"
    CALIBRATION_EXPIRED = "calibration_expired"
    NITX_OUT_OF_RANGE = "nitx_out_of_range"
    NITX_SHOTS_INSUFFICIENT = "nitx_shots_insufficient"
    NITX_IQR_TOO_HIGH = "nitx_iqr_too_high"
    RISK_RULE_UNKNOWN = "risk_rule_unknown"
    RISK_LEVEL_MISMATCH = "risk_level_mismatch"
    BASELINE_ALREADY_COUNTED = "baseline_already_counted"


DEFAULT_RISK_RULE = "1"
DEFAULT_QUALITY_RULE = "1"
DEFAULT_STATS_RULE = "1"
