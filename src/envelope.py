"""领域事件信封的基础字段校验。"""

from __future__ import annotations

from datetime import datetime

from .events import AGGREGATE_TYPES, EVENT_TYPES

REQUIRED = (
    "event_id",
    "event_type",
    "aggregate_type",
    "aggregate_id",
    "site_id",
    "occurred_at",
    "version",
    "summary",
    "payload",
)


def validate_event(record: object) -> list[str]:
    if not isinstance(record, dict):
        return ["事件必须是 JSON 对象"]
    errors = [f"缺少字段：{name}" for name in REQUIRED if name not in record]
    if "version" in record and (
        not isinstance(record["version"], int)
        or isinstance(record["version"], bool)
        or record["version"] < 1
    ):
        errors.append("version 必须是正整数")
    if "event_type" in record and record["event_type"] not in EVENT_TYPES:
        errors.append(f"未知事件类型：{record.get('event_type')}")
    if "aggregate_type" in record and record["aggregate_type"] not in AGGREGATE_TYPES:
        errors.append(f"未知聚合类型：{record.get('aggregate_type')}")
    if "site_id" in record and (not isinstance(record["site_id"], str) or not record["site_id"].strip()):
        errors.append("site_id 必须是非空字符串")
    if "payload" in record and not isinstance(record["payload"], dict):
        errors.append("payload 必须是 JSON 对象")
    if "occurred_at" in record:
        try:
            parsed = datetime.fromisoformat(str(record["occurred_at"]).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                errors.append("occurred_at 必须包含时区")
        except ValueError:
            errors.append("occurred_at 必须是 ISO 8601 时间")
    return errors
