"""领域事件信封的基础字段校验。

信封只做结构性校验：必填字段、版本为正整数、时间为带时区的 ISO 8601、
事件/聚合类型在契约枚举内。业务规则由各领域模块负责。
"""

from __future__ import annotations

from datetime import datetime

from .contracts import Aggregates, Events

REQUIRED = (
    "event_id", "event_type", "aggregate_type", "aggregate_id",
    "occurred_at", "version", "summary",
)

_EVENT_TYPES = {v for k, v in vars(Events).items() if not k.startswith("_")}
_AGGREGATE_TYPES = {v for k, v in vars(Aggregates).items() if not k.startswith("_")}


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
    if "occurred_at" in record:
        try:
            parsed = datetime.fromisoformat(str(record["occurred_at"]).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                errors.append("occurred_at 必须包含时区")
        except ValueError:
            errors.append("occurred_at 必须是 ISO 8601 时间")
    if "event_type" in record and record["event_type"] not in _EVENT_TYPES:
        errors.append(f"未知事件类型：{record['event_type']}")
    if "aggregate_type" in record and record["aggregate_type"] not in _AGGREGATE_TYPES:
        errors.append(f"未知聚合类型：{record['aggregate_type']}")
    return errors
