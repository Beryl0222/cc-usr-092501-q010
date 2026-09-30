"""站点上传管线：命令批次 → 领域事件。

语义边界（关键设计）：

- *结构性/引用性错误*（JSON 损坏、信封非法、未知命令、参与者未招募、
  upload_id 复用、同凭证内容冲突）→ 整批隔离：只追加一条
  BATCH_QUARANTINED，命令事件一个都不落库，其他站点批次不受影响。
- *质量规则失败*（校准过期、数值越界、风险复算不一致等）→ 事实照常接收
  留痕，事件全部落库；对应访视在冻结闸门被排除，中央复算输出原因码。

幂等：同一凭证 + 同一规范化内容重传，直接回放首次结果，不产生第二条
UPLOAD_ACCEPTED；同凭证 + 不同内容一律隔离。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .contracts import Aggregates, Events, FollowupStatus, VisitKind
from .envelope import validate_event
from .identity import new_id
from .projection import Projection
from .store import EventStore

COMMAND_TYPES = {
    "enroll", "consent", "withdrawal",
    "device_register", "calibration",
    "questionnaire_publish", "rule_publish", "visit",
    "questionnaire_update", "nitx_update", "risk_update",
    "followup_update", "referral_update",
}

# 更正命令 -> (事件类型, 摘要, 必填字段)
_UPDATE_SPECS = {
    "questionnaire_update": (
        Events.QUESTIONNAIRE_RESPONSE_RECORDED, "问卷应答更正（新版本）",
        ("version", "age", "diabetes", "alcohol"),
    ),
    "nitx_update": (
        Events.NITX_RESULT_RECORDED, "无创结果更正（新版本）",
        ("device_id", "calibration_id", "measured_at", "valid_shots",
         "lsm_kpa", "lsm_iqr", "alt", "ast", "platelets"),
    ),
    "risk_update": (
        Events.RISK_STRATIFIED, "风险分层更正（新版本）",
        ("rule_version", "level"),
    ),
    "followup_update": (
        Events.FOLLOWUP_STATUS_RECORDED, "随访状态更正（新版本）",
        ("status",),
    ),
}


class Quarantine(Exception):
    """致命错误：整批必须隔离。"""


@dataclass
class UploadResult:
    upload_id: str
    accepted: bool
    idempotent: bool = False
    event_count: int = 0
    quarantine_reason: str | None = None
    event_ids: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "upload_id": self.upload_id,
            "status": "accepted" if self.accepted else "quarantined",
            "idempotent": self.idempotent,
            "event_count": self.event_count,
            "quarantine_reason": self.quarantine_reason,
            "event_ids": self.event_ids,
        }


def canonical_hash(raw: bytes) -> str:
    """先解析再规范化序列化：空白/键序差异不算内容冲突。"""
    parsed = json.loads(raw.decode("utf-8"))
    canonical = json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _looks_like_idcard(value: str) -> bool:
    """识别疑似身份证号（15/18 位，末位可为 X），防止身份直接入库。"""
    v = value.strip()
    if len(v) == 18 and v[:17].isdigit() and (v[17].isdigit() or v[17] in "Xx"):
        return True
    return len(v) == 15 and v.isdigit()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class BatchTranslator:
    """把一个批次翻译成事件列表。

    翻译在一个从已提交事件流重建的临时投影上做引用检查；每发出一条事件
    立即应用到临时投影，因此同批次内“先招募、后知情/访视”可以串联。
    临时投影与线上投影隔离，翻译失败不留任何痕迹。
    """

    def __init__(self, scratch: Projection, versions: dict[tuple[str, str], int],
                 site_id: str, upload_id: str, site_salt: str) -> None:
        self.p = scratch
        self.versions = versions
        self.site_id = site_id
        self.upload_id = upload_id
        self.site_salt = site_salt
        self.events: list[dict[str, Any]] = []
        self._counter = 0

    def _next_event_id(self) -> str:
        self._counter += 1
        return f"ev-{self.upload_id}-{self._counter:04d}"

    def _bump(self, aggregate_type: str, aggregate_id: str) -> int:
        key = (aggregate_type, aggregate_id)
        version = self.versions.get(key, 0) + 1
        self.versions[key] = version
        return version

    def _emit(self, event_type: str, aggregate_type: str, aggregate_id: str,
              summary: str, payload: dict[str, Any], occurred_at: str) -> None:
        event = {
            "event_id": self._next_event_id(),
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "occurred_at": occurred_at,
            "version": self._bump(aggregate_type, aggregate_id),
            "summary": summary,
            "site_id": self.site_id,
            "upload_id": self.upload_id,
            "payload": payload,
        }
        self.events.append(event)
        # 带上临时序号后应用到临时投影，供本批后续命令引用
        event_for_projection = dict(event)
        event_for_projection["seq"] = 10**12 + self._counter
        self.p.apply(event_for_projection)

    def _require_participant(self, pid: str) -> None:
        if self.p.participant(pid) is None:
            raise Quarantine(f"命令引用了未招募的参与者：{pid}")

    def translate(self, batch: dict[str, Any]) -> None:
        commands = batch.get("commands")
        if not isinstance(commands, list) or not commands:
            raise Quarantine("批次缺少非空 commands 数组")
        seen_visits: set[str] = set()
        for index, command in enumerate(commands):
            if not isinstance(command, dict):
                raise Quarantine(f"第 {index} 条命令不是对象")
            kind = command.get("type")
            if kind not in COMMAND_TYPES:
                raise Quarantine(f"第 {index} 条命令类型未知：{kind}")
            at = str(command.get("at") or _now())
            getattr(self, f"_cmd_{kind}")(command, at, index, seen_visits)

    # ---------- 身份与知情 ----------
    def _cmd_enroll(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        # 首选站点本地映射好的研究标识；若随批次带来本地身份证号，则用站点
        # 盐即时派生。身份证号只在这一步出现，绝不进入事件 payload。
        pid = c.get("pid")
        id_card = c.get("id_card")
        if not pid and id_card:
            from .identity import pseudonym
            pid = pseudonym(self.site_id, str(id_card), self.site_salt)
        if not isinstance(pid, str) or not pid:
            raise Quarantine(f"第 {i} 条 enroll 缺少 pid 或本地 id_card")
        if _looks_like_idcard(pid):
            raise Quarantine(
                f"第 {i} 条 enroll 疑似直接提交身份证号，必须先经本地映射生成研究标识"
            )
        if self.p.participant(pid) is not None:
            raise Quarantine(f"参与者已招募，重复登记：{pid}")
        self._emit(
            Events.PARTICIPANT_ENROLLED, Aggregates.PARTICIPANT, pid,
            "站点登记参与者", {"site_id": self.site_id}, at,
        )

    def _cmd_consent(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        pid = c.get("pid")
        scope = c.get("scope")
        if not pid or scope not in ("full", "aggregates_only"):
            raise Quarantine(f"第 {i} 条 consent 字段非法")
        self._require_participant(pid)
        owner = self.p.canonical(pid)
        payload = {"scope": scope, "from": c.get("from"),
                   "statement": c.get("statement", "")}
        self._emit(
            Events.CONSENT_RECORDED, Aggregates.PARTICIPANT, owner,
            f"知情同意留痕：{scope}", payload, at,
        )

    def _cmd_withdrawal(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        pid = c.get("pid")
        aggregates = c.get("aggregates")
        if not pid or aggregates not in ("retain", "remove"):
            raise Quarantine(f"第 {i} 条 withdrawal 字段非法")
        self._require_participant(pid)
        participant = self.p.participant(pid)
        if participant is not None and participant.withdrawn:
            raise Quarantine(f"参与者已撤回，撤回事件不可重复：{pid}")
        owner = self.p.canonical(pid)
        self._emit(
            Events.WITHDRAWAL_APPLIED, Aggregates.PARTICIPANT, owner,
            "研究撤回", {"at": c.get("at", at), "aggregates": aggregates}, at,
        )

    # ---------- 主数据 ----------
    def _cmd_device_register(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        device_id = c.get("device_id")
        if not device_id:
            raise Quarantine(f"第 {i} 条 device_register 缺少 device_id")
        if device_id in self.p.devices:
            raise Quarantine(f"设备已登记：{device_id}")
        self._emit(
            Events.DEVICE_REGISTERED, Aggregates.DEVICE, device_id,
            "无创检查设备登记",
            {"site_id": c.get("site_id", self.site_id), "model": c.get("model")}, at,
        )

    def _cmd_calibration(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        device_id = c.get("device_id")
        calibration_id = c.get("calibration_id")
        calibrated_at = c.get("calibrated_at")
        if not device_id or not calibration_id or not calibrated_at:
            raise Quarantine(f"第 {i} 条 calibration 字段不完整")
        if device_id not in self.p.devices:
            raise Quarantine(f"校准指向未登记设备：{device_id}")
        self._emit(
            Events.DEVICE_CALIBRATED, Aggregates.DEVICE, device_id,
            f"设备校准 {calibration_id}",
            {"calibration_id": calibration_id, "calibrated_at": calibrated_at,
             "firmware": c.get("firmware")}, at,
        )

    def _cmd_questionnaire_publish(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        version = c.get("version")
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            raise Quarantine(f"第 {i} 条 questionnaire_publish 版本非法")
        aggregate_id = f"questionnaire:{version}"
        if str(version) in self.p.questionnaires:
            raise Quarantine(f"问卷版本已发布：{version}")
        self._emit(
            Events.QUESTIONNAIRE_PUBLISHED, Aggregates.QUESTIONNAIRE, aggregate_id,
            f"问卷 v{version} 发布", {"version": version}, at,
        )

    def _cmd_rule_publish(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        kind = c.get("kind")
        version = c.get("version")
        if kind not in ("risk", "quality", "stats") or not isinstance(version, int) \
                or isinstance(version, bool) or version < 1:
            raise Quarantine(f"第 {i} 条 rule_publish 字段非法")
        if self.p.rule_body(kind, version) is not None:
            raise Quarantine(f"规则已发布：{kind} v{version}")
        aggregate_id = f"rule:{kind}:{version}"
        body = c.get("body") or _builtin_rule(kind)
        if body is None:
            raise Quarantine(f"第 {i} 条规则缺少 body 且无内置版本：{kind} v{version}")
        body = {"kind": kind, "version": version, **body}
        self._emit(
            Events.RULE_PUBLISHED, Aggregates.RULE, aggregate_id,
            f"{kind} 规则 v{version} 发布", body, at,
        )

    # ---------- 访视及各组成部分 ----------
    def _cmd_visit(self, c: dict[str, Any], at: str, i: int, seen: set[str]) -> None:
        visit_id = c.get("visit_id")
        pid = c.get("pid")
        kind = c.get("kind", VisitKind.BASELINE)
        visit_date = c.get("visit_date")
        if not visit_id or not pid or not visit_date:
            raise Quarantine(f"第 {i} 条 visit 缺少 visit_id/pid/visit_date")
        if kind not in (VisitKind.BASELINE, VisitKind.FOLLOWUP):
            raise Quarantine(f"第 {i} 条 visit 类型非法：{kind}")
        if visit_id in seen or visit_id in self.p.visits:
            raise Quarantine(f"访视标识重复或已存在：{visit_id}")
        seen.add(visit_id)
        self._require_participant(pid)
        owner = self.p.canonical(pid)

        self._emit(
            Events.VISIT_RECORDED, Aggregates.VISIT, visit_id,
            f"{kind} 访视登记",
            {"pid": owner, "site_id": self.site_id, "kind": kind,
             "visit_date": visit_date}, at,
        )
        if isinstance(c.get("questionnaire"), dict):
            self._component(
                Events.QUESTIONNAIRE_RESPONSE_RECORDED, visit_id,
                "问卷应答", c["questionnaire"], at,
                required=("version", "age", "diabetes", "alcohol"),
            )
        if isinstance(c.get("nitx"), dict):
            self._component(
                Events.NITX_RESULT_RECORDED, visit_id, "无创检查结果", c["nitx"], at,
                required=("device_id", "calibration_id", "measured_at", "valid_shots",
                          "lsm_kpa", "lsm_iqr", "alt", "ast", "platelets"),
            )
        if isinstance(c.get("risk"), dict):
            risk = c["risk"]
            if "rule_version" not in risk or "level" not in risk:
                raise Quarantine(f"访视 {visit_id} 的 risk 缺少 rule_version/level")
            self._component(Events.RISK_STRATIFIED, visit_id, "风险分层", risk, at,
                            required=("rule_version", "level"))
        if isinstance(c.get("followup"), dict):
            followup = c["followup"]
            if followup.get("status") not in FollowupStatus.VALUES:
                raise Quarantine(f"访视 {visit_id} 的随访状态非法")
            self._component(
                Events.FOLLOWUP_STATUS_RECORDED, visit_id, "随访/失访状态",
                followup, at, required=("status",),
            )
        if isinstance(c.get("referral"), dict):
            ref = c["referral"]
            referral_id = ref.get("referral_id")
            if not referral_id or not ref.get("indication"):
                raise Quarantine(f"访视 {visit_id} 的转诊字段不完整")
            if referral_id in self.p.referrals:
                raise Quarantine(f"转诊记录已存在：{referral_id}")
            created_at = ref.get("created_at", at)
            self._emit(
                Events.REFERRAL_RECORDED, Aggregates.REFERRAL, referral_id,
                f"临床转诊：{ref['indication']}",
                {"pid": owner, "visit_id": visit_id, "site_id": self.site_id,
                 "indication": ref["indication"],
                 "status": ref.get("status", "open"), "created_at": created_at},
                created_at,
            )

    def _component(self, event_type: str, visit_id: str, summary: str,
                   payload: dict[str, Any], at: str, required: tuple[str, ...]) -> None:
        missing = [name for name in required if name not in payload]
        if missing:
            raise Quarantine(f"访视 {visit_id} 的 {summary} 缺少字段：{','.join(missing)}")
        self._emit(event_type, Aggregates.VISIT, visit_id, summary, payload, at)

    # ---------- 迟到更正：在同一访视聚合上发新版本 ----------
    def _cmd_questionnaire_update(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        self._update_component(c, at, i, "questionnaire_update")

    def _cmd_nitx_update(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        self._update_component(c, at, i, "nitx_update")

    def _cmd_risk_update(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        self._update_component(c, at, i, "risk_update")

    def _cmd_followup_update(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        self._update_component(c, at, i, "followup_update")

    def _update_component(self, c: dict[str, Any], at: str, i: int, kind: str) -> None:
        visit_id = c.get("visit_id")
        if not visit_id or visit_id not in self.p.visits:
            raise Quarantine(f"第 {i} 条 {kind} 指向不存在的访视：{visit_id}")
        event_type, summary, required = _UPDATE_SPECS[kind]
        payload = {k: v for k, v in c.items() if k not in ("type", "visit_id", "at")}
        missing = [name for name in required if name not in payload]
        if missing:
            raise Quarantine(
                f"访视 {visit_id} 的 {summary} 缺少字段：{','.join(missing)}"
            )
        if kind == "followup_update" and payload["status"] not in FollowupStatus.VALUES:
            raise Quarantine(f"访视 {visit_id} 的随访状态非法")
        self._emit(event_type, Aggregates.VISIT, visit_id, summary, payload, at)

    def _cmd_referral_update(self, c: dict[str, Any], at: str, i: int, _: set) -> None:
        referral_id = c.get("referral_id")
        status = c.get("status")
        if not referral_id or not status:
            raise Quarantine(f"第 {i} 条 referral_update 字段不完整")
        if referral_id not in self.p.referrals:
            raise Quarantine(f"转诊记录不存在：{referral_id}")
        self._emit(
            Events.REFERRAL_UPDATED, Aggregates.REFERRAL, referral_id,
            f"临床转诊状态更新：{status}",
            {"status": status, "updated_at": c.get("at", at)}, c.get("at", at),
        )


def _builtin_rule(kind: str) -> dict[str, Any] | None:
    if kind == "risk":
        from .risk import RISK_RULE_V1
        return dict(RISK_RULE_V1)
    if kind == "quality":
        from .quality import QUALITY_RULE_V1
        return dict(QUALITY_RULE_V1)
    if kind == "stats":
        return {"name": "prevalence-v1", "strata": ["risk", "site", "province"]}
    return None


def seal_batch(store: EventStore, projection: Projection, token_id: str,
               raw: bytes) -> UploadResult:
    """校验并落库一个上传批次。调用方须已完成鉴权。

    翻译在从已提交事件重建的临时投影上进行；失败或并发落败都不会污染
    线上投影，也不会推高任何聚合版本。
    """
    token = store.get_token(token_id)
    if token is None or not token["active"]:
        raise Quarantine("上传凭证无效或已停用")
    site_id = token["site_id"]
    site_salt = store.site_salt(site_id)

    # 1) 规范化内容哈希（坏 JSON 直接隔离）
    try:
        content_hash = canonical_hash(raw)
        batch = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        return _quarantine(store, projection, token_id, site_id,
                           new_id("U"), f"内容不是合法 JSON：{error}")

    upload_id = batch.get("upload_id") if isinstance(batch, dict) else None
    if not isinstance(upload_id, str) or not upload_id:
        return _quarantine(store, projection, token_id, site_id,
                           new_id("U"), "批次缺少 upload_id")

    # 2) 幂等 / 冲突判定（同一凭证）
    first = store.first_use(token_id)
    if first is not None:
        if first["content_hash"] == content_hash:
            prior = store.get_batch(first["upload_id"])
            return UploadResult(
                upload_id=first["upload_id"],
                accepted=prior["status"] == "accepted",
                idempotent=True,
                event_count=0,
                quarantine_reason=prior["reason"],
            )
        return _quarantine(
            store, projection, token_id, site_id, upload_id,
            f"凭证 {token_id[:10]}… 曾提交不同内容（sha256={content_hash[:12]}），"
            "整批隔离以防污染", conflicting_hash=content_hash,
        )

    # upload_id 不得在不同凭证间复用
    if store.get_batch(upload_id) is not None:
        return _quarantine(store, projection, token_id, site_id, upload_id,
                           "upload_id 已被其他提交使用")

    # 3) 从已提交事件重建临时投影与版本号。翻译只作用于副本，
    #    失败或并发落败都不会污染线上投影，也不会推高任何版本。
    scratch, scratch_versions = build_scratch(store)
    translator = BatchTranslator(scratch, scratch_versions, site_id, upload_id, site_salt)
    try:
        translator.translate(batch)
    except Quarantine as error:
        return _quarantine(store, projection, token_id, site_id, upload_id,
                           str(error))

    # 4) 信封终检
    for event in translator.events:
        errors = validate_event(event)
        if errors:
            return _quarantine(store, projection, token_id, site_id, upload_id,
                               "；".join(errors))

    # 5) 原子提交：命令事件 + UPLOAD_ACCEPTED + 批次台账 + 凭证首用硬约束。
    # 任一约束冲突（并发同凭证、聚合版本竞争）都整事务回滚后重试或隔离。
    import sqlite3

    for attempt in range(3):
        # 每次重试都基于最新已提交事件重新计算版本
        if attempt > 0:
            scratch, scratch_versions = build_scratch(store)
            translator = BatchTranslator(scratch, scratch_versions, site_id, upload_id, site_salt)
            try:
                translator.translate(batch)
            except Quarantine as error:
                return _quarantine(store, projection, token_id, site_id,
                                   upload_id, str(error))

        accepted = {
            "event_id": f"ev-{upload_id}-accept",
            "event_type": Events.UPLOAD_ACCEPTED,
            "aggregate_type": Aggregates.UPLOAD,
            "aggregate_id": upload_id,
            "occurred_at": _now(),
            "version": scratch_versions.get((Aggregates.UPLOAD, upload_id), 0) + 1,
            "summary": f"站点 {site_id} 批次接收：{len(translator.events)} 个事件",
            "site_id": site_id,
            "upload_id": upload_id,
            "payload": {"event_count": len(translator.events),
                        "content_hash": content_hash},
        }

        try:
            with store.transaction() as conn:
                for event in translator.events:
                    event["seq"] = store._insert(conn, event)
                accepted["seq"] = store._insert(conn, accepted)
                conn.execute(
                    """INSERT INTO upload_batches(upload_id, token_id, site_id,
                                                  content_hash, status, reason, received_at)
                       VALUES (?,?,?,?, 'accepted', NULL, ?)""",
                    (upload_id, token_id, site_id, content_hash, _now()),
                )
                # 硬约束：与并发的同凭证首用抢锁，败者整事务回滚
                conn.execute(
                    """INSERT INTO token_first_use(token_id, content_hash, upload_id, status)
                       VALUES (?,?,?, 'accepted')""",
                    (token_id, content_hash, upload_id),
                )
            break
        except sqlite3.IntegrityError:
            winner = store.first_use(token_id)
            if winner is not None:
                if winner["content_hash"] == content_hash:
                    prior = store.get_batch(winner["upload_id"])
                    return UploadResult(
                        upload_id=winner["upload_id"],
                        accepted=prior["status"] == "accepted",
                        idempotent=True, event_count=0,
                        quarantine_reason=prior["reason"],
                    )
                return _quarantine(
                    store, projection, token_id, site_id, upload_id,
                    "并发提交竞争：同一凭证已提交不同内容，本批整批隔离",
                    conflicting_hash=content_hash,
                )
            # 聚合版本竞争：稍后基于最新事件流重试
            continue
    else:
        return _quarantine(store, projection, token_id, site_id, upload_id,
                           "并发版本竞争多次重试仍失败，整批隔离")

    for event in translator.events:
        projection.apply(event)
    projection.apply(accepted)

    return UploadResult(
        upload_id=upload_id, accepted=True,
        event_count=len(translator.events),
        event_ids=[e["event_id"] for e in translator.events],
    )


def build_scratch(store: EventStore) -> tuple[Projection, dict[tuple[str, str], int]]:
    """从已提交事件重建临时投影，并统计每个聚合的最大版本号。"""
    committed = store.events()
    scratch = Projection()
    scratch.rebuild(committed)
    versions: dict[tuple[str, str], int] = {}
    for event in committed:
        key = (event["aggregate_type"], event["aggregate_id"])
        versions[key] = max(versions.get(key, 0), event["version"])
    return scratch, versions


def _quarantine(store: EventStore, projection: Projection,
                token_id: str, site_id: str, upload_id: str, reason: str,
                conflicting_hash: str | None = None) -> UploadResult:
    """落一条隔离事件与台账；命令事件一个都不写。

    隔离事件的版本号按已提交事件流实时计算，绝不使用翻译期的临时计数。
    """
    _, versions = build_scratch(store)
    content_hash = conflicting_hash or hashlib.sha256(upload_id.encode()).hexdigest()
    event = {
        "event_id": f"ev-{upload_id}-quarantine",
        "event_type": Events.BATCH_QUARANTINED,
        "aggregate_type": Aggregates.UPLOAD,
        "aggregate_id": upload_id,
        "occurred_at": _now(),
        "version": versions.get((Aggregates.UPLOAD, upload_id), 0) + 1,
        "summary": f"站点 {site_id} 批次隔离：{reason}",
        "site_id": site_id,
        "upload_id": upload_id,
        "payload": {"reason": reason, "content_hash": content_hash},
    }
    inserted_seq: int | None = None
    with store.transaction() as conn:
        try:
            inserted_seq = store._insert(conn, event)
        except sqlite3.IntegrityError:
            # 隔离事件本身撞键（极少），隔离结论仍由台账表达
            pass
        conn.execute(
            """INSERT INTO upload_batches(upload_id, token_id, site_id, content_hash,
                                          status, reason, received_at)
               VALUES (?,?,?,?, 'quarantined', ?, ?)
               ON CONFLICT(upload_id) DO NOTHING""",
            (upload_id, token_id, site_id, content_hash, reason, _now()),
        )
        conn.execute(
            """INSERT INTO token_first_use(token_id, content_hash, upload_id, status)
               VALUES (?,?,?, 'quarantined')
               ON CONFLICT(token_id) DO NOTHING""",
            (token_id, content_hash, upload_id),
        )
    if inserted_seq is not None:
        event["seq"] = inserted_seq
        projection.apply(event)
    return UploadResult(upload_id=upload_id, accepted=False,
                        quarantine_reason=reason)
