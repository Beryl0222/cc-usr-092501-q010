"""只读投影：从事件流重放出当前读模型。

事件是唯一事实来源；投影可随时丢弃并从 1 号事件重建。投影里的“当前值”
始终是该聚合上版本最大的事件（迟到更正以新版本事件覆盖当前值，但历史
事件原样保留，供溯源与已发布快照使用）。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from .contracts import Aggregates, Events, RuleKind


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_date(value: str) -> date:
    return parse_dt(value).date()


@dataclass
class Participant:
    pid: str
    home_site: str
    enrolled_seq: int
    aliases: set[str] = field(default_factory=set)
    consent: dict[str, Any] | None = None
    withdrawn: bool = False
    withdrawn_at: str | None = None
    aggregates_retained: bool = False  # 撤回后是否仍允许保留既有聚合

    def all_pids(self) -> set[str]:
        return self.aliases | {self.pid}


@dataclass
class Visit:
    visit_id: str
    pid: str
    site_id: str
    kind: str
    visit_date: str
    seq: int
    upload_ids: set[str] = field(default_factory=set)
    questionnaire: dict[str, Any] | None = None
    nitx: dict[str, Any] | None = None
    risk: dict[str, Any] | None = None
    followup: dict[str, Any] | None = None
    referral_ids: list[str] = field(default_factory=list)


@dataclass
class Referral:
    referral_id: str
    pid: str
    visit_id: str
    site_id: str
    indication: str
    status: str
    created_at: str
    updated_at: str


@dataclass
class Device:
    device_id: str
    site_id: str | None
    model: str | None
    # 校准保留历史：每次 DEVICE_CALIBRATED 追加一条；访视按其声明的
    # calibration_id 回到当时的校准窗口校验，设备后续重新校准不追溯否定旧访视。
    calibrations: dict[str, dict[str, Any]] = field(default_factory=dict)
    calibration_order: list[str] = field(default_factory=list)

    @property
    def calibration(self) -> dict[str, Any] | None:
        return self.calibrations[self.calibration_order[-1]] if self.calibration_order else None


@dataclass
class Snapshot:
    snapshot_id: str
    frozen_seq: int
    payload: dict[str, Any]
    published: dict[str, Any] | None = None


class Projection:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.seq = 0
        self.participants: dict[str, Participant] = {}
        self.alias_owner: dict[str, str] = {}  # 任一化名 -> 规范参与者
        self.visits: dict[str, Visit] = {}
        self.referrals: dict[str, Referral] = {}
        self.devices: dict[str, Device] = {}
        self.questionnaires: dict[str, int] = {}   # version -> 发布序号
        self.rules: dict[str, dict[int, dict[str, Any]]] = {
            RuleKind.RISK: {}, RuleKind.QUALITY: {}, RuleKind.STATS: {}
        }
        self.snapshots: dict[str, Snapshot] = {}

    # ---------- 重建 ----------
    def rebuild(self, events: list[dict[str, Any]]) -> None:
        with self._lock:
            fresh = Projection()
            for event in events:
                fresh._apply(event)
            self.__dict__.update(fresh.__dict__)

    def apply(self, event: dict[str, Any]) -> None:
        with self._lock:
            self._apply(event)

    def _apply(self, e: dict[str, Any]) -> None:
        self.seq = e["seq"]
        p = e.get("payload", {})
        t, at, aid = e["event_type"], e["aggregate_type"], e["aggregate_id"]
        if t == Events.PARTICIPANT_ENROLLED:
            participant = Participant(
                pid=aid, home_site=p["site_id"], enrolled_seq=e["seq"]
            )
            participant.aliases.add(aid)
            self.participants[aid] = participant
            self.alias_owner[aid] = aid
        elif t == Events.LINKAGE_RECORDED:
            owner_key = self.alias_owner.get(aid, aid)
            owner = self.participants.setdefault(
                owner_key, Participant(pid=owner_key, home_site=p["site_id"],
                                       enrolled_seq=e["seq"])
            )
            other = p["pseudonym"]
            owner.aliases.add(other)
            self.alias_owner[other] = owner.pid
        elif t == Events.CONSENT_RECORDED:
            owner = self._participant(aid)
            owner.consent = p
        elif t == Events.WITHDRAWAL_APPLIED:
            owner = self._participant(aid)
            owner.withdrawn = True
            owner.withdrawn_at = p["at"]
            owner.aggregates_retained = bool(p.get("aggregates") == "retain")
        elif t == Events.DEVICE_REGISTERED:
            self.devices[aid] = Device(
                device_id=aid, site_id=p.get("site_id"),
                model=p.get("model"),
            )
        elif t == Events.DEVICE_CALIBRATED:
            device = self.devices.setdefault(
                aid, Device(device_id=aid, site_id=None, model=None)
            )
            cal_id = str(p.get("calibration_id"))
            device.calibrations[cal_id] = p
            if cal_id not in device.calibration_order:
                device.calibration_order.append(cal_id)
        elif t == Events.QUESTIONNAIRE_PUBLISHED:
            self.questionnaires[str(p["version"])] = e["seq"]
        elif t == Events.RULE_PUBLISHED:
            kind = p["kind"]
            self.rules.setdefault(kind, {})[int(p["version"])] = p
        elif t == Events.VISIT_RECORDED:
            owner = self.alias_owner.get(p["pid"], p["pid"])
            visit = Visit(
                visit_id=aid, pid=owner, site_id=p["site_id"], kind=p["kind"],
                visit_date=p["visit_date"], seq=e["seq"],
            )
            if e.get("upload_id"):
                visit.upload_ids.add(e["upload_id"])
            self.visits[aid] = visit
        elif t in (
            Events.QUESTIONNAIRE_RESPONSE_RECORDED, Events.NITX_RESULT_RECORDED,
            Events.RISK_STRATIFIED, Events.FOLLOWUP_STATUS_RECORDED,
        ):
            visit = self.visits[aid]
            if e.get("upload_id"):
                visit.upload_ids.add(e["upload_id"])
            slot = {
                Events.QUESTIONNAIRE_RESPONSE_RECORDED: "questionnaire",
                Events.NITX_RESULT_RECORDED: "nitx",
                Events.RISK_STRATIFIED: "risk",
                Events.FOLLOWUP_STATUS_RECORDED: "followup",
            }[t]
            setattr(visit, slot, p)
        elif t == Events.REFERRAL_RECORDED:
            referral = Referral(
                referral_id=aid, pid=self.alias_owner.get(p["pid"], p["pid"]),
                visit_id=p["visit_id"], site_id=p["site_id"],
                indication=p["indication"], status=p.get("status", "open"),
                created_at=p["created_at"], updated_at=p["created_at"],
            )
            self.referrals[aid] = referral
            visit = self.visits.get(p["visit_id"])
            if visit is not None and aid not in visit.referral_ids:
                visit.referral_ids.append(aid)
        elif t == Events.REFERRAL_UPDATED:
            referral = self.referrals[aid]
            referral.status = p["status"]
            referral.updated_at = p["updated_at"]
        elif t == Events.ANALYSIS_FROZEN:
            self.snapshots[aid] = Snapshot(
                snapshot_id=aid, frozen_seq=p["as_of_seq"], payload=p, published=None
            )
        elif t == Events.SNAPSHOT_PUBLISHED:
            self.snapshots[aid].published = p
        # UPLOAD_ACCEPTED / BATCH_QUARANTINED / LOCK 事件只用于审计，无投影状态

    def _participant(self, pid: str) -> Participant:
        owner = self.alias_owner.get(pid, pid)
        return self.participants[owner]

    # ---------- 查询 ----------
    def canonical(self, pid: str) -> str:
        return self.alias_owner.get(pid, pid)

    def participant(self, pid: str) -> Participant | None:
        return self.participants.get(self.canonical(pid))

    def visits_for(self, pid: str) -> list[Visit]:
        owner = self.participant.get(pid)
        pids = owner.all_pids() if owner else {pid}
        return [v for v in self.visits.values() if v.pid in pids]

    def latest_rule(self, kind: str) -> tuple[int, dict[str, Any]]:
        versions = self.rules.get(kind, {})
        if not versions:
            return 0, {}
        version = max(versions)
        return version, versions[version]

    def rule_body(self, kind: str, version: int) -> dict[str, Any] | None:
        return self.rules.get(kind, {}).get(int(version))

    def aggregates_allowed(self, pid: str) -> bool:
        """该参与者当前是否允许进入聚合统计。"""
        participant = self.participant.get(pid)
        if participant is None:
            return False
        if participant.withdrawn:
            return participant.aggregates_retained
        return participant.consent is not None
