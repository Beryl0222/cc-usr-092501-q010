"""队列治理核心服务。

把只可追加的事件流折叠成当前投影，并在其上实现治理动作：

* 招募站点以原始记录上传，服务端构造带版本的领域事件（站点不持有版本号）。
* 知情/撤回、访视与组成部分、设备校准、问卷版本、无创结果、风险分层、
  转诊、随访失访分别在各自聚合上留痕。
* 冻结准入门禁：必需组成部分齐备并通过质量规则后访视才能进入分析批次。
* 迟到更正：已冻结但快照**未发布**的访视允许以新版本更正并影响下次复算；
  已发布快照的访视拒绝原地更正，患病率快照永久保留原分母与口径。
* 撤回：停止未来分析，按许可决定既有聚合是否保留；临床转诊视图独立于研究视图。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .catalog import (
    ALL_SECTIONS,
    SEC_NONINVASIVE,
    Catalog,
)
from .events import (
    ANALYSIS_FROZEN,
    CALIBRATION_RECORDED,
    CONSENT_GRANTED,
    CROSS_SITE_LINK_RESOLVED,
    DEVICE_REGISTERED,
    FOLLOWUP_STATUS_RECORDED,
    PARTICIPANT_ENROLLED,
    REFERRAL_OUTCOME_RECORDED,
    REFERRAL_RECORDED,
    RISK_STRATIFIED,
    SECTION_CORRECTED,
    SECTION_RECORDED,
    SNAPSHOT_PUBLISHED,
    VISIT_FROZEN,
    VISIT_RECORDED,
    WITHDRAWAL_APPLIED,
    CENTRAL_SITE,
    make_event,
    now_iso,
)
from .quality import evaluate_section, stratify
from .repository import ConflictQuarantine, EventStore, content_fingerprint


def require(record: dict[str, Any], key: str) -> Any:
    if key not in record or record[key] in (None, ""):
        raise GovernanceError(f"记录缺少必填字段：{key}")
    return record[key]


class GovernanceError(Exception):
    """业务规则拒绝。"""


class NotAuthorized(GovernanceError):
    """访问被角色或站点边界拒绝。"""


# ---------------------------------------------------------------------------
# 投影
# ---------------------------------------------------------------------------


@dataclass
class SectionState:
    name: str
    versions: list[dict[str, Any]] = field(default_factory=list)

    @property
    def current(self) -> dict[str, Any]:
        return self.versions[-1]

    @property
    def version_no(self) -> int:
        return len(self.versions)


@dataclass
class VisitState:
    visit_id: str
    study_id: str
    site_id: str
    visit_type: str
    planned_at: str
    sections: dict[str, SectionState] = field(default_factory=dict)
    frozen_snapshot: str | None = None
    frozen_at: str | None = None
    frozen_ruleset: int | None = None
    published_snapshot: str | None = None
    risk: dict[str, Any] | None = None


@dataclass
class ParticipantState:
    study_id: str
    home_site_id: str
    seen_sites: list[str] = field(default_factory=list)
    birth_year: int | None = None
    sex: str | None = None
    enrolled_at: str | None = None
    consent: dict[str, Any] | None = None
    withdrawn: bool = False
    withdrawal: dict[str, Any] | None = None
    followup_status: str = "active"
    followup_seen: bool = False


class Projection:
    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog
        self.participants: dict[str, ParticipantState] = {}
        self.visits: dict[str, VisitState] = {}
        self.devices: dict[str, dict[str, Any]] = {}
        self.snapshots: dict[str, dict[str, Any]] = {}
        self.referrals: dict[str, dict[str, Any]] = {}
        self.linkages: list[dict[str, Any]] = []
        self.aggregate_versions: dict[str, int] = {}

    def apply(self, event: dict[str, Any]) -> None:
        etype = event["event_type"]
        agg = event["aggregate_id"]
        self.aggregate_versions[agg] = event["version"]
        payload = event["payload"]
        handler: Callable[[dict[str, Any], dict[str, Any]], None] | None = getattr(
            self, f"_on_{etype.lower()}", None
        )
        if handler is not None:
            handler(event, payload)

    # --- 参与者 / 知情 ---------------------------------------------------

    def _on_participant_enrolled(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        study_id = p["study_id"]
        site_id = event["site_id"]
        participant = self.participants.get(study_id)
        if participant is None:
            self.participants[study_id] = ParticipantState(
                study_id=study_id,
                home_site_id=site_id,
                seen_sites=[site_id],
                birth_year=p.get("birth_year"),
                sex=p.get("sex"),
                enrolled_at=p.get("enrolled_at", event["occurred_at"]),
            )
        elif site_id not in participant.seen_sites:
            participant.seen_sites.append(site_id)

    def _on_cross_site_link_resolved(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        self.linkages.append(
            {
                "study_id": p["study_id"],
                "home_site_id": p["home_site_id"],
                "reviewing_site_id": p["reviewing_site_id"],
                "resolved_at": event["occurred_at"],
                "event_id": event["event_id"],
            }
        )
        participant = self.participants.get(p["study_id"])
        if participant and p["reviewing_site_id"] not in participant.seen_sites:
            participant.seen_sites.append(p["reviewing_site_id"])

    def _on_consent_granted(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        study_id = p["study_id"]
        participant = self.participants.get(study_id)
        if participant:
            participant.consent = {
                "scope": p.get("scope", "baseline+followup"),
                "permissions": p.get("permissions", {}),
                "granted_at": event["occurred_at"],
                "event_id": event["event_id"],
            }

    def _on_withdrawal_applied(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        participant = self.participants.get(p["study_id"])
        if participant:
            participant.withdrawn = True
            participant.withdrawal = {
                "reason": p.get("reason"),
                "retain_aggregates": bool(p.get("retain_aggregates", False)),
                "applied_at": p.get("effective_at", event["occurred_at"]),
                "event_id": event["event_id"],
            }

    # --- 访视与组成部分 ---------------------------------------------------

    def _on_visit_recorded(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        self.visits[p["visit_id"]] = VisitState(
            visit_id=p["visit_id"],
            study_id=p["study_id"],
            site_id=event["site_id"],
            visit_type=p.get("visit_type", "baseline"),
            planned_at=p["planned_at"],
        )

    def _on_section_recorded(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        visit = self.visits.get(p["visit_id"])
        if visit is None:
            return
        self._add_section(visit, p, event, corrected=False)

    def _on_section_corrected(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        visit = self.visits.get(p["visit_id"])
        if visit is None:
            return
        self._add_section(visit, p, event, corrected=True)

    @staticmethod
    def _add_section(visit: VisitState, p: dict[str, Any], event: dict[str, Any], *, corrected: bool) -> None:
        name = p["name"]
        section = visit.sections.setdefault(name, SectionState(name=name))
        section.versions.append(
            {
                "section_version": len(section.versions) + 1,
                "data": p["data"],
                "recorded_at": event["occurred_at"],
                "event_id": event["event_id"],
                "upload_id": event.get("upload_id"),
                "corrected": corrected,
            }
        )

    # --- 设备 -------------------------------------------------------------

    def _on_device_registered(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        self.devices[p["device_id"]] = {
            "device_id": p["device_id"],
            "site_id": event["site_id"],
            "model": p.get("model"),
            "active": p.get("active", True),
            "calibrated_at": None,
        }

    def _on_calibration_recorded(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        device = self.devices.setdefault(
            p["device_id"],
            {"device_id": p["device_id"], "site_id": event["site_id"], "model": None, "active": True},
        )
        if p.get("active") is False:
            device["active"] = False
        else:
            device["calibrated_at"] = p["calibrated_at"]
            device["calibration_by"] = p.get("calibration_by")

    # --- 分析批次 ---------------------------------------------------------

    def _on_visit_frozen(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        visit = self.visits.get(p["visit_id"])
        if visit:
            visit.frozen_snapshot = p["snapshot_id"]
            visit.frozen_at = event["occurred_at"]
            visit.frozen_ruleset = p["ruleset_version"]

    def _on_analysis_frozen(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        self.snapshots[p["snapshot_id"]] = {
            "snapshot_id": p["snapshot_id"],
            "label": p.get("label", ""),
            "visit_ids": list(p["visit_ids"]),
            "ruleset_version": p["ruleset_version"],
            "frozen_at": event["occurred_at"],
            "published_at": None,
            "stats": None,
        }

    def _on_snapshot_published(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        snapshot = self.snapshots.get(p["snapshot_id"])
        if snapshot is None:
            return
        snapshot["published_at"] = event["occurred_at"]
        snapshot["stats"] = p["stats"]
        snapshot["caliber"] = p.get("caliber", {})
        for visit_id in snapshot["visit_ids"]:
            visit = self.visits.get(visit_id)
            if visit:
                visit.published_snapshot = snapshot["snapshot_id"]

    def _on_risk_stratified(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        visit = self.visits.get(p["visit_id"])
        if visit:
            visit.risk = {
                "band": p["band"],
                "fib4": p["fib4"],
                "lsm_kpa": p["lsm_kpa"],
                "ruleset_version": p["ruleset_version"],
                "event_id": event["event_id"],
            }

    def _on_referral_recorded(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        self.referrals[p["referral_id"]] = {
            "referral_id": p["referral_id"],
            "study_id": p["study_id"],
            "visit_id": p.get("visit_id"),
            "site_id": event["site_id"],
            "band": p.get("band"),
            "recommended_facility": p.get("recommended_facility"),
            "referred_at": p.get("referred_at", event["occurred_at"]),
            "outcome": None,
        }

    def _on_referral_outcome_recorded(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        referral = self.referrals.get(p["referral_id"])
        if referral:
            referral["outcome"] = {
                "status": p["status"],
                "outcome_at": p.get("outcome_at", event["occurred_at"]),
                "event_id": event["event_id"],
            }

    def _on_followup_status_recorded(self, event: dict[str, Any], p: dict[str, Any]) -> None:
        participant = self.participants.get(p["study_id"])
        if participant:
            participant.followup_status = p["status"]
            participant.followup_seen = True


# ---------------------------------------------------------------------------
# 服务
# ---------------------------------------------------------------------------


@dataclass
class IngestResult:
    status: str
    upload_id: str
    site_id: str
    count: int = 0
    reason: str | None = None
    linkages: list[dict[str, str]] = field(default_factory=list)


class GovernanceService:
    def __init__(self, store: EventStore, *, catalog: Catalog | None = None) -> None:
        self.store = store
        self.catalog = catalog or Catalog()

    # --- 投影重建 ---------------------------------------------------------

    def build_projection(self) -> Projection:
        projection = Projection(self.catalog)
        for event in self.store.replay():
            if event["event_type"] in ("RULESET_PUBLISHED", "QUESTIONNAIRE_VERSION_PUBLISHED"):
                self.catalog.apply_events([event])
            projection.apply(event)
        return projection

    def _next_version(self, projection: Projection, aggregate_id: str) -> int:
        return projection.aggregate_versions.get(aggregate_id, 0) + 1

    # --- 站点上传 ---------------------------------------------------------

    def ingest_upload(self, site_id: str, upload_id: str, records: list[dict[str, Any]]) -> IngestResult:
        if not isinstance(records, list) or not records:
            raise GovernanceError("上传必须是非空记录数组")
        fingerprint = content_fingerprint(records)

        # 先按原始内容指纹处理凭证语义：安全重传不得因“访视已存在”等当前状态被误判
        kind, existing = self.store.classify_upload(site_id, upload_id, fingerprint)
        if kind == "duplicate":
            return IngestResult(
                "duplicate", upload_id, site_id,
                count=existing["event_count"], reason="内容一致，安全重传",
            )
        if kind == "poisoned":
            return IngestResult(
                "quarantined", upload_id, site_id,
                reason=f"该上传凭证此前已隔离（{existing['reason']}），请使用新凭证重传",
            )
        if kind == "conflict":
            if existing["site_id"] != site_id:
                reason = "上传凭证曾被另一站点使用"
            else:
                reason = "同一上传凭证内容不一致，整批已隔离"
                self.store.flag_content_conflict(site_id, upload_id, fingerprint)
            return IngestResult("quarantined", upload_id, site_id, reason=reason)

        # 新凭证：基于当前投影构建事件并做语义校验
        projection = self.build_projection()
        events: list[dict[str, Any]] = []
        errors: list[str] = []

        # 构造一条即应用一条：同批次内的后继记录（招募→知情→访视→组成部分）
        # 以及同一访视记录内的组成部分事件都能看到前序事实；
        # 版本号完全以投影为准
        def emit(event: dict[str, Any]) -> None:
            projection.apply(event)
            events.append(event)

        def version_for(aggregate_id: str) -> int:
            return self._next_version(projection, aggregate_id)

        for index, record in enumerate(records):
            try:
                self._build_record_events(
                    site_id, upload_id, index, record, projection, version_for, emit
                )
            except GovernanceError as error:
                errors.append(f"记录 {index}（{record.get('record_type', '?')}）：{error}")
                continue

        if errors:
            self.store.quarantine_upload(site_id, upload_id, fingerprint, errors[0])
            return IngestResult("quarantined", upload_id, site_id, reason=errors[0])

        linkages = self._cross_site_links(site_id, records, projection)
        try:
            accepted = self.store.accept_upload(
                site_id, upload_id, events, content_hash=fingerprint
            )
        except ConflictQuarantine as error:
            return IngestResult("quarantined", upload_id, site_id, reason=error.reason)

        if accepted["status"] == "accepted" and linkages:
            linkage_events = []
            for link in linkages:
                aggregate_id = f"participant:{link['study_id']}"
                linkage_events.append(
                    make_event(
                        event_id=f"link-{upload_id}-{link['study_id']}",
                        event_type=CROSS_SITE_LINK_RESOLVED,
                        aggregate_type="participant_link",
                        aggregate_id=aggregate_id,
                        site_id=CENTRAL_SITE,
                        version=self._next_version(projection, aggregate_id)
                        + len(linkage_events),
                        summary=f"跨区复查联动：{link['reviewing_site_id']} 复查 {link['home_site_id']} 招募的参与者",
                        payload=link,
                    )
                )
            self.store.append_system(linkage_events)

        return IngestResult(
            accepted["status"], upload_id, site_id,
            count=accepted.get("count", 0), reason=accepted.get("reason"), linkages=linkages,
        )

    @staticmethod
    def _cross_site_links(
        site_id: str, records: list[dict[str, Any]], projection: Projection
    ) -> list[dict[str, str]]:
        links = []
        for record in records:
            if record.get("record_type") != "enrollment":
                continue
            study_id = record.get("study_id")
            participant = projection.participants.get(study_id) if study_id else None
            if participant and participant.home_site_id != site_id:
                links.append(
                    {
                        "study_id": study_id,
                        "home_site_id": participant.home_site_id,
                        "reviewing_site_id": site_id,
                    }
                )
        return links

    def _build_record_events(
        self,
        site_id: str,
        upload_id: str,
        index: int,
        record: dict[str, Any],
        projection: Projection,
        version_for: Callable[[str], int],
        emit: Callable[[dict[str, Any]], None],
    ) -> None:
        rtype = record.get("record_type")
        event_id = f"{upload_id}#{index}"
        # site_id 在每条事件上显式给出；这里只透传可选字段
        common = {"upload_id": upload_id, "occurred_at": record.get("client_at")}

        def mk(**kwargs: Any) -> dict[str, Any]:
            kwargs["site_id"] = site_id
            return make_event(**kwargs, **{k: v for k, v in common.items() if v is not None})

        if rtype == "enrollment":
            study_id = require(record, "study_id")
            agg = f"participant:{study_id}"
            emit(
                mk(
                    event_id=event_id,
                    event_type=PARTICIPANT_ENROLLED,
                    aggregate_type="participant_link",
                    aggregate_id=agg,
                    version=version_for(agg),
                    summary=f"{site_id} 招募参与者",
                    payload={
                        "study_id": study_id,
                        "birth_year": require(record, "birth_year"),
                        "sex": require(record, "sex"),
                        "enrolled_at": require(record, "enrolled_at"),
                    },
                )
            )
            return

        if rtype == "consent":
            study_id = require(record, "study_id")
            if study_id not in projection.participants:
                raise GovernanceError("知情同意指向未招募的参与者")
            agg = f"consent:{study_id}"
            emit(
                mk(
                    event_id=event_id,
                    event_type=CONSENT_GRANTED,
                    aggregate_type="consent",
                    aggregate_id=agg,
                    version=version_for(agg),
                    summary=f"{study_id} 签署知情同意",
                    payload={
                        "study_id": study_id,
                        "scope": record.get("scope", "baseline+followup"),
                        "permissions": record.get(
                            "permissions",
                            {"future_analysis": True, "retain_aggregates": True},
                        ),
                    },
                )
            )
            return

        if rtype == "visit":
            study_id = require(record, "study_id")
            if study_id not in projection.participants:
                raise GovernanceError(f"访视指向未招募的参与者：{study_id}")
            visit_id = require(record, "visit_id")
            if visit_id in projection.visits:
                raise GovernanceError(f"访视已存在：{visit_id}")
            agg = f"visit:{visit_id}"
            emit(
                mk(
                    event_id=event_id,
                    event_type=VISIT_RECORDED,
                    aggregate_type="study_visit",
                    aggregate_id=agg,
                    version=version_for(agg),
                    summary=f"{site_id} 登记访视 {visit_id}",
                    payload={
                        "visit_id": visit_id,
                        "study_id": study_id,
                        "visit_type": record.get("visit_type", "baseline"),
                        "planned_at": require(record, "planned_at"),
                    },
                )
            )
            sections = record.get("sections", {})
            if not isinstance(sections, dict):
                raise GovernanceError("sections 必须是对象")
            for name, data in sorted(sections.items()):
                self._section_event(
                    site_id, upload_id, f"{event_id}:{name}", visit_id, name, data,
                    version_for, projection, common, emit,
                )
            return

        if rtype in ("section", "section_correction"):
            visit_id = require(record, "visit_id")
            if visit_id not in projection.visits:
                raise GovernanceError(f"组成部分指向未知访视：{visit_id}")
            name = require(record, "section")
            if name not in ALL_SECTIONS:
                raise GovernanceError(f"未知访视组成部分：{name}")
            self._section_event(
                site_id, upload_id, event_id, visit_id, name, require(record, "data"),
                version_for, projection, common, emit,
                force_correction=rtype == "section_correction",
            )
            return

        if rtype == "device":
            device_id = require(record, "device_id")
            agg = f"device:{device_id}"
            emit(
                mk(
                    event_id=event_id,
                    event_type=DEVICE_REGISTERED,
                    aggregate_type="device",
                    aggregate_id=agg,
                    version=version_for(agg),
                    summary=f"{site_id} 注册设备 {device_id}",
                    payload={
                        "device_id": device_id,
                        "model": require(record, "model"),
                        "active": record.get("active", True),
                    },
                )
            )
            return

        if rtype == "calibration":
            device_id = require(record, "device_id")
            agg = f"device:{device_id}"
            emit(
                mk(
                    event_id=event_id,
                    event_type=CALIBRATION_RECORDED,
                    aggregate_type="calibration",
                    aggregate_id=agg,
                    version=version_for(agg),
                    summary=f"{site_id} 记录设备校准 {device_id}",
                    payload={
                        "device_id": device_id,
                        "calibrated_at": require(record, "calibrated_at"),
                        "calibration_by": record.get("calibration_by"),
                        "active": record.get("active", True),
                    },
                )
            )
            return

        if rtype == "referral":
            referral_id = require(record, "referral_id")
            agg = f"referral:{referral_id}"
            emit(
                mk(
                    event_id=event_id,
                    event_type=REFERRAL_RECORDED,
                    aggregate_type="referral",
                    aggregate_id=agg,
                    version=version_for(agg),
                    summary=f"{site_id} 记录临床转诊 {referral_id}",
                    payload={
                        "referral_id": referral_id,
                        "study_id": require(record, "study_id"),
                        "visit_id": record.get("visit_id"),
                        "band": require(record, "band"),
                        "recommended_facility": record.get("recommended_facility"),
                        "referred_at": record.get("referred_at", record.get("client_at") or now_iso()),
                    },
                )
            )
            return

        if rtype == "referral_outcome":
            referral_id = require(record, "referral_id")
            if referral_id not in projection.referrals:
                raise GovernanceError(f"转诊结局指向未知转诊：{referral_id}")
            agg = f"referral:{referral_id}"
            emit(
                mk(
                    event_id=event_id,
                    event_type=REFERRAL_OUTCOME_RECORDED,
                    aggregate_type="referral",
                    aggregate_id=agg,
                    version=version_for(agg),
                    summary=f"{site_id} 更新转诊结局 {referral_id}",
                    payload={
                        "referral_id": referral_id,
                        "status": require(record, "status"),
                        "outcome_at": record.get("outcome_at"),
                    },
                )
            )
            return

        if rtype == "followup":
            study_id = require(record, "study_id")
            if study_id not in projection.participants:
                raise GovernanceError(f"随访状态指向未知参与者：{study_id}")
            agg = f"followup:{study_id}"
            emit(
                mk(
                    event_id=event_id,
                    event_type=FOLLOWUP_STATUS_RECORDED,
                    aggregate_type="followup",
                    aggregate_id=agg,
                    version=version_for(agg),
                    summary=f"{site_id} 更新随访状态 {study_id}",
                    payload={"study_id": study_id, "status": require(record, "status")},
                )
            )
            return

        if rtype == "withdrawal":
            study_id = require(record, "study_id")
            if study_id not in projection.participants:
                raise GovernanceError(f"撤回指向未知参与者：{study_id}")
            participant = projection.participants[study_id]
            if participant.withdrawn:
                raise GovernanceError(f"参与者已撤回：{study_id}")
            agg = f"participant:{study_id}"
            emit(
                mk(
                    event_id=event_id,
                    event_type=WITHDRAWAL_APPLIED,
                    aggregate_type="participant_link",
                    aggregate_id=agg,
                    version=version_for(agg),
                    summary=f"{study_id} 撤回研究使用许可",
                    payload={
                        "study_id": study_id,
                        "reason": record.get("reason"),
                        "retain_aggregates": bool(record.get("retain_aggregates", False)),
                        "effective_at": record.get("effective_at", record.get("client_at") or now_iso()),
                    },
                )
            )
            return

        raise GovernanceError(f"未知记录类型：{rtype}")

    def _section_event(
        self,
        site_id: str,
        upload_id: str,
        event_id: str,
        visit_id: str,
        name: str,
        data: dict[str, Any],
        version_for: Callable[[str], int],
        projection: Projection,
        common: dict[str, Any],
        emit: Callable[[dict[str, Any]], None],
        *,
        force_correction: bool = False,
    ) -> None:
        if not isinstance(data, dict):
            raise GovernanceError(f"组成部分 {name} 的 data 必须是对象")
        visit = projection.visits[visit_id]
        if visit.published_snapshot:
            raise GovernanceError(
                f"访视 {visit_id} 已进入已发布快照 {visit.published_snapshot}，"
                "迟到更正须以新访视/新批次提交，已发布口径保持不变"
            )
        def mk(**kwargs: Any) -> dict[str, Any]:
            kwargs["site_id"] = site_id
            return make_event(**kwargs, **{k: v for k, v in common.items() if v is not None})

        agg = f"section:{visit_id}:{name}"
        existing = visit.sections.get(name)
        is_correction = force_correction or existing is not None
        if force_correction and existing is None:
            raise GovernanceError(f"不能对从未登记的组成部分 {name} 提交更正")
        emit(
            mk(
                event_id=event_id,
                event_type=SECTION_CORRECTED if is_correction else SECTION_RECORDED,
                aggregate_type="visit_section",
                aggregate_id=agg,
                
                version=version_for(agg),
                summary=f"{site_id} {'更正' if is_correction else '登记'}访视 {visit_id} 的 {name}",
                payload={"visit_id": visit_id, "name": name, "data": data},
            )
        )

    # --- 规则目录发布 -----------------------------------------------------

    def publish_ruleset(self, rules: "Ruleset") -> str:
        """发布新版质量/分层规则；只能追加新版本。"""
        from .catalog import ruleset_payload
        from .events import RULESET_PUBLISHED

        if self.catalog.has_ruleset(rules.version):
            raise GovernanceError(f"规则版本已存在：v{rules.version}")
        event = make_event(
            event_id=f"ruleset-v{rules.version}",
            event_type=RULESET_PUBLISHED,
            aggregate_type="ruleset",
            aggregate_id=f"ruleset:v{rules.version}",
            site_id=CENTRAL_SITE,
            version=1,
            summary=f"发布规则版本 v{rules.version}：{rules.label}",
            payload=ruleset_payload(rules),
        )
        self.store.append_system([event])
        self.catalog.publish_ruleset(rules)
        return event["event_id"]

    def publish_questionnaire(self, questionnaire: "QuestionnaireVersion") -> str:
        """发布新版问卷。"""
        from .events import QUESTIONNAIRE_VERSION_PUBLISHED

        event = make_event(
            event_id=f"questionnaire-{questionnaire.instrument_id}-v{questionnaire.version}",
            event_type=QUESTIONNAIRE_VERSION_PUBLISHED,
            aggregate_type="questionnaire",
            aggregate_id=f"questionnaire:{questionnaire.instrument_id}:v{questionnaire.version}",
            site_id=CENTRAL_SITE,
            version=1,
            summary=f"发布问卷 {questionnaire.instrument_id} v{questionnaire.version}",
            payload={
                "instrument_id": questionnaire.instrument_id,
                "version": questionnaire.version,
                "required_items": list(questionnaire.required_items),
            },
        )
        self.store.append_system([event])
        self.catalog.publish_questionnaire(questionnaire)
        return event["event_id"]

    # --- 质量门禁与冻结 ---------------------------------------------------

    def evaluate_visit(self, visit: VisitState, projection: Projection) -> dict[str, Any]:
        """对单个访视运行当前规则下的质量检查。"""
        rules = self.catalog.ruleset_for(visit.planned_at)
        participant = projection.participants.get(visit.study_id)
        reasons: list[dict[str, str]] = []
        if participant is None:
            reasons.append({"code": "UNKNOWN_PARTICIPANT", "detail": "参与者不存在"})
            return {"eligible": False, "reasons": reasons, "ruleset_version": rules.version}
        if participant.withdrawn:
            reasons.append({"code": "WITHDRAWN", "detail": "参与者已撤回研究使用"})
        if participant.consent is None:
            reasons.append({"code": "NO_CONSENT", "detail": "缺少知情同意"})
        required = rules.required_sections.get(visit.visit_type, ())
        device_lookup = lambda device_id: projection.devices.get(device_id)  # noqa: E731
        for name in required:
            section = visit.sections.get(name)
            if section is None:
                reasons.append({"code": "MISSING_SECTION", "detail": f"缺少组成部分：{name}"})
                continue
            questionnaire = None
            if name == "questionnaire":
                data = section.current["data"]
                try:
                    questionnaire = self.catalog.get_questionnaire(
                        data.get("instrument_id", "CLH_BASELINE"), data.get("version", 0)
                    )
                except ValueError:
                    questionnaire = None
            issues = evaluate_section(
                name,
                section.current["data"],
                rules,
                questionnaire.required_items if questionnaire else (),
                device_lookup,
            )
            reasons.extend(issues)
        return {
            "eligible": not reasons,
            "reasons": reasons,
            "ruleset_version": rules.version,
            "section_versions": {
                name: visit.sections[name].version_no for name in visit.sections
            },
        }

    def freeze_batch(
        self,
        snapshot_id: str,
        visit_ids: list[str],
        *,
        label: str = "",
        exclude_failures: bool = False,
    ) -> dict[str, Any]:
        """冻结分析批次。默认混合质量时整批拒绝；exclude_failures=True 时只冻结合格者。"""
        projection = self.build_projection()
        if snapshot_id in projection.snapshots:
            raise GovernanceError(f"快照标识已存在：{snapshot_id}")
        selected = [self._require_visit(projection, vid) for vid in dict.fromkeys(visit_ids)]
        evaluated = []
        for visit in selected:
            evaluation = self.evaluate_visit(visit, projection)
            if visit.frozen_snapshot:
                # 已冻结访视仍暴露当前质量/撤回原因，同时附重复冻结原因
                evaluation["eligible"] = False
                evaluation["reasons"].append({
                    "code": "ALREADY_FROZEN",
                    "detail": f"访视已在分析批次 {visit.frozen_snapshot} 中",
                })
            evaluated.append((visit, evaluation))
        passed = [(v, e) for v, e in evaluated if e["eligible"]]
        failed = [
            {"visit_id": v.visit_id, "study_id": v.study_id, "site_id": v.site_id, "reasons": e["reasons"]}
            for v, e in evaluated if not e["eligible"]
        ]
        if failed and not exclude_failures:
            return {"status": "rejected", "snapshot_id": snapshot_id, "exclusions": failed}
        if not passed:
            return {"status": "empty", "snapshot_id": snapshot_id, "exclusions": failed}

        ruleset_version = max(e["ruleset_version"] for _, e in passed)
        events: list[dict[str, Any]] = []
        for visit, evaluation in passed:
            agg = f"visit:{visit.visit_id}"
            events.append(
                make_event(
                    event_id=f"freeze-{snapshot_id}-{visit.visit_id}",
                    event_type=VISIT_FROZEN,
                    aggregate_type="study_visit",
                    aggregate_id=agg,
                    site_id=CENTRAL_SITE,
                    version=self._next_version(projection, agg),
                    summary=f"访视 {visit.visit_id} 通过质量门禁，进入 {snapshot_id}",
                    payload={
                        "visit_id": visit.visit_id,
                        "snapshot_id": snapshot_id,
                        "ruleset_version": evaluation["ruleset_version"],
                    },
                )
            )
            risk = self._risk_event_for(snapshot_id, visit, projection)
            if risk is not None:
                events.append(risk)
                projection.apply(risk)
        events.append(
            make_event(
                event_id=f"freeze-{snapshot_id}",
                event_type=ANALYSIS_FROZEN,
                aggregate_type="analysis_snapshot",
                aggregate_id=f"snapshot:{snapshot_id}",
                site_id=CENTRAL_SITE,
                version=1,
                summary=f"冻结分析批次 {snapshot_id}",
                payload={
                    "snapshot_id": snapshot_id,
                    "label": label,
                    "visit_ids": [v.visit_id for v, _ in passed],
                    "ruleset_version": ruleset_version,
                },
            )
        )
        self.store.append_system(events)
        return {
            "status": "frozen",
            "snapshot_id": snapshot_id,
            "frozen": [v.visit_id for v, _ in passed],
            "exclusions": failed,
        }

    def _risk_event_for(
        self, snapshot_id: str, visit: VisitState, projection: Projection
    ) -> dict[str, Any] | None:
        noninv = visit.sections.get(SEC_NONINVASIVE)
        if noninv is None:
            return None
        data = noninv.current["data"]
        labs = ("ast", "alt", "platelets")
        if not all(isinstance(data.get(k), (int, float)) for k in labs):
            return None
        participant = projection.participants.get(visit.study_id)
        age = None
        if participant and participant.birth_year:
            age = int(visit.planned_at[:4]) - participant.birth_year
        if not age or age <= 0:
            return None
        rules = self.catalog.ruleset_for(visit.planned_at)
        result = stratify(
            rules,
            age=age,
            ast=float(data["ast"]),
            alt=float(data["alt"]),
            platelets=float(data["platelets"]),
            lsm_kpa=float(data["lsm_kpa"]),
        )
        agg = f"risk:{visit.visit_id}"
        return make_event(
            event_id=f"risk-{snapshot_id}-{visit.visit_id}",
            event_type=RISK_STRATIFIED,
            aggregate_type="risk_assessment",
            aggregate_id=agg,
            site_id=CENTRAL_SITE,
            version=self._next_version(projection, agg),
            summary=f"访视 {visit.visit_id} 风险分层：{result['band']}",
            payload={"visit_id": visit.visit_id, **result},
        )

    def publish_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        """复算并发布患病率快照；发布后分母与口径不可变。"""
        projection = self.build_projection()
        snapshot = self._require_snapshot(projection, snapshot_id)
        if snapshot["published_at"]:
            raise GovernanceError(f"快照已发布：{snapshot_id}")
        report = self.recompute(snapshot_id)
        from .analysis import _recompute_risk as recompute_risk

        events: list[dict[str, Any]] = []
        # 迟到更正可能改变分层；发布前为受影响访视补记新版本风险事件
        for visit_id in report["caliber"]["members_counted"]:
            visit = projection.visits[visit_id]
            band, risk, _missing = recompute_risk(self, visit, projection)
            if risk is None:
                continue
            if visit.risk is None or visit.risk["band"] != band:
                agg = f"risk:{visit_id}"
                events.append(
                    make_event(
                        event_id=f"risk-publish-{snapshot_id}-{visit_id}",
                        event_type=RISK_STRATIFIED,
                        aggregate_type="risk_assessment",
                        aggregate_id=agg,
                        site_id=CENTRAL_SITE,
                        version=self._next_version(projection, agg),
                        summary=f"发布前复算访视 {visit_id} 风险分层：{band}",
                        payload={"visit_id": visit_id, **risk},
                    )
                )
        agg = f"snapshot:{snapshot_id}"
        events.append(
            make_event(
                event_id=f"publish-{snapshot_id}",
                event_type=SNAPSHOT_PUBLISHED,
                aggregate_type="analysis_snapshot",
                aggregate_id=agg,
                site_id=CENTRAL_SITE,
                version=self._next_version(projection, agg),
                summary=f"发布患病率快照 {snapshot_id}（分母 {report['denominator']}）",
                payload={
                    "snapshot_id": snapshot_id,
                    "stats": {
                        "denominator": report["denominator"],
                        "strata": report["strata"],
                        "prevalence": report["prevalence"],
                        "excluded_at_publish": report["excluded"],
                    },
                    "caliber": report["caliber"],
                },
            )
        )
        self.store.append_system(events)
        return {"status": "published", "snapshot_id": snapshot_id, "report": report}

    def recompute(self, snapshot_id: str) -> dict[str, Any]:
        """中央复算命令：从冻结快照生成分层统计与缺失/排除原因。"""
        from .analysis import recompute

        return recompute(self, snapshot_id)

    # --- 视图 -------------------------------------------------------------

    def participant_view(self, study_id: str, auth: dict[str, Any]) -> dict[str, Any]:
        projection = self.build_projection()
        participant = projection.participants.get(study_id)
        if participant is None:
            raise GovernanceError("参与者不存在")
        self._guard_participant(participant, auth)
        if auth["role"] == "clinician":
            # 分权：临床角色只见基本信息，研究访视与无创结果不返回
            return {
                "study_id": study_id,
                "home_site_id": participant.home_site_id,
                "birth_year": participant.birth_year,
                "sex": participant.sex,
                "clinical_note": "研究访视数据对临床角色不可见，请使用 /referrals 接口",
            }
        visits = [self._visit_summary(v, auth) for v in projection.visits.values() if v.study_id == study_id]
        visits = [v for v in visits if v is not None]
        return {
            "study_id": study_id,
            "home_site_id": participant.home_site_id,
            "seen_sites": participant.seen_sites,
            "birth_year": participant.birth_year,
            "sex": participant.sex,
            "consent_scope": participant.consent["scope"] if participant.consent else None,
            "withdrawn": participant.withdrawn,
            "withdrawal": participant.withdrawal,
            "followup_status": participant.followup_status,
            "visits": visits,
        }

    def visit_view(self, visit_id: str, auth: dict[str, Any]) -> dict[str, Any]:
        projection = self.build_projection()
        visit = self._require_visit(projection, visit_id)
        if auth["role"] == "clinician":
            raise NotAuthorized("临床角色无权查看研究访视数据")
        participant = projection.participants.get(visit.study_id)
        self._guard_participant(participant, auth, visit_site=visit.site_id)
        return self._visit_detail(visit)

    def trace_result(self, visit_id: str, auth: dict[str, Any]) -> dict[str, Any]:
        """追溯一条结果来自哪次访视、哪版问卷/规则与哪次上传。"""
        projection = self.build_projection()
        visit = self._require_visit(projection, visit_id)
        participant = projection.participants.get(visit.study_id)
        self._guard_participant(participant, auth, visit_site=visit.site_id)
        rules = self.catalog.ruleset_for(visit.planned_at)
        return {
            "visit_id": visit.visit_id,
            "study_id": visit.study_id,
            "site_id": visit.site_id,
            "visit_type": visit.visit_type,
            "planned_at": visit.planned_at,
            "sections": {
                name: {
                    "current_version": section.version_no,
                    "event_id": section.current["event_id"],
                    "upload_id": section.current.get("upload_id"),
                    "recorded_at": section.current["recorded_at"],
                    "history": [
                        {
                            "section_version": sv["section_version"],
                            "event_id": sv["event_id"],
                            "upload_id": sv.get("upload_id"),
                            "recorded_at": sv["recorded_at"],
                        }
                        for sv in section.versions
                    ],
                }
                for name, section in sorted(visit.sections.items())
            },
            "ruleset_version_at_visit": rules.version,
            "frozen": {
                "snapshot_id": visit.frozen_snapshot,
                "at": visit.frozen_at,
                "ruleset_version": visit.frozen_ruleset,
                "published_snapshot": visit.published_snapshot,
            },
            "risk": visit.risk,
        }

    def referral_view(self, referral_id: str, auth: dict[str, Any]) -> dict[str, Any]:
        projection = self.build_projection()
        referral = projection.referrals.get(referral_id)
        if referral is None:
            raise GovernanceError("转诊记录不存在")
        if auth["role"] not in ("coordinator", "clinician", "site_user"):
            raise NotAuthorized("研究角色无权访问临床转诊记录")
        if auth["role"] == "site_user" and auth.get("site_id") != referral["site_id"]:
            raise NotAuthorized("站点只能查看本站点的转诊记录")
        return dict(referral)

    # --- 访问控制 ---------------------------------------------------------

    @staticmethod
    def _guard_participant(
        participant: ParticipantState | None,
        auth: dict[str, Any],
        *,
        visit_site: str | None = None,
    ) -> None:
        if participant is None:
            raise GovernanceError("参与者不存在")
        role = auth["role"]
        if role == "coordinator":
            return
        if role == "site_user":
            allowed_sites = set(participant.seen_sites)
            if visit_site:
                allowed_sites.add(visit_site)
            if auth.get("site_id") not in allowed_sites:
                raise NotAuthorized("站点只能查看自身参与者")
            return
        if role == "researcher":
            if participant.withdrawn:
                raise NotAuthorized("参与者已撤回，研究视图不可用（临床记录请走转诊视图）")
            return
        if role == "clinician":
            # 临床角色可看参与者基本信息以承接转诊；研究访视数据在 visit 层拒绝
            return

    def _visit_summary(self, visit: VisitState, auth: dict[str, Any]) -> dict[str, Any] | None:
        if auth["role"] == "site_user" and auth.get("site_id") != visit.site_id:
            return None
        return {
            "visit_id": visit.visit_id,
            "site_id": visit.site_id,
            "visit_type": visit.visit_type,
            "planned_at": visit.planned_at,
            "frozen_snapshot": visit.frozen_snapshot,
            "published_snapshot": visit.published_snapshot,
            "risk_band": visit.risk["band"] if visit.risk else None,
        }

    def _visit_detail(self, visit: VisitState) -> dict[str, Any]:
        return {
            "visit_id": visit.visit_id,
            "study_id": visit.study_id,
            "site_id": visit.site_id,
            "visit_type": visit.visit_type,
            "planned_at": visit.planned_at,
            "frozen_snapshot": visit.frozen_snapshot,
            "published_snapshot": visit.published_snapshot,
            "sections": {
                name: {
                    "current_version": section.version_no,
                    "data": section.current["data"],
                    "recorded_at": section.current["recorded_at"],
                    "event_id": section.current["event_id"],
                }
                for name, section in sorted(visit.sections.items())
            },
            "risk": visit.risk,
        }

    # --- 工具 -------------------------------------------------------------

    @staticmethod
    def _require_visit(projection: Projection, visit_id: str) -> VisitState:
        visit = projection.visits.get(visit_id)
        if visit is None:
            raise GovernanceError(f"未知访视：{visit_id}")
        return visit

    @staticmethod
    def _require_snapshot(projection: Projection, snapshot_id: str) -> dict[str, Any]:
        snapshot = projection.snapshots.get(snapshot_id)
        if snapshot is None:
            raise GovernanceError(f"未知分析快照：{snapshot_id}")
        return snapshot
