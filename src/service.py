"""队列治理服务门面：鉴权、上传、关联、冻结复算、溯源与分权查询。

进程恢复：用 CohortService.open(path) 打开——投影从事件流整体重建，
冻结锁依赖 TTL 在崩溃后自动过期可接管，因此不需要额外的恢复流程。
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import analytics, snapshots
from .access import AccessDenied, Principal
from .contracts import (
    Aggregates, Components, Events, Roles, RuleKind, Scopes,
)
from .identity import new_linkage_code, new_salt, new_token
from .projection import Projection
from .store import EventStore, LockBusy, StoreError
from .uploads import Quarantine, seal_batch

RETRYABLE_UPLOAD = "retryable"


class CohortService:
    def __init__(self, store: EventStore) -> None:
        self.store = store
        self.projection = Projection()
        self._guard = threading.RLock()
        self._rebuild()

    @classmethod
    def open(cls, path: str | Path) -> "CohortService":
        return cls(EventStore(path))

    def _rebuild(self) -> None:
        self.projection.rebuild(self.store.events())

    # ---------- 引导：站点 / 用户 / 凭证 ----------
    def bootstrap_site(self, site_id: str, province: str, name: str) -> str:
        with self._guard:
            salt = new_salt()
            self.store.create_site(site_id, province, name, salt)
            return salt

    def create_user(self, user_key: str, role: str, site_id: str | None = None) -> None:
        if role == Roles.SITE_COORDINATOR and not site_id:
            raise ValueError("站点协调员必须绑定站点")
        if site_id is not None and self.store.get_site(site_id) is None:
            raise ValueError(f"未知站点：{site_id}")
        self.store.create_user(user_key, role, site_id)

    def principal(self, user_key: str) -> Principal:
        row = self.store.get_user(user_key)
        if row is None:
            raise AccessDenied("未知用户")
        return Principal(user_key=row["user_key"], role=row["role"],
                         site_id=row["site_id"])

    def issue_token(self, principal: Principal, site_id: str) -> str:
        principal.require(Scopes.TOKEN_WRITE)
        principal.require_site(site_id)
        token = new_token()
        self.store.create_token(token, site_id)
        return token

    # ---------- 上传 ----------
    def upload(self, principal: Principal, token_id: str, raw: bytes) -> dict[str, Any]:
        principal.require(Scopes.UPLOAD_WRITE)
        token = self.store.get_token(token_id)
        if token is None:
            raise AccessDenied("上传凭证不存在")
        principal.require_site(token["site_id"])
        # 冻结锁库期间：内容未必有问题，不隔离，返回可重试信号。
        # 检查与落库同在服务互斥段内，与 freeze 严格互斥。
        with self._guard:
            if self.store.lock_holder(snapshots.FREEZE_SCOPE) is not None:
                return {"status": "locked", "upload_id": None,
                        "reason": "分析批次冻结中，请稍后重试",
                        RETRYABLE_UPLOAD: True}
            result = seal_batch(self.store, self.projection, token_id, raw)
        return result.as_dict()

    # ---------- 跨区复查：一次性关联码 ----------
    def issue_linkage_code(self, principal: Principal, pid: str) -> str:
        principal.require(Scopes.IDENTITY_MAP)
        participant = self.projection.participant(pid)
        if participant is None:
            raise StoreError(f"参与者不存在：{pid}")
        principal.require_site(participant.home_site)
        code = new_linkage_code()
        self.store.issue_linkage_code(code, participant.pid, participant.home_site)
        return code

    def consume_linkage_code(self, principal: Principal, code: str,
                             consumer_pid: str) -> dict[str, Any]:
        """另一站点凭一次性码把本站点化名归并到原研究标识。"""
        principal.require(Scopes.IDENTITY_MAP)
        row = self.store.get_linkage_code(code)
        if row is None:
            raise StoreError("关联码无效")
        if row["consumed_at"] is not None:
            raise StoreError("关联码已被使用")
        consumer = self.projection.participant(consumer_pid)
        if consumer is None:
            raise StoreError(f"本站点参与者不存在：{consumer_pid}")
        principal.require_site(consumer.home_site)
        owner_pid = row["pid"]
        owner = self.projection.participant(owner_pid)
        if owner is None:
            raise StoreError("关联码指向的研究标识已不存在")
        if consumer.home_site == row["site_id"]:
            raise StoreError("跨区关联必须由招募站点以外的站点消费")

        version = self._next_version(Aggregates.PARTICIPANT, owner_pid)
        at = datetime.now(timezone.utc).isoformat()
        event = {
            "event_id": f"ev-link-{code}",
            "event_type": Events.LINKAGE_RECORDED,
            "aggregate_type": Aggregates.PARTICIPANT,
            "aggregate_id": owner_pid,
            "occurred_at": at,
            "version": version,
            "summary": f"跨区复查归并：{consumer_pid} -> {owner_pid}",
            "site_id": consumer.home_site,
            "payload": {"site_id": consumer.home_site, "pseudonym": consumer_pid,
                        "linkage_code": code},
        }
        with self._guard:
            seq = self.store.append_event_and_consume_linkage(
                event, code, consumer_pid, consumer.home_site
            )
        event["seq"] = seq
        self.projection.apply(event)
        return {"canonical_pid": owner_pid, "aliases": sorted(owner.all_pids()),
                "event_id": event["event_id"]}

    def _next_version(self, aggregate_type: str, aggregate_id: str) -> int:
        rows = self.store.events_for(aggregate_type, aggregate_id)
        return (max((e["version"] for e in rows), default=0)) + 1

    # ---------- 研究数据查询（research 通道） ----------
    def get_participant(self, principal: Principal, pid: str) -> dict[str, Any]:
        principal.require(Scopes.RESEARCH_READ)
        participant = self.projection.participant(pid)
        if participant is None:
            raise StoreError(f"参与者不存在：{pid}")
        principal.require_site(participant.home_site)
        return {
            "pid": participant.pid,
            "home_site": participant.home_site,
            "aliases": sorted(participant.all_pids()),
            "consent": participant.consent,
            "withdrawn": participant.withdrawn,
            "withdrawn_at": participant.withdrawn_at,
            "aggregates_retained": participant.aggregates_retained,
        }

    def list_participants(self, principal: Principal) -> list[dict[str, Any]]:
        principal.require(Scopes.RESEARCH_READ)
        out = []
        for participant in self.projection.participants.values():
            if principal.role == Roles.SITE_COORDINATOR \
                    and participant.home_site != principal.site_id:
                continue
            out.append({"pid": participant.pid, "home_site": participant.home_site,
                        "withdrawn": participant.withdrawn})
        return out

    def trace_result(self, principal: Principal, visit_id: str) -> dict[str, Any]:
        """溯源：某条结果来自哪次访视、哪批上传、哪版规则与质量结论。"""
        principal.require(Scopes.RESEARCH_READ)
        visit = self.projection.visits.get(visit_id)
        if visit is None:
            raise StoreError(f"访视不存在：{visit_id}")
        principal.require_site(visit.site_id)

        def _component_events(event_type: str) -> list[dict[str, Any]]:
            rows = self.store.events_for(Aggregates.VISIT, visit_id)
            return [
                {
                    "event_id": e["event_id"], "seq": e["seq"],
                    "version": e["version"], "occurred_at": e["occurred_at"],
                    "upload_id": e.get("upload_id"),
                    "site_id": e.get("site_id"),
                }
                for e in rows if e["event_type"] == event_type
            ]

        from .quality import evaluate_visit
        qv, _ = self.projection.latest_rule(RuleKind.QUALITY)
        quality_body = self.projection.rule_body(RuleKind.QUALITY, qv) or {}
        qresults = evaluate_visit(visit, self.projection, quality_body) if qv else {}

        risk_rule = None
        if visit.risk:
            risk_rule = self.projection.rule_body(
                RuleKind.RISK, int(visit.risk.get("rule_version", 0))
            )

        in_snapshots = []
        for snap in self.projection.snapshots.values():
            for row in snap.payload.get("included", []):
                if row["visit_id"] == visit_id:
                    in_snapshots.append({
                        "snapshot_id": snap.snapshot_id,
                        "frozen_seq": snap.frozen_seq,
                        "published": snap.published is not None,
                    })
            for row in snap.payload.get("excluded", []):
                if row["visit_id"] == visit_id:
                    in_snapshots.append({
                        "snapshot_id": snap.snapshot_id,
                        "frozen_seq": snap.frozen_seq,
                        "published": snap.published is not None,
                        "excluded_reasons": row["reasons"],
                    })

        return {
            "visit_id": visit.visit_id,
            "pid": visit.pid,
            "site_id": visit.site_id,
            "kind": visit.kind,
            "visit_date": visit.visit_date,
            "upload_ids": sorted(visit.upload_ids),
            "components": {
                Components.QUESTIONNAIRE: {
                    "current": visit.questionnaire,
                    "history": _component_events(Events.QUESTIONNAIRE_RESPONSE_RECORDED),
                },
                Components.NITX: {
                    "current": visit.nitx,
                    "history": _component_events(Events.NITX_RESULT_RECORDED),
                },
                Components.RISK: {
                    "current": visit.risk,
                    "rule_version": visit.risk.get("rule_version") if visit.risk else None,
                    "rule_body": risk_rule,
                    "history": _component_events(Events.RISK_STRATIFIED),
                },
                Components.FOLLOWUP: {
                    "current": visit.followup,
                    "history": _component_events(Events.FOLLOWUP_STATUS_RECORDED),
                },
            },
            "quality": {
                "rule_version": qv,
                "components": (
                    {name: r.as_dict() for name, r in qresults.items()}
                    if qresults else None
                ),
            },
            "in_snapshots": in_snapshots,
        }

    # ---------- 临床转诊（referral 通道，与研究数据分离） ----------
    def list_referrals(self, principal: Principal) -> list[dict[str, Any]]:
        principal.require(Scopes.REFERRAL_READ)
        out = []
        for referral in self.projection.referrals.values():
            if principal.role == Roles.CLINICIAN and principal.site_id is not None \
                    and referral.site_id != principal.site_id:
                continue
            if principal.role == Roles.SITE_COORDINATOR \
                    and referral.site_id != principal.site_id:
                continue
            out.append({
                "referral_id": referral.referral_id,
                "pid": referral.pid,
                "visit_id": referral.visit_id,
                "site_id": referral.site_id,
                "indication": referral.indication,
                "status": referral.status,
                "created_at": referral.created_at,
                "updated_at": referral.updated_at,
            })
        return out

    # ---------- 冻结 / 复算 / 发布 ----------
    def freeze(self, principal: Principal, holder: str,
               quality_version: int | None = None) -> dict[str, Any]:
        principal.require(Scopes.FREEZE_WRITE)
        with self._guard:
            return snapshots.freeze(
                self.store, self.projection, holder, quality_version
            )

    def recompute(self, principal: Principal, snapshot_id: str,
                  stats_version: int | None = None) -> dict[str, Any]:
        """从冻结快照复算但不发布（预览）。"""
        principal.require(Scopes.RESEARCH_READ)
        snap = self.projection.snapshots.get(snapshot_id)
        if snap is None:
            raise StoreError(f"快照不存在：{snapshot_id}")
        return analytics.recompute(snap.payload, stats_version or 1)

    def publish(self, principal: Principal, snapshot_id: str,
                stats_version: int | None = None) -> dict[str, Any]:
        principal.require(Scopes.FREEZE_WRITE)
        with self._guard:
            return analytics.publish(
                self.store, self.projection, snapshot_id, stats_version
            )

    def list_snapshots(self, principal: Principal) -> list[dict[str, Any]]:
        principal.require(Scopes.RESEARCH_READ)
        return [
            {
                "snapshot_id": s.snapshot_id,
                "frozen_seq": s.frozen_seq,
                "published": s.published is not None,
                "denominator": s.payload.get("denominator"),
                "excluded": len(s.payload.get("excluded", [])),
                "quality_version": s.payload.get("quality_version"),
            }
            for s in sorted(self.projection.snapshots.values(),
                            key=lambda x: x.frozen_seq)
        ]

    def published_stats(self, principal: Principal, snapshot_id: str) -> dict[str, Any]:
        principal.require(Scopes.RESEARCH_READ)
        snap = self.projection.snapshots.get(snapshot_id)
        if snap is None or snap.published is None:
            raise StoreError("快照不存在或尚未发布")
        return {"snapshot_id": snapshot_id, **snap.published}

    def list_batches(self, principal: Principal) -> list[dict[str, Any]]:
        principal.require(Scopes.RESEARCH_READ)
        site_filter = principal.site_id if principal.role == Roles.SITE_COORDINATOR else None
        rows = self.store.list_batches(site_filter)
        return [
            {"upload_id": r["upload_id"], "site_id": r["site_id"],
             "status": r["status"], "reason": r["reason"],
             "received_at": r["received_at"]}
            for r in rows
        ]

    def close(self) -> None:
        self.store.close()
