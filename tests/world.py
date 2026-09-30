"""测试夹具：构造一个就绪的多站点队列治理世界。"""

from __future__ import annotations

import json

from src.identity import pseudonym
from src.service import CohortService

PROVINCES = ["北京", "上海", "广东", "浙江", "江苏", "四川", "湖北",
             "山东", "河南", "湖南", "福建", "陕西", "辽宁"]


def make_world(db_path: str = ":memory:") -> CohortService:
    svc = CohortService.open(db_path)
    for index, province in enumerate(PROVINCES, start=1):
        site_id = f"S{index:02d}"
        svc.bootstrap_site(site_id, province, f"{province}站点")
    svc.create_user("admin", "admin")
    svc.create_user("analyst", "central-analyst")
    for index in range(1, len(PROVINCES) + 1):
        site_id = f"S{index:02d}"
        svc.create_user(f"coord{index:02d}", "site-coordinator", site_id)
        svc.create_user(f"doctor{index:02d}", "clinician", site_id)
    return svc


def p(svc: CohortService, site_index: int, id_card: str) -> str:
    site_id = f"S{site_index:02d}"
    return pseudonym(site_id, id_card, svc.store.get_site(site_id)["salt"])


def batch(upload_id: str, commands: list[dict]) -> bytes:
    return json.dumps({"upload_id": upload_id, "commands": commands},
                      ensure_ascii=False).encode()


def upload(svc: CohortService, coord_key: str, site_index: int,
           upload_id: str, commands: list[dict]) -> dict:
    principal = svc.principal(coord_key)
    admin = svc.principal("admin")
    site_id = f"S{site_index:02d}"
    token = svc.issue_token(admin, site_id)
    return svc.upload(principal, token, batch(upload_id, commands))


def master_commands(quality_version: int = 1, risk_version: int = 1,
                    questionnaire_version: int = 1) -> list[dict]:
    """中央主数据：规则与问卷版本。设备校准属各站点，见 site_device_commands。"""
    return [
        {"type": "rule_publish", "kind": "risk", "version": risk_version},
        {"type": "rule_publish", "kind": "quality", "version": quality_version},
        {"type": "rule_publish", "kind": "stats", "version": 1},
        {"type": "questionnaire_publish", "version": questionnaire_version},
    ]


def site_device_commands(site_index: int, calibrated_at: str = "2026-01-01T00:00:00+08:00",
                         calibration_id: str | None = None) -> list[dict]:
    site_id = f"D{site_index:02d}"
    cal = calibration_id or f"C{site_index:02d}"
    return [
        {"type": "device_register", "device_id": site_id, "model": "FibroScan-X"},
        {"type": "calibration", "device_id": site_id, "calibration_id": cal,
         "calibrated_at": calibrated_at},
    ]


def enroll_consent(pid: str, when: str = "2026-03-01",
                   scope: str = "full") -> list[dict]:
    return [
        {"type": "enroll", "pid": pid},
        {"type": "consent", "pid": pid, "scope": scope, "from": when},
    ]


def nitx(measured_date: str, *, site_index: int = 1, device=None,
         calibration=None, shots=10, lsm=12.0, iqr=2.0, alt=30, ast=40,
         platelets=200) -> dict:
    return {
        "device_id": device or f"D{site_index:02d}",
        "calibration_id": calibration or f"C{site_index:02d}",
        "measured_at": f"{measured_date}T10:00:00+08:00",
        "valid_shots": shots, "lsm_kpa": lsm, "lsm_iqr": iqr,
        "alt": alt, "ast": ast, "platelets": platelets,
    }


def visit(visit_id: str, pid: str, date: str, *, site_index: int = 1, age=55,
          diabetes="yes", alcohol="heavy", level=None, referral=None,
          **nitx_kw) -> dict:
    nitx_kw.setdefault("site_index", site_index)
    n = nitx(date, **nitx_kw)
    # 默认按 risk-v1 复算，避免夹具上传与引擎不一致；测试需要故意不一致时显式传 level
    if level is None:
        from src.risk import RISK_RULE_V1, classify, risk_inputs
        q = {"age": age, "diabetes": diabetes, "alcohol": alcohol}
        level = classify(RISK_RULE_V1, risk_inputs(q, n))["level"]
    cmd = {
        "type": "visit", "visit_id": visit_id, "pid": pid,
        "kind": "baseline", "visit_date": date,
        "questionnaire": {"version": 1, "age": age, "diabetes": diabetes,
                          "alcohol": alcohol},
        "nitx": n,
        "risk": {"rule_version": 1, "level": level},
    }
    if referral:
        cmd["referral"] = {"referral_id": f"R-{visit_id}",
                           "indication": referral, "status": "open"}
    return cmd


def setup_world_with_master(svc: CohortService) -> None:
    """中央主数据放站点 1；各站点注册自己的设备与有效校准。"""
    upload(svc, "coord01", 1, "U-master", master_commands())
    for index in range(1, len(PROVINCES) + 1):
        upload(svc, f"coord{index:02d}", index, f"U-device{index:02d}",
               site_device_commands(index))
