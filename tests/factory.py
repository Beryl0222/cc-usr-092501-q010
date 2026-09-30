"""测试共享夹具：合法证件号生成与合格访视记录工厂。"""

from __future__ import annotations

from src.identity import SiteIdentityMapper, derive_study_id

WEIGHTS = (7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2)
CHECK_CODES = "10X98765432"

PEPPER = "test-linkage-pepper"
SITE_SECRET = "test-site-secret"


def make_id_number(region: str, birth: str, sequence: str) -> str:
    """生成校验位正确的 18 位证件号（仅测试用）。"""
    body = f"{region}{birth}{sequence}"
    assert len(body) == 17 and body.isdigit()
    checksum = sum(int(d) * w for d, w in zip(body, WEIGHTS)) % 11
    return body + CHECK_CODES[checksum]


def mapping(site_id: str, id_number: str) -> tuple[str, str, int, str]:
    mapper = SiteIdentityMapper(site_id, SITE_SECRET, PEPPER)
    m = mapper.enroll(id_number)
    return m.study_id, m.site_alias, m.birth_year, m.sex


def study_id_of(id_number: str) -> str:
    return derive_study_id(PEPPER, id_number)


def enrollment(site_id: str, study_id: str, birth_year: int, sex: str, at: str) -> dict:
    return {
        "record_type": "enrollment",
        "study_id": study_id,
        "birth_year": birth_year,
        "sex": sex,
        "enrolled_at": at,
        "client_at": at,
    }


def consent(study_id: str) -> dict:
    return {
        "record_type": "consent",
        "study_id": study_id,
        "scope": "baseline+followup",
        "permissions": {"future_analysis": True, "retain_aggregates": True},
    }


def device(device_id: str, model: str = "FibroScan-TEST") -> dict:
    return {"record_type": "device", "device_id": device_id, "model": model}


def calibration(device_id: str, calibrated_at: str, *, active: bool = True) -> dict:
    return {
        "record_type": "calibration",
        "device_id": device_id,
        "calibrated_at": calibrated_at,
        "calibration_by": "市级计量站",
        "active": active,
    }


def valid_noninv(device_id: str, exam_at: str, *, lsm: float = 8.2, ast=35.0, alt=30.0, platelets=180) -> dict:
    return {
        "device_id": device_id,
        "exam_at": exam_at,
        "valid_shots": 12,
        "iqr_median_ratio": 0.18,
        "lsm_kpa": lsm,
        "cap_dbm": 245.0,
        "ast": ast,
        "alt": alt,
        "platelets": platelets,
    }


def baseline_sections(device_id: str, exam_at: str, *, noninv=None, drinker="never") -> dict:
    return {
        "baseline": {"birth_year": 1968, "sex": "male", "enrolled_at": exam_at},
        "questionnaire": {
            "instrument_id": "CLH_BASELINE",
            "version": 1,
            "items": {
                "diabetes_history": "none",
                "drinker_status": drinker,
                "alcohol_grams_per_day": 0,
                "prior_liver_disease": "no",
            },
        },
        "diabetes": {"status": "none"},
        "alcohol": {"drinker_status": drinker, "alcohol_grams_per_day": 0},
        "noninvasive": noninv or valid_noninv(device_id, exam_at),
    }


def followup_sections(device_id: str, exam_at: str, *, noninv=None) -> dict:
    sections = baseline_sections(device_id, exam_at, noninv=noninv)
    del sections["baseline"]
    return sections


def visit(visit_id: str, study_id: str, planned_at: str, sections: dict, *, visit_type: str = "baseline") -> dict:
    return {
        "record_type": "visit",
        "visit_id": visit_id,
        "study_id": study_id,
        "visit_type": visit_type,
        "planned_at": planned_at,
        "sections": sections,
        "client_at": planned_at,
    }


def referral(referral_id: str, study_id: str, visit_id: str, band: str = "high") -> dict:
    return {
        "record_type": "referral",
        "referral_id": referral_id,
        "study_id": study_id,
        "visit_id": visit_id,
        "band": band,
        "recommended_facility": "市人民医院肝病科",
    }
