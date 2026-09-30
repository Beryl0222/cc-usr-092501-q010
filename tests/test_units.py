"""身份化名、信封枚举、风险引擎与分权的单元测试。"""

from __future__ import annotations

import unittest

from src.access import AccessDenied, Principal
from src.contracts import Diabetes, AlcoholStatus, RiskLevel, Roles, Scopes
from src.envelope import validate_event
from src.identity import new_linkage_code, new_salt, pseudonym
from src.risk import classify, risk_inputs, RISK_RULE_V1


class IdentityTest(unittest.TestCase):
    def test_pseudonym_stable_per_site_but_differs_across_sites(self) -> None:
        salt1, salt2 = new_salt(), new_salt()
        a = pseudonym("S1", "110101199001011234", salt1)
        same = pseudonym("S1", "110101199001011234", salt1)
        other_site = pseudonym("S2", "110101199001011234", salt2)
        other_person = pseudonym("S1", "11010119900202223X", salt1)
        self.assertEqual(a, same)
        self.assertNotEqual(a, other_site)
        self.assertNotEqual(a, other_person)
        self.assertNotIn("110101199001011234", a)
        self.assertTrue(a.startswith("P"))

    def test_whitespace_idcard_normalizes(self) -> None:
        salt = new_salt()
        self.assertEqual(
            pseudonym("S1", " 110101199001011234 ", salt),
            pseudonym("S1", "110101199001011234", salt),
        )

    def test_linkage_code_high_entropy_and_unique(self) -> None:
        codes = {new_linkage_code() for _ in range(100)}
        self.assertEqual(len(codes), 100)
        self.assertTrue(all(c.startswith("L") for c in codes))


class EnvelopeTest(unittest.TestCase):
    def _base(self) -> dict:
        return {
            "event_id": "e1", "event_type": "VISIT_RECORDED",
            "aggregate_type": "study_visit", "aggregate_id": "V1",
            "occurred_at": "2026-09-24T10:00:00+08:00", "version": 1,
            "summary": "x",
        }

    def test_valid(self) -> None:
        self.assertEqual(validate_event(self._base()), [])

    def test_unknown_event_type_rejected(self) -> None:
        rec = self._base()
        rec["event_type"] = "NOT_A_THING"
        self.assertTrue(any("未知事件类型" in e for e in validate_event(rec)))

    def test_unknown_aggregate_rejected(self) -> None:
        rec = self._base()
        rec["aggregate_type"] = "mystery"
        self.assertTrue(any("未知聚合类型" in e for e in validate_event(rec)))


class RiskEngineTest(unittest.TestCase):
    def _inputs(self, age, ast, alt, platelets, diabetes=Diabetes.NO,
                alcohol=AlcoholStatus.NONE):
        return {"age": age, "ast": ast, "alt": alt, "platelets": platelets,
                "diabetes": diabetes, "alcohol": alcohol}

    def test_fib4_bands(self) -> None:
        # FIB-4 = 30*20/(220*sqrt(20)) ≈ 0.61 -> low
        low = classify(RISK_RULE_V1, self._inputs(30, 20, 20, 220))
        self.assertEqual(low["level"], RiskLevel.LOW)
        # 55*40/(200*sqrt(30)) ≈ 2.01 -> medium
        mid = classify(RISK_RULE_V1, self._inputs(55, 40, 30, 200))
        self.assertEqual(mid["level"], RiskLevel.MEDIUM)
        # 70*80/(90*sqrt(40)) ≈ 9.84 -> high
        high = classify(RISK_RULE_V1, self._inputs(70, 80, 40, 90))
        self.assertEqual(high["level"], RiskLevel.HIGH)

    def test_diabetes_and_heavy_alcohol_bump(self) -> None:
        base = classify(RISK_RULE_V1, self._inputs(55, 40, 30, 200))
        bumped = classify(
            RISK_RULE_V1,
            self._inputs(55, 40, 30, 200, diabetes=Diabetes.YES,
                         alcohol=AlcoholStatus.HEAVY),
        )
        self.assertEqual(base["level"], RiskLevel.MEDIUM)
        self.assertEqual(bumped["level"], RiskLevel.HIGH)
        self.assertIn("diabetes", bumped["adjustments"])
        self.assertIn("heavy_alcohol", bumped["adjustments"])

    def test_missing_inputs_yields_none(self) -> None:
        result = classify(RISK_RULE_V1, risk_inputs(None, None))
        self.assertIsNone(result["level"])
        self.assertIsNone(result["score"])


class AccessControlTest(unittest.TestCase):
    def test_clinician_referral_only(self) -> None:
        doctor = Principal("d", Roles.CLINICIAN, "S01")
        self.assertTrue(doctor.has(Scopes.REFERRAL_READ))
        with self.assertRaises(AccessDenied):
            doctor.require(Scopes.RESEARCH_READ)

    def test_site_coordinator_scoped(self) -> None:
        coord = Principal("c", Roles.SITE_COORDINATOR, "S01")
        coord.require_site("S01")
        with self.assertRaises(AccessDenied):
            coord.require_site("S02")

    def test_central_roles_cross_site(self) -> None:
        analyst = Principal("a", Roles.CENTRAL_ANALYST)
        analyst.require_site("S01")  # 不抛异常
        self.assertTrue(analyst.is_cross_site())

    def test_unknown_role_has_no_scope(self) -> None:
        self.assertEqual(Principal("x", "ghost").scopes, ())


if __name__ == "__main__":
    unittest.main()
