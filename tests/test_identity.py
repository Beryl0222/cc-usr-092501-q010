"""本地身份映射测试：研究标识跨站点一致、证件号不落库、校验位校验。"""

from __future__ import annotations

import unittest

from src.identity import (
    SiteIdentityMapper,
    derive_site_alias,
    derive_study_id,
    validate_id_number,
)
from tests import factory


class IdentityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.id_number = factory.make_id_number("370102", "19680512", "321")

    def test_checksum_validated(self) -> None:
        self.assertEqual(validate_id_number(self.id_number), self.id_number)
        with self.assertRaises(ValueError):
            validate_id_number(self.id_number[:-1] + ("0" if self.id_number[-1] != "0" else "1"))

    def test_study_id_is_stable_across_sites(self) -> None:
        site_a = SiteIdentityMapper("site-3701", "secret-a", factory.PEPPER)
        site_b = SiteIdentityMapper("site-4401", "secret-b", factory.PEPPER)
        a = site_a.enroll(self.id_number)
        b = site_b.enroll(self.id_number)
        self.assertEqual(a.study_id, b.study_id)
        self.assertTrue(a.study_id.startswith("CLC-"))

    def test_site_alias_is_site_scoped(self) -> None:
        a = SiteIdentityMapper("site-3701", "secret-a", factory.PEPPER).enroll(self.id_number)
        b = SiteIdentityMapper("site-4401", "secret-b", factory.PEPPER).enroll(self.id_number)
        self.assertNotEqual(a.site_alias, b.site_alias)

    def test_pepper_change_changes_study_id(self) -> None:
        self.assertNotEqual(
            derive_study_id(factory.PEPPER, self.id_number),
            derive_study_id("other-pepper", self.id_number),
        )

    def test_demographics_extracted(self) -> None:
        m = SiteIdentityMapper("site-3701", factory.SITE_SECRET, factory.PEPPER).enroll(self.id_number)
        self.assertEqual((m.birth_year, m.sex), (1968, "male"))

    def test_raw_id_is_not_in_mapping(self) -> None:
        m = SiteIdentityMapper("site-3701", factory.SITE_SECRET, factory.PEPPER).enroll(self.id_number)
        self.assertNotIn(self.id_number, repr(m.__dict__))
        self.assertNotEqual(derive_site_alias(factory.SITE_SECRET, "site-3701", self.id_number), self.id_number)


if __name__ == "__main__":
    unittest.main()
