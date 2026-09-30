"""本地身份映射。

招募站点在本地用持有的密钥把身份证件标识派生为研究标识：

* 研究标识 ``study_id`` 由中央发放的跨区联动胡椒（linkage pepper）与证件号
  经 HMAC-SHA256 派生。同一人在任何省份得到同一研究标识，因此跨区复查可以
  识别为同一参与者的**又一次访视**，而不会把多次合法随访误合并，也不需要
  中央按身份证号去重。
* 站点本地别名 ``site_alias`` 由站点自有密钥派生，仅供该站点本地核对，
  其他站点与中央视图都无法据其反查证件号。

证件号本身绝不进入事件、日志或数据库；派生函数带域分隔前缀，防止不同用途
之间的哈希复用。胡椒与站点密钥通过带外渠道发放，不写进仓库。
"""

from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass

_ID_RE = re.compile(r"^\d{17}[\dXx]$")
# 加权因子与模 11 校验码（GB 11643）
_WEIGHTS = (7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2)
_CHECK_CODES = "10X98765432"

STUDY_ID_PREFIX = "CLC-"  # community liver cohort


def normalize_id_number(raw: str) -> str:
    """去除空白并统一末位 X 的大小写。"""
    if not isinstance(raw, str):
        raise ValueError("证件号必须是字符串")
    value = re.sub(r"\s+", "", raw).upper()
    if not _ID_RE.match(value):
        raise ValueError("证件号格式不符合 18 位居民身份证")
    return value


def validate_id_number(raw: str) -> str:
    """校验格式与校验位，返回规范证件号。"""
    value = normalize_id_number(raw)
    checksum = sum(int(d) * w for d, w in zip(value[:17], _WEIGHTS)) % 11
    if _CHECK_CODES[checksum] != value[17]:
        raise ValueError("证件号校验位不正确")
    return value


def _domain_hmac(key: str, domain: str, value: str) -> str:
    digest = hmac.new(key.encode("utf-8"), f"{domain}|{value}".encode("utf-8"), hashlib.sha256)
    return digest.hexdigest()


def derive_study_id(linkage_pepper: str, id_number: str) -> str:
    """由跨区联动胡椒派生研究标识；同一证件号在所有站点结果一致。"""
    if not linkage_pepper:
        raise ValueError("缺少跨区联动胡椒")
    normalized = normalize_id_number(id_number)
    return STUDY_ID_PREFIX + _domain_hmac(linkage_pepper, "study-id-v1", normalized)[:24]


def derive_site_alias(site_secret: str, site_id: str, id_number: str) -> str:
    """由站点密钥派生仅本站点可识别的本地别名。"""
    if not site_secret:
        raise ValueError("缺少站点密钥")
    normalized = normalize_id_number(id_number)
    return _domain_hmac(site_secret, f"site-alias|{site_id}", normalized)[:20]


def demographics_from_id(id_number: str) -> dict[str, int | str]:
    """从证件号提取风险分层所需的出生年份与性别（不提取其余身份信息）。"""
    value = validate_id_number(id_number)
    birth_year = int(value[6:10])
    sex = "male" if int(value[16]) % 2 == 1 else "female"
    return {"birth_year": birth_year, "sex": sex}


@dataclass(frozen=True)
class LocalMapping:
    site_id: str
    study_id: str
    site_alias: str
    birth_year: int
    sex: str


class SiteIdentityMapper:
    """招募站点本地使用的映射器；证件号只存在于调用期内存中。"""

    def __init__(self, site_id: str, site_secret: str, linkage_pepper: str) -> None:
        self.site_id = site_id
        self._site_secret = site_secret
        self._linkage_pepper = linkage_pepper

    def enroll(self, id_number: str) -> LocalMapping:
        validate_id_number(id_number)
        demo = demographics_from_id(id_number)
        return LocalMapping(
            site_id=self.site_id,
            study_id=derive_study_id(self._linkage_pepper, id_number),
            site_alias=derive_site_alias(self._site_secret, self.site_id, id_number),
            birth_year=demo["birth_year"],
            sex=demo["sex"],
        )
