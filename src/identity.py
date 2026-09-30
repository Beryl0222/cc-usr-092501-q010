"""本地身份映射：身份证号从不离开站点，研究标识由站点盐 HMAC 生成。

- 化名只对（站点、身份证号）稳定：同一人跨站点得到不同化名，天然防止
  跨站点凭标识直接关联。
- 跨区复查经显式关联码（linkage code）在研究标识层面合并，关联码由
  招募站点生成、中央或另一站点凭码登记，不携带身份证号。
- 研究标识不含身份证号或其可逆编码，只在数据库里保存化名与站点的映射行，
  且映射行与研究数据分权（identity:map 范围）访问。
"""

from __future__ import annotations

import hashlib
import hmac
import secrets

STUDY_PREFIX = "P"
LINK_PREFIX = "L"


def pseudonym(site_id: str, id_card: str, salt: str) -> str:
    """对站点内身份证号做 HMAC-SHA256，截断为研究标识。"""
    digest = hmac.new(
        salt.encode("utf-8"),
        f"{site_id}|{id_card.strip()}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{STUDY_PREFIX}{digest[:24]}"


def new_salt() -> str:
    return secrets.token_hex(32)


def new_linkage_code() -> str:
    """跨区复查关联码：一次性、高熵、无身份含义。"""
    return f"{LINK_PREFIX}{secrets.token_urlsafe(18)}"


def new_token() -> str:
    """上传凭证。"""
    return f"ut_{secrets.token_urlsafe(32)}"


def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(8)}"
