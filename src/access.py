"""分权访问控制。

两类数据通道严格分开：
- research:read 访问研究数据（化名、问卷、无创、风险、统计），看不到临床
  转诊明细；
- referral:read 只访问临床转诊记录，不暴露研究库其余内容。

站点维度的隔离：site-coordinator 的所有读写都被限制在本站点参与者范围内；
central-analyst / admin 可跨站点。身份映射（身份证号 ↔ 化名）额外需要
identity:map，且只有 admin 与本站点协调员可以执行。
"""

from __future__ import annotations

from dataclasses import dataclass

from .contracts import ROLE_SCOPES, Roles, Scopes


class AccessDenied(Exception):
    """无权执行该操作。"""


@dataclass(frozen=True)
class Principal:
    user_key: str
    role: str
    site_id: str | None = None

    @property
    def scopes(self) -> tuple[str, ...]:
        return ROLE_SCOPES.get(self.role, ())

    def has(self, scope: str) -> bool:
        return scope in self.scopes

    def require(self, scope: str) -> None:
        if not self.has(scope):
            raise AccessDenied(f"角色 {self.role} 缺少范围 {scope}")

    def require_site(self, site_id: str) -> None:
        """站点角色只能触及本站点；中央角色不限。"""
        if self.role == Roles.SITE_COORDINATOR and self.site_id != site_id:
            raise AccessDenied("站点只能访问自身参与者与数据")

    def is_cross_site(self) -> bool:
        return self.role in (Roles.ADMIN, Roles.CENTRAL_ANALYST)
