"""队列治理 HTTP API（仅标准库 http.server）。

鉴权使用不记名令牌（Bearer），角色与站点边界在每个请求上执行：

* ``coordinator``：中央协调，冻结/发布/复算/全量视图。
* ``researcher``：研究视图；撤回的参与者不再可见。
* ``clinician``：仅可访问临床转诊记录。
* ``site_user``：只能向本站点上传并查看本站点参与者。

并发锁库（:class:`~src.repository.LibraryLocked`）返回 503 + Retry-After，
供自动化场景验证“稍后重试”，服务进程不丢失任何已提交数据。
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from .governance import GovernanceError, GovernanceService, NotAuthorized
from .repository import ConflictQuarantine, EventStore, LibraryLocked

ROLE_COORDINATOR = "coordinator"
ROLE_RESEARCHER = "researcher"
ROLE_CLINICIAN = "clinician"
ROLE_SITE = "site_user"


def create_app(
    store: EventStore, *, host: str = "127.0.0.1", port: int = 0
) -> tuple[ThreadingHTTPServer, GovernanceService]:
    service = GovernanceService(store)

    class Handler(BaseHTTPRequestHandler):
        server_version = "CohortGovernance/0.1"

        def log_message(self, fmt: str, *args: Any) -> None:  # 静音默认访问日志
            return

        # ---- 框架辅助 ----------------------------------------------------

        def _send_json(self, status: int, body: Any, extra_headers: dict[str, str] | None = None) -> None:
            data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            for key, value in (extra_headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(data)

        def _read_json(self) -> Any:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0:
                raise GovernanceError("缺少请求体")
            raw = self.rfile.read(length)
            try:
                return json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as error:
                raise GovernanceError(f"请求体不是合法 JSON：{error}") from error

        def _auth(self, *, roles: tuple[str, ...] | None = None) -> dict[str, Any]:
            header = self.headers.get("Authorization", "")
            if not header.startswith("Bearer "):
                raise NotAuthorized("缺少 Bearer 令牌")
            auth = store.authorize(header[len("Bearer ") :].strip())
            if auth is None:
                raise NotAuthorized("令牌无效或已失效")
            if roles and auth["role"] not in roles:
                raise NotAuthorized(f"角色 {auth['role']} 无权执行该操作")
            return auth

        def _handle(self, fn: Callable[[], Any]) -> None:
            try:
                result = fn()
            except NotAuthorized as error:
                self._send_json(403, {"error": "forbidden", "detail": str(error)})
            except GovernanceError as error:
                self._send_json(400, {"error": "rejected", "detail": str(error)})
            except LibraryLocked as error:
                self._send_json(
                    503,
                    {"error": "library_locked", "detail": str(error)},
                    {"Retry-After": "1"},
                )
            except (KeyError, ValueError) as error:
                self._send_json(400, {"error": "bad_request", "detail": str(error)})
            else:
                if result is None:
                    self._send_json(204, {})
                else:
                    self._send_json(200, result)

        # ---- 路由 --------------------------------------------------------

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            query = parse_qs(parsed.query)

            def route() -> Any:
                if parts == ["health"]:
                    return {"status": "ok"}
                if parts == ["uploads"]:
                    auth = self._auth(roles=(ROLE_COORDINATOR, ROLE_SITE))
                    site_id = query.get("site", [None])[0]
                    if auth["role"] == ROLE_SITE:
                        site_id = auth["site_id"]
                    return {"uploads": store.list_uploads(site_id)}
                if len(parts) == 2 and parts[0] == "participants":
                    auth = self._auth()
                    return service.participant_view(parts[1], auth)
                if len(parts) == 2 and parts[0] == "visits":
                    auth = self._auth()
                    return service.visit_view(parts[1], auth)
                if len(parts) == 3 and parts[0] == "visits" and parts[2] == "trace":
                    auth = self._auth(roles=(ROLE_COORDINATOR, ROLE_RESEARCHER, ROLE_SITE))
                    return service.trace_result(parts[1], auth)
                if len(parts) == 2 and parts[0] == "referrals":
                    auth = self._auth(roles=(ROLE_COORDINATOR, ROLE_CLINICIAN, ROLE_SITE))
                    return service.referral_view(parts[1], auth)
                if len(parts) == 3 and parts[0] == "snapshots" and parts[2] == "report":
                    self._auth(roles=(ROLE_COORDINATOR, ROLE_RESEARCHER))
                    return service.recompute(parts[1])
                self._send_json(404, {"error": "not_found", "detail": parsed.path})
                return None

            self._handle(route)

        def do_POST(self) -> None:
            parts = [p for p in urlparse(self.path).path.split("/") if p]

            def route() -> Any:
                if len(parts) == 3 and parts[0] == "uploads":
                    auth = self._auth(roles=(ROLE_COORDINATOR, ROLE_SITE))
                    site_id, upload_id = parts[1], parts[2]
                    if auth["role"] == ROLE_SITE and auth["site_id"] != site_id:
                        raise NotAuthorized("站点只能使用自身站点标识上传")
                    records = self._read_json()
                    result = service.ingest_upload(site_id, upload_id, records)
                    status = 200 if result.status in ("accepted", "duplicate") else 409
                    self._send_json(
                        status,
                        {
                            "status": result.status,
                            "upload_id": result.upload_id,
                            "count": result.count,
                            "reason": result.reason,
                            "linkages": result.linkages,
                        },
                    )
                    return None
                if parts == ["admin", "freeze"]:
                    self._auth(roles=(ROLE_COORDINATOR,))
                    body = self._read_json()
                    result = service.freeze_batch(
                        body["snapshot_id"],
                        body["visit_ids"],
                        label=body.get("label", ""),
                        exclude_failures=bool(body.get("exclude_failures", False)),
                    )
                    self._send_json(200 if result["status"] != "rejected" else 422, result)
                    return None
                if len(parts) == 3 and parts[:2] == ["admin", "publish"]:
                    self._auth(roles=(ROLE_COORDINATOR,))
                    result = service.publish_snapshot(parts[2])
                    self._send_json(200, result)
                    return None
                self._send_json(404, {"error": "not_found", "detail": self.path})
                return None

            try:
                route()
            except NotAuthorized as error:
                self._send_json(403, {"error": "forbidden", "detail": str(error)})
            except GovernanceError as error:
                self._send_json(400, {"error": "rejected", "detail": str(error)})
            except LibraryLocked as error:
                self._send_json(503, {"error": "library_locked", "detail": str(error)}, {"Retry-After": "1"})
            except ConflictQuarantine as error:
                self._send_json(409, {"error": "quarantined", "detail": error.reason})

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    return server, service


def serve_in_thread(store: EventStore) -> tuple[ThreadingHTTPServer, GovernanceService, threading.Thread]:
    """测试辅助：在后台线程启动服务。"""
    server, service = create_app(store)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, service, thread
