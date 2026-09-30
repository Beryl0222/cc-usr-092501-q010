"""HTTP API（仅标准库）。

鉴权：每个请求带
  Authorization: Bearer <user_key>
上传另外在 JSON 里带 token（上传凭证）。研究通道与转诊通道用不同路径分权；
冻结/发布需要 freeze:write。

路由：
  POST /admin/sites                 建立站点（admin）
  POST /admin/users                 建立用户（admin）
  POST /admin/tokens                领取上传凭证（admin/本站协调员）
  POST /uploads                     上传站点批次
  GET  /uploads                     批次台账
  POST /linkages/code               生成跨区关联码
  POST /linkages/consume            消费关联码（跨区归并）
  GET  /participants                参与者列表（站点内）
  GET  /participants/<pid>          参与者研究信息
  GET  /visits/<vid>/trace          结果溯源（访视/上传/规则版本/质量/快照）
  GET  /referrals                   临床转诊（referral 通道）
  POST /freezes                     冻结分析批次
  POST /snapshots/<id>/recompute    从冻结快照复算（预览）
  POST /snapshots/<id>/publish      发布快照（不可变）
  GET  /snapshots                   快照列表
  GET  /snapshots/<id>/stats        已发布统计
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .access import AccessDenied
from .service import CohortService
from .store import LockBusy, StoreError
from .uploads import Quarantine

JSON = "application/json; charset=utf-8"


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "CohortGovernance/0.1"

    # ---- 工具 ----
    def _send(self, code: int, body: dict | list) -> None:
        data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", JSON)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _principal(self):
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            raise AccessDenied("缺少 Bearer 凭证")
        return self.server.service.principal(auth[len("Bearer "):].strip())

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise StoreError(f"请求体不是合法 JSON：{error}")
        if not isinstance(parsed, dict):
            raise StoreError("请求体必须是 JSON 对象")
        return parsed

    def _handle(self, method: str):
        try:
            self.route(method)
        except AccessDenied as error:
            self._send(403, {"error": "access_denied", "detail": str(error)})
        except LockBusy as error:
            self._send(409, {"error": "lock_busy", "detail": str(error)})
        except (StoreError, ValueError, Quarantine) as error:
            self._send(400, {"error": "bad_request", "detail": str(error)})

    def do_GET(self) -> None:
        self._handle("GET")

    def do_POST(self) -> None:
        self._handle("POST")

    def log_message(self, *args) -> None:  # 静音默认访问日志
        pass

    # ---- 路由 ----
    def route(self, method: str) -> None:
        svc: CohortService = self.server.service
        path = urlparse(self.path).path.rstrip("/") or "/"
        segments = [s for s in path.split("/") if s]
        principal = self._principal()

        if method == "POST" and path == "/admin/sites":
            body = self._read_json()
            salt = svc.bootstrap_site(body["site_id"], body["province"], body["name"])
            self._send(201, {"site_id": body["site_id"], "salt": salt})
            return
        if method == "POST" and path == "/admin/users":
            body = self._read_json()
            svc.create_user(body["user_key"], body["role"], body.get("site_id"))
            self._send(201, {"user_key": body["user_key"], "role": body["role"]})
            return
        if method == "POST" and path == "/admin/tokens":
            body = self._read_json()
            token = svc.issue_token(principal, body["site_id"])
            self._send(201, {"token": token, "site_id": body["site_id"]})
            return
        if method == "POST" and path == "/uploads":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw.decode("utf-8"))
                token = body["token"]
            except (UnicodeDecodeError, json.JSONDecodeError, KeyError) as error:
                self._send(400, {"error": "bad_request",
                                 "detail": f"上传体需为含 token 的 JSON：{error}"})
                return
            result = svc.upload(principal, token, raw)
            status = result.get("status")
            code = 202 if status == "locked" else (
                200 if status == "accepted" else 422
            )
            self._send(code, result)
            return
        if method == "GET" and path == "/uploads":
            self._send(200, svc.list_batches(principal))
            return
        if method == "POST" and path == "/linkages/code":
            body = self._read_json()
            code = svc.issue_linkage_code(principal, body["pid"])
            self._send(201, {"linkage_code": code})
            return
        if method == "POST" and path == "/linkages/consume":
            body = self._read_json()
            result = svc.consume_linkage_code(principal, body["linkage_code"],
                                              body["consumer_pid"])
            self._send(200, result)
            return
        if method == "GET" and path == "/participants":
            self._send(200, svc.list_participants(principal))
            return
        if method == "GET" and len(segments) == 2 and segments[0] == "participants":
            self._send(200, svc.get_participant(principal, segments[1]))
            return
        if method == "GET" and len(segments) == 3 and segments[0] == "visits" \
                and segments[2] == "trace":
            self._send(200, svc.trace_result(principal, segments[1]))
            return
        if method == "GET" and path == "/referrals":
            self._send(200, svc.list_referrals(principal))
            return
        if method == "POST" and path == "/freezes":
            body = self._read_json()
            event = svc.freeze(principal, body.get("holder", principal.user_key),
                               body.get("quality_version"))
            self._send(201, {"snapshot_id": event["aggregate_id"],
                             "summary": event["summary"],
                             "denominator": event["payload"]["denominator"],
                             "excluded": len(event["payload"]["excluded"])})
            return
        if len(segments) == 3 and segments[0] == "snapshots":
            snap_id, action = segments[1], segments[2]
            if method == "POST" and action == "recompute":
                self._send(200, svc.recompute(principal, snap_id,
                                              self._read_json().get("stats_version")))
                return
            if method == "POST" and action == "publish":
                event = svc.publish(principal, snap_id,
                                    self._read_json().get("stats_version"))
                self._send(201, {"snapshot_id": snap_id, "summary": event["summary"]})
                return
        if method == "GET" and path == "/snapshots":
            self._send(200, svc.list_snapshots(principal))
            return
        if method == "GET" and len(segments) == 3 and segments[0] == "snapshots" \
                and segments[2] == "stats":
            self._send(200, svc.published_stats(principal, segments[1]))
            return

        self._send(404, {"error": "not_found", "path": path})


def build_server(service: CohortService, host: str = "127.0.0.1",
                 port: int = 8080) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), ApiHandler)
    server.service = service  # type: ignore[attr-defined]
    server.daemon_threads = True
    return server


def serve(service: CohortService, host: str = "127.0.0.1",
          port: int = 8080) -> None:
    server = build_server(service, host, port)
    print(f"队列治理服务监听 http://{host}:{port}")
    server.serve_forever()
