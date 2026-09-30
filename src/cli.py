"""队列治理命令入口。

子命令：

  validate  <事件文件>                校验单个领域事件（默认行为，兼容旧用法）
  ingest    --site S --upload U 文件   提交站点上传批次（JSON 记录数组）
  freeze    --snapshot ID --visits …  冻结分析批次
  publish   --snapshot ID             复算并发布患病率快照
  recompute --snapshot ID             从冻结快照复算并输出缺失/排除原因
  token     --role ROLE [--site S]    发放访问令牌
  verify                              事件库完整性自检
  serve     --port PORT               启动 HTTP API
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .envelope import validate_event
from .governance import GovernanceError, GovernanceService
from .repository import ConflictQuarantine, EventStore


def _load_json(path: str) -> object:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def cmd_validate(args: argparse.Namespace) -> int:
    record = _load_json(args.file)
    errors = validate_event(record)
    if errors:
        print("；".join(errors), file=sys.stderr)
        return 1
    print(f"事件有效：{record['event_id']}")
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    records = _load_json(args.file)
    if not isinstance(records, list):
        print("上传文件必须是记录数组", file=sys.stderr)
        return 2
    with EventStore(args.db) as store:
        result = GovernanceService(store).ingest_upload(args.site, args.upload, records)
    print(json.dumps(result.__dict__, ensure_ascii=False, indent=2))
    return 0 if result.status in ("accepted", "duplicate") else 1


def cmd_freeze(args: argparse.Namespace) -> int:
    with EventStore(args.db) as store:
        result = GovernanceService(store).freeze_batch(
            args.snapshot, args.visits.split(","),
            label=args.label, exclude_failures=args.exclude_failures,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] in ("frozen", "empty") else 1


def cmd_publish(args: argparse.Namespace) -> int:
    with EventStore(args.db) as store:
        result = GovernanceService(store).publish_snapshot(args.snapshot)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def cmd_recompute(args: argparse.Namespace) -> int:
    with EventStore(args.db) as store:
        report = GovernanceService(store).recompute(args.snapshot)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


def cmd_token(args: argparse.Namespace) -> int:
    with EventStore(args.db) as store:
        token = store.mint_token(args.role, args.site)
    print(token)
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    with EventStore(args.db) as store:
        print(json.dumps(store.verify(), ensure_ascii=False, indent=2))
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    from .api import create_app

    store = EventStore(args.db)
    server, _ = create_app(store, port=args.port)
    server.server_address  # 已绑定
    host, port = server.server_address[0], server.server_address[1]
    print(f"队列治理 API 监听于 http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="src.cli", description="社区肝病队列治理")
    sub = parser.add_subparsers(dest="command")

    p_validate = sub.add_parser("validate", help="校验领域事件文件")
    p_validate.add_argument("file")
    p_validate.set_defaults(func=cmd_validate)

    def add_db(p: argparse.ArgumentParser) -> None:
        p.add_argument("--db", default="cohort_governance.db", help="SQLite 事件库路径")

    p_ingest = sub.add_parser("ingest", help="提交站点上传批次")
    add_db(p_ingest)
    p_ingest.add_argument("--site", required=True)
    p_ingest.add_argument("--upload", required=True)
    p_ingest.add_argument("file")
    p_ingest.set_defaults(func=cmd_ingest)

    p_freeze = sub.add_parser("freeze", help="冻结分析批次")
    add_db(p_freeze)
    p_freeze.add_argument("--snapshot", required=True)
    p_freeze.add_argument("--visits", required=True, help="逗号分隔的访视标识")
    p_freeze.add_argument("--label", default="")
    p_freeze.add_argument("--exclude-failures", action="store_true")
    p_freeze.set_defaults(func=cmd_freeze)

    p_publish = sub.add_parser("publish", help="发布患病率快照")
    add_db(p_publish)
    p_publish.add_argument("--snapshot", required=True)
    p_publish.set_defaults(func=cmd_publish)

    p_recompute = sub.add_parser("recompute", help="复算冻结快照")
    add_db(p_recompute)
    p_recompute.add_argument("--snapshot", required=True)
    p_recompute.set_defaults(func=cmd_recompute)

    p_token = sub.add_parser("token", help="发放访问令牌")
    add_db(p_token)
    p_token.add_argument("--role", required=True, choices=["coordinator", "researcher", "clinician", "site_user"])
    p_token.add_argument("--site", default=None)
    p_token.set_defaults(func=cmd_token)

    p_verify = sub.add_parser("verify", help="事件库完整性自检")
    add_db(p_verify)
    p_verify.set_defaults(func=cmd_verify)

    p_serve = sub.add_parser("serve", help="启动 HTTP API")
    add_db(p_serve)
    p_serve.add_argument("--port", type=int, default=8080)
    p_serve.set_defaults(func=cmd_serve)

    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    # 兼容旧用法：python3 -m src.cli data/sample.json
    if argv and not argv[0].startswith("-") and argv[0] not in {
        "validate", "ingest", "freeze", "publish", "recompute", "token", "verify", "serve",
    } and Path(argv[0]).exists():
        argv = ["validate", *argv]
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 2
    try:
        return args.func(args)
    except (OSError, json.JSONDecodeError) as error:
        print(f"无法读取文件：{error}", file=sys.stderr)
        return 2
    except GovernanceError as error:
        print(f"业务拒绝：{error}", file=sys.stderr)
        return 1
    except ConflictQuarantine as error:
        print(f"批次隔离：{error.reason}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
