"""命令入口。

兼容旧用法（校验单个事件文件）：
    python3 -m src.cli data/sample.json

启动治理服务：
    python3 -m src.cli serve --db data/cohort.db --port 8080
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .envelope import validate_event


def _validate_file(path: str) -> int:
    try:
        record = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"无法读取事件：{error}", file=sys.stderr)
        return 2
    errors = validate_event(record)
    if errors:
        print("；".join(errors), file=sys.stderr)
        return 1
    print(f"事件有效：{record['event_id']}")
    return 0


def _serve(args: argparse.Namespace) -> int:
    from .api import serve
    from .service import CohortService

    service = CohortService.open(args.db)
    try:
        serve(service, host=args.host, port=args.port)
    except KeyboardInterrupt:
        pass
    finally:
        service.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "serve":
        parser = argparse.ArgumentParser(prog="src.cli serve")
        parser.add_argument("--db", default="data/cohort.db")
        parser.add_argument("--host", default="127.0.0.1")
        parser.add_argument("--port", type=int, default=8080)
        return _serve(parser.parse_args(argv[1:]))
    if len(argv) == 1:
        return _validate_file(argv[0])
    print("用法：python3 -m src.cli <事件文件> | python3 -m src.cli serve [--db PATH] [--port N]",
          file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
