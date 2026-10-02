"""命令行入口。

* ``python -m cr serve --data ./data [--port 8080] [--demo]``
    启动 HTTP API 与节点进度页面；``--demo`` 启动时自动跑完整故事线。
* ``python -m cr demo --data ./data``
    在终端运行完整演示并打印恢复验证报告（Markdown）。
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys

from .cluster import Cluster
from .demo import run_full_demo
from .web import serve


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cr",
        description="高性能计算作业的抢占与检查点恢复服务",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_serve = sub.add_parser("serve", help="启动 HTTP API 与节点进度页面")
    p_serve.add_argument("--data", default="./data", help="持久化目录")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8080)
    p_serve.add_argument("--reset", action="store_true", help="启动前清空数据")
    p_serve.add_argument("--demo", action="store_true",
                         help="启动后自动执行完整演示故事线")

    p_demo = sub.add_parser("demo", help="在终端运行完整演示并输出报告")
    p_demo.add_argument("--data", default="./data-demo")
    p_demo.add_argument("--reset", action="store_true",
                        help="运行前清空数据目录")
    p_demo.add_argument("--job", default="sim-001")
    p_demo.add_argument("--quiet", action="store_true", help="只打印报告")

    args = parser.parse_args(argv)

    if getattr(args, "reset", False) and os.path.isdir(args.data):
        shutil.rmtree(args.data)

    if args.cmd == "serve":
        httpd, cluster = serve(args.data, host=args.host, port=args.port)
        print(f"✔ 服务已启动: http://{args.host}:{args.port}/")
        print(f"  数据目录: {os.path.abspath(args.data)}")
        if args.demo:
            result = run_full_demo(
                cluster, job_id="sim-001",
                reports_dir=os.path.join(args.data, "reports"),
            )
            print(f"✔ 自动演示完成，总体结论: {result['overall']}")
        try:
            while True:
                __import__("time").sleep(3600)
        except KeyboardInterrupt:
            httpd.shutdown()
        return 0

    if args.cmd == "demo":
        cluster = Cluster(args.data)
        result = run_full_demo(
            cluster, job_id=args.job,
            reports_dir=os.path.join(args.data, "reports"),
        )
        if not args.quiet:
            print("\n==================== 演示时间线 ====================")
            for item in result["timeline"]:
                print(f"[{item['stage']}] {item['title']}")
        print("\n" + result["markdown"])
        path = result["report_paths"].get("markdown")
        print(f"报告已写入: {path}")
        return 0 if result["overall"] == "PASS" else 1

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
