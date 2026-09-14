"""独立入口：先验证「能不能连上 QQ 网关、能不能收到群消息」。

用法（拿到 AppID / AppSecret 之后，**先在沙箱**验证）：
    QQ_APPID=xxx QQ_SECRET=yyy PYTHONPATH=src .venv/bin/python -m build_mcp.channels.cli --sandbox

这一步只打印收到的事件，**不接入 agent**，所以零风险。
接入 agent 是下一步：把 web.main 里那段起 run 的逻辑抽成可注入的函数，
再把它传给 ChannelHub.start_run 即可（通道层已经留好了注入口）。
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

from .qq_official import QQConfig, QQGateway


async def _main(cfg: QQConfig) -> None:
    async def on_inbound(ib):                      # noqa: ANN001
        print(f"\n[IN] {ib.chat_type}  chat={ib.chat_id}  user={ib.user_id}\n"
              f"     msg_id={ib.msg_id}\n     text={ib.text!r}", flush=True)

    gw = QQGateway(cfg, on_inbound)
    print(f"[*] 连接 QQ 网关（{'沙箱' if cfg.sandbox else '正式'}）pid={os.getpid()}，Ctrl-C 退出",
          flush=True)
    try:
        await gw.run_forever()
    finally:
        await gw.aclose()


def main() -> None:
    p = argparse.ArgumentParser(description="QQ 官方机器人连通性自检（只收不发）")
    p.add_argument("--appid", default=os.environ.get("QQ_APPID", ""))
    p.add_argument("--secret", default=os.environ.get("QQ_SECRET", ""))
    p.add_argument("--sandbox", action="store_true", help="使用沙箱网关")
    a = p.parse_args()
    if not a.appid or not a.secret:
        print("需要 --appid/--secret，或环境变量 QQ_APPID / QQ_SECRET", file=sys.stderr)
        sys.exit(2)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(_main(QQConfig(a.appid, a.secret, a.sandbox)))
    except KeyboardInterrupt:
        print("\n[*] 已退出")


if __name__ == "__main__":
    main()
