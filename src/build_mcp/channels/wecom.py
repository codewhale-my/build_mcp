"""企业微信「群机器人」webhook —— 单向推送适配器（零门槛，今天就能用）。

为什么先给这个：
  * **个人微信**没有可用官方 API；Linux 无头跑不了客户端 hook（那是 Windows-only）；
    协议端 = 封号风险，且要真机跑客户端。
  * **能收能发**的两条路（企微自建应用 / 公众号）都卡在**备案域名**：
    企微「可信域名」明确不支持 IP，且要求 ICP 备案、备案主体要与企业一致；
    公众号的「消息接收 URL」虽然能填 IP，但前提同样是**该 IP 已完成备案**。
  * 群机器人 webhook 只需要一个 key，主动 POST 即可 —— 不需要回调、不需要域名、不需要备案。

代价说清楚：**只能推、不能收**（它没有回调能力）。要让 IM 那边能下发指令，
必须先有备案域名，那是另一条路。
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

from .core import Outbound

logger = logging.getLogger(__name__)

WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send"


class WeComWebhookTransport:
    """把 Outbound 发到企微群机器人。只支持 text。"""

    def __init__(self, key: str, client: Optional[httpx.AsyncClient] = None) -> None:
        self.key = key
        self._client = client or httpx.AsyncClient(timeout=15)

    async def send(self, msg: Outbound) -> None:
        if not (msg.text or "").strip():
            return
        r = await self._client.post(
            WEBHOOK, params={"key": self.key},
            json={"msgtype": "text", "text": {"content": msg.text}})
        r.raise_for_status()
        j = r.json()
        if j.get("errcode"):
            logger.warning("企微推送失败：%s", j)

    async def aclose(self) -> None:
        await self._client.aclose()
