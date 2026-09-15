"""QQ 官方机器人（QQ 开放平台）—— WebSocket 长连接适配器。

为什么走这条路（而不是 NapCat / LLOneBot 这类协议端）：
  * **不需要域名和备案**：长连接是「出网」的。webhook 模式要求 HTTPS 回调地址，
    且官方明确「回调地址不能是 IP、域名需完成备案」——我们手上只有 IP 证书。
  * **内存**：只多一条 WS 连接（几 MB）。协议端要无头跑 NTQQ 客户端（Electron 量级），
    这台 1.6G 的机器扛不住（前车之鉴：龙虾）。
  * **封号风险**：官方接口没有协议端的封号风险。

协议要点（逐条对照官方文档 bot.q.qq.com/wiki）：
  * 鉴权头：Authorization: QQBot <access_token>
  * 取 token：POST https://bots.qq.com/app/getAppAccessToken  {appId, clientSecret}
  * 取网关：GET  https://api.sgroup.qq.com/gateway（沙箱换成 sandbox.api.sgroup.qq.com）
  * OpCode：10 Hello(带 heartbeat_interval) → 2 Identify → 0 Dispatch
            1 心跳 / 11 ACK / 7 Reconnect / 6 Resume / 9 Invalid Session
  * intents：1<<25 = GROUP_AND_C2C_EVENT（群@消息 + 单聊 + 被拉入群等）
  * 发消息：POST /v2/groups/{group_openid}/messages  {content, msg_type:0, msg_id}
            msg_id = 被动回复锚点，有窗口期与条数限制（分片别太多）

待用真实 AppID 实测的点（文档里这么写，但我无法离线验证）：
  * 请求体字段名 clientSecret 是否等同于「AppSecret」；
  * 被动回复的窗口期 / 最大条数，够不够分片发送；
  * 群的 content 里 @机器人 前缀是否已被平台剥离。
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional

import httpx
import websockets

from .core import Inbound, Outbound

logger = logging.getLogger(__name__)

INTENT_GROUP_AND_C2C = 1 << 25
TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
API_BASE = "https://api.sgroup.qq.com"
SANDBOX_API_BASE = "https://sandbox.api.sgroup.qq.com"

# 主人白名单文件：一行一个 openid（`#` 后可写说明，空格分列）。
# ⚠️ QQ 官方接口出于隐私【不返回 QQ 号】，只给 openid —— 所以「谁是主人」只能按
# openid 登记。openid 在「同一机器人 + 同一场景」下稳定不变，但**跨场景不同**：
# 同一人的「某群 member_openid」和「单聊 user_openid」是两个值，需要分别登记。
OWNER_FILE = "/home/admin/.secrets/qq_owners.txt"


def read_owner_ids(text: str, extra: str = "") -> List[str]:
    """解析主人 openid：文件内容 + 环境变量（逗号分隔）。纯函数，可离线测。"""
    out: List[str] = []
    for ln in (text or "").splitlines():
        ln = ln.split("#", 1)[0].strip()
        if not ln:
            continue
        oid = ln.split()[0].strip()
        if oid and oid not in out:
            out.append(oid)
    for oid in (extra or "").replace(" ", "").split(","):
        oid = oid.strip()
        if oid and oid not in out:
            out.append(oid)
    return out


@dataclass
class QQConfig:
    appid: str
    secret: str
    sandbox: bool = False

    @property
    def api_base(self) -> str:
        return SANDBOX_API_BASE if self.sandbox else API_BASE


# ── 事件 → Inbound（纯函数，可离线测）────────────────────────────────────────

def parse_dispatch(t: str, d: Dict[str, Any]) -> Optional[Inbound]:
    """把 op=0 的 Dispatch 事件转成通道无关的 Inbound；无关事件返回 None。"""
    if t in ("GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"):
        # 前者 = 群里 @机器人；后者 = 群消息·全量模式（群主开了「获取群内全部消息」
        # 后，群里每一条不 @ 机器人的消息也会推过来）。两者字段完全一致，
        # 差别只在 event 名：出站都用同一个群接口，被动回复都是用同一条 id。
        author = d.get("author") or {}
        return Inbound(
            channel="qq", chat_type="group",
            chat_id=str(d.get("group_openid") or ""),
            user_id=str(author.get("member_openid") or ""),
            user_name=str(author.get("username") or ""),
            text=(d.get("content") or "").strip(),
            msg_id=str(d.get("id") or ""), raw=d, event=t,
        )
    if t == "C2C_MESSAGE_CREATE":               # 单聊
        author = d.get("author") or {}
        return Inbound(
            channel="qq", chat_type="c2c",
            chat_id=str(author.get("user_openid") or ""),
            user_id=str(author.get("user_openid") or ""),
            user_name=str(author.get("username") or ""),
            text=(d.get("content") or "").strip(),
            msg_id=str(d.get("id") or ""), raw=d, event=t,
        )
    return None


# ── 出站 ────────────────────────────────────────────────────────────────────

class QQTransport:
    """负责取 access_token 和发消息。"""

    def __init__(self, cfg: QQConfig, client: Optional[httpx.AsyncClient] = None) -> None:
        self.cfg = cfg
        self._client = client or httpx.AsyncClient(timeout=15)
        self._token = ""
        self._exp = 0.0
        self._msg_seq: Dict[str, int] = {}   # msg_id → 已发送条数（防去重拒收）

    async def token(self) -> str:
        """access_token 有有效期，提前 60 秒续期。"""
        if self._token and time.time() < self._exp - 60:
            return self._token
        r = await self._client.post(TOKEN_URL, json={
            "appId": self.cfg.appid, "clientSecret": self.cfg.secret})
        r.raise_for_status()
        j = r.json()
        self._token = j["access_token"]
        self._exp = time.time() + float(j.get("expires_in") or 7200)
        return self._token

    def _url(self, msg: Outbound) -> str:
        if msg.chat_type == "group":
            return f"{self.cfg.api_base}/v2/groups/{msg.chat_id}/messages"
        return f"{self.cfg.api_base}/v2/users/{msg.chat_id}/messages"

    async def send(self, msg: Outbound) -> None:
        tok = await self.token()
        body: Dict[str, Any] = {"content": msg.text, "msg_type": 0}
        if msg.reply_to:
            body["msg_id"] = msg.reply_to       # 被动回复
            # 同一条入站消息要回多条（ack + 分片正文）：第 2 条起必须递增 msg_seq，
            # 否则腾讯按「重复消息」拒收（400 code=40054005 消息被去重，请检查请求msgseq）。
            seq = self._msg_seq.get(msg.reply_to, 0) + 1
            if len(self._msg_seq) > 512:        # 锚点 map 防无界增长
                self._msg_seq.clear()
            self._msg_seq[msg.reply_to] = seq
            body["msg_seq"] = seq
        r = await self._client.post(self._url(msg), json=body,
                                    headers={"Authorization": f"QQBot {tok}"})
        if r.status_code >= 300:
            logger.warning("QQ 发送失败 %s：%s", r.status_code, r.text[:300])
        r.raise_for_status()

    async def aclose(self) -> None:
        await self._client.aclose()


# ── 网关长连接 ──────────────────────────────────────────────────────────────

class Reconnect(RuntimeError):
    """服务端要求重连（op=7 被顶号 / op=9 Invalid Session）：应尽快重连，不走长退避。"""


class QQGateway:
    """维护与 QQ 网关的 WS 长连接，把事件交给 on_inbound。"""

    def __init__(self, cfg: QQConfig,
                 on_inbound: Callable[[Inbound], Awaitable[None]],
                 transport: Optional[QQTransport] = None) -> None:
        self.cfg = cfg
        self.transport = transport or QQTransport(cfg)
        self.on_inbound = on_inbound
        self._session_id = ""
        self._seq = 0

    async def gateway_url(self) -> str:
        tok = await self.transport.token()
        r = await self.transport._client.get(
            f"{self.cfg.api_base}/gateway",
            headers={"Authorization": f"QQBot {tok}"})
        r.raise_for_status()
        return r.json()["url"]

    async def run_forever(self) -> None:
        backoff = 1
        while True:
            try:
                await self._session()
                backoff = 1                      # 正常结束也重置
            except asyncio.CancelledError:
                raise
            except Reconnect as e:
                # 被顶号 / Invalid Session：属于「服务端让我重连」，应当尽快回来，
                # 不能沿用指数退避（否则一次抖动就退到 60s，消息尽数落在断窗里丢掉）。
                logger.warning("QQ 网关需重连：%s；2s 后重连", e)
                await asyncio.sleep(2)
                backoff = 1
            except Exception as e:               # noqa: BLE001 —— 网关必须永不退出
                logger.warning("QQ 网关断开：%s；%ds 后重连", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _session(self) -> None:
        url = await self.gateway_url()
        async with websockets.connect(url, max_size=2 ** 22) as ws:
            hello = json.loads(await ws.recv())
            hb = float((hello.get("d") or {}).get("heartbeat_interval") or 40000) / 1000.0
            tok = await self.transport.token()
            if self._session_id:                 # 有会话优先 Resume，尽量不丢事件
                payload = {"op": 6, "d": {"token": f"QQBot {tok}",
                                          "session_id": self._session_id, "seq": self._seq}}
            else:
                payload = {"op": 2, "d": {
                    "token": f"QQBot {tok}",
                    "intents": INTENT_GROUP_AND_C2C,
                    "shard": [0, 1],
                    "properties": {"$os": "linux", "$browser": "build-mcp",
                                   "$device": "build-mcp"},
                }}
            await ws.send(json.dumps(payload))
            hb_task = asyncio.create_task(self._heartbeat(ws, hb))
            try:
                async for raw in ws:
                    await self._on_frame(json.loads(raw))
            finally:
                hb_task.cancel()

    async def _heartbeat(self, ws, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            await ws.send(json.dumps({"op": 1, "d": self._seq}))

    async def _on_frame(self, p: Dict[str, Any]) -> None:
        op = p.get("op")
        if p.get("s"):
            self._seq = p["s"]
        if op == 0:                              # Dispatch
            t = p.get("t") or ""
            d = p.get("d") or {}
            if t == "READY":
                self._session_id = d.get("session_id") or self._session_id
                logger.info("QQ 网关就绪 session=%s", (self._session_id or "")[:8])
            elif t in ("GROUP_ADD_ROBOT", "GROUP_DEL_ROBOT"):
                logger.info("机器人%s群：%s",
                            "被拉入" if t == "GROUP_ADD_ROBOT" else "被移出",
                            d.get("group_openid"))
            ib = parse_dispatch(t, d)
            if ib:
                await self.on_inbound(ib)
        elif op == 7:
            raise Reconnect("服务端要求重连(op=7)——通常是同 AppID 的另一条连接顶号")
        elif op == 9:
            self._session_id = ""
            self._seq = 0
            raise Reconnect("Invalid Session(op=9)，将重新 Identify")

    async def aclose(self) -> None:
        await self.transport.aclose()
