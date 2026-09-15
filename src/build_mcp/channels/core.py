"""通道适配层的「通道无关内核」。

依赖注入而不是 import web 层，原因有二：
  1. 可离线自测 —— selftest 用假 transport / 假 fetch，不碰库、不发网络；
  2. 与 web/main.py 解耦 —— 不和另一个 agent 的改动互踩。

关键复用：入站消息只触发「起一个后台 run」（就是断网照跑那套 RUNS），
通道本身只是订阅者。于是 IM 侧天然继承三件事：
  * 掉线 / 切后台 / 刷新 → 生成继续；
  * thinking / tool / answer 处理过程全程可见；
  * 结果落库，回来还能看。
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Optional, Protocol

logger = logging.getLogger(__name__)

MAX_CHUNK = 800        # 单条消息字符上限（IM 侧普遍 ~1000，留余量）
POLL_INTERVAL = 0.6    # 跟随 run 事件的轮询间隔（秒）


# ── 数据模型 ────────────────────────────────────────────────────────────────

@dataclass
class Inbound:
    """一条来自 IM 的入站消息（通道无关）。"""
    channel: str                    # "qq" / "wecom" / "wechat"
    chat_id: str                    # 群 openid / 单聊 openid
    chat_type: str                  # "group" | "c2c"
    user_id: str                    # 发送者在通道内的 id（QQ 是 openid）
    user_name: str = ""
    text: str = ""
    msg_id: str = ""                # 平台消息 id（QQ 被动回复的锚点）
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Outbound:
    """一条要发出去的消息（通道无关）。"""
    chat_id: str
    chat_type: str
    text: str
    reply_to: str = ""              # 被动回复锚点（QQ 需要，企微可空）


class Transport(Protocol):
    """通道实现只需提供 send。"""
    async def send(self, msg: Outbound) -> None: ...


# 注入点：由 web 层在启动时提供
StartRun = Callable[..., Awaitable[str]]           # (user_id=, query=, model=, source=) -> run_id
FetchRun = Callable[[str, int], Awaitable[dict]]   # (run_id, since) -> {"status","answer","events",...}


# ── 文本分片 ────────────────────────────────────────────────────────────────

def split_text(text: str, limit: int = MAX_CHUNK) -> list[str]:
    """按行切分，尽量不把一行劈开；超长单行才硬切。保证不丢字符。"""
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    out: list[str] = []
    buf = ""
    for line in text.splitlines(keepends=True):
        while len(line) > limit:            # 单行本身超限 → 硬切
            if buf:
                out.append(buf)
                buf = ""
            out.append(line[:limit])
            line = line[limit:]
        if len(buf) + len(line) > limit:
            out.append(buf)
            buf = ""
        buf += line
    if buf:
        out.append(buf)
    return out


# ── 会话映射 ────────────────────────────────────────────────────────────────

class SessionMap:
    """(通道, 会话, 用户) → 内部 user_id。

    现在是内存字典（进程重启即失效，等于开新会话）。要持久化，
    只需把 resolve 换成 store 里的一个 upsert 函数，对外接口不变。
    """
    def __init__(self, alloc_base: int = 100000) -> None:
        self._m: Dict[tuple, int] = {}
        self._next = alloc_base

    def resolve(self, channel: str, chat_id: str, user_id: str) -> int:
        key = (channel, chat_id, user_id)
        if key not in self._m:
            self._next += 1
            self._m[key] = self._next
        return self._m[key]

    def __len__(self) -> int:
        return len(self._m)


# ── 跟随 run 事件 ───────────────────────────────────────────────────────────

class RunStream:
    """轮询 run 的增量事件。断线重连 = 带上 since 继续问，天然不丢。"""

    def __init__(self, fetch: FetchRun, interval: float = POLL_INTERVAL) -> None:
        self.fetch = fetch
        self.interval = interval

    async def follow(self, run_id: str, since: int = 0,
                     timeout: float = 1800.0) -> AsyncIterator[dict]:
        deadline = time.monotonic() + timeout
        cursor = since
        while True:
            data = await self.fetch(run_id, cursor)
            for ev in data.get("events") or []:
                cursor = max(cursor, int(ev.get("seq") or 0))
                yield ev
            status = data.get("status")
            if status in ("done", "error"):
                yield {"type": "__final__", "status": status,
                       "answer": data.get("answer") or "",
                       "error": data.get("error") or ""}
                return
            if time.monotonic() > deadline:
                yield {"type": "__final__", "status": "timeout",
                       "answer": data.get("answer") or "", "error": "跟随超时"}
                return
            await asyncio.sleep(self.interval)


# ── 内核 ────────────────────────────────────────────────────────────────────

class ChannelHub:
    """入站 → 起 run → 跟随事件 → 出站。"""

    def __init__(self, transport: Transport, start_run: StartRun, fetch_run: FetchRun,
                 sessions: Optional[SessionMap] = None, *,
                 model: str = "", progress: str = "brief", max_chunk: int = MAX_CHUNK,
                 max_progress: int = 1, max_replies: int = 5, ack: str = ""):
        self.transport = transport
        self.start_run = start_run
        self.stream = RunStream(fetch_run)
        self.sessions = sessions or SessionMap()
        self.model = model
        self.progress = progress          # off / brief（工具开始行）/ full（工具+思考）
        self.max_chunk = max_chunk
        # IM 被动回复有「条数 + 时间窗」双限制（QQ：同一条消息最多 5 条、约 5 分钟）。
        # 所以必须给「一条入站 = 最多发几条」设硬预算，否则工具调用多的时候
        # 进度条会把额度吃光，最后正文发不出去（实测教训）。
        self.max_progress = max(0, int(max_progress))
        self.max_replies = max(1, int(max_replies))
        self.ack = ack                    # 立刻回执（让用户知道收到了），可空

    async def handle(self, msg: Inbound) -> str:
        """处理一条入站消息，返回 run_id（便于测试与日志关联）。"""
        text = (msg.text or "").strip()
        if not text:
            return ""
        uid = self.sessions.resolve(msg.channel, msg.chat_id, msg.user_id)
        run_id = await self.start_run(user_id=uid, query=text, model=self.model,
                                      source=f"{msg.channel}:{msg.chat_type}",
                                      sender=msg.user_id, chat_id=msg.chat_id)
        # 这里必须打【完整】发送者 id：主人白名单是按 openid 登记的，
        # 截断了就没法从日志里取证到底是哪个 openid 在说话。
        logger.info("📥 [%s/%s] sender=%s chat=%s → run=%s (user=%d)",
                    msg.channel, msg.chat_type, msg.user_id, msg.chat_id, run_id, uid)

        sent = 0
        if self.ack:                      # 立刻回执，占 1 条额度
            if await self._send(msg, self.ack):
                sent += 1

        final: Dict[str, Any] = {"status": "running", "answer": ""}
        async for ev in self.stream.follow(run_id):
            if ev.get("type") == "__final__":
                final = ev
                break
            if (ev.get("type") == "tool" and self.progress != "off"
                    and ev.get("status") == "start"
                    and sent < self.max_progress + (1 if self.ack else 0)):
                if await self._send(msg, f"🔧 {ev.get('name')}…"):
                    sent += 1

        answer = (final.get("answer") or "").strip()
        if not answer:
            answer = f"⚠️ 未能完成（{final.get('status')}）{final.get('error') or ''}".strip()
        chunks = split_text(answer, self.max_chunk)
        room = self.max_replies - sent
        if len(chunks) > room:            # 额度不够：截断而不是发不出去
            chunks = chunks[:max(0, room)]
            if chunks:
                chunks[-1] += "\n…（内容过长，已截断）"
        if not chunks:
            logger.warning("⚠️ 回复额度已用尽，正文未发出（run=%s）", run_id)
        for chunk in chunks:
            if not await self._send(msg, chunk):
                break                     # 失败就不再往下发，避免刷屏报错
        return run_id

    async def _send(self, msg: Inbound, text: str) -> bool:
        """发一条；失败返回 False（QQ 被动回复窗口过期 / 无权限时会失败）。"""
        try:
            await self.transport.send(Outbound(chat_id=msg.chat_id, chat_type=msg.chat_type,
                                               text=text, reply_to=msg.msg_id))
            return True
        except Exception as e:            # noqa: BLE001
            logger.warning("⚠️ 出站失败[%s/%s]：%s", msg.channel, msg.chat_type, str(e)[:160])
            return False
