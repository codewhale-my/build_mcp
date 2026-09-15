"""IM（QQ 群 / 单聊）的「滚动摘要」—— 异步、惰性，绝不占回复路径。

为什么要这么设计（成本/延迟实测口径，别改坏）：
  * **同步压缩**会在回答前多一次模型调用（+1~3 秒），而且摘要每轮都在变 →
    固定的系统前缀被打碎 → 前缀缓存全部落空（命中/未命中单价差 30 倍）。
    又慢又贵，所以压缩必须丢到后台。
  * **≤10 条原文直接带**（窗口内只做字符串轻裁剪，见 `main._light_trim`）：
    10 条群消息 ≈ 200~400 token（约 ¥0.0003），
    而为了压它跑一次模型要 1000+ 输入 + 200 输出（≈ ¥0.001~0.002）——
    压比不压还贵。所以只压「被窗口挤出去」的旧消息。

机制：
  note(chat_id, line) 攒行（O(1)，不调模型）→ 后台任务 run_forever 每 60 秒扫一次，
  满足阈值（攒够 MIN_LINES 条，或距上次合成 ≥ MAX_AGE 秒且至少 MIN_BATCH 条）
  才调一次模型：`旧摘要 + 新消息 → 新摘要`，写完落库 + 刷内存缓存。
  注入时只读缓存（纯文本，几十 token）。
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List

from openai import AsyncOpenAI

from build_mcp.client.conversation import (
    LLM_API_KEY,
    LLM_BASE_URL,
    im_model_key,
    resolve_llm_spec,
)
from build_mcp.web import store

logger = logging.getLogger(__name__)

MIN_LINES = 20          # 攒够这么多条才值得压一次
MIN_BATCH = 4           # 时间到点时，至少要有这么多条新消息才压（避免碎压）
MAX_AGE = 300.0         # 距上次合成超过 5 分钟，且攒了 MIN_BATCH 条 → 压
MAX_CHARS = 4000        # 喂给模型的原始行总长上限（token 闸门）
SUMMARY_CHARS = 400     # 摘要目标长度上限（压得狠一点，省 token）

_SYS = (
    "你是聊天记录压缩器。把给定的聊天片段压成简短的中文摘要，只保留："
    "聊过的话题、出现的人名/昵称、给出的结论或决定、未解决的问题。"
    "丢掉寒暄、表情、重复和无意义的话。"
    f"用要点式短句（每行一个要点，行首用「- 」），总长不超过 {SUMMARY_CHARS} 字，"
    "不要解释你在做什么，不要输出摘要以外的东西。"
)


class _ChatState:
    __slots__ = ("summary", "pending", "lines", "last", "loaded", "busy")

    def __init__(self) -> None:
        self.summary: str = ""
        self.pending: List[str] = []
        self.lines: int = 0
        self.last: float = 0.0
        self.loaded: bool = False
        self.busy: bool = False


_STATES: Dict[str, _ChatState] = {}


def _state(chat_id: str) -> _ChatState:
    st = _STATES.get(chat_id)
    if st is None:
        st = _ChatState()
        _STATES[chat_id] = st
    return st


def note(chat_id: str, line: str) -> None:
    """记一行聊天内容（纯内存，O(1)，绝不在回复路径里调模型）。"""
    line = (line or "").strip()
    if not chat_id or not line:
        return
    st = _state(str(chat_id))
    st.pending.append(line[:400])
    if len(st.pending) > 200:                 # 兜底：极端情况别把内存吃满
        del st.pending[:-200]


def get(chat_id: str) -> str:
    """读该会话的摘要（首次会从库里懒加载）。回复路径只用这个。"""
    key = str(chat_id or "")
    if not key:
        return ""
    st = _state(key)
    if not st.loaded:
        try:
            row = store.get_im_summary(key)
            st.summary = str(row.get("summary") or "")
            st.lines = int(row.get("lines") or 0)
            st.last = float(row.get("updated_at") or 0.0)
        except Exception as e:                # noqa: BLE001  读不到摘要不能影响回复
            logger.warning("读取 IM 摘要失败 chat=%s：%s", key, e)
        st.loaded = True
    return st.summary


async def _compress(chat_id: str, st: _ChatState) -> None:
    """把「旧摘要 + 新行」交给模型压成新摘要（后台任务里跑）。"""
    batch = st.pending
    st.pending = []
    st.busy = True
    try:
        spec = resolve_llm_spec(im_model_key())
        client = AsyncOpenAI(api_key=spec.get("api_key") or LLM_API_KEY,
                             base_url=spec.get("base_url") or LLM_BASE_URL,
                             timeout=30.0)
        body = "\n".join(batch)[-MAX_CHARS:]
        user = (f"已有摘要：\n{st.summary}\n\n新增聊天记录：\n{body}\n\n"
                "把「已有摘要 + 新增记录」合并成一份新摘要。")
        r = await client.chat.completions.create(
            model=spec.get("model") or "glm-4-flash",
            messages=[{"role": "system", "content": _SYS},
                      {"role": "user", "content": user}],
            temperature=0.2, max_tokens=400,
        )
        text = (r.choices[0].message.content or "").strip()[:SUMMARY_CHARS * 2]
        if not text:
            raise RuntimeError("模型返回空摘要")
        st.summary = text
        st.lines += len(batch)
        st.last = time.time()
        store.put_im_summary(chat_id, st.summary, st.lines)
        u = getattr(r, "usage", None)
        logger.info("🗜 [im-summary] chat=%s 压缩 %d 条 → 摘要 %d 字（累计 %d 条；tokens in=%s out=%s）",
                    chat_id[-8:], len(batch), len(text), st.lines,
                    getattr(u, "prompt_tokens", "?"), getattr(u, "completion_tokens", "?"))
    except Exception as e:                    # noqa: BLE001  压缩失败不能丢内容
        st.pending = (batch + st.pending)[-200:]     # 放回去，下次再压
        logger.warning("⚠️ [im-summary] 压缩失败 chat=%s：%s", chat_id[-8:], str(e)[:200])
    finally:
        st.busy = False


async def flush(force: bool = False) -> int:
    """扫一遍所有会话，够阈值就压一次。返回本次压缩的会话数。"""
    now = time.time()
    n = 0
    for chat_id, st in list(_STATES.items()):
        if st.busy or not st.pending:
            continue
        due = (len(st.pending) >= MIN_LINES
               or (len(st.pending) >= MIN_BATCH and now - st.last >= MAX_AGE))
        if force and st.pending:
            due = True
        if due:
            await _compress(chat_id, st)
            n += 1
    return n


async def run_forever(interval: float = 60.0) -> None:
    """后台循环：默认每分钟检查一次（压缩花的是后台时间，不占用回复）。"""
    logger.info("🗜 IM 滚动摘要后台任务已启动（攒够 %d 条 或 %d 秒检查一次）",
                MIN_LINES, int(interval))
    while True:
        try:
            await asyncio.sleep(interval)
            await flush()
        except asyncio.CancelledError:
            raise
        except Exception as e:                # noqa: BLE001
            logger.warning("⚠️ [im-summary] 后台循环异常：%s", str(e)[:200])
