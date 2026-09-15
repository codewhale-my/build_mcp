"""通道层离线自测：不连网、不碰库，验证纯逻辑。

跑法：
    PYTHONPATH=src .venv/bin/python -m build_mcp.channels.selftest

httpx.MockTransport 让「取 token / 发消息 / 企微推送」的参数与请求头
都能被逐字节断言，不需要真的 AppID。
"""
from __future__ import annotations

import asyncio
import inspect
import json
import sys
import traceback
from typing import Any, Dict, List

import httpx

from .core import (ChannelHub, Inbound, Outbound, SessionMap,
                   allmsg_chance_hit, allmsg_should_reply, split_text)
from .qq_official import (API_BASE, SANDBOX_API_BASE, TOKEN_URL, QQConfig,
                          QQTransport, parse_dispatch, read_owner_ids)
from .wecom import WeComWebhookTransport

CASES: List[Any] = []


def case(fn):                                       # noqa: ANN001
    CASES.append(fn)
    return fn


def _mock(handler):                                 # noqa: ANN001
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ── 1. 分片 ────────────────────────────────────────────────────────────────

@case
def test_split_short():
    assert split_text("你好") == ["你好"]
    assert split_text("   ") == []
    assert split_text("") == []


@case
def test_split_long_no_loss():
    text = "".join(f"第{i}行内容\n" for i in range(300))     # 约 2400 字
    text = text.rstrip("\n")
    chunks = split_text(text, 800)
    assert len(chunks) >= 3, f"应切成多片，实际 {len(chunks)}"
    assert all(len(c) <= 800 for c in chunks), [len(c) for c in chunks]
    assert "".join(chunks) == text, "分片不能丢字符"


@case
def test_split_single_huge_line():
    text = "x" * 2500
    chunks = split_text(text, 800)
    assert [len(c) for c in chunks] == [800, 800, 800, 100]
    assert "".join(chunks) == text


# ── 2. 会话映射 ────────────────────────────────────────────────────────────

@case
def test_session_map_stable():
    sm = SessionMap(alloc_base=100000)
    a = sm.resolve("qq", "G1", "U1")
    b = sm.resolve("qq", "G1", "U1")
    c = sm.resolve("qq", "G1", "U2")
    d = sm.resolve("qq", "G2", "U1")
    assert a == b and a != c and a != d
    assert a == 100001 and len(sm) == 3


# ── 3. 事件解析 ────────────────────────────────────────────────────────────

@case
def test_parse_group_at():
    d = {"id": "MSG1", "group_openid": "G1", "content": " 帮我查天气 ",
         "author": {"member_openid": "U1"}}
    ib = parse_dispatch("GROUP_AT_MESSAGE_CREATE", d)
    assert ib and ib.chat_type == "group" and ib.chat_id == "G1"
    assert ib.user_id == "U1" and ib.msg_id == "MSG1" and ib.text == "帮我查天气"


@case
def test_parse_c2c():
    d = {"id": "M2", "content": "在吗", "author": {"user_openid": "U9"}}
    ib = parse_dispatch("C2C_MESSAGE_CREATE", d)
    assert ib and ib.chat_type == "c2c" and ib.chat_id == "U9" and ib.text == "在吗"


@case
def test_parse_ignores_other_events():
    assert parse_dispatch("GROUP_ADD_ROBOT", {"group_openid": "G1"}) is None
    assert parse_dispatch("READY", {"session_id": "s"}) is None


# ── 4. 内核端到端（假 transport + 假 fetch）────────────────────────────────

class FakeTransport:
    def __init__(self) -> None:
        self.sent: List[Outbound] = []

    async def send(self, msg: Outbound) -> None:
        self.sent.append(msg)


@case
async def test_hub_end_to_end():
    answer = "".join(f"第{i}行内容\n" for i in range(300)).rstrip("\n")
    events = [{"seq": 1, "type": "tool", "name": "search", "status": "start"},
              {"seq": 2, "type": "thinking", "delta": "嗯"},
              {"seq": 3, "type": "answer", "delta": "ok"}]
    calls = {"n": 0}

    async def fake_fetch(run_id: str, since: int) -> dict:
        calls["n"] += 1
        if calls["n"] == 1:
            return {"status": "running", "answer": "", "events": events}
        return {"status": "done", "answer": answer, "events": []}

    started: Dict[str, Any] = {}

    async def fake_start(**kw) -> str:
        started.update(kw)
        return "run-abc"

    tr = FakeTransport()
    hub = ChannelHub(tr, fake_start, fake_fetch, progress="brief", max_chunk=800)
    hub.stream.interval = 0.01                       # 别让测试等 0.6s

    run_id = await hub.handle(Inbound(channel="qq", chat_type="group", chat_id="G1",
                                      user_id="U1", text="@机器人 你好", msg_id="MSG1"))
    assert run_id == "run-abc"
    assert started["user_id"] == 100001 and started["source"] == "qq:group"
    assert started["query"] == "@机器人 你好"

    assert tr.sent[0].text == "🔧 search…", tr.sent[0].text      # 过程回显
    assert tr.sent[0].reply_to == "MSG1" and tr.sent[0].chat_id == "G1"
    # 精确断言片数（别写「>=N」那种拍脑袋的期望值：2289 字 / 800 = 3 片）
    expected = split_text(answer, 800)
    assert len(expected) == 3, f"本用例应切 3 片，实际 {len(expected)}"
    assert [m.text for m in tr.sent[1:]] == expected, "分片内容必须与答案逐片一致"
    assert "".join(m.text for m in tr.sent[1:]) == answer, "最终答案必须完整发回"
    assert len(tr.sent) == 1 + len(expected)


@case
async def test_hub_empty_answer_reports():
    async def fake_fetch(run_id: str, since: int) -> dict:
        return {"status": "error", "answer": "", "events": [], "error": "模型超时"}

    async def fake_start(**kw) -> str:
        return "run-x"

    tr = FakeTransport()
    hub = ChannelHub(tr, fake_start, fake_fetch)
    hub.stream.interval = 0.01
    await hub.handle(Inbound(channel="qq", chat_type="c2c", chat_id="U9",
                             user_id="U9", text="hi"))
    assert len(tr.sent) == 1 and "⚠️" in tr.sent[0].text


@case
async def test_hub_ignores_empty_text():
    async def fake_start(**kw) -> str:
        raise AssertionError("空消息不该起 run")

    tr = FakeTransport()
    hub = ChannelHub(tr, fake_start, None)           # type: ignore[arg-type]
    assert await hub.handle(Inbound(channel="qq", chat_type="group",
                                    chat_id="G1", user_id="U1", text="   ")) == ""
    assert tr.sent == []


# ── 5. QQ 出站（用 MockTransport 断言请求本身）─────────────────────────────

@case
async def test_qq_transport_token_and_send():
    calls: List[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        if req.url.host == "bots.qq.com":
            return httpx.Response(200, json={"access_token": "tok123", "expires_in": 7200})
        return httpx.Response(200, json={"id": "SENT1", "timestamp": 1})

    tr = QQTransport(QQConfig(appid="A1", secret="S1"), client=_mock(handler))
    await tr.send(Outbound(chat_id="G1", chat_type="group", text="你好", reply_to="M1"))

    assert str(calls[0].url) == TOKEN_URL
    assert json.loads(calls[0].content) == {"appId": "A1", "clientSecret": "S1"}

    assert str(calls[1].url) == f"{API_BASE}/v2/groups/G1/messages"
    assert calls[1].headers["authorization"] == "QQBot tok123"
    # 被动回复必须带 msg_seq（同一条入站消息第 1 条回复 = 1）；
    # 少了它腾讯按「重复消息」拒收：400 code=40054005（2026-09-14 线上实锤）。
    assert json.loads(calls[1].content) == {
        "content": "你好", "msg_type": 0, "msg_id": "M1", "msg_seq": 1}

    # 第二次发送必须复用 token（不再打 bots.qq.com），且 msg_seq 递增为 2
    await tr.send(Outbound(chat_id="G1", chat_type="group", text="再来", reply_to="M1"))
    assert sum(1 for r in calls if r.url.host == "bots.qq.com") == 1
    assert str(calls[2].url) == f"{API_BASE}/v2/groups/G1/messages"
    assert json.loads(calls[2].content) == {
        "content": "再来", "msg_type": 0, "msg_id": "M1", "msg_seq": 2}

    # 换一条入站消息：msg_seq 从 1 重新开始（锚点是 msg_id）
    await tr.send(Outbound(chat_id="G1", chat_type="group", text="另一条", reply_to="M2"))
    assert json.loads(calls[3].content)["msg_seq"] == 1
    await tr.aclose()


@case
async def test_owner_allowlist_parse():
    """主人白名单解析：文件 + 环境变量合并，注释/空行/重复/多列都要容错。"""
    txt = "# 主人列表\nOPENID_A  # 群里的主人\n\nOPENID_B\nOPENID_A\n"
    assert read_owner_ids(txt) == ["OPENID_A", "OPENID_B"]
    assert read_owner_ids(txt, "X1, X2") == ["OPENID_A", "OPENID_B", "X1", "X2"]
    assert read_owner_ids("", "") == []
    assert read_owner_ids("A A\nB", "B,C") == ["A", "B", "C"]
    assert read_owner_ids("   \n# 全是注释\n", "  ") == []


@case
async def test_qq_transport_c2c_and_sandbox():
    calls: List[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        if req.url.host == "bots.qq.com":
            return httpx.Response(200, json={"access_token": "t", "expires_in": 100})
        return httpx.Response(200, json={"id": "S"})

    cfg = QQConfig(appid="A", secret="S", sandbox=True)
    assert cfg.api_base == SANDBOX_API_BASE
    tr = QQTransport(cfg, client=_mock(handler))
    await tr.send(Outbound(chat_id="U1", chat_type="c2c", text="hi"))   # 无 msg_id
    assert str(calls[1].url) == f"{SANDBOX_API_BASE}/v2/users/U1/messages"
    assert json.loads(calls[1].content) == {"content": "hi", "msg_type": 0}
    await tr.aclose()


# ── 6. 企微推送 ────────────────────────────────────────────────────────────

@case
async def test_wecom_payload():
    calls: List[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        return httpx.Response(200, json={"errcode": 0, "errmsg": "ok"})

    tr = WeComWebhookTransport("KEY123", client=_mock(handler))
    await tr.send(Outbound(chat_id="", chat_type="group", text="日报：全部正常"))
    assert calls[0].url.params["key"] == "KEY123"
    assert json.loads(calls[0].content) == {"msgtype": "text",
                                            "text": {"content": "日报：全部正常"}}
    await tr.aclose()


# ── 跑 ─────────────────────────────────────────────────────────────────────

@case
async def test_hub_ack_fn_by_identity():
    """立刻回执必须按发送者身份走 ack_fn（主人/访客文案不同），且失败要能回退。"""
    async def fake_fetch(run_id: str, since: int) -> dict:
        return {"status": "done", "answer": "正文", "events": []}

    async def fake_start(**kw) -> str:
        return "run-t"

    pick = lambda m: "主人版" if m.user_id == "OWNER1" else "访客版"   # noqa: E731

    tr = FakeTransport()
    hub = ChannelHub(tr, fake_start, fake_fetch, ack="固定文案", progress="off",
                     max_progress=0, ack_fn=pick)
    hub.stream.interval = 0.01
    await hub.handle(Inbound(channel="qq", chat_type="group", chat_id="G1",
                             user_id="OWNER1", text="hi", msg_id="M1"))
    assert tr.sent[0].text == "主人版", tr.sent[0].text

    tr2 = FakeTransport()
    hub2 = ChannelHub(tr2, fake_start, fake_fetch, ack="固定文案", progress="off",
                      max_progress=0, ack_fn=pick)
    hub2.stream.interval = 0.01
    await hub2.handle(Inbound(channel="qq", chat_type="c2c", chat_id="U9",
                              user_id="U9", text="hi", msg_id="M2"))
    assert tr2.sent[0].text == "访客版", tr2.sent[0].text

    # ack_fn 抛异常必须回退固定文案，绝不能把整条消息搞挂
    tr3 = FakeTransport()
    hub3 = ChannelHub(tr3, fake_start, fake_fetch, ack="固定文案", progress="off",
                      max_progress=0, ack_fn=lambda m: 1 / 0)
    hub3.stream.interval = 0.01
    await hub3.handle(Inbound(channel="qq", chat_type="group", chat_id="G1",
                              user_id="U1", text="hi", msg_id="M3"))
    assert tr3.sent[0].text == "固定文案", tr3.sent[0].text


@case
def test_parse_group_all_message():
    """全量群消息（不 @）：与 @ 消息同构，但要带 event 与发送者昵称。"""
    ib = parse_dispatch("GROUP_MESSAGE_CREATE", {
        "id": "M-ALL-1", "content": "  大家早上好  ", "group_openid": "G-ALL",
        "author": {"member_openid": "U-ALL", "username": "小明", "member_role": "member"},
    })
    assert ib is not None
    assert (ib.chat_type, ib.chat_id, ib.user_id) == ("group", "G-ALL", "U-ALL")
    assert ib.text == "大家早上好" and ib.msg_id == "M-ALL-1"
    assert ib.event == "GROUP_MESSAGE_CREATE" and ib.user_name == "小明"
    # 出站仍走群接口：chat_type 必须还是 "group"（改成别的会把 URL 拼坏）
    assert ib.chat_type == "group"
    # @ 事件也要带 event（日志取证 / 分流都靠它）
    ib2 = parse_dispatch("GROUP_AT_MESSAGE_CREATE",
                         {"id": "M1", "content": "hi", "group_openid": "G1",
                          "author": {"member_openid": "U1"}})
    assert ib2.event == "GROUP_AT_MESSAGE_CREATE" and ib2.user_id == "U1"
    # 无关事件依旧返回 None
    assert parse_dispatch("GROUP_ADD_ROBOT", {"group_openid": "G1"}) is None


@case
def test_allmsg_should_reply():
    """插话规则：keyword / owner / question / any / off 五态，任一命中即插。"""
    K = dict(keywords=["机器人", "小助手"])
    assert allmsg_should_reply("叫一下机器人", rules=["keyword"], **K) is True
    assert allmsg_should_reply("今天天气不错", rules=["keyword"], **K) is False
    assert allmsg_should_reply("在吗？", rules=["question"]) is True
    assert allmsg_should_reply("这个多少钱", rules=["question"]) is True
    assert allmsg_should_reply("我去吃饭了", rules=["question"]) is False
    # 「什么」故意不算疑问词："没什么事"这种陈述句太常见，算进去会明显变吵
    assert allmsg_should_reply("今天吃什么", rules=["question"]) is False
    assert allmsg_should_reply("随便说点", rules=["owner"], is_owner=False) is False
    assert allmsg_should_reply("随便说点", rules=["owner"], is_owner=True) is True
    assert allmsg_should_reply("", rules=["any"]) is False
    assert allmsg_should_reply("啥都行", rules=["off"]) is False
    assert allmsg_should_reply("啥都行", rules=["any"]) is True
    assert allmsg_should_reply("随便", rules=["keyword", "question"], **K) is False
    assert allmsg_should_reply("随便?", rules=["keyword", "question"], **K) is True
    assert allmsg_should_reply("x", rules=None) is False      # 默认 keyword、且无关键词


@case
async def test_hub_prepare_fn_gate():
    """prepare_fn 是最后一道闸门：None 不起 run / 不发消息；字符串则替换 query。"""
    started: List[Dict[str, Any]] = []

    async def fake_start(**kw) -> str:
        started.append(kw)
        return "run-p"

    async def fake_fetch(run_id: str, since: int) -> dict:
        return {"status": "done", "answer": "好", "events": []}

    tr = FakeTransport()
    hub = ChannelHub(tr, fake_start, fake_fetch, ack="", progress="off", max_progress=0,
                     prepare_fn=lambda m: None)
    hub.stream.interval = 0.01
    rid = await hub.handle(Inbound(channel="qq", chat_type="group", chat_id="G1",
                                   user_id="U1", text="闲聊一句",
                                   event="GROUP_MESSAGE_CREATE"))
    assert rid == "" and started == [] and tr.sent == [], "被闸门拦下就不该起 run、不该发消息"

    tr2 = FakeTransport()
    hub2 = ChannelHub(tr2, fake_start, fake_fetch, ack="", progress="off", max_progress=0,
                      prepare_fn=lambda m: f"[群聊上下文]{m.text}")
    hub2.stream.interval = 0.01
    await hub2.handle(Inbound(channel="qq", chat_type="group", chat_id="G1",
                              user_id="U1", text="正文", event="GROUP_MESSAGE_CREATE"))
    assert started and started[-1]["query"] == "[群聊上下文]正文", started
    assert [m.text for m in tr2.sent] == ["好"]
    # event 必须透传给 start_run：插话要据此换语气、换模型
    assert started[-1]["event"] == "GROUP_MESSAGE_CREATE", started[-1].get("event")

    # 闸门自己抛异常时必须放行（不能因为写错配置把消息全吞了）
    tr3 = FakeTransport()
    hub3 = ChannelHub(tr3, fake_start, fake_fetch, ack="", progress="off", max_progress=0,
                      prepare_fn=lambda m: 1 / 0)
    hub3.stream.interval = 0.01
    await hub3.handle(Inbound(channel="qq", chat_type="group", chat_id="G1",
                              user_id="U1", text="照样要跑", event="GROUP_MESSAGE_CREATE"))
    assert started[-1]["query"] == "照样要跑"


@case
def test_allmsg_chance_hit():
    """概率闸门：0 永不、1 必中、边界严格用 <（rnd=chance 不算中）。"""
    assert allmsg_chance_hit(0, 0.0) is False
    assert allmsg_chance_hit(0.0, 0.99) is False
    assert allmsg_chance_hit(1, 0.99) is True
    assert allmsg_chance_hit(1.0, 0.0) is True
    assert allmsg_chance_hit(0.1, 0.05) is True
    assert allmsg_chance_hit(0.1, 0.1) is False        # 边界：不算中
    assert allmsg_chance_hit(0.1, 0.999) is False
    assert allmsg_chance_hit("0.5", 0.4) is True       # 配置里是字符串也要认
    assert allmsg_chance_hit("", 0.0) is False
    assert allmsg_chance_hit(None, 0.0) is False
    # 1000 次 1/10 抽样应落在 6%~14%（防"概率写反"这类低级错）
    hits = sum(1 for i in range(1000) if allmsg_chance_hit(0.1, (i % 100) / 100))
    assert 60 <= hits <= 140, hits


def run_all() -> int:
    # 自测跑在仓库里，会顺带初始化 build_mcp 的日志（往 ./log/ 写文件）。
    # 把第三方 INFO 压掉，免得断言输出被 httpx 请求日志淹了。
    import logging
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)
    ok = fail = 0
    for fn in CASES:
        try:
            if inspect.iscoroutinefunction(fn):
                asyncio.run(fn())
            else:
                fn()
            print(f"  PASS  {fn.__name__}")
            ok += 1
        except Exception as e:                        # noqa: BLE001
            traceback.print_exc()
            print(f"  FAIL  {fn.__name__}: {e}")
            fail += 1
    print(f"\n结果：{ok} 通过 / {fail} 失败 / 共 {len(CASES)}")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(run_all())
