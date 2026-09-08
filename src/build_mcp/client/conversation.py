import asyncio
import json
import os
from typing import List, Dict, Any
from contextlib import AsyncExitStack
from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory

from openai import AsyncOpenAI
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.types import Tool

from build_mcp.common.config import load_config

# ====================== 配置区 ======================
config = load_config("config.yaml")
LLM_BASE_URL = config["llm_base_url"]
LLM_API_KEY = config["llm_api_key"]
LLM_MODEL = config["llm_model"]
# 思考模式：true=先推理(reasoning_content)再回答，前端可展示思考过程。
# 注：DeepSeek 思考模式下 temperature 无效(被忽略)，由 reasoning_effort 控制风格。
LLM_THINKING = bool(config.get("llm_thinking", True))
LLM_REASONING_EFFORT = str(config.get("llm_reasoning_effort", "low")).strip() or "low"

# ========== 对话 System Prompt（CLI 与 Web 共用） ==========
SYSTEM_PROMPT = (
    "你是支持工具调用的智能助手。"
    "严格只响应对话中**最后一条用户消息**，不要重复回答历史中已经解决的问题。"
    "如果需要调用工具就调用，不需要就直接回答。"
    "你的思考与推理过程（reasoning_content）必须使用中文，禁止使用英文或其它语言思考。"
)

# ========== WSL/Linux 专用配置 ==========
# WSL 系统npx，PATH里面直接调用
WEBSEARCH_ARGS = ["--yes", "open-websearch@latest"]
WEBSEARCH_ENV = {
    "MODE": "stdio",
    "DISABLE_HTTP": "true",
    "NO_COLOR": "1",
    "FORCE_COLOR": "0"
}

# filesystem允许操作的目录
FILESYSTEM_WORKSPACE = str(Path.home() / "fs_workspace")
FILESYSTEM_ARGS = [
    "-y",
    "@modelcontextprotocol/server-filesystem",
    FILESYSTEM_WORKSPACE
]

TERMINAL_ARGS = ["--yes", "mcp-server-terminal", "--headless"]
TERMINAL_ENV = {
    "NO_COLOR": "1",
    "FORCE_COLOR": "0",
    # --headless 模式在 tmux 会话中静默执行命令、不弹 xterm 窗口，无需 DISPLAY；
    # 保留 DISPLAY 仅为将来切回 visual 模式时兼容
    "DISPLAY": os.environ.get("DISPLAY") or ":0",
    "MCP_TERMINAL_ALLOWED_PATHS": str(Path.home() / "build-mcp"),
    "MCP_TERMINAL_BLOCKED_COMMANDS": "rm,rm‑rf,format,dd,mkfs",
    "MCP_TERMINAL_MAX_SESSIONS": "3",
    "MCP_TERMINAL_IDLE_TIMEOUT": "300000"
}


MCP_SERVERS = [
    {
        "name": "amap",
        "params": StdioServerParameters(
            command="uv",
            args=["run", "build_mcp"]
        )
    },
    {
        "name": "websearch",
        "params": StdioServerParameters(
            command="npx",
            args=WEBSEARCH_ARGS,
            env=WEBSEARCH_ENV
        )
    },
    {
        "name": "filesystem",
        "params": StdioServerParameters(
            command="npx",
            args=FILESYSTEM_ARGS,
            env={
                "NO_COLOR": "1",
                "FORCE_COLOR": "0"
            }
        )
    },
    {
        "name": "terminal",
        "params": StdioServerParameters(
            command="bash",
            args=[
                "-c",
                r'''npx --yes mcp-server-terminal --headless 2>&1 | while IFS= read -r line; do if [[ "$line" == "{"* ]]; then echo "$line"; else >&2 echo "$line"; fi; done'''
            ],
            env=TERMINAL_ENV
        )
    }
]

# 初始化全部MCP服务：由调用方传入长期存活的 exit_stack，持有所有子进程资源
async def init_all_mcp_sessions(exit_stack: AsyncExitStack):
    """
    初始化全部MCP服务。

    必须由调用方传入一个长期存活的 AsyncExitStack：所有 stdio_client 和
    ClientSession 的 context manager 都会注册到这个 exit_stack 上，保证
    MCP 子进程在整个服务生命周期内不被提前回收/关闭。

    注意：不能像 `stdio_client(params).__aenter__()` 那样创建临时对象——
    临时 context manager 无人持有会被 GC，立刻关闭子进程管道，导致
    initialize() 抛 Connection closed（web 版之前失败的根因）。

    返回 dict：
        tool_name_to_session: 工具名 -> session，供 agent_loop 路由 call_tool
        openai_tools:         OpenAI function-call 格式的工具列表
        sessions:             [{"name": ..., "session": ...}, ...] 便于查看/调试
    """
    tool_name_to_session: Dict[str, ClientSession] = {}
    openai_tools: List[Dict[str, Any]] = []
    sessions: List[Dict[str, Any]] = []
    ok_count = 0

    for server_cfg in MCP_SERVERS:
        name = server_cfg["name"]
        params = server_cfg["params"]
        try:
            print(f"🔌正在连接MCP[{name}]")
            read, write = await exit_stack.enter_async_context(stdio_client(params))
            session: ClientSession = await exit_stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            print(f"✅MCP[{name}] 连接成功")

            sessions.append({"name": name, "session": session})
            tools_resp = await session.list_tools()
            for t in tools_resp.tools:
                tool_name_to_session[t.name] = session
                openai_tools.append(mcp_tool_to_openai_function(t))
            ok_count += 1
        except Exception as e:
            # 单个服务失败不应拖垮整体，跳过继续下一个
            print(f"❌MCP[{name}] 启动失败，跳过该服务: {e}")
            continue

    print(f"📋MCP服务启动完成：成功 {ok_count}/{len(MCP_SERVERS)}，"
          f"共加载 {len(openai_tools)} 个工具")
    return {
        "tool_name_to_session": tool_name_to_session,
        "openai_tools": openai_tools,
        "sessions": sessions,
    }


def mcp_tool_to_openai_function(tool: Tool) -> Dict[str, Any]:
    """把MCP的Tool对象，转换为OpenAI function‑call格式"""
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.input_schema
        }
    }


def _sanitize(obj: Any) -> Any:
    """
    递归清除字符串中的非法代理项(surrogate, U+D800~U+DFFF)。

    外部来源(工具返回、子进程输出、抓取的网页等)可能含非 UTF-8 字节，
    某些环节解码后会产生孤立代理项——它们在 str 里能存活，但一旦
    .encode('utf-8') 就会抛 UnicodeEncodeError，导致请求序列化崩溃。
    发送给 LLM 前统一清洗，确保任何来源的脏文本都被过滤。
    """
    if isinstance(obj, str):
        return obj.encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(i) for i in obj]
    return obj


class AgentResult:
    """agent_loop 的返回结果：最终回答 + 全部轮次的思考过程文本"""
    __slots__ = ("answer", "thinking")

    def __init__(self, answer: str = "", thinking: str = ""):
        self.answer = answer
        self.thinking = thinking


def _extract_reasoning(msg) -> str:
    """从 ChatCompletionMessage 里取出 reasoning_content(思考文本)，兼容不同 SDK 暴露方式。"""
    try:
        rc = getattr(msg, "reasoning_content", None)
        if not rc:
            extra = getattr(msg, "model_extra", None) or {}
            rc = extra.get("reasoning_content")
        return _sanitize(str(rc)) if rc else ""
    except Exception:
        return ""


def _delta_field(delta, name):
    """从流式 chunk 的 ChoiceDelta 中取字段(如 reasoning_content)，兼容 attr/model_extra 两种暴露。"""
    try:
        v = getattr(delta, name, None)
        if v is None:
            extra = getattr(delta, "model_extra", None) or {}
            v = extra.get(name)
        return v
    except Exception:
        return None


def _build_llm_request(work_messages: list[dict], openai_tools) -> Dict[str, Any]:
    """组装一次 chat.completions 请求参数（按 llm_thinking 开关决定是否开思考模式）。"""
    req: Dict[str, Any] = {
        "model": LLM_MODEL,
        # 统一清洗后再发送：工具返回等外部文本可能携带非法代理项，会炸序列化
        "messages": _sanitize(work_messages),
        "tools": openai_tools,
    }
    if LLM_THINKING:
        # 开启 DeepSeek 思考模式：先输出 reasoning_content 再给 content
        req["extra_body"] = {"thinking": {"type": "enabled"}}
        req["reasoning_effort"] = LLM_REASONING_EFFORT
    else:
        req["temperature"] = 0.3  # 非思考模式沿用原温度
    return req


async def agent_loop_stream(
    tool_name_to_session: Dict[str, ClientSession],
    openai_tools: List[Dict[str, Any]],
    user_query: str,
    history_messages: list[dict],
):
    """
    流式执行一轮「推理 + 工具调用」，逐事件产出给调用方(Web SSE / CLI)。

    产出的事件 dict：
      {"type": "thinking", "delta": str}    思考过程增量(模型思考时实时下发)
      {"type": "answer",   "delta": str}    最终回答增量
      {"type": "tool", "name": str, "status": "start"|"ok"|"error", "note": str}
                                            (note 仅 start/error 时可能带参数或原因)
      {"type": "done", "answer": str, "thinking": str}   整轮结束(携带全文)
      {"type": "error", "message": str}    硬错误(网络/API/解析等)

    不修改传入的 history_messages，中间工具过程只在本轮内部。
    工具轮次的 reasoning_content 会随 assistant 消息一起回传(DeepSeek 要求，
    否则带 tools 的后续请求会 400)。
    """
    work_messages = history_messages.copy()
    work_messages.append({"role": "user", "content": user_query})

    client = AsyncOpenAI(
        api_key=LLM_API_KEY,
        base_url=LLM_BASE_URL
    )

    thinking_parts: List[str] = []   # 各轮思考文本，done 时合并
    max_round = 1000

    for _ in range(max_round):
        req = _build_llm_request(work_messages, openai_tools)

        # ---------- 发起流式请求 ----------
        try:
            stream = await client.chat.completions.create(**req, stream=True)
        except Exception as e:
            yield {"type": "error", "message": f"模型请求失败：{e}"}
            return

        content_acc = ""            # 本轮最终回答累积
        thinking_acc = ""           # 本轮思考文本累积
        tool_slots: Dict[int, dict] = {}   # tool_call index -> 聚合后的调用
        try:
            async for chunk in stream:
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta

                rc = _delta_field(delta, "reasoning_content")
                if rc:
                    thinking_acc += rc
                    yield {"type": "thinking", "delta": rc}

                c = _delta_field(delta, "content")
                if c:
                    content_acc += c
                    yield {"type": "answer", "delta": c}

                # 聚合分片到达的 tool_calls（id/name 首次出现，arguments 可能多片拼接）
                for tc in (delta.tool_calls or []):
                    slot = tool_slots.setdefault(
                        tc.index,
                        {"id": "", "type": "function", "function": {"name": "", "arguments": ""}},
                    )
                    if tc.id:
                        slot["id"] = tc.id
                    if tc.function:
                        if tc.function.name:
                            slot["function"]["name"] = tc.function.name
                        if tc.function.arguments:
                            slot["function"]["arguments"] += tc.function.arguments
        except Exception as e:
            yield {"type": "error", "message": f"流式读取中断：{e}"}
            return
        finally:
            try:
                await stream.close()
            except Exception:
                pass

        if thinking_acc:
            thinking_parts.append(thinking_acc)

        tool_calls = [tool_slots[i] for i in sorted(tool_slots)] if tool_slots else None

        # ---------- 无工具调用：本轮即最终回答 ----------
        if not tool_calls:
            yield {
                "type": "done",
                "answer": content_acc,
                "thinking": "\n\n".join(p for p in thinking_parts if p),
            }
            return

        # ---------- 有工具调用：执行并进入下一轮 ----------
        asst: Dict[str, Any] = {
            "role": "assistant",
            "content": content_acc or None,
            "tool_calls": tool_calls,
        }
        if thinking_acc:
            asst["reasoning_content"] = thinking_acc
        work_messages.append(_sanitize(asst))

        for tc in tool_calls:
            tool_name = tc["function"]["name"] or "?"
            args_raw = tc["function"]["arguments"] or ""
            try:
                tool_args = json.loads(args_raw)
            except Exception:
                tool_args = {}
            print(f"\n🔧准备调用MCP工具：{tool_name}, 参数：{tool_args}")
            yield {"type": "tool", "name": tool_name, "status": "start", "note": args_raw[:100]}

            if tool_name not in tool_name_to_session:
                tool_content = f"错误：工具 {tool_name} 不存在"
                print(f"❌{tool_content}")
                yield {"type": "tool", "name": tool_name, "status": "error", "note": tool_content}
            else:
                session = tool_name_to_session[tool_name]
                try:
                    tool_result = await session.call_tool(tool_name, arguments=tool_args)
                    tool_content = tool_result.content[0].text
                    print(f"✅工具[{tool_name}]返回结果")
                    yield {"type": "tool", "name": tool_name, "status": "ok"}
                except Exception as e:
                    tool_content = f"工具调用异常: {str(e)}"
                    print(f"❌{tool_content}")
                    yield {"type": "tool", "name": tool_name, "status": "error", "note": str(e)[:200]}

            work_messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": _sanitize(tool_content),
            })

    # 超过最大轮次
    yield {
        "type": "done",
        "answer": "已达到最大工具调用轮次，停止处理。",
        "thinking": "\n\n".join(p for p in thinking_parts if p),
    }


async def agent_loop(
    tool_name_to_session: Dict[str, ClientSession],
    openai_tools: List[Dict[str, Any]],
    user_query: str,
    history_messages: list[dict]
) -> AgentResult:
    """
    聚合版：消费 agent_loop_stream，攒齐后返回 AgentResult(answer/thinking)。
    供 CLI 及不需要流式的调用方使用；Web 端直接用 agent_loop_stream。
    """
    thinking_parts: List[str] = []
    answer = ""

    async for ev in agent_loop_stream(
        tool_name_to_session=tool_name_to_session,
        openai_tools=openai_tools,
        user_query=user_query,
        history_messages=history_messages,
    ):
        t = ev.get("type")
        if t == "thinking":
            thinking_parts.append(ev.get("delta", ""))
        elif t == "answer":
            answer += ev.get("delta", "")
        elif t == "done":
            return AgentResult(answer=ev.get("answer") or answer, thinking=ev.get("thinking") or "")
        elif t == "error":
            return AgentResult(
                answer=f"（发生错误）{ev.get('message', '未知错误')}",
                thinking="\n\n".join(thinking_parts),
            )
    return AgentResult(answer=answer or "（无回答）", thinking="\n\n".join(thinking_parts))


async def main():
    exit_stack = AsyncExitStack()
    try:
        state = await init_all_mcp_sessions(exit_stack)

        sessions = state["sessions"]
        if not sessions:
            print("所有MCP服务全部启动失败，程序退出")
            return

        tool_name_to_session = state["tool_name_to_session"]
        all_openai_tools = state["openai_tools"]

        print(f"📋已加载全部工具列表：{[x['function']['name'] for x in all_openai_tools]}")
        print("💡输入问题和助手对话，输入 quit 或 exit 退出\n")

        messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]

        # uv 的 standalone Python 不带 readline，input() 的方向键会变成乱码；
        # 改用 prompt_toolkit：自带行编辑/方向键，并把历史存到 ~/.conver_mcp_history
        prompt_session = PromptSession(
            history=FileHistory(str(Path.home() / ".conver_mcp_history"))
        )
        while True:
            try:
                # 注意:main() 已运行在 asyncio 事件循环内,必须用 prompt_async()。
                # prompt() 同步版内部会再调 asyncio.run(),在有 loop 时直接报错。
                user_input = (await prompt_session.prompt_async("👤请输入你的问题：")).strip()
            except (EOFError, KeyboardInterrupt):
                # Ctrl+D / Ctrl+C 都算正常退出，不再打 traceback
                print("\n👋退出程序")
                break

            if user_input.lower() in ("quit", "exit"):
                print("👋退出程序")
                break
            if not user_input:
                continue

            result = await agent_loop(
                tool_name_to_session=tool_name_to_session,
                openai_tools=all_openai_tools,
                user_query=user_input,
                history_messages=messages
            )

            if result.thinking:
                print(f"\n🤔 思考过程：\n{result.thinking}")
            print(f"\n🤖大模型最终回答：\n{result.answer}")

            # 手动把干净的「用户提问 + 最终回答」追加进全局历史
            messages.append({"role": "user", "content": user_input})
            messages.append({"role": "assistant", "content": result.answer})

            print("-" * 80)

    finally:
        # 安全释放全部子进程、流资源
        await exit_stack.aclose()
        print("\n🧹全部MCP资源已释放完毕")


def cli_main():
    """pyproject.toml scripts 同步入口"""
    asyncio.run(main())


if __name__ == "__main__":
    asyncio.run(main())
