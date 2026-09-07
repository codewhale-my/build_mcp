import asyncio
import json
from typing import List, Dict, Any, Optional
from contextlib import AsyncExitStack
from pathlib import Path

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

TERMINAL_ARGS = ["--yes", "mcp-server-terminal"]
TERMINAL_ENV = {
    "NO_COLOR": "1",
    "FORCE_COLOR": "0",
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
                r'''npx --yes mcp-server-terminal 2>&1 | while IFS= read -r line; do if [[ "$line" == "{"* ]]; then echo "$line"; else >&2 echo "$line"; fi; done'''
            ],
            env=TERMINAL_ENV
        )
    }
]


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


async def agent_loop(
    tool_name_to_session: Dict[str, ClientSession],
    openai_tools: List[Dict[str, Any]],
    user_query: str,
    history_messages: list[dict]
) -> str:
    """
    执行一轮工具调用推理，返回最终回答文本
    不修改传入的 history_messages，中间工具过程只在本轮内部
    """
    # 工作副本：历史 + 当前用户问题，所有中间操作只在副本上进行
    work_messages = history_messages.copy()
    work_messages.append({"role": "user", "content": user_query})

    client = AsyncOpenAI(
        api_key=LLM_API_KEY,
        base_url=LLM_BASE_URL
    )

    max_round = 5
    for _ in range(max_round):
        resp = await client.chat.completions.create(
            model=LLM_MODEL,
            messages=work_messages,
            tools=openai_tools,
            temperature=0.3
        )
        choice = resp.choices[0]
        msg = choice.message

        # 没有工具调用，直接返回最终回答
        if not msg.tool_calls:
            return msg.content

        # 有工具调用，只追加到工作副本，不污染全局历史
        work_messages.append(msg.model_dump())

        for tool_call in msg.tool_calls:
            tool_name = tool_call.function.name
            tool_args = json.loads(tool_call.function.arguments)
            print(f"\n🔧准备调用MCP工具：{tool_name}, 参数：{tool_args}")

            if tool_name not in tool_name_to_session:
                tool_content = f"错误：工具 {tool_name} 不存在"
                print(f"❌{tool_content}")
            else:
                session = tool_name_to_session[tool_name]
                try:
                    tool_result = await session.call_tool(tool_name, arguments=tool_args)
                    tool_content = tool_result.content[0].text
                    print(f"✅工具返回结果")
                except Exception as e:
                    tool_content = f"工具调用异常: {str(e)}"
                    print(f"❌{tool_content}")

            work_messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": tool_content
            })

    # 超过最大轮次
    return "已达到最大工具调用轮次，停止处理。"


async def main():
    exit_stack = AsyncExitStack()
    # 保存所有成功连接的会话
    sessions: List[ClientSession] = []
    # 工具名映射到对应session，用于路由call_tool
    tool_name_to_session: Dict[str, ClientSession] = {}
    all_openai_tools: List[Dict[str, Any]] = []

    try:
        # 循环启动全部MCP服务
        for srv_cfg in MCP_SERVERS:
            srv_name = srv_cfg["name"]
            params = srv_cfg["params"]
            try:
                read, write = await exit_stack.enter_async_context(stdio_client(params))
                session: ClientSession = await exit_stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
                print(f"✅MCP[{srv_name}] 连接成功")
                sessions.append(session)

                # 获取该服务所有工具，注册路由
                tools_resp = await session.list_tools()
                for t in tools_resp.tools:
                    tool_name_to_session[t.name] = session
                    all_openai_tools.append(mcp_tool_to_openai_function(t))

            except Exception as e:
                print(f"❌MCP[{srv_name}] 启动失败，跳过该服务: {e}")
                continue

        if not sessions:
            print("所有MCP服务全部启动失败，程序退出")
            return

        print(f"\n📋已加载全部工具列表：{[x['function']['name'] for x in all_openai_tools]}")
        print("💡输入问题和助手对话，输入 quit 或 exit 退出\n")

        messages: list[dict] = [
            {
                "role": "system",
                "content": (
                    "你是支持工具调用的智能助手。"
                    "严格只响应对话中**最后一条用户消息**，不要重复回答历史中已经解决的问题。"
                    "如果需要调用工具就调用，不需要就直接回答。"
                )
            }
        ]
        while True:
            try:
                user_input = input("👤请输入你的问题：").strip()
            except EOFError:
                print("\n👋退出程序")
                break

            if user_input.lower() in ("quit", "exit"):
                print("👋退出程序")
                break
            if not user_input:
                continue

            final_answer = await agent_loop(
                tool_name_to_session=tool_name_to_session,
                openai_tools=all_openai_tools,
                user_query=user_input,
                history_messages=messages
            )

            print(f"\n🤖大模型最终回答：\n{final_answer}")

            # 手动把干净的「用户提问 + 最终回答」追加进全局历史
            messages.append({"role": "user", "content": user_input})
            messages.append({"role": "assistant", "content": final_answer})

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
