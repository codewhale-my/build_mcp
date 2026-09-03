import asyncio
import json
from typing import List, Dict, Any

from openai import AsyncOpenAI
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from build_mcp.common.config import load_config
from mcp.types import Tool

# ====================== 配置区 ======================
# 大模型配置，兼容所有OpenAI兼容接口

config = load_config("config.yaml")
LLM_BASE_URL = config["llm_base_url"]
LLM_API_KEY = config["llm_api_key"]
LLM_MODEL = config["llm_model"]

# MCP服务启动命令，替换成你自己的启动方式
MCP_SERVER_PARAMS = StdioServerParameters(
    command="uv",
    args=["run", "build_mcp"]
)

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
            command="uvx",
            args=["open‑websearch‑mcp"]
        )
    }
]
# ====================================================


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

async def agent_loop(session: ClientSession, user_query: str, messages: list[dict]):
    """
    Agent主循环：
    1. 获取MCP全部工具
    2. 发给大模型，模型决定是否调用工具
    3. 执行MCP工具，回传结果给大模型
    """
    # 1. 获取MCP服务上所有可用工具
    tools_resp = await session.list_tools()
    mcp_tools: List[Tool] = tools_resp.tools
    openai_tools = [mcp_tool_to_openai_function(t) for t in mcp_tools]

    messages.append({"role": "user", "content": user_query})

    client = AsyncOpenAI(
        api_key=LLM_API_KEY,
        base_url=LLM_BASE_URL
    )

    max_round = 5  # 限制工具调用轮次，防止死循环
    for _ in range(max_round):
        # 请求大模型
        resp = await client.chat.completions.create(
            model=LLM_MODEL,
            messages=messages,
            tools=openai_tools,
            temperature=0.3
        )
        choice = resp.choices[0]
        msg = choice.message

        # 大模型直接输出回答，不需要调用工具，结束循环
        if not msg.tool_calls:
            print(f"\n🤖大模型最终回答：\n{msg.content}")
            return messages

        # ✅关键修复：assistant消息只追加1次，放到for循环外面
        messages.append(msg.model_dump())

        # 遍历所有要调用的工具
        for tool_call in msg.tool_calls:
            tool_name = tool_call.function.name
            tool_args = json.loads(tool_call.function.arguments)
            print(f"\n🔧准备调用MCP工具：{tool_name}, 参数：{tool_args}")

            # 实际调用MCP服务的工具
            tool_result = await session.call_tool(tool_name, arguments=tool_args)
            tool_content = tool_result.content[0].text
            print(f"✅MCP工具返回结果：{tool_content}")

            # 每个tool_call对应一条tool消息
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": tool_content
            })

    print("\n⚠️达到最大调用轮次，终止")
    messages.append({"role": "assistant", "content": "已达到最大工具调用轮次，停止处理。"})
    print("🤖大模型最终回答：\n已达到最大工具调用轮次，停止处理。")
    return messages


async def main():
    async with stdio_client(MCP_SERVER_PARAMS) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            print("✅MCP客户端连接成功，已握手")
            print("💡输入问题和助手对话，输入 quit 或 exit 退出\n")

            # ========== 对话上下文，全局维护，不要放while循环里面 ==========
            messages: list[dict] = []

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

                # 传入历史，拿到更新后的历史
                messages = await agent_loop(session, user_input, messages)
                print("-" * 60)



def cli_main():
    """给pyproject scripts用的同步包装入口"""
    import asyncio
    asyncio.run(main())

if __name__ == "__main__":
    asyncio.run(main())
