import os
import time
import json
import asyncio
import secrets
import hmac
import logging
from contextlib import asynccontextmanager, AsyncExitStack
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from build_mcp.client.conversation import agent_loop_stream, init_all_mcp_sessions, SYSTEM_PROMPT

logger = logging.getLogger(__name__)

# ====================== 鉴权配置 ======================
# 密码从环境变量 MCP_WEB_PASSWORD 读取，不写死在代码里。
# 未设置时回退到内置开发密码（本地调试方便），并在启动日志中醒目警告。
AUTH_PASSWORD = os.environ.get("MCP_WEB_PASSWORD", "").strip()
if not AUTH_PASSWORD:
    AUTH_PASSWORD = "admin123"
    logger.warning(
        "⚠️ 未设置环境变量 MCP_WEB_PASSWORD，正在使用【内置开发密码 admin123】。"
        "公网/内网穿透部署前务必设置强密码！"
    )

TOKEN_TTL = int(os.environ.get("MCP_WEB_TOKEN_TTL", str(12 * 3600)))          # token 有效期(秒)，默认12小时
LOGIN_WINDOW = int(os.environ.get("MCP_WEB_LOGIN_WINDOW", "300"))              # 登录限流窗口(秒)，默认5分钟
LOGIN_MAX_ATTEMPTS = int(os.environ.get("MCP_WEB_LOGIN_MAX", "20"))            # 窗口内最多尝试次数，防爆破

# 已签发 token -> 过期 unix 时间戳
_tokens: Dict[str, float] = {}
# ip -> [最近登录尝试时间戳]
_login_attempts: Dict[str, list] = {}

_bearer = HTTPBearer(auto_error=False)  # 不自动报错，由我们统一返回 401


def _client_ip(request: Request) -> str:
    """取客户端 IP：优先 x-forwarded-for（cpolar/nginx 等反代场景），否则取直连地址。"""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_login_rate(request: Request):
    """登录接口限流：同一 IP 在窗口期内最多尝试 N 次。"""
    ip = _client_ip(request)
    now = time.time()
    window = _login_attempts.setdefault(ip, [])
    _login_attempts[ip] = [t for t in window if now - t < LOGIN_WINDOW]
    if len(_login_attempts[ip]) >= LOGIN_MAX_ATTEMPTS:
        raise HTTPException(status_code=429, detail="尝试次数过多，请 5 分钟后再试")
    _login_attempts[ip].append(now)


def require_auth(credentials: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> str:
    """FastAPI 依赖：校验 Authorization: Bearer <token>，非法则 401。"""
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail="未登录，请先访问 /api/login 获取令牌",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = credentials.credentials
    exp = _tokens.get(token)
    now = time.time()
    if exp is None:
        raise HTTPException(status_code=401, detail="登录已失效，请重新登录")
    if exp < now:
        _tokens.pop(token, None)
        raise HTTPException(status_code=401, detail="登录已过期，请重新登录")
    return token


# 全局 exit_stack：持有所有 stdio_client / ClientSession 的 context manager，
# 保证 MCP 子进程存活到 FastAPI 退出，避免被 GC 提前回收导致 Connection closed
_exit_stack = AsyncExitStack()

# 全局 MCP 状态：init_all_mcp_sessions(exit_stack) 的返回结果
# {"tool_name_to_session": ..., "openai_tools": [...], "sessions": [...]}
mcp_state: Dict[str, Any] | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """程序生命周期：启动初始化MCP，关闭销毁MCP子进程"""
    global mcp_state
    logger.info("🔄 正在初始化全部MCP服务...")
    try:
        mcp_state = await init_all_mcp_sessions(_exit_stack)
        logger.info("✅ 全部MCP服务初始化完成")
    except Exception as e:
        logger.exception("❌ MCP服务初始化失败")
        mcp_state = None
    yield
    # 服务关闭时统一释放所有 MCP 子进程
    logger.info("🛑 FastAPI服务退出，正在释放MCP子进程...")
    await _exit_stack.aclose()
    logger.info("🧹全部MCP资源已释放完毕")


# docs(/docs 接口文档)默认关闭，避免公网暴露 API 结构；调试时设 MCP_WEB_DOCS=1 开启
app = FastAPI(
    title="MCP Agent Web",
    lifespan=lifespan,
    docs_url="/docs" if os.environ.get("MCP_WEB_DOCS") == "1" else None,
    redoc_url=None,
)

# CORS：默认不开启（页面与 API 同源，无需跨域）。
# 如需允许特定前端跨域访问，设置环境变量 MCP_WEB_CORS=https://a.com,https://b.com
_cors_origins = [o.strip() for o in os.environ.get("MCP_WEB_CORS", "").split(",") if o.strip()]
if _cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=False,
    )
    logger.info("🌐 CORS 已启用，允许来源：%s", _cors_origins)


class LoginRequest(BaseModel):
    password: str


class ChatRequest(BaseModel):
    query: str


@app.post("/api/login")
async def login(req: LoginRequest, request: Request):
    """密码登录，成功签发随机 token。带 IP 限流防爆破。"""
    _check_login_rate(request)
    if not hmac.compare_digest(req.password.encode("utf-8"), AUTH_PASSWORD.encode("utf-8")):
        raise HTTPException(status_code=401, detail="密码错误")
    token = secrets.token_urlsafe(32)
    _tokens[token] = time.time() + TOKEN_TTL
    logger.info("🔑 登录成功，IP=%s，token 有效期 %s 秒", _client_ip(request), TOKEN_TTL)
    return {"token": token, "expires_in": TOKEN_TTL}


@app.post("/api/logout")
async def logout(token: str = Depends(require_auth)):
    """主动登出：立即吊销当前 token。"""
    _tokens.pop(token, None)
    return {"ok": True}


@app.get("/api/me")
async def me(token: str = Depends(require_auth)):
    """校验当前 token 是否有效，供前端启动时探测登录态。"""
    return {"ok": True}


def _sse(event: dict) -> str:
    """把事件 dict 打包成一条 SSE 帧（data: {...}），中文不转义。"""
    return "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"


@app.post("/api/chat")
async def chat(req: ChatRequest, token: str = Depends(require_auth)):
    global mcp_state
    if not mcp_state:
        raise HTTPException(status_code=500, detail="MCP服务尚未初始化，请检查日志")
    if not mcp_state["tool_name_to_session"]:
        raise HTTPException(status_code=500, detail="没有任何MCP服务连接成功，无法调用工具")

    async def event_gen():
        try:
            async for ev in agent_loop_stream(
                tool_name_to_session=mcp_state["tool_name_to_session"],
                openai_tools=mcp_state["openai_tools"],
                user_query=req.query,
                # 每次请求独立会话（仅含 system），FastAPI 并发下避免共享可变历史产生竞态
                history_messages=[{"role": "system", "content": SYSTEM_PROMPT}],
            ):
                yield _sse(ev)
        except asyncio.CancelledError:
            # 客户端断开/中止：正常结束生成器，不留半个事件
            raise
        except Exception as e:
            logger.exception("❌ /api/chat 流式处理异常")
            yield _sse({"type": "error", "message": f"服务内部错误：{e}"})

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",   # 避免中间代理缓冲拖慢实时性
        },
    )


# 挂载静态网页，static文件夹放在项目根目录，里面放index.html
app.mount("/", StaticFiles(directory="static", html=True), name="static")
