"""
MCP Agent Web 服务：多用户版
=============================
- 首次登录自动注册：用户名不存在时必须携带【有效邀请码】才能注册；
  已有账号则只校验用户名+密码（无需邀请码）。
- 每个用户独立的文件空间：filesystem MCP 以该用户目录为 root 单独拉起子进程，
  天然沙箱，互不可见。
- 历史消息按用户持久化到 SQLite（~/build-mcp-data/app.db），只读自己的。
- 登录签发 Bearer token（内存态，服务重启需重新登录，历史不丢）。

环境变量：
  MCP_WEB_INVITE_CODES  启动时导入的邀请码，逗号分隔，可带 :备注，如 "CODE1:张三,CODE2"
                        （留空则新用户无法注册，仅已有账号可登录）
  MCP_WEB_TOKEN_TTL     token 有效期秒数，默认 12h
  MCP_WEB_DB / MCP_WEB_DATA_DIR / MCP_WEB_FS_ROOT  数据/文件空间位置（一般不用改）
"""
import asyncio
import json
import logging
import os
import secrets
import shutil
import time
from contextlib import asynccontextmanager, AsyncExitStack
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from build_mcp.client.conversation import (
    agent_loop_stream,
    init_all_mcp_sessions,
    SYSTEM_PROMPT,
)
from build_mcp.client.conversation import (
    StdioServerParameters,
    stdio_client,
    mcp_tool_to_openai_function,
)
from mcp.client.session import ClientSession
from build_mcp.web import store
from build_mcp.web.store import (
    init_db,
    get_user_by_id,
    get_user_by_name,
    create_user,
    find_invite,
    consume_invite,
    add_message,
    list_messages,
    recent_llm_messages,
    clear_messages,
    user_filesystem_dir,
    USERNAME_RE,
    verify_password,
)

logger = logging.getLogger(__name__)

# ====================== 鉴权配置 ======================
# Web 只启动共享服务（地图/搜索/终端）；filesystem 按用户独立拉起，见 ensure_user_fs()
SHARED_MCP_INCLUDE = ["amap", "websearch", "terminal"]

TOKEN_TTL = int(os.environ.get("MCP_WEB_TOKEN_TTL", str(12 * 3600)))   # token 有效期(秒)
LOGIN_WINDOW = int(os.environ.get("MCP_WEB_LOGIN_WINDOW", "300"))       # 登录限流窗口(秒)
LOGIN_MAX_ATTEMPTS = int(os.environ.get("MCP_WEB_LOGIN_MAX", "20"))     # 窗口内最多尝试次数
INVITE_CODES = os.environ.get("MCP_WEB_INVITE_CODES", "").strip()
if not INVITE_CODES:
    logger.warning(
        "⚠️ 未设置环境变量 MCP_WEB_INVITE_CODES，新用户将无法注册"
        "（仅已有账号可登录）。部署后可用 start_web.sh 或 "
        "`uv run python -m build_mcp.web.store invite <CODE> [备注]` 添加邀请码。"
    )

# token -> {"uid": int, "exp": float}
_tokens: Dict[str, dict] = {}
# ip -> [最近登录/注册尝试时间戳]
_login_attempts: Dict[str, list] = {}
# user_id -> asyncio.Lock：同一用户的对话串行，避免上下文乱序
_chat_locks: Dict[int, asyncio.Lock] = {}
# user_id -> {"session","names","openai_tools"}：每用户 filesystem 会话缓存
_user_fs_cache: Dict[int, dict] = {}
_fs_boot_lock = asyncio.Lock()

_bearer = HTTPBearer(auto_error=False)  # 不自动报错，由我们统一返回 401


def _client_ip(request: Request) -> str:
    """取客户端 IP：优先 x-forwarded-for（cpolar/nginx 等反代场景），否则取直连地址。"""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_login_rate(request: Request):
    """登录/注册接口限流：同一 IP 在窗口期内最多尝试 N 次。"""
    ip = _client_ip(request)
    now = time.time()
    window = _login_attempts.setdefault(ip, [])
    _login_attempts[ip] = [t for t in window if now - t < LOGIN_WINDOW]
    if len(_login_attempts[ip]) >= LOGIN_MAX_ATTEMPTS:
        raise HTTPException(status_code=429, detail="尝试次数过多，请 5 分钟后再试")
    _login_attempts[ip].append(now)


def _issue_token(uid: int) -> str:
    token = secrets.token_urlsafe(32)
    _tokens[token] = {"uid": uid, "exp": time.time() + TOKEN_TTL}
    return token


def require_user(credentials: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> dict:
    """FastAPI 依赖：校验 Bearer token，返回当前用户 {id, username}，非法则 401。"""
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail="未登录",
            headers={"WWW-Authenticate": "Bearer"},
        )
    rec = _tokens.get(credentials.credentials)
    now = time.time()
    if rec is None:
        raise HTTPException(status_code=401, detail="登录已失效，请重新登录")
    if rec["exp"] < now:
        _tokens.pop(credentials.credentials, None)
        raise HTTPException(status_code=401, detail="登录已过期，请重新登录")
    user = get_user_by_id(rec["uid"])
    if user is None:
        _tokens.pop(credentials.credentials, None)
        raise HTTPException(status_code=401, detail="账号不存在，请重新登录")
    return {"id": user["id"], "username": user["username"], "token": credentials.credentials}


# 全局 exit_stack：持有所有 stdio_client / ClientSession 的 context manager，
# 保证 MCP 子进程（含各用户的 filesystem）存活到 FastAPI 退出
_exit_stack = AsyncExitStack()

# 共享 MCP 状态：{"tool_name_to_session","openai_tools","sessions"}（不含 filesystem）
shared_mcp: Dict[str, Any] | None = None


async def ensure_user_fs(user: dict) -> dict:
    """
    为该用户懒启动独立的 filesystem MCP 子进程（root=该用户专属目录）。
    结果缓存，同一用户复用；生命周期挂在全局 exit_stack 上随服务退出。
    """
    uid = user["id"]
    cached = _user_fs_cache.get(uid)
    if cached:
        return cached
    async with _fs_boot_lock:
        cached = _user_fs_cache.get(uid)
        if cached:
            return cached
        user_dir = user_filesystem_dir(user["username"], uid)
        user_dir.mkdir(parents=True, exist_ok=True)
        params = StdioServerParameters(
            command="npx",
            args=["-y", "@modelcontextprotocol/server-filesystem", str(user_dir)],
            env={"NO_COLOR": "1", "FORCE_COLOR": "0"},
        )
        read, write = await _exit_stack.enter_async_context(stdio_client(params))
        session: ClientSession = await _exit_stack.enter_async_context(
            ClientSession(read, write)
        )
        await session.initialize()
        tools_resp = await session.list_tools()
        names: set[str] = set()
        fs_tools = []
        for t in tools_resp.tools:
            names.add(t.name)
            fs_tools.append(mcp_tool_to_openai_function(t))
        rec = {"session": session, "names": names, "openai_tools": fs_tools}
        _user_fs_cache[uid] = rec
        logger.info(
            "📂 用户[%s]文件空间就绪：%s（工具 %d 个）",
            user["username"], user_dir, len(fs_tools),
        )
        return rec


def _migrate_legacy_fs():
    """把旧版共享 fs_workspace 根目录下的遗留文件收进 _legacy_*（不参与任何用户空间）。"""
    try:
        entries = [p for p in store.FS_ROOT.iterdir() if p.name != "users"]
        if entries:
            dst = store.FS_ROOT / f"_legacy_{time.strftime('%Y%m%d_%H%M%S')}"
            dst.mkdir()
            for p in entries:
                shutil.move(str(p), str(dst / p.name))
            logger.warning(
                "📦 原 fs_workspace 根目录 %d 个旧文件已移入 %s（每个用户的空间在 users/ 子目录下，互不可见）",
                len(entries), dst,
            )
    except Exception:
        logger.exception("旧文件迁移失败（不影响启动）")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """程序生命周期：初始化共享MCP + 数据库；关闭时统一销毁MCP子进程"""
    global shared_mcp
    init_db(INVITE_CODES)
    _migrate_legacy_fs()
    logger.info("🔄 正在初始化共享MCP服务(amap/websearch/terminal)...")
    try:
        shared_mcp = await init_all_mcp_sessions(_exit_stack, include=SHARED_MCP_INCLUDE)
        logger.info("✅ 共享MCP服务初始化完成")
    except Exception as e:
        logger.exception("❌ MCP服务初始化失败")
        shared_mcp = None
    yield
    logger.info("🛑 FastAPI服务退出，正在释放MCP子进程...")
    await _exit_stack.aclose()
    logger.info("🧹全部MCP资源已释放完毕")


# docs 默认关闭，避免公网暴露 API 结构；调试时设 MCP_WEB_DOCS=1 开启
app = FastAPI(
    title="MCP Agent Web",
    lifespan=lifespan,
    docs_url="/docs" if os.environ.get("MCP_WEB_DOCS") == "1" else None,
    redoc_url=None,
)

# CORS：默认不开启（页面与 API 同源，无需跨域）
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


class AuthRequest(BaseModel):
    username: str = ""
    password: str = ""
    invite: str = ""   # 新用户注册必需


class ChatRequest(BaseModel):
    query: str


@app.post("/api/auth")
async def auth(req: AuthRequest, request: Request):
    """
    登录 / 首次注册（自动判断）：
      - 用户名已存在 → 校验密码，成功直接登录；
      - 用户名不存在 → 必须提供未使用的有效邀请码，成功即注册并登录。
    """
    _check_login_rate(request)
    username = req.username.strip()
    if not USERNAME_RE.match(username):
        raise HTTPException(status_code=400, detail="用户名需为 2~24 位中文/字母/数字/下划线或横线")
    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="密码至少 6 位")

    user = get_user_by_name(username)
    if user:
        if not verify_password(req.password, user["pass_salt"], user["pass_hash"]):
            logger.info("🔒 登录失败(密码错误)：%s，IP=%s", username, _client_ip(request))
            raise HTTPException(status_code=401, detail="密码错误")
        token = _issue_token(user["id"])
        logger.info("🔑 登录成功：%s，IP=%s", username, _client_ip(request))
        return {"token": token, "username": user["username"], "is_new": False}

    # ---- 新用户：校验邀请码并注册 ----
    code = (req.invite or "").strip().upper()
    if not code:
        raise HTTPException(status_code=403, detail="该用户名尚未注册，请填写邀请码完成注册")
    ic = find_invite(code)
    if not ic:
        raise HTTPException(status_code=403, detail="邀请码无效或已被使用")
    try:
        uid = create_user(username, req.password)
    except Exception:
        # 极少数并发同名注册撞车：让用户直接去登录
        raise HTTPException(status_code=409, detail="用户名已被注册，请直接登录")
    consume_invite(ic["id"], uid)
    token = _issue_token(uid)
    logger.info("🎉 新用户注册成功：%s(id=%s)，IP=%s", username, uid, _client_ip(request))
    return {"token": token, "username": username, "is_new": True}


@app.post("/api/logout")
async def logout(user: dict = Depends(require_user)):
    """主动登出：立即吊销当前 token。"""
    _tokens.pop(user["token"], None)
    return {"ok": True}


@app.get("/api/me")
async def me(user: dict = Depends(require_user)):
    """校验当前 token 是否有效，供前端启动时探测登录态。"""
    return {"ok": True, "username": user["username"]}


@app.get("/api/history")
async def history(user: dict = Depends(require_user), limit: int = 200):
    """当前用户的历史消息（只返回自己的）。"""
    limit = max(1, min(limit, 500))
    msgs = list_messages(user["id"], limit=limit)
    return {
        "username": user["username"],
        "messages": [
            {"id": m["id"], "role": m["role"], "text": m["text"], "ts": m["ts"]}
            for m in msgs
        ],
    }


@app.delete("/api/history")
async def history_clear(user: dict = Depends(require_user)):
    """清空当前用户的历史消息。"""
    clear_messages(user["id"])
    return {"ok": True}


def _sse(event: dict) -> str:
    """把事件 dict 打包成一条 SSE 帧（data: {...}），中文不转义。"""
    return "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"


@app.post("/api/chat")
async def chat(req: ChatRequest, user: dict = Depends(require_user)):
    global shared_mcp
    if not shared_mcp or not shared_mcp.get("tool_name_to_session"):
        raise HTTPException(status_code=500, detail="MCP服务尚未就绪，请稍后再试")

    # 用户独立的 filesystem 会话（首次自动拉起）
    fs = await ensure_user_fs(user)
    tool_map = dict(shared_mcp["tool_name_to_session"])
    for name in fs["names"]:
        tool_map[name] = fs["session"]
    openai_tools = shared_mcp["openai_tools"] + fs["openai_tools"]

    # 带上该用户最近 8 轮问答作为上下文（服务端持久化历史）
    history_messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for m in recent_llm_messages(user["id"], turns=8):
        history_messages.append({"role": m["role"], "content": m["text"]})

    lock = _chat_locks.setdefault(user["id"], asyncio.Lock())

    async def event_gen():
        answer: str | None = None
        failed = False
        try:
            # 同一用户串行：锁跨整个流式过程持有，防止上下文竞态
            async with lock:
                async for ev in agent_loop_stream(
                    tool_name_to_session=tool_map,
                    openai_tools=openai_tools,
                    user_query=req.query,
                    history_messages=history_messages,
                ):
                    if ev.get("type") == "done":
                        answer = ev.get("answer") or ""
                    elif ev.get("type") == "error":
                        failed = True
                    yield _sse(ev)
        except asyncio.CancelledError:
            # 客户端断开/中止：正常结束生成器，不落库
            raise
        except Exception as e:
            logger.exception("❌ /api/chat 流式处理异常")
            failed = True
            yield _sse({"type": "error", "message": f"服务内部错误：{e}"})
        finally:
            # 完整结束才写入历史（中止/出错不写，保持历史干净成对）
            if answer is not None and answer.strip() and not failed:
                add_message(user["id"], "user", req.query)
                add_message(user["id"], "assistant", answer.strip())

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
