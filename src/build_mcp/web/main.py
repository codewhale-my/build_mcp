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
import base64
import json
import logging
import os
import secrets
import shutil
import subprocess
import time
import re
from contextlib import asynccontextmanager, AsyncExitStack
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, Any, Optional

from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    Depends,
    WebSocket,
    WebSocketDisconnect,
    UploadFile,
    File as FastFile,
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from build_mcp.client.conversation import (
    agent_loop_stream,
    init_all_mcp_sessions,
    list_llm_models,
    resolve_llm_spec,
    SYSTEM_PROMPT,
)
from build_mcp.client.conversation import (
    StdioServerParameters,
    stdio_client,
    mcp_tool_to_openai_function,
)
from mcp.client.session import ClientSession
from build_mcp.web import store
from build_mcp.web.whatsnew import latest_version, payload_for
from build_mcp.web.store import (
    init_db,
    get_user_by_id,
    get_user_by_name,
    create_user,
    find_invite,
    consume_invite,
    normalize_code,
    add_message,
    list_messages,
    recent_llm_messages,
    clear_messages,
    user_filesystem_dir,
    set_user_seen_version,
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

# ================== 用户本机文件桥(浏览器 File System Access) ==================
# user_id -> {"ws": WebSocket|None, "pending": {fsop_id: asyncio.Future}}
_user_ws: Dict[int, dict] = {}
_fsop_seq = 0            # fsop 消息自增 id
FSOP_TIMEOUT = 40        # 等待浏览器执行文件操作的最长秒数
FSOP_MAX_TEXT = 500_000  # read 文本最大字节(超出截断并提示)

# user_filesystem 工具的 OpenAI function 定义(挂到每个 chat 请求的工具列表)
USER_FS_TOOL_DEF = {
    "type": "function",
    "function": {
        "name": "user_filesystem",
        "description": (
            "操作【用户已授权】的本地电脑文件夹（浏览器端执行，只能访问用户亲手授权的那一个目录）。"
            "用于用户要求读取/修改自己电脑上的文件（如“读我电脑上的笔记.txt”“把这份报告存到我授权的文件夹”）。"
            "path 一律用相对该授权根目录的路径，正斜杠分隔，如 '笔记/日报.md'；对目录本身用 path='' 或目录名。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "op": {"type": "string", "enum": ["list", "read", "write"]},
                "path": {"type": "string", "description": "相对授权目录的路径；目录操作给目录路径"},
                "content": {"type": "string", "description": "write 时的文本内容"},
            },
            "required": ["op", "path"],
        },
    },
}


async def _fsop_request(user: dict, op: str, path: str, content: str = "") -> dict:
    """向该用户的浏览器发一次文件操作请求并等待结果。返回 {ok, data|error}。"""
    conn = _user_ws.get(user["id"])
    if not conn or not conn.get("ws"):
        return {"ok": False, "error": "浏览器未连接本机文件通道(可能未登录或页面已关)。请保持页面打开后重试。"}
    ws: WebSocket = conn["ws"]
    global _fsop_seq
    _fsop_seq += 1
    fid = _fsop_seq
    fut: asyncio.Future = asyncio.get_event_loop().create_future()
    conn["pending"][fid] = fut
    try:
        await ws.send_json({"type": "fsop", "id": fid, "op": op, "path": path or "", "content": content or ""})
    except Exception:
        conn["pending"].pop(fid, None)
        return {"ok": False, "error": "向浏览器发送文件操作失败(连接可能已断开)，请刷新页面重试。"}
    try:
        res = await asyncio.wait_for(fut, timeout=FSOP_TIMEOUT)
    except asyncio.TimeoutError:
        return {"ok": False, "error": f"浏览器 {FSOP_TIMEOUT}s 未响应文件操作。若弹出了授权确认，请点击允许；否则检查“本机文件”是否已授权。"}
    finally:
        conn["pending"].pop(fid, None)
    return res


class _UserFsShim:
    """把 user_filesystem 工具调用桥到浏览器；鸭子类型对齐 MCP ClientSession.call_tool。

    agent_loop_stream 只依赖 session.call_tool(name, arguments) 返回
    result.content[0].text，因此可以无侵入地挂进 tool_name_to_session。
    """

    def __init__(self, user: dict):
        self.user = user

    async def call_tool(self, name: str, arguments: dict):
        op = str(arguments.get("op") or "list").strip().lower()
        path = str(arguments.get("path") or "").strip()
        content = str(arguments.get("content") or "")
        # 路径清洗：反斜杠转正斜杠、去掉开头的 / 与盘符，禁止 ..
        path = path.replace("\\", "/").lstrip("/")
        parts = [p for p in path.split("/") if p and p not in (".", "..")]
        # 去掉可能的盘符前缀 C: D:
        if parts and len(parts[0]) == 2 and parts[0][1] == ":":
            parts = parts[1:]
        clean = "/".join(parts)
        if op not in ("list", "read", "write"):
            text = f"不支持的操作：{op}(可选 list/read/write)"
        else:
            res = await _fsop_request(self.user, op, clean, content)
            if res.get("ok"):
                text = res.get("data", "完成")
            else:
                text = "错误：" + res.get("error", "未知错误")
        return SimpleNamespace(content=[SimpleNamespace(text=text)])

_bearer = HTTPBearer(auto_error=False)  # 不自动报错，由我们统一返回 401


def _client_ip(request: Request) -> str:
    """取客户端 IP：优先 x-forwarded-for（cpolar/nginx 等反代场景），否则取直连地址。"""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


# ============ 用户浏览器精确定位(Geolocation) → 文字地址 ============
# 前端授权定位后会上报经纬度；这里复用 MCP 服务同一份高德 key 做逆地理编码，
# 直接把地址注入对话上下文，模型无需再为此多调一次工具。
_amap_sdk = None


def _get_amap_sdk():
    """惰性构建一个高德 SDK（只用于逆地理编码）；失败返回 None，不影响主流程。"""
    global _amap_sdk
    if _amap_sdk is None:
        try:
            from build_mcp.common.config import load_config
            from build_mcp.services.gd_sdk import GdSDK

            cfg = load_config("config.yaml")
            # 单独给一个只报 warn 的 logger：gd_sdk 会把每次请求+响应都记 info，
            # 直接挂到 web logger 上会把 web.log 刷满高德返回体
            quiet = logging.getLogger("build_mcp.amap_quiet")
            quiet.setLevel(logging.WARNING)
            _amap_sdk = GdSDK(
                config={
                    "base_url": "https://restapi.amap.com",
                    "api_key": cfg.get("api_key", ""),
                    "max_retries": 1,
                    "retry_delay": 0.5,
                },
                logger=quiet,
            )
        except Exception:
            logger.exception("初始化高德 SDK 失败（仅影响精确定位的地址解析）")
            _amap_sdk = False
    return _amap_sdk or None


async def _reverse_geocode(lng: float, lat: float) -> str:
    """经纬度 → 文字地址；任何异常都返回空串，绝不阻断对话。"""
    sdk = _get_amap_sdk()
    if not sdk:
        return ""
    try:
        res = await sdk.regeo(f"{lng:.6f},{lat:.6f}")
    except Exception as e:
        logger.warning("逆地理编码失败: %s", e)
        return ""
    if isinstance(res, dict) and isinstance(res.get("formatted_address"), str):
        return res["formatted_address"]
    return ""


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


def _user_from_token(tok: str) -> dict:
    """校验 token 值，返回用户信息；非法/过期则 401。供 header 与查询参数两条通道共用。"""
    rec = _tokens.get(tok)
    now = time.time()
    if rec is None:
        raise HTTPException(status_code=401, detail="登录已失效，请重新登录")
    if rec["exp"] < now:
        _tokens.pop(tok, None)
        raise HTTPException(status_code=401, detail="登录已过期，请重新登录")
    user = get_user_by_id(rec["uid"])
    if user is None:
        _tokens.pop(tok, None)
        raise HTTPException(status_code=401, detail="账号不存在，请重新登录")
    return {"id": user["id"], "username": user["username"], "token": tok}


def require_user(credentials: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> dict:
    """FastAPI 依赖：校验 Bearer token，返回当前用户 {id, username}，非法则 401。"""
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail="未登录",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return _user_from_token(credentials.credentials)


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


class GeoFix(BaseModel):
    """浏览器 Geolocation 上报的坐标（用户授权后前端才带）。"""
    lat: float
    lng: float
    acc: Optional[float] = None   # 精度（米）


class ChatRequest(BaseModel):
    query: str
    model: str = ""   # 前端选择的模型 key（见 config.yaml 的 llm_models）；空=用默认
    geo: Optional[GeoFix] = None   # 用户已授权精确定位时的坐标，优先于 IP 定位


class SeenRequest(BaseModel):
    """把某次更新标记为"已看过"。version 留空则用当前最新版本。"""
    version: str = ""


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
    code = normalize_code(req.invite)
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
    return {
        "ok": True,
        "username": user["username"],
        "user_root": str(user_filesystem_dir(user["username"], user["id"]).resolve()),
    }


@app.get("/api/models")
async def models(user: dict = Depends(require_user)):
    """可选回答模型清单（前端下拉用）：默认项 + 每个模型的 key/label/model/thinking。"""
    return list_llm_models()


@app.get("/api/whatsnew")
async def whatsnew(user: dict = Depends(require_user)):
    """
    更新说明：返回全部条目 + 该用户尚未看过的新条目。

    是否自动弹窗由 should_show 决定（登录后前端拿它判断），
    看过与否按用户维度记在 users.last_seen_version（跨设备一致，清缓存也不重复弹）。
    """
    row = get_user_by_id(user["id"]) or {}
    return payload_for(row.get("last_seen_version") or "")


@app.post("/api/whatsnew/seen")
async def whatsnew_seen(req: SeenRequest, user: dict = Depends(require_user)):
    """把更新标记为已看过（用户关掉弹窗时调用），下次登录不再弹。"""
    version = (req.version or "").strip() or latest_version()
    if version:
        set_user_seen_version(user["id"], version)
    return {"ok": True, "version": version}


@app.get("/api/history")
async def history(user: dict = Depends(require_user), limit: int = 200):
    """当前用户的历史消息（只返回自己的）。"""
    limit = max(1, min(limit, 500))
    msgs = list_messages(user["id"], limit=limit)
    return {
        "username": user["username"],
        "user_root": str(user_filesystem_dir(user["username"], user["id"]).resolve()),
        "messages": [
            {"id": m["id"], "role": m["role"], "text": m["text"], "ts": m["ts"],
             "model": m.get("model") or ""}
            for m in msgs
        ],
    }


@app.delete("/api/history")
async def history_clear(user: dict = Depends(require_user)):
    """清空当前用户的历史消息。"""
    clear_messages(user["id"])
    return {"ok": True}


def _safe_user_file(user: dict, path: str) -> Path:
    """把请求的相对/绝对路径解析到用户沙箱内的文件；越权/穿越一律 403。"""
    base = user_filesystem_dir(user["username"], user["id"]).resolve()
    raw = Path(path or "")
    cand = (base / raw).resolve() if not raw.is_absolute() else raw.resolve()
    if cand != base and base not in cand.parents:
        raise HTTPException(status_code=403, detail="禁止访问工作空间以外的文件")
    if not cand.is_file():
        raise HTTPException(status_code=404, detail="文件不存在")
    return cand


# ================== 图片 OCR：让纯文本模型也能"读到"图片里的文字 ==================
IMG_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".gif", ".tiff", ".tif"}
OCR_LANGS = "chi_sim+eng"
OCR_TIMEOUT = 40          # 单张图片识别上限秒数
OCR_MAX_CHARS = 1200      # 注入提示词的单图文本上限

def _ocr_image(path: Path) -> str:
    """调用 tesseract 识别图片文字；未安装 / 失败 / 超时一律返回空串。"""
    if not shutil.which("tesseract"):
        return ""
    try:
        r = subprocess.run(
            ["tesseract", str(path), "stdout", "-l", OCR_LANGS, "--psm", "6"],
            capture_output=True, text=True, timeout=OCR_TIMEOUT,
        )
        return (r.stdout or "").strip()
    except Exception:
        return ""

def _ocr_sidecar(path: Path) -> Path:
    return path.with_name(path.name + ".ocr.txt")

def _image_ocr_text(path: Path) -> str:
    """取图片 OCR 文本：优先读缓存 sidecar，没有则现识别并落盘缓存。"""
    side = _ocr_sidecar(path)
    try:
        if side.is_file():
            return side.read_text(encoding="utf-8", errors="ignore").strip()
    except Exception:
        pass
    text = _ocr_image(path)
    if text:
        try:
            side.write_text(text, encoding="utf-8")
        except Exception:
            pass
    return text

async def _image_note_for_query(user: dict, query: str) -> str:
    """
    扫描用户消息里的 [图片: 路径] 引用，把图片 OCR 文本注入系统提示词，
    并禁止模型去 read_media_file / 终端读图片二进制（纯文本模型只会拿到乱码）。
    """
    refs = re.findall(r"\[图片:\s*([^\]]+?)\s*\]", query or "")
    if not refs:
        return ""
    items = []
    for raw in refs[:3]:
        p = raw.strip()
        try:
            f = _safe_user_file(user, p)
        except HTTPException:
            items.append(f"- {p}：文件不存在或不在你的工作空间")
            continue
        if f.suffix.lower() not in IMG_EXTS:
            items.append(f"- {f.name}：不是常见图片格式")
            continue
        text = await asyncio.to_thread(_image_ocr_text, f)
        if text:
            ell = "…" if len(text) > OCR_MAX_CHARS else ""
            items.append(f"- {f.name}，OCR 识别到 {len(text)} 字：\n「{text[:OCR_MAX_CHARS]}{ell}」")
        else:
            items.append(f"- {f.name}：没有识别出文字（可能是照片/画面类图片，而非文字截图）")
    if not items:
        return ""
    return (
        "\n\n[图片内容（系统自动注入）] 用户在消息中引用了图片。当前模型无法直接观看图片画面，"
        "回答涉及图片时请完全依据下面的 OCR 文本，并严格遵守：\n"
        "1. 禁止调用 read_media_file / read_file / get_file_info / 终端命令去读取图片本体——"
        "图片是二进制文件，读出来只会是乱码，浪费工具调用；\n"
        "2. OCR 文本里有答案就直接回答，不要复述本段系统说明；\n"
        "3. 若 OCR 文本为空或与问题无关（图片是照片、画面而非文字截图），"
        "请坦诚告知用户：你目前只能读出图片里的文字，看不到画面内容。\n"
        + "\n".join(items)
    )


@app.get("/api/files/download")
async def file_download(
    path: str,
    token: str = "",
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
):
    """下载自己工作空间里的文件（仅限本用户沙箱内，自动带附件下载头）。

    token 支持两种携带方式：Authorization: Bearer（fetch 用）或 ?token=（<a href> 新标签页导航用）。
    """
    tok = ""
    if credentials is not None:
        tok = credentials.credentials
    elif token:
        tok = token
    if not tok:
        raise HTTPException(
            status_code=401,
            detail="未登录",
            headers={"WWW-Authenticate": "Bearer"},
        )
    user = _user_from_token(tok)
    f = _safe_user_file(user, path)
    return FileResponse(f, filename=f.name, content_disposition_type="attachment")


@app.post("/api/uploads")
async def upload_file(file: UploadFile = FastFile(...), user: dict = Depends(require_user)):
    """移动端/不支持目录授权的浏览器：把用户选择的本地文件存入其服务器工作空间 _uploads/。

    返回 {path}（相对 user_root），AI 随后可用 filesystem 工具读取。
    """
    base = user_filesystem_dir(user["username"], user["id"])
    up = base / "_uploads"
    up.mkdir(parents=True, exist_ok=True)
    name = (file.filename or "file").replace("\\", "/").split("/")[-1]
    # 文件名字符清洗：只保留安全字符集
    import re as _re
    name = _re.sub(r"[^\w\u4e00-\u9fa5.\-]", "_", name)[-80:]
    if not name:
        name = "file"
    dest = up / name
    n = 0
    while dest.exists():
        n += 1
        stem, _, ext = name.rpartition(".")
        dest = up / f"{stem}_{n}.{ext}" if ext else up / f"{name}_{n}"
    size = 0
    with open(dest, "wb") as fh:
        while True:
            chunk = await file.read(1024 * 256)
            if not chunk:
                break
            fh.write(chunk)
            size += len(chunk)
    rel = str(dest.relative_to(base))
    logger.info("📤 用户[%s] 上传 %s (%d bytes) -> %s", user["username"], name, size, rel)
    return {"ok": True, "name": name, "path": rel, "size": size}


def _sse(event: dict) -> str:
    """把事件 dict 打包成一条 SSE 帧（data: {...}），中文不转义。"""
    return "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    """浏览器↔后端长连接：承接 fsop 请求与结果回传。token 经查询参数携带。"""
    token = websocket.query_params.get("token", "")
    try:
        user = _user_from_token(token)
    except HTTPException:
        await websocket.close(code=4401)
        return
    uid = user["id"]
    await websocket.accept()
    conn = _user_ws.setdefault(uid, {"ws": None, "pending": {}})
    # 多标签页时以最新连接为准
    conn["ws"] = websocket
    logger.info("🔌 用户[%s] 本机文件通道已连接", user["username"])
    try:
        while True:
            msg = await websocket.receive_json()
            if msg.get("type") == "fsop_result":
                fid = msg.get("id")
                fut = conn["pending"].get(fid)
                if fut and not fut.done():
                    fut.set_result({
                        "ok": bool(msg.get("ok")),
                        "data": msg.get("data", ""),
                        "error": msg.get("error", ""),
                    })
            # 其余消息(心跳 pong 等)忽略
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("ws 通道异常(用户 %s)", user["username"])
    finally:
        if conn.get("ws") is websocket:
            conn["ws"] = None
        # 未决的 fsop 全部按失败收尾，避免挂起
        for fut in conn["pending"].values():
            if not fut.done():
                fut.set_result({"ok": False, "error": "浏览器连接已断开"})
        conn["pending"].clear()
        logger.info("🔌 用户[%s] 本机文件通道已断开", user["username"])


@app.post("/api/chat")
async def chat(req: ChatRequest, request: Request, user: dict = Depends(require_user)):
    global shared_mcp
    if not shared_mcp or not shared_mcp.get("tool_name_to_session"):
        raise HTTPException(status_code=500, detail="MCP服务尚未就绪，请稍后再试")

    # 用户独立的 filesystem 会话（首次自动拉起）
    fs = await ensure_user_fs(user)
    tool_map = dict(shared_mcp["tool_name_to_session"])
    for name in fs["names"]:
        tool_map[name] = fs["session"]
    # 用户本机文件工具(浏览器授权目录)：shim 伪装成 session 无侵入接入
    tool_map["user_filesystem"] = _UserFsShim(user)
    openai_tools = shared_mcp["openai_tools"] + fs["openai_tools"] + [USER_FS_TOOL_DEF]

    # 带上该用户最近 8 轮问答作为上下文（服务端持久化历史）
    user_dir = user_filesystem_dir(user["username"], user["id"])
    sys_note = (
        "\n\n[文件交付约定] 当你在用户的专属工作空间里生成或保存文件时，"
        "请在回复正文中写出该文件的完整路径（形如："
        f"{user_dir}/文件名.ext，从根目录写到扩展名）。"
        "前端会把回复中出现的这个路径自动变成可点击的下载链接，"
        "让用户点击即可把文件保存到自己的手机/电脑。"
        "\n\n[本机文件工具 user_filesystem] 仅当用户明确要求操作“自己电脑/本地”的文件时使用"
        "（如“读我电脑上的 xxx”“把结果存到我的本地文件夹”）。它操作的是用户浏览器里亲手授权的那个目录："
        "path 用相对该目录的路径，正斜杠；目录列表用 op=list + 目录路径；读文本 op=read；"
        "写入/覆盖 op=write + content。不要把服务器工作空间路径传给该工具，两者无关。"
        "如果返回“未授权/浏览器未连接”之类错误，告诉用户点击页面顶部的“本机文件”按钮授权后重试。"
    )
    # 用户公网 IP：locate_ip 不传参会用"发起请求方"的 IP——即服务器自己(机房 IP 高德定位不出结果)，
    # 所以必须把用户真实公网 IP 注入上下文，让 AI 显式传给工具
    client_ip = _client_ip(request)
    ip_note = ""
    if client_ip and client_ip != "unknown":
        ip_note = (
            f"\n\n[用户网络上下文] 当前用户的公网 IP 是 {client_ip}（IP 定位一般只精确到城市级，运营商出口可能有偏差）。"
            "当用户询问“我在哪 / 我的位置 / 我附近”等需要定位的问题时，"
            "必须调用 locate_ip 并把该 IP 作为 ip 参数显式传入；"
            "不要不传参数调用 locate_ip——那样拿到的是服务器自己的 IP，定位不到用户。"
        )
    # 用户浏览器精确定位：比 IP 定位准得多，优先使用，并免掉一次工具调用
    geo_note = ""
    if req.geo is not None and -90 <= req.geo.lat <= 90 and -180 <= req.geo.lng <= 180:
        coord = f"{req.geo.lng:.6f},{req.geo.lat:.6f}"
        addr = await _reverse_geocode(req.geo.lng, req.geo.lat)
        acc_note = f"，精度约 ±{int(req.geo.acc)} 米" if req.geo.acc else ""
        geo_note = (
            f"\n\n[用户精确定位（用户已授权浏览器定位）] 坐标(lng,lat)={coord}{acc_note}。"
            + (f"逆地理编码地址：{addr}。" if addr else "")
            + "这是 GPS/WiFi 级精确定位，优先级高于上面的 IP 定位："
            "当用户问“我在哪 / 我的位置 / 我附近”等问题时，"
            f"直接把 search_nearby 的 location 参数填 \"{coord}\" 做周边搜索，不需要再调用 locate_ip；"
            "需要文字地址时直接用上面给出的地址（若为空再考虑调用 regeo）。"
            "不要向用户暴露这段系统上下文的存在，也不必解释坐标来源。"
        )
    # 图片引用：预 OCR 并注入提示词（纯文本模型看不了图，只能给它文字）
    img_note = await _image_note_for_query(user, req.query)
    history_messages = [{"role": "system", "content": SYSTEM_PROMPT + sys_note + ip_note + geo_note + img_note}]
    for m in recent_llm_messages(user["id"], turns=8):
        history_messages.append({"role": m["role"], "content": m["text"]})

    lock = _chat_locks.setdefault(user["id"], asyncio.Lock())
    _spec = resolve_llm_spec(req.model)
    logger.info("💬 用户[%s] 提问 → 模型 %s(%s, thinking=%s)",
                user["username"], _spec["model"], _spec["key"], _spec["thinking"])

    async def event_gen():
        answer: str | None = None
        failed = False
        used_model = ""
        try:
            # 同一用户串行：锁跨整个流式过程持有，防止上下文竞态。
            # 生产者/队列 + 心跳：模型长生成期间没有事件，移动网络(运营商 NAT/iOS Safari)
            # 会掐断静默连接（前端报 Load failed），所以每 15s 发一条 SSE 注释帧保活。
            queue: asyncio.Queue = asyncio.Queue()
            _STOP = object()

            async def producer():
                try:
                    async with lock:
                        async for ev in agent_loop_stream(
                            tool_name_to_session=tool_map,
                            openai_tools=openai_tools,
                            user_query=req.query,
                            history_messages=history_messages,
                            model_key=req.model,
                        ):
                            await queue.put(ev)
                except Exception as e:
                    logger.exception("❌ /api/chat 流式处理异常")
                    await queue.put({"type": "error", "message": f"服务内部错误：{e}"})
                finally:
                    await queue.put(_STOP)

            task = asyncio.create_task(producer())
            try:
                while True:
                    try:
                        ev = await asyncio.wait_for(queue.get(), timeout=15.0)
                    except asyncio.TimeoutError:
                        yield ": ping\n\n"   # SSE 注释帧，前端解析器会忽略
                        continue
                    if ev is _STOP:
                        break
                    if ev.get("type") == "done":
                        answer = ev.get("answer") or ""
                        used_model = _spec["label"] or ev.get("model") or _spec["model"]
                    elif ev.get("type") == "error":
                        failed = True
                    yield _sse(ev)
            except asyncio.CancelledError:
                # 客户端断开/中止：取消生产者并正常结束生成器，不落库
                task.cancel()
                raise
            finally:
                if not task.done():
                    task.cancel()
        except Exception as e:
            logger.exception("❌ /api/chat 流式处理异常")
            failed = True
            yield _sse({"type": "error", "message": f"服务内部错误：{e}"})
        finally:
            # 完整结束才写入历史（中止/出错不写，保持历史干净成对）
            if answer is not None and answer.strip() and not failed:
                add_message(user["id"], "user", req.query)
                add_message(user["id"], "assistant", answer.strip(), used_model)

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
