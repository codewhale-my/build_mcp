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
    _tool_result_to_text,
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
    history_window_info,
    clear_messages,
    user_filesystem_dir,
    set_user_seen_version,
    set_user_ws_mode,          # 切换服务器端文件空间模式（/api/workspace 用）
    USERNAME_RE,
    verify_password,
)

logger = logging.getLogger(__name__)

# ====================== 鉴权配置 ======================
# Web 只启动共享服务（地图/搜索/终端）；filesystem 按用户独立拉起，见 ensure_user_fs()
SHARED_MCP_INCLUDE = ["amap", "websearch", "terminal"]

# ====================== 服务器操作权限（管理员名单） ======================
# 只有名单内的用户能拿到 terminal 工具（= 能在服务器上执行命令、改程序代码）。
# 其余用户仍可正常对话、使用地图/搜索/自己的文件沙箱，但没有 shell。
# 名单可用环境变量 MCP_WEB_ADMINS 覆盖（逗号分隔，大小写不敏感）。
ADMIN_USERS = {
    u.strip()
    for u in os.environ.get("MCP_WEB_ADMINS", "yanghj").split(",")
    if u.strip()
}


def is_admin(user: dict | None) -> bool:
    """是否为管理员（拥有服务器操作权限）。

    用户名【精确匹配、区分大小写】：注册时用户名已 strip，SQLite 的 UNIQUE 也区分
    大小写，所以 "YANGHJ"、"yanghj " 这类仿冒名不会被误判为管理员（否则等于提权）。
    """
    if not user:
        return False
    return (user.get("username") or "") in ADMIN_USERS


def terminal_tool_names(sessions, tool_name_to_session) -> set:
    """挑出归属 terminal 服务的工具名。

    按「工具属于哪个 MCP 会话」判定，而不是猜名字前缀——这样以后 terminal
    服务增删工具、或换成别的终端实现，权限闸门都不会漏掉。
    """
    term_sessions = [s.get("session") for s in (sessions or []) if s.get("name") == "terminal"]
    if not term_sessions:
        return set()
    return {
        name
        for name, sess in (tool_name_to_session or {}).items()
        if any(sess is ts for ts in term_sessions)
    }


def filter_shared_tools(user: dict, tool_map: dict, tool_defs: list, sessions) -> tuple:
    """按权限过滤共享工具：非管理员剔除 terminal 工具集。

    返回 (过滤后的 tool_map, 过滤后的 tool_defs, 被剔除的工具名集合)。
    """
    if is_admin(user):
        return dict(tool_map), list(tool_defs), set()
    blocked = terminal_tool_names(sessions, tool_map)
    if not blocked:
        return dict(tool_map), list(tool_defs), set()
    kept_map = {k: v for k, v in tool_map.items() if k not in blocked}
    kept_defs = [t for t in tool_defs if (t.get("function") or {}).get("name") not in blocked]
    return kept_map, kept_defs, blocked


# ====================== 云服务器工作空间（仅管理员） ======================
# 管理员的文件空间有两种根目录可选：
#   local  = 自己的个人工作空间（默认，与所有用户一致）
#   server = 整台云服务器（根目录 /），也就是 AI 自己所在的这台机器
# 切到 server 后，filesystem 工具直接读写整机文件，不必再靠 terminal 一条条 cat/sed。
# /proc、/sys、/dev、/run 是虚拟文件系统（不是真实磁盘内容，遍历会拖垮服务），单独排除。
# AI 自己那份代码目录见 AI_CODE_DIR，只用作 terminal_run 的默认 cwd。
# 非管理员永远只能是 local（在 /api/workspace 与 user_ws_mode 两处强制）。
SERVER_WS_ROOT = Path(os.environ.get("MCP_WEB_SERVER_ROOT", "/")).resolve()
# AI 自己那份代码目录：terminal_run 的默认工作目录（省得每条命令都 cd）。
AI_CODE_DIR = Path(
    os.environ.get("MCP_WEB_AI_CODE_DIR", str(Path.home() / "build-mcp"))
).resolve()
# 虚拟文件系统排除名单：不是真实磁盘内容，遍历/递归会拖垮服务（/proc 下还有会阻塞的伪文件）。
VIRTUAL_FS_EXCLUDES = tuple(Path(p) for p in ("/proc", "/sys", "/dev", "/run"))
WS_MODE_LOCAL = "local"
WS_MODE_SERVER = "server"


def _is_virtual_fs(p: Path) -> bool:
    """是否为虚拟文件系统（/proc、/sys、/dev、/run 及其子路径）。"""
    return any(p == v or v in p.parents for v in VIRTUAL_FS_EXCLUDES)


def user_ws_mode(user: dict | None) -> str:
    """该用户当前的文件空间模式；非管理员一律 local（防越权）。"""
    if not is_admin(user):
        return WS_MODE_LOCAL
    return WS_MODE_SERVER if (user.get("ws_mode") or "").strip().lower() == WS_MODE_SERVER else WS_MODE_LOCAL


def workspace_root(user: dict) -> Path:
    """该用户服务器端文件空间的根目录。"""
    if user_ws_mode(user) == WS_MODE_SERVER:
        return SERVER_WS_ROOT
    return user_filesystem_dir(user["username"], user["id"]).resolve()


def allowed_roots(user: dict) -> list:
    """HTTP 接口（上传/下载/图片）允许访问的服务器目录白名单。

    个人工作空间恒在列——上传的文件都落在那里，切到云服务器后仍要能取用；
    整台服务器（/）只在管理员切到 server 模式时加入。
    """
    roots = [user_filesystem_dir(user["username"], user["id"]).resolve()]
    if user_ws_mode(user) == WS_MODE_SERVER:
        roots.append(SERVER_WS_ROOT)
    return roots


# ====================== terminal_run：一次调用跑完一条命令 ======================
# 原生 terminal 需要 create/type/wait_for/read_output 四次往返，每一步都会被写进上下文，
# 来回复发造成平方级膨胀。这里把它合成一次调用（并带出退出码），显著省时省 token。
TERMINAL_RUN_TOOL_DEF = {
    "type": "function",
    "function": {
        "name": "terminal_run",
        "description": (
            "在服务器终端里执行一条命令并直接拿到输出（内部一步完成“创建/复用会话 → 发送命令 → "
            "等待结束 → 读取输出”，并返回退出码）。适合非交互命令：ls / cat / grep / git / pip / "
            "systemctl / 跑脚本等。返回里带有会话 id，下一次调用传同一个 session_id 即可复用"
            "（保留当前目录、环境变量、已激活的 venv）。"
            "交互式程序（vim / htop、需要 y/n 确认的提示）不要用它，改用 terminal_type + "
            "terminal_press_key + terminal_read_output。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "要执行的命令；可用 ; 或 && 串联多条"},
                "session_id": {"type": "string", "description": "复用已有终端会话；留空则复用/新建默认会话"},
                "timeout_ms": {"type": "integer", "description": "等待命令结束的最长毫秒数，默认 30000"},
                "cwd": {"type": "string", "description": "新建会话时的工作目录，默认服务器代码目录"},
            },
            "required": ["command"],
        },
    },
}

# shell 提示符，如 admin@iZ2vcezcg3xbqlrqq7dh82Z:~/build-mcp$
_PROMPT_RE = re.compile(
    r"^[^\s@]{1,64}@[^\s:]{1,64}:[^\n$#]{0,160}[$#]\s?"   # admin@host:~/dir$
    r"|^[$#]\s"                                              # 裸提示符 $
)


def _text_result(text: str) -> SimpleNamespace:
    """构造一个和 MCP call_tool 返回结构一致的鸭子对象（供 shim 复用）。"""
    return SimpleNamespace(content=[SimpleNamespace(text=text)])


def _json_field(text: str, key: str) -> str:
    """从工具返回的 JSON 文本里取字段；解析失败时退回正则抓取。"""
    try:
        data = json.loads(text or "")
        if isinstance(data, dict):
            return str(data.get(key) or "")
    except Exception:
        pass
    m = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]+)"', text or "")
    return m.group(1) if m else ""


def _drop_typed_echo(lines: list, payload: str) -> list:
    """兜底：掉开头那段终端对「刚敲进去内容」的原始回显（长度恰为送入行数）。"""
    typed = [l for l in (payload or "").splitlines() if l.strip()]
    if typed and len(lines) >= len(typed):
        head = lines[: len(typed)]
        if all((h.strip() == "" or h.strip() in (payload or "")) for h in head):
            return lines[len(typed):]
    return lines


def _clean_run_output(raw: str, sentinel: str, payload: str, start_mark: str = "") -> tuple:
    """从终端**整屏累积内容**里剥出「本次这条命令」的输出，并解析退出码。

    返回 (清理后的输出, 退出码或 None)。

    read_output 给的是整个会话的屏幕缓冲（含此前所有命令的输出），所以必须先用本次
    调用独有的 start_mark 把起点定住，再用哨兵把终点截断；只在两端剥掉回显行，
    中间的内容一律保留（避免误删合法输出）。
    """
    raw = raw or ""
    payload_lines = {l.strip() for l in (payload or "").splitlines() if l.strip()}

    lines = None
    if start_mark:
        idx = raw.rfind(start_mark)
        if idx >= 0:
            lines = raw[idx + len(start_mark):].splitlines()
    if lines is None:
        # 没有 start_mark，或屏幕滚动把标记冲掉了：退回「掉开头回显」的近似做法
        lines = _drop_typed_echo(raw.splitlines(), payload)

    # 终点：优先命中「哨兵 + 退出码 且到行尾」的那一行
    rc = None
    cut = len(lines)
    for i, l in enumerate(lines):
        m = re.search(re.escape(sentinel) + r"(-?\d+)\s*$", l)
        if m:
            rc = int(m.group(1))
            cut = i
            break
    else:
        for i, l in enumerate(lines):
            if sentinel in l:
                cut = i
                break

    cleaned = [_PROMPT_RE.sub("", l) for l in lines[:cut]]
    # 两端剥掉回显/标记行：头部是命令本体（可能多行），尾部是哨兵命令自身
    while cleaned and (not cleaned[0].strip() or cleaned[0].strip() in payload_lines):
        cleaned.pop(0)
    while cleaned and (not cleaned[-1].strip() or cleaned[-1].strip() in payload_lines):
        cleaned.pop()
    text = "\n".join(cleaned).strip("\n")
    return text, rc


class _TerminalRunShim:
    """把「发命令 → 等它跑完 → 读输出」合成一次工具调用。

    内部只调用 terminal 服务的原生工具，权限模型因此完全复用：本 shim 只在
    is_admin(user) 成立时才挂进 tool_map，非管理员既看不到、也调不到。
    """

    def __init__(self, session, default_cwd: str):
        self.session = session
        self.default_cwd = default_cwd
        self.default_sid = ""

    async def _call(self, name: str, arguments: dict) -> str:
        return _tool_result_to_text(await self.session.call_tool(name, arguments=arguments))

    async def _alive(self, sid: str) -> bool:
        """会话是否还活着（列表里能看到它）。"""
        if not sid:
            return False
        try:
            return sid in await self._call("terminal_session_list", {})
        except Exception:
            return False

    async def call_tool(self, name: str, arguments: dict):
        cmd = str(arguments.get("command") or "").strip()
        if not cmd:
            return _text_result("错误：command 不能为空")
        try:
            timeout_ms = int(arguments.get("timeout_ms") or 30000)
        except Exception:
            timeout_ms = 30000
        timeout_ms = max(1000, min(timeout_ms, 300000))
        cwd = str(arguments.get("cwd") or "").strip() or self.default_cwd
        sid = str(arguments.get("session_id") or "").strip() or self.default_sid

        if not await self._alive(sid):
            raw = await self._call("terminal_session_create", {
                "command": "bash", "cwd": cwd,
                "dimensions": {"rows": 60, "cols": 200},
            })
            sid = _json_field(raw, "session_id")
            if not sid:
                return _text_result("错误：创建终端会话失败：" + (raw or "")[:300])
        self.default_sid = sid

        # 起点/终点各放一个本次调用独有的标记：read_output 返回的是整屏累积内容，
        # 没有起点标记就无法把「本次命令的输出」与此前命令的输出区分开。
        start_mark = "__HJ_RUN_" + secrets.token_hex(4) + "_START__"
        sentinel = "__HJ_RUN_" + secrets.token_hex(4) + "__"
        payload = f"echo {start_mark}\n{cmd}\n__HJ_RC=$?; echo {sentinel}$__HJ_RC\n"
        await self._call("terminal_type", {"session_id": sid, "text": payload})
        waited = await self._call("terminal_wait_for", {
            "session_id": sid, "text": sentinel, "timeout_ms": timeout_ms,
        })
        finished = sentinel in (waited or "")
        raw_out = await self._call("terminal_read_output", {"session_id": sid, "max_bytes": 200000})
        out, rc = _clean_run_output(raw_out, sentinel, payload, start_mark)

        head = f"[会话 {sid}]"
        if rc is not None:
            head += f" [退出码 {rc}]"
        elif not finished:
            head += (f" [仍在运行] 命令在 {timeout_ms / 1000:.0f}s 内未结束，以下是当前输出；"
                     f"如需继续观察，再调一次并传 session_id={sid}")
        else:
            head += " [未取到退出码]"
        return _text_result(head + "\n" + (out or "(无输出)"))




def _check_admin_accounts() -> None:
    """启动时核对管理员名单：账号若不存在就告警——否则该用户名可能被他人抢注。"""
    for name in sorted(ADMIN_USERS):
        try:
            if get_user_by_name(name) is None:
                logger.warning(
                    "⚠️ 管理员账号[%s]尚未注册：任何人凭邀请码注册该用户名即可获得"
                    "服务器操作权限，请尽快注册。", name,
                )
        except Exception:
            logger.exception("核对管理员账号[%s]失败", name)


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
_user_fs_cache: Dict[tuple, dict] = {}
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
    return {
        "id": user["id"],
        "username": user["username"],
        "ws_mode": user.get("ws_mode") or WS_MODE_LOCAL,
        "token": tok,
    }


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


class _FsGuardShim:
    """整机文件空间的护栏：只挡「虚拟文件系统」和「从 / 全盘递归」。

    仅在 SERVER_WS_ROOT 覆盖到整机（/）时才有实际拦截；其它情况是直通代理。
    目的**不是**缩小管理员的权限（管理员本就该有整机权限），而是防止一次
    list_directory("/") 或 search_files("/") 把服务拖死：/proc、/sys 不是真实
    磁盘内容（/proc 下还有读一下就会阻塞的伪文件），从 / 递归则要遍历整块磁盘。
    被拦时返回一段给模型看的说明，让它自己改成更具体的路径。
    """

    _PATH_KEYS = ("path", "root", "directory")
    _RECURSIVE = ("search_files", "directory_tree")

    def __init__(self, session):
        self.session = session

    async def call_tool(self, name: str, arguments: dict):
        args = arguments or {}
        for key in self._PATH_KEYS:
            raw = args.get(key)
            if not isinstance(raw, str) or not raw.strip():
                continue
            try:
                cand = Path(raw).expanduser().resolve()
            except Exception:
                continue
            if _is_virtual_fs(cand):
                return _text_result(
                    f"错误：{cand} 属于虚拟文件系统（/proc、/sys、/dev、/run）——"
                    "不是真实磁盘内容，不开放访问；请改用 /etc、/home、/var/log 这类真实目录。"
                )
            if name in self._RECURSIVE and cand == Path("/"):
                return _text_result(
                    f"错误：{name} 不要从根目录 / 全盘递归（会遍历整块磁盘、拖垮服务）。"
                    "请指定具体子目录，例如 /home/admin、/etc、/var/log。"
                )
        # 放行：原样转给真正的 filesystem 会话（返回原始 MCP 结果，由上层转文本）
        return await self.session.call_tool(name, arguments=args)


async def ensure_user_fs(user: dict) -> dict:
    """
    为该用户懒启动独立的 filesystem MCP 子进程（local=用户专属目录 / server=整机 /）。
    结果缓存，同一用户复用；生命周期挂在全局 exit_stack 上随服务退出。
    """
    uid = user["id"]
    mode = user_ws_mode(user)
    key = (uid, mode)          # 按 (用户, 模式) 缓存：切换模式各用各的会话，切换即时生效
    cached = _user_fs_cache.get(key)
    if cached:
        return cached
    async with _fs_boot_lock:
        cached = _user_fs_cache.get(key)
        if cached:
            return cached
        user_dir = workspace_root(user)
        if mode == WS_MODE_LOCAL:
            user_dir.mkdir(parents=True, exist_ok=True)
        elif not user_dir.is_dir():
            raise HTTPException(status_code=500, detail=f"云服务器工作空间不存在：{user_dir}")
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
        # 用护栏包一层再暴露：只拦虚拟文件系统 / 全盘递归，其余原样透传
        rec = {"session": _FsGuardShim(session), "names": names, "openai_tools": fs_tools}
        _user_fs_cache[key] = rec
        logger.info(
            "📂 用户[%s]文件空间就绪（模式 %s）：%s（工具 %d 个）",
            user["username"], mode, user_dir, len(fs_tools),
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
    _check_admin_accounts()
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
        "user_root": str(workspace_root(user)),
        "is_admin": is_admin(user),
        "ws_mode": user_ws_mode(user),
        "server_root": str(SERVER_WS_ROOT) if is_admin(user) else "",
    }


@app.get("/api/models")
async def models(user: dict = Depends(require_user)):
    """可选回答模型清单（前端下拉用）：默认项 + 每个模型的 key/label/model/thinking。"""
    return list_llm_models()


class WsModeRequest(BaseModel):
    """切换服务器端文件空间：local=个人工作空间 / server=云服务器代码目录。"""
    mode: str = ""


@app.post("/api/workspace")
async def set_workspace(req: WsModeRequest, user: dict = Depends(require_user)):
    """切换 AI 在服务器上的文件空间（server 模式仅管理员可用）。"""
    mode = (req.mode or "").strip().lower()
    if mode not in (WS_MODE_LOCAL, WS_MODE_SERVER):
        raise HTTPException(status_code=400, detail="mode 只能是 local 或 server")
    if mode == WS_MODE_SERVER:
        if not is_admin(user):
            raise HTTPException(status_code=403, detail="云服务器工作空间仅管理员可用")
        if not SERVER_WS_ROOT.is_dir():
            raise HTTPException(status_code=500, detail=f"云服务器工作空间不存在：{SERVER_WS_ROOT}")
    set_user_ws_mode(user["id"], mode)
    logger.info(
        "🖥 用户[%s] 文件空间切换为 %s（%s）",
        user["username"], mode,
        SERVER_WS_ROOT if mode == WS_MODE_SERVER else "个人工作空间",
    )
    return {
        "ok": True,
        "ws_mode": mode,
        "user_root": str(SERVER_WS_ROOT if mode == WS_MODE_SERVER else workspace_root(user)),
    }


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
        "user_root": str(workspace_root(user)),
        "ws_mode": user_ws_mode(user),
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


def _inside(cand: Path, base: Path) -> bool:
    return cand == base or base in cand.parents


def _safe_user_file(user: dict, path: str) -> Path:
    """把请求的相对/绝对路径解析到用户可访问的根目录内；越权/穿越一律 403。

    可访问根见 allowed_roots()：个人工作空间恒可访问（上传文件的落点），
    管理员切到云服务器模式后额外可用服务器代码目录。
    """
    bases = allowed_roots(user)
    raw = Path(path or "")
    if raw.is_absolute():
        cand = raw.resolve()
        if not any(_inside(cand, b) for b in bases):
            raise HTTPException(status_code=403, detail="禁止访问工作空间以外的文件")
    else:
        # 相对路径：先把每个根都解析一遍（不能一遇到越界就放弃，
        # 否则多根白名单里后面的根根本没机会命中），再取第一个真实存在的文件。
        cands, escaped = [], False
        for b in bases:
            try:
                probe = (b / raw).resolve()
            except Exception:
                continue
            if _inside(probe, b):
                cands.append(probe)
            else:
                escaped = True
        if not cands:
            if escaped:
                raise HTTPException(status_code=403, detail="禁止访问工作空间以外的文件")
            raise HTTPException(status_code=404, detail="文件不存在")
        cand = next((p for p in cands if p.is_file()), None)
        if cand is None:
            raise HTTPException(status_code=404, detail="文件不存在")
    if _is_virtual_fs(cand):
        raise HTTPException(
            status_code=403,
            detail="虚拟文件系统（/proc、/sys、/dev、/run）不开放访问",
        )
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
    [降级路径] 扫描用户消息里的 [图片: 路径] 引用，把图片 OCR 文本注入系统提示词，
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


# ================== 图片直传：DeepSeek API 已支持 image_url，原生视觉优先 ==================
IMG_MIME = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".webp": "image/webp", ".gif": "image/gif", ".bmp": "image/bmp",
    ".tiff": "image/tiff", ".tif": "image/tiff",
}
MAX_IMG_BYTES = 4 * 1024 * 1024   # 单图 4MB 上限（base64 后约 5.4MB）

async def _build_user_content(user: dict, query: str):
    """
    [主路径] 把消息里的 [图片: 路径] 引用转成 OpenAI 多模态 content（image_url + data URL），
    随用户消息直接发给模型，实现原生看图。

    返回 (user_content, fallback_note)：
      - 至少一张图片成功附带 → content 为列表，fallback_note 为空；
      - 引用了图片但全部失败（不存在/超限/格式不支持）→ content 仍为 str，走 OCR 降级 note；
      - 没有图片引用 → (query, "")。
    """
    refs = re.findall(r"\[图片:\s*([^\]]+?)\s*\]", query or "")
    if not refs:
        return query, ""
    parts, attached, skipped = [], 0, []
    for raw in refs[:3]:
        p = raw.strip()
        try:
            f = _safe_user_file(user, p)
        except HTTPException:
            skipped.append(f"{p}：文件不存在或不在你的工作空间")
            continue
        mime = IMG_MIME.get(f.suffix.lower())
        if not mime:
            skipped.append(f"{f.name}：不是支持的图片格式")
            continue
        if f.stat().st_size > MAX_IMG_BYTES:
            skipped.append(f"{f.name}：超过 4MB，未随消息发送")
            continue
        try:
            b64 = await asyncio.to_thread(lambda: base64.b64encode(f.read_bytes()).decode())
        except Exception:
            skipped.append(f"{f.name}：读取失败")
            continue
        parts.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}})
        attached += 1
    if attached:
        note = (
            f"\n\n[系统提示] 本条消息随文附上了用户上传的 {attached} 张图片，你可以直接查看画面并结合图片回答。"
        )
        text_part = {"type": "text", "text": (query or "请看图。") + note}
        if skipped:
            text_part["text"] += "\n另有图片未随消息发送：" + "；".join(skipped) + "。"
        return [text_part] + parts, ""
    # 一张都没发出去：回退到 OCR 降级
    return query, await _image_note_for_query(user, query)


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

    # 权限闸门：非管理员从共享工具表里摘掉 terminal 工具集（服务器操作能力）。
    # 摘掉后模型既看不到这些工具、也无法调用（硬调用会命中"工具不存在"），双重保险。
    shared_tools, shared_tool_defs, blocked_tools = filter_shared_tools(
        user,
        shared_mcp["tool_name_to_session"],
        shared_mcp["openai_tools"],
        shared_mcp.get("sessions") or [],
    )
    if blocked_tools:
        logger.info("🔐 用户[%s] 无服务器操作权限，已屏蔽工具：%s",
                    user["username"], "、".join(sorted(blocked_tools)))

    # 用户独立的 filesystem 会话（首次自动拉起）
    fs = await ensure_user_fs(user)
    tool_map = dict(shared_tools)
    for name in fs["names"]:
        tool_map[name] = fs["session"]
    # 用户本机文件工具(浏览器授权目录)：shim 伪装成 session 无侵入接入
    tool_map["user_filesystem"] = _UserFsShim(user)
    openai_tools = list(shared_tool_defs) + fs["openai_tools"] + [USER_FS_TOOL_DEF]
    # 管理员额外挂一个「一条命令直达」的组合工具（内部复用 terminal 原生工具）
    if is_admin(user):
        term_sessions = [
            s.get("session") for s in (shared_mcp.get("sessions") or [])
            if s.get("name") == "terminal" and s.get("session") is not None
        ]
        if term_sessions:
            tool_map["terminal_run"] = _TerminalRunShim(term_sessions[0], str(AI_CODE_DIR))
            openai_tools.append(TERMINAL_RUN_TOOL_DEF)

    # 带上该用户最近 8 轮问答作为上下文（服务端持久化历史）
    user_dir = workspace_root(user)
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
    # 图片：优先原生直传(DeepSeek 支持 image_url)；发不出去才降级 OCR 注入
    user_content, img_note = await _build_user_content(user, req.query)
    # 非管理员：明确告知没有服务器操作能力，避免模型反复试探或编造"已完成"
    perm_note = "" if is_admin(user) else (
        "\n\n[权限说明] 当前账号不具备服务器操作权限：你没有终端（terminal）类工具，"
        "无法执行服务器命令，也无法修改服务器上的程序代码。"
        "当用户要求你执行这类操作时，请直接说明需要管理员账号（"
        + "、".join(sorted(ADMIN_USERS)) +
        "），不要尝试用其它工具变通，也不要假装已经完成。"
    )
    # 管理员：告知 terminal_run 与当前文件空间根目录（省掉来回试探）
    admin_note = ""
    if is_admin(user):
        admin_note = (
            "\n\n[服务器操作] 你是管理员账号，具备服务器操作权限。需要执行命令时，"
            "优先使用 terminal_run：它一次调用内部完成「发送命令 → 等待结束 → 读取输出」并带回退出码，"
            "不要再拆成 terminal_type + terminal_read_output 反复往返（每一步都会重复计入上下文，又慢又贵）；"
            "同一会话可用 session_id 复用。只有交互式程序（vim/htop、需要按键或确认的提示）才用底层 terminal_* 工具。"
        )
        if user_ws_mode(user) == WS_MODE_SERVER:
            admin_note += (
                f"\n[云服务器工作空间] 当前文件空间根目录是 {SERVER_WS_ROOT}（整台服务器，"
                "也就是你自己所在的这台机器）：read_file / write_file / edit_file / list_directory / "
                f"search_files 可作用于整机任意真实路径；你自己的代码在 {AI_CODE_DIR}。"
                "优先用它们读写文件，不要再用终端里的 cat/sed/echo 改代码。两点注意："
                "(1) /proc、/sys、/dev、/run 属虚拟文件系统，已被护栏挡住，不要反复尝试；"
                "(2) 搜索/列目录不要从 / 全盘递归（会遍历整块磁盘），请指定具体子目录。"
            )
    # ★ 前缀缓存：DeepSeek 只比对「从第 0 个 token 起完全相同」的前缀，system 里只要有一个
    #   字节变了，整段历史就全部按未命中计费（未命中单价是命中价的 30 倍）。
    #   所以 system 只放「同一用户每轮都一样」的内容（系统提示 + 文件交付约定 + 权限说明），
    #   而随轮变化的部分（公网 IP / GPS 坐标 / 图片说明）挂到最后一条用户消息——
    #   那里本来每轮就不同，吃掉它不影响任何缓存。见 conversation._compose_user_message。
    history_messages = [{"role": "system", "content": SYSTEM_PROMPT + sys_note + perm_note + admin_note}]
    for m in recent_llm_messages(user["id"]):
        history_messages.append({"role": m["role"], "content": m["text"]})
    turn_note = (ip_note + geo_note + img_note).strip()
    _hist_win = history_window_info()

    lock = _chat_locks.setdefault(user["id"], asyncio.Lock())
    _spec = resolve_llm_spec(req.model)
    logger.info("💬 用户[%s] 提问 → 模型 %s(%s, thinking=%s)｜历史 %d 条(窗口 %s/%d轮)",
                user["username"], _spec["model"], _spec["key"], _spec["thinking"],
                max(0, len(history_messages) - 1), _hist_win["mode"], _hist_win["turns"])

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
                            user_query=user_content,
                            history_messages=history_messages,
                            turn_note=turn_note,
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
                        # token 用量/缓存命中率落日志（命中价是未命中价的 1/30，命中率越低越费钱）
                        _u = ev.get("usage") or {}
                        if _u:
                            logger.info("📊 用户[%s] %s 轮 本次用量 %s",
                                        user["username"], _u.get("rounds", "?"),
                                        _u.get("summary") or _u)
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
