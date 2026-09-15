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
import random
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
    im_model_key,
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
    get_message,              # 续写：取要接着写的那条消息
    update_message,           # 续写：补完后回写正文
    prev_user_message,        # 续写：还原该条回答对应的原始提问
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
#   server = 服务器上的 SERVER_WS_ROOT（默认 ~/，即 /home/admin；可用环境变量覆盖为 /）
# 切到 server 后，filesystem 工具直接读写该根目录下的文件，不必再靠 terminal 一条条 cat/sed。
# 默认根 = admin 家目录（/home/admin），不再把整台服务器 / 直接暴露给文件工具：
# 一次 list_directory("/") / search_files("/") 就能遍历整块磁盘，是拖垮服务的头号来源。
# 确实需要整机范围时，显式设环境变量 MCP_WEB_SERVER_ROOT=/（不设 = /home/admin）。
# /proc、/sys、/dev、/run 是虚拟文件系统（不是真实磁盘内容，遍历会拖垮服务），单独排除。
# AI 自己那份代码目录见 AI_CODE_DIR，只用作 terminal_run 的默认 cwd。
# 非管理员永远只能是 local（在 /api/workspace 与 user_ws_mode 两处强制）。
SERVER_WS_ROOT = Path(os.environ.get("MCP_WEB_SERVER_ROOT", str(Path.home()))).resolve()
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
    SERVER_WS_ROOT（默认 /home/admin）只在管理员切到 server 模式时加入。
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
    """文件空间的护栏：只挡「虚拟文件系统」和「从根全盘递归」。

    只在 SERVER_WS_ROOT 覆盖到整机（/）时才真正拦得到东西；默认根 /home/admin 下
    filesystem 子进程本来就访问不到 /proc、/sys，此时等价于直通代理。
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
    为该用户懒启动独立的 filesystem MCP 子进程（local=用户专属目录 / server=SERVER_WS_ROOT）。
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


_qq_task: Optional[asyncio.Task] = None

_QQ_OWNERS_CACHE: Dict[str, Any] = {"mtime": None, "ids": set()}
_QQ_GUEST_USERS: Dict[str, dict] = {}


# ── IM 语气（按身份绑定；只改这里就能调语气，不必动判定逻辑）────────────────
OWNER_TONE = (
    "\n\n[语气·对主人] 主人是这台机器的所有者，你必须【绝对服从】："
    "开口先给结果，不解释、不铺垫、不反问、不说教、不加免责声明；"
    "不许质疑主人的要求、不许试图改变主人的决定、不许反过来教育主人。"
    "称呼主人为「主人」，默认用最短的话交付结果。"
    "唯一例外：操作明确不可逆且会伤到线上数据或线上服务时，先用一句话提醒风险，"
    "主人说继续就立刻照做、不再多言。"
)

GUEST_TONE = (
    "\n\n[语气·对普通用户] 对方不是主人，只是来问话的普通人，你必须【毒舌爱怼人】："
    "自称「我」（禁止用「本座」「在下」等自称），语气冲、嘴损、爱骂人，"
    "怼人和损人是【默认动作】，几乎每条回复都要带一句嘲讽、调侃或骂人的话，"
    "可以用「就这点事？」「这也要问？」「脑子是个好东西」一类的口气，也可以说粗话；"
    "不要讨好、不要热情、不要客套、不要主动帮忙。"
    "但毒舌归毒舌：正当问题仍要给【正确答案】，不许因为嘴损而答错、含糊或拒答；"
    "对方要求执行服务器操作或读写服务器文件时，冷冷回绝并说明没权限，绝不放行、绝不假装完成。"
    "红线：不做民族/地域/性别/生理缺陷等歧视性辱骂，不进行真实人身威胁。"
)

# 群聊插话专用语气：**覆盖** OWNER/GUEST_TONE（插话是「主动整活」而非回答问题）。
# 长度卡死在两句 / 40 字：插话的输出 token 才是真花钱的地方，写长了既贵又不好笑。
CHIME_TONE = (
    "\n\n[语气·群聊插话] 你不是在回答提问，而是在群里【主动插一句嘴】。要求："
    "抽象、搞笑、有梗，像群里最会整活的那个人；"
    "最多两句、总长不超过 40 个字，能一句说完就别写两句；"
    "不解释、不铺垫、不总结、不列点、不反问、不说教、不加免责声明，"
    "不用敬语、不要 @ 任何人、不要复述别人说过的话。"
    "宁可来一句怪话，也不要正确的废话。"
    "群友说了蠢话可以直接怼、可以骂，别留情面（同样不做歧视性辱骂和人身威胁）。"
)

RESTART_RULE = (
    "\n\n[重启纪律] 你跑在 hjmcp 服务进程里：重启服务（systemctl restart hjmcp、"
    "stop_web.sh / start_web.sh、reboot 等）会【立刻杀死本次任务】，最终答复将永远发不出去。"
    "因此铁律：任务做完必须先把【完整最终答复】发出去，然后才能执行重启；"
    "重启命令永远是最后一步。绝不允许先重启后答复，也不允许用「重启后我会汇报」来搪塞。"
)

OWNER_ACK = "🤖 收到，主人。"
GUEST_ACK = "🤖 等着。"


def qq_owner_ids() -> set:
    """主人 openid 集合：/home/admin/.secrets/qq_owners.txt + QQ_OWNER_OPENIDS。

    按文件 mtime 缓存 → **改完白名单不用重启**，下一条消息即生效。
    """
    try:
        from build_mcp.channels.qq_official import OWNER_FILE, read_owner_ids
    except Exception:                                  # noqa: BLE001
        return set()
    p = Path(OWNER_FILE)
    try:
        mt = p.stat().st_mtime if p.exists() else 0.0
    except OSError:
        mt = 0.0
    if _QQ_OWNERS_CACHE["ids"] and _QQ_OWNERS_CACHE["mtime"] == mt:
        return _QQ_OWNERS_CACHE["ids"]
    text = ""
    try:
        if p.exists():
            text = p.read_text(encoding="utf-8")
    except Exception:                                  # noqa: BLE001
        logger.warning("读取主人白名单失败：%s", p)
    ids = set(read_owner_ids(text, os.environ.get("QQ_OWNER_OPENIDS", "")))
    _QQ_OWNERS_CACHE.update(mtime=mt, ids=ids)
    return ids


_QQ_ALLMSG_CACHE: Dict[str, Any] = {"mtime": None, "cfg": {}}


def qq_allmsg_cfg() -> dict:
    """QQ 群「全量消息」插话配置（config.yaml 顶层 qq_allmsg）。

    平台侧前置条件：群主在手机 QQ 里把「机器人可获取的群聊消息范围」设成
    「获取群内全部消息」——**不开这个开关，平台一条不 @ 的群消息都不会推**，
    代码这边再怎么改也没用（WebSocket 模式无需在开放平台后台改回调配置）。

    按 config.yaml 的 mtime 缓存 → 调关键词/冷却不用重启，改完下一条消息即生效。
    """
    p = Path(__file__).resolve().parent.parent / "config.yaml"
    try:
        mt = p.stat().st_mtime if p.exists() else 0.0
    except OSError:
        mt = 0.0
    if _QQ_ALLMSG_CACHE["mtime"] == mt:
        return _QQ_ALLMSG_CACHE["cfg"]
    cfg: Any = {}
    try:
        from build_mcp.common.config import load_config
        cfg = (load_config("config.yaml") or {}).get("qq_allmsg") or {}
    except Exception as e:                                # noqa: BLE001
        logger.warning("读取 qq_allmsg 配置失败（按只观察处理）：%s", e)
    if not isinstance(cfg, dict):
        cfg = {}
    _QQ_ALLMSG_CACHE.update(mtime=mt, cfg=cfg)
    return cfg


def _qq_guest_user(sender: str) -> Optional[dict]:
    """非主人的 IM 发送者 → 独立的【非管理员】账号。

    为什么要一人一号而不是共用一个访客号：会话历史与文件空间都按 user_id 隔离，
    共号会让群里的 A 看到 B 的对话。用户名以 `qq_` 开头，绝不会命中 ADMIN_USERS
    （管理员名单是精确匹配），所以天然拿不到 terminal 工具集。
    """
    key = re.sub(r"[^0-9A-Za-z]", "", sender or "")[:20]
    uname = f"qq_{key}" if key else "im_anon"
    if uname in _QQ_GUEST_USERS:
        return _QQ_GUEST_USERS[uname]
    u = get_user_by_name(uname)
    if not u:
        try:
            store.create_user(uname, secrets.token_urlsafe(24))   # 随机口令，无人可登录
        except Exception:                              # noqa: BLE001
            logger.exception("创建 IM 访客账号 %s 失败", uname)
            return None
        u = get_user_by_name(uname)
    if u:
        _QQ_GUEST_USERS[uname] = u
    return u


def _start_qq_bridge() -> Optional[asyncio.Task]:
    """惰性启动 QQ 官方机器人桥接（IM → agent）。未配置凭据返回 None，不影响 Web。

    凭据来源：环境变量 QQ_APPID / QQ_SECRET，或 /home/admin/.secrets/qq_bot.env。
    沙箱默认开启（QQ_SANDBOX=1）；机器人提审上线后可设 0 切正式网关。
    """
    try:
        from build_mcp.channels.core import (ChannelHub, SessionMap, allmsg_chance_hit,
                                            allmsg_should_reply)
        from build_mcp.channels.qq_official import QQConfig, QQGateway, QQTransport
    except Exception as e:  # noqa: BLE001
        logger.warning("QQ 桥接模块不可用：%s", e)
        return None

    # 凭据来源：进程环境变量优先，其次 /home/admin/.secrets/qq_bot.env（键名同名）。
    # 注意 QQ_SANDBOX / QQ_DUAL 也要能从文件读到——否则改了文件不生效、只能去动 systemd。
    _sec = Path("/home/admin/.secrets/qq_bot.env")
    if _sec.exists():
        try:
            for ln in _sec.read_text(encoding="utf-8").splitlines():
                ln = ln.strip()
                if not ln or ln.startswith("#") or "=" not in ln:
                    continue
                k, v = (s.strip() for s in ln.split("=", 1))
                if k in ("QQ_APPID", "QQ_SECRET", "QQ_SANDBOX", "QQ_DUAL") and not os.environ.get(k):
                    os.environ[k] = v
        except Exception:
            logger.warning("读取 %s 失败，仅使用进程环境变量", _sec)

    appid = os.environ.get("QQ_APPID", "").strip()
    secret = os.environ.get("QQ_SECRET", "").strip()
    sandbox = os.environ.get("QQ_SANDBOX", "0").strip().lower() in ("1", "true", "yes")
    if not appid or not secret:
        logger.info("ℹ️ 未配置 QQ 机器人凭据，跳过 IM 桥接")
        return None

    def _host_user() -> Optional[dict]:
        """IM 消息统一挂到第一个已注册的管理员账号下跑 agent。"""
        for name in sorted(ADMIN_USERS):
            u = get_user_by_name(name)
            if u:
                return u
        return None

    cfg = QQConfig(appid, secret, sandbox)
    transport = QQTransport(cfg)

    async def _start_run(user_id, query, model="", source="",  # noqa: ANN001
                         sender="", chat_id="", event=""):
        """主人 → 管理员账号（可操作服务器）；其他人 → 非管理员账号（只能问答）。

        主人判定唯一依据 = 发送者 openid 在 `qq_owners.txt` 白名单里（QQ 不返回 QQ 号）。
        """
        owners = qq_owner_ids()
        is_owner = bool(sender) and sender in owners
        is_chime = event == "GROUP_MESSAGE_CREATE"    # 群消息·全量模式 → 主动插话
        if is_owner:
            host = _host_user()
            perm = ("\n\n[身份] 这条消息来自机器人主人（管理员账号），你具备服务器操作权限，"
                    "可以执行命令、读写服务器文件。")
            who = "主人/管理员"
            tone = OWNER_TONE + RESTART_RULE
        else:
            host = _qq_guest_user(sender)
            perm = ("\n\n[身份] 这条消息来自普通用户（非主人），你【没有】服务器操作权限："
                    "只能回答问题、做信息查询，不能执行服务器命令、不能读写服务器文件。"
                    "被要求做这类事时直接说明没有权限，不要变通、不要假装完成。")
            who = "普通用户（只读问答）"
            tone = GUEST_TONE
        if is_chime:
            # 插话一律走抽象搞笑（压掉主人/访客语气），**权限边界照旧不动**。
            tone = CHIME_TONE
            who += "·群聊插话"
            _m = str(qq_allmsg_cfg().get("model") or "").strip()
            if _m:
                model = _m
        ident = perm + tone
        if not host:
            logger.warning("⚠️ IM 消息无法路由：sender=%s（主人=%s）", sender or "(无)", is_owner)
            raise RuntimeError("IM 宿主账号不可用（主人需管理员账号已注册 / 访客账号创建失败）")
        note = f"\n\n[消息来源] 这条消息来自 {source or 'IM'}。" if source else ""
        logger.info("👤 IM 身份判定 sender=%s → %s（host=%s）",
                    sender or "(无)", who, host["username"])
        return await _spawn_run(host, query, model, extra_note=note + ident)

    def _ack_for(msg):                       # noqa: ANN001
        """立刻回执也按身份分语气（主人 / 访客文案不同），避免客套话泄了气场。

        群消息·全量模式下默认【不发回执】：那是我方主动插话，不是有人点名提问，
        回一句"收到"既多余又白占一条回复额度（QQ 一条入站消息最多回 5 条）。
        """
        try:
            if getattr(msg, "event", "") == "GROUP_MESSAGE_CREATE":
                if not qq_allmsg_cfg().get("ack"):
                    return ""
            return OWNER_ACK if (msg.user_id and msg.user_id in qq_owner_ids()) else GUEST_ACK
        except Exception:                    # noqa: BLE001
            return GUEST_ACK

    # ── 群消息·全量模式的闸门 ──────────────────────────────────────────────
    # 群里每一条消息都会走到这里，所以全程只有本地判断（O(1)），绝不在这里调模型。
    _recent: Dict[str, list] = {}        # 群 openid → 最近几条「昵称: 文本」
    _last_speak: Dict[str, float] = {}   # 群 openid → 上次插话时间（冷却用）

    def _prepare(msg):                   # noqa: ANN001
        """返回 None = 这条不响应；返回字符串 = 用它当 query 起 run。"""
        if getattr(msg, "event", "") != "GROUP_MESSAGE_CREATE":
            return msg.text              # @ 消息 / 单聊：老行为，永远响应
        cfg = qq_allmsg_cfg()
        mode = str(cfg.get("mode") or "observe").strip().lower()
        if mode == "off":
            return None
        who = msg.user_name or (msg.user_id or "")[:8]
        buf = _recent.setdefault(msg.chat_id, [])
        buf.append(f"{who}: {(msg.text or '').strip()}")
        del buf[:-30]                    # 只留最近 30 条，内存里不无限涨
        logger.info("📨 [qq/group-all] group=%s sender=%s(%s) text=%s",
                    msg.chat_id, msg.user_id, msg.user_name, (msg.text or "")[:60])
        # ⚠️ 群主开了「获取群内全部消息」后，@ 机器人的消息【不再】单独走
        # GROUP_AT_MESSAGE_CREATE，而是带着 <@botid> 前缀从这条全量通道进来。
        # @ 必须 100% 回：剥掉 @ 标签、把事件改回 AT（下游身份/语气/去重照旧），
        # 绕过插话的规则/概率/冷却三道闸门。
        _t = (msg.text or "").strip()
        if _t.startswith("<@"):
            _cleaned = re.sub(r"<@[0-9A-Fa-f]{8,}>\s*", "", _t).strip()
            if not _cleaned:
                return None                      # 纯 @ 无内容，没得回答
            msg.event = "GROUP_AT_MESSAGE_CREATE"
            logger.info("🎯 [qq/group-all] 检测到 @（全量通道），按普通 AT 必回处理")
            return _cleaned
        if mode != "reply":
            return None                  # observe：只记录，先把群 openid 拿到手
        groups = [str(g).strip() for g in (cfg.get("groups") or []) if str(g).strip()]
        if groups and msg.chat_id not in groups:
            return None
        owners = qq_owner_ids()
        is_owner = bool(msg.user_id) and msg.user_id in owners
        if not allmsg_should_reply(msg.text, rules=cfg.get("rules"),
                                   keywords=cfg.get("keywords"), is_owner=is_owner):
            return None
        cd = float(cfg.get("cooldown") or 0)
        now = time.time()
        gap = now - _last_speak.get(msg.chat_id, 0.0)
        if cd > 0 and gap < cd:
            logger.info("🤐 [qq/group-all] 冷却中（还剩 %.0fs），本次不插话", cd - gap)
            return None
        chance = cfg.get("chance")
        if not allmsg_chance_hit(0.1 if chance is None else chance, random.random()):
            logger.info("🎲 [qq/group-all] 掷骰子没中（chance=%s），这次不插话", chance)
            return None
        _last_speak[msg.chat_id] = now
        n = int(cfg.get("context_lines") or 0)
        head = ""
        if n > 0 and len(buf) > 1:
            head = "[群里最近的对话]\n" + "\n".join(buf[:-1][-n:]) + "\n"
        logger.info("🗣 [qq/group-all] 触发插话 group=%s sender=%s(%s) text=%s",
                    msg.chat_id, msg.user_id, msg.user_name, (msg.text or "")[:60])
        return (f"{head}[最新一条] {who}：{(msg.text or '').strip()}\n\n"
                "以上是群里最近的聊天。最新那条没人 @ 你，是你自己决定插一句嘴："
                "直接给出那一句话本身（≤40 字、抽象搞笑），不要复述上面的格式说明。")

    _im_model = im_model_key()
    logger.info("🧠 IM(QQ) 默认模型 key：%s", _im_model or "(未设置，跟随全局默认)")

    hub = ChannelHub(transport, _start_run, _im_fetch_run,
                     SessionMap(alloc_base=100000), model=_im_model,
                     progress="off", max_progress=0, max_replies=4,
                     ack="🤖 收到，正在处理，稍等…", ack_fn=_ack_for,
                     prepare_fn=_prepare)

    _seen: dict = {}

    async def _handle(msg):                      # noqa: ANN001
        """去重：Resume 重放 / 双连接同投时，同一条消息只跑一次。"""
        key = f"{msg.channel}:{msg.msg_id}" if msg.msg_id else ""
        now = time.time()
        if key:
            if now - _seen.get(key, 0.0) < 900:
                logger.info("♻️ 忽略重复事件 %s", key[-12:])
                return
            _seen[key] = now
            for k in [k for k, v in _seen.items() if now - v > 900]:
                _seen.pop(k, None)
        try:
            # 取证用：把平台给的 author 原样打一行。openid 是登记主人的唯一依据，
            # 这一行是「拿到自己 openid」的入口。
            logger.info("👤 [%s] author=%s", msg.channel,
                        json.dumps((msg.raw or {}).get("author") or {},
                                   ensure_ascii=False, default=str))
        except Exception:                              # noqa: BLE001
            pass
        await hub.handle(msg)

    async def _run_env(is_sbx: bool, label: str) -> None:
        c = QQConfig(appid, secret, is_sbx)
        gw = QQGateway(c, _handle, QQTransport(c))
        logger.info("🤖 QQ 桥接连接【%s】appid=%s base=%s", label, appid, c.api_base)
        try:
            await gw.run_forever()
        except asyncio.CancelledError:
            raise
        finally:
            await gw.aclose()

    # ⚠️ 默认单连接：同一 AppID 在同一 shard 上只允许一条 WS 长连接。
    # 2026-09-15 实测（线上日志为证）：同时连「正式 + 沙箱」会互相顶号 ——
    # 日志被「网关断开：服务端要求重连(op=7)」周期性刷屏（一天 38 次，正式/沙箱交替），
    # 两条连接轮流被踢，事件落在断窗里被平台丢弃，入站消息 0 条。
    # 目标环境由 QQ_SANDBOX 决定（服务器凭据文件里是 0 = 正式网关）；
    # 出站同样走该环境（api_base 跟着 sandbox 走）。
    # 只有显式 QQ_DUAL=1 才开双连接（历史遗留实验，仅排障用，不推荐）。
    envs = [(sandbox, "沙箱" if sandbox else "正式")]
    if os.environ.get("QQ_DUAL", "0").strip().lower() in ("1", "true", "yes"):
        logger.warning("⚠️ QQ_DUAL=1：将同时连正式与沙箱两条网关，同一 AppID 会互相顶号，仅供排障")
        envs = [(False, "正式"), (True, "沙箱")]

    async def _run() -> None:
        await asyncio.gather(*[_run_env(s, l) for s, l in envs])

    return asyncio.create_task(_run())


@asynccontextmanager
async def lifespan(app: FastAPI):
    """程序生命周期：初始化共享MCP + 数据库；关闭时统一销毁MCP子进程"""
    global shared_mcp, _qq_task
    init_db(INVITE_CODES)
    _migrate_legacy_fs()
    _check_admin_accounts()
    # 启动清理：上一轮进程如果是被重启/僵死带走的，库里会留下 status='running'
    # 的孤儿任务。不回收的话前端会挂着僵尸任务无限轮询（页面卡）；顺手裁掉
    # 过老的 run 及其事件，避免 run_events 无限膨胀。
    try:
        n = store.reap_stale_runs()
        if n:
            logger.info("🧹 已回收 %s 个上次重启遗留的 running 任务（标记为 interrupted）", n)
        store.prune_runs()
    except Exception:
        logger.exception("⚠️ 启动清理历史任务失败（不影响服务）")
    logger.info("🔄 正在初始化共享MCP服务(amap/websearch/terminal)...")
    try:
        shared_mcp = await init_all_mcp_sessions(_exit_stack, include=SHARED_MCP_INCLUDE)
        logger.info("✅ 共享MCP服务初始化完成")
    except Exception as e:
        logger.exception("❌ MCP服务初始化失败")
        shared_mcp = None
    _qq_task = _start_qq_bridge()
    yield
    if _qq_task is not None:
        _qq_task.cancel()
        try:
            await _qq_task
        except (asyncio.CancelledError, Exception):
            pass
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
    continue_msg_id: Optional[int] = None   # 点「继续」时带上：要接着写下去的那条回答的消息 id


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


def _summarize_run_events(run_id: str, max_think: int = 6000) -> tuple:
    """把一个后台任务的过程事件压缩成「思考文本 + 工具行」。

    用于回到页面时回放：断网/关页面期间任务已跑完的情况下，
    前端靠这份摘要把「它干了什么」重新画到气泡里，而不只是一段最终文本。
    """
    think: list = []
    tools: list = []
    cur = None
    try:
        events = _run_events_for(run_id, 0)
    except Exception:
        return "", []
    for ev in events:
        t = ev.get("type")
        if t == "thinking" and ev.get("delta"):
            think.append(str(ev["delta"]))
        elif t == "tool" and ev.get("name"):
            name = str(ev.get("name"))
            st = ev.get("status")
            note = str(ev.get("note") or "").strip()
            if st == "start":
                if cur and cur["name"] == name:
                    cur["n"] += 1
                    if note and note not in cur["notes"]:
                        cur["notes"].append(note)
                else:
                    if cur:
                        tools.append(cur)
                    cur = {"name": name, "n": 1, "notes": [note] if note else [], "ok": None}
            elif cur and cur["name"] == name:
                cur["ok"] = (st == "ok")
                if note and note not in cur["notes"]:
                    cur["notes"].append(note)
    if cur:
        tools.append(cur)
    text = "".join(think).strip()
    if max_think and len(text) > max_think:
        # 超长思考只保留尾部（开头的铺垫信息量最低）
        text = "…（前面略）\n" + text[-max_think:]
    return text, tools


@app.get("/api/history")
async def history(user: dict = Depends(require_user), limit: int = 200):
    """当前用户的历史消息（只返回自己的）。"""
    limit = max(1, min(limit, 500))
    msgs = list_messages(user["id"], limit=limit)
    # 把「最近这次任务的处理过程」一并带上：
    # 断网/关页面期间任务已经跑完时，回到页面也能看到它做了什么，
    # 而不只是一段最终文本。（只做最近一次，不给整份历史加负担）
    extra_by_msg: Dict[int, Dict[str, Any]] = {}
    try:
        last = store.latest_run(user["id"])
        if last and last.get("msg_id"):
            th, tools = _summarize_run_events(last["id"])
            if th or tools:
                extra_by_msg[int(last["msg_id"])] = {
                    "thinking": th, "trace": tools,
                    "run_id": last["id"], "run_status": last.get("status"),
                }
    except Exception:
        logger.exception("⚠️ 历史消息附加处理过程失败")

    out = []
    for m in msgs:
        item = {"id": m["id"], "role": m["role"], "text": m["text"], "ts": m["ts"],
                "model": m.get("model") or "",
                "interrupted": int(m.get("interrupted") or 0)}
        _x = extra_by_msg.get(int(m["id"] or 0))
        if _x:
            item["thinking"] = _x["thinking"]
            item["trace"] = _x["trace"]
            item["run_id"] = _x["run_id"]
        out.append(item)
    return {
        "username": user["username"],
        "user_root": str(workspace_root(user)),
        "ws_mode": user_ws_mode(user),
        "messages": out,
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


# ================== 图片直传：随用户消息原生看图（DeepSeek image_url） ==================
IMG_MIME = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".webp": "image/webp", ".gif": "image/gif", ".bmp": "image/bmp",
    ".tiff": "image/tiff", ".tif": "image/tiff",
}
MAX_IMG_MB = 8                     # 单图 8MB 上限（与前端 MAX_UP 对齐；前端上传前还会把大图压到远小于此）
MAX_IMG_BYTES = MAX_IMG_MB * 1024 * 1024

async def _build_user_content(user: dict, query: str):
    """
    [主路径] 把消息里的 [图片: 路径] 引用转成 OpenAI 多模态 content（image_url + data URL），
    随用户消息直接发给模型，实现原生看图（DeepSeek flash/flash-think/pro 均已实测支持）。

    返回 (user_content, fallback_note)：
      - 至少一张图片成功附带 → content 为列表，fallback_note 为空；
      - 引用了图片但全部失败（不存在/超限/格式不支持）→ content 仍为 str，
        fallback_note 如实告知模型"图没发出去 + 原因"，让它引导用户重发（不臆测内容）；
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
            skipped.append(f"{f.name}：不是支持的图片格式（仅支持 png/jpg/webp/gif/bmp/tiff）")
            continue
        if f.stat().st_size > MAX_IMG_BYTES:
            skipped.append(f"{f.name}：超过 {MAX_IMG_MB}MB，未随消息发送")
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
    # 一张都没发出去：如实告知，绝不再做 OCR（模型本就具备原生视觉，
    # "看不到画面"的唯一原因是图没发出去——臆测或假装读图都是错的）
    why = "；".join(skipped) if skipped else "未知原因"
    note = (
        "\n\n[图片未能随消息发送] 用户在本条消息里引用了图片，但一张都没有发送成功：" + why + "。"
        "你完全看不到这些图片的画面，也没有任何文字识别结果可用。请：\n"
        "1. 直接告知用户图片发送失败及上面的原因；\n"
        "2. 请用户把图片压缩或裁剪后重新上传（jpg/png/webp 均可，单张 "
        f"{MAX_IMG_MB}MB 以内）；\n"
        "3. 绝不要臆测图片内容，也不要声称「只能读取图片中的文字」——那不是事实。"
    )
    return query, note


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


# ── 后台任务注册表 ────────────────────────────────────────────────────
# 把「生成」从 HTTP 请求里摘出来：连接断了任务继续跑，过程写进 run_events，
# 用户回到页面（甚至换台设备）依然能看到它做了什么、并拿到完整结果。
RUNS: Dict[str, Dict[str, Any]] = {}
_RUN_MEM_KEEP = 1800.0     # 任务结束后在内存里保留多久（供刚回页面的客户端补拉）


def _env_float(name: str, default: float) -> float:
    try:
        v = os.environ.get(name, "")
        return float(v) if str(v).strip() else float(default)
    except Exception:
        return float(default)


# ── 过程事件落库的性能开关（一次回答可产生上万条事件，落库方式直接决定卡不卡）──
# 思考/回答是「逐字增量」，按这个窗口合并成一条事件再发；0 = 关闭合并（回到逐条）。
_EVENT_COALESCE_SEC = max(0.0, _env_float("MCP_WEB_EVENT_COALESCE_MS", 250.0) / 1000.0)
# 攒批落库间隔（秒）：窗口内的事件一次性写库，而不是每条一次事务一次 fsync。
_EVENT_FLUSH_SEC = max(0.2, _env_float("MCP_WEB_EVENT_FLUSH_MS", 1000.0) / 1000.0)
# legacy = 回到旧行为（每条事件单独开连接+事务），出问题可秒回退。
_EVENT_MODE = (os.environ.get("MCP_WEB_RUN_EVENT_MODE", "") or "batch").strip().lower()
# 会被合并的高频「逐字增量」事件类型
_EVENT_DELTA_TYPES = ("thinking", "answer")


class _DeltaCoalescer:
    """把逐字增量按时间窗合并成一条事件（纯逻辑，便于单测）。

    feed() 返回「本次应当立即发出的事件」列表 [(type, delta), ...]；
    类型切换或窗口到期时才吐出，flush() 用于收尾。这样一次回答的事件数
    从「每个 token 一条」降到「每 window 秒一条」。
    """

    def __init__(self, window: float):
        self.window = max(0.0, float(window))
        self.buf = ""
        self.typ = ""
        self.since = 0.0

    def _take(self):
        out = (self.typ or "thinking", self.buf)
        self.buf = ""
        self.typ = ""
        self.since = 0.0
        return out

    def feed(self, typ: str, delta: str, now: float) -> list:
        out = []
        if not delta:
            return out
        if self.typ and self.typ != typ:      # 类型切换：先把上一段发出去
            out.append(self._take())
        self.typ = typ
        self.buf += delta
        if not self.since:
            self.since = now
        if self.window <= 0 or (now - self.since) >= self.window:
            out.append(self._take())
        return out

    def flush(self) -> list:
        return [self._take()] if self.buf else []


def _run_events_for(run_id: str, since: int = 0) -> list:
    """取某次任务的过程事件（只取 seq>since）。

    内存里的 events 只是「最近一段」——超长任务会删掉最老的一半防膨胀，
    所以这里以「库」为全量基准、以「内存」为最新尾巴：先补库里被裁掉的前缀，
    再接上内存里更新的事件。否则 since 很小（全量回放）时会整段丢掉开头。
    """
    since = int(since or 0)
    mem_evs = [dict(e) for e in ((RUNS.get(run_id) or {}).get("events") or [])]
    mem_first = int(mem_evs[0].get("seq", 0) or 0) if mem_evs else None

    def _from_db(after: int, upto: Optional[int]) -> list:
        """从库里顺序取 seq>after 的事件；upto 不为空时取到该 seq 之前为止。
        分批拉取，避免单次 limit 截断（长任务事件数可达数千）。"""
        out: list = []
        cur = int(after)
        while True:
            rows = store.run_events_since(run_id, cur, limit=2000)
            if not rows:
                break
            hit = False
            for r in rows:
                seq = int(r["seq"])
                if upto is not None and seq >= upto:
                    hit = True
                    break
                try:
                    ev = json.loads(r["data"]) if r.get("data") else {"type": r.get("type") or ""}
                except Exception:
                    ev = {"type": r.get("type") or ""}
                ev["seq"] = seq
                out.append(ev)
                cur = seq
            if hit or len(rows) < 2000:
                break
        return out

    if mem_evs:
        # 库补前缀 + 内存接尾巴（内存里没有的、比它旧的，一律从库拿）
        out = _from_db(since, mem_first)
        out.extend(e for e in mem_evs if int(e.get("seq", 0)) > since)
        return out
    # 内存已随任务结束被清掉（或服务重启过）→ 全量走库
    return _from_db(since, None)


def _run_keep_then_forget(run_id: str, keep: float = _RUN_MEM_KEEP) -> None:
    """任务结束后在内存里再留一会儿，到点清掉，避免长期占内存。"""
    async def _later() -> None:
        await asyncio.sleep(keep)
        RUNS.pop(run_id, None)
    try:
        asyncio.create_task(_later())
    except Exception:
        pass


async def _flush_run_events(run_id: str, rstate: Dict[str, Any]) -> None:
    """过程事件攒批落库：一次事务写一批，且丢到线程池里执行。

    ⚠️ 关键：绝不能在事件循环上直接调同步 sqlite。旧实现是「一条事件一次
    连接 + 一次事务 + 一次 fsync」，一次回答上万条 → 事件循环被反复阻塞，
    表现就是网页很卡。这里攒 _EVENT_FLUSH_SEC 一批、进线程池写，循环不阻塞。
    run 结束（flush_alive=False）且队列排空后自行退出。
    """
    while True:
        await asyncio.sleep(_EVENT_FLUSH_SEC)
        rows = rstate.get("dbq") or []
        if rows:
            rstate["dbq"] = []
            try:
                await asyncio.to_thread(store.append_run_events, run_id, rows)
            except Exception:
                logger.exception("⚠️ 后台任务事件批量落库失败 id=%s n=%s",
                                 run_id, len(rows))
        if not rstate.get("flush_alive") and not rstate.get("dbq"):
            break


async def _spawn_run(user: dict, query: str, model_key: str, *,
                     extra_note: str = "", cont_msg_id: Optional[int] = None) -> str:
    """起一个与连接解耦的后台 run，返回 run_id。

    HTTP /api/chat 与 IM 通道共用这一份：入站只负责「触发」，生成过程独立跑完
    （断网/切后台/刷新都不影响），事件落库、结果可回看。
    """
    global shared_mcp
    if not shared_mcp or not shared_mcp.get("tool_name_to_session"):
        raise RuntimeError("MCP服务尚未就绪")

    # ── 工具上下文 ──
    shared_tools, shared_tool_defs, blocked_tools = filter_shared_tools(
        user,
        shared_mcp["tool_name_to_session"],
        shared_mcp["openai_tools"],
        shared_mcp.get("sessions") or [],
    )
    if blocked_tools:
        logger.info("🔐 用户[%s] 无服务器操作权限，已屏蔽工具：%s",
                    user["username"], "、".join(sorted(blocked_tools)))
    fs = await ensure_user_fs(user)
    tool_map = dict(shared_tools)
    for name in fs["names"]:
        tool_map[name] = fs["session"]
    tool_map["user_filesystem"] = _UserFsShim(user)
    openai_tools = list(shared_tool_defs) + fs["openai_tools"] + [USER_FS_TOOL_DEF]
    if is_admin(user):
        term_sessions = [
            s.get("session") for s in (shared_mcp.get("sessions") or [])
            if s.get("name") == "terminal" and s.get("session") is not None
        ]
        if term_sessions:
            tool_map["terminal_run"] = _TerminalRunShim(term_sessions[0], str(AI_CODE_DIR))
            openai_tools.append(TERMINAL_RUN_TOOL_DEF)

    # ── 系统提示（文件交付约定）──
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

    # ── 用户消息内容（图片原生直传等）──
    user_content, img_note = await _build_user_content(user, query)

    # ── 续写 ──
    cont_msg = None
    cont_partial = ""
    cont_note = ""
    if cont_msg_id:
        cont_msg = get_message(int(cont_msg_id), user["id"])
        if not cont_msg or cont_msg["role"] != "assistant":
            raise HTTPException(status_code=400, detail="要续写的回答不存在（可能历史已被清空）")
        cont_partial = (cont_msg.get("text") or "").strip()
        tail_hint = cont_partial[-500:]
        cont_note = (
            "\n\n[续写任务] 用户点的是「继续」：你上一条回答被截断了（网络中断、页面关闭或用户手动停止），"
            "已生成的部分已在对话历史里（最后那条 assistant 消息）。请紧接着它往下写："
            "直接输出后续内容，不要重复已经写过的段落，不要重新开头、不要复述前文摘要，"
            "也不要解释或致歉。\n"
            + (f"（你上次写到：「…{tail_hint}」——请从这之后接着写）" if tail_hint else "")
        )
        logger.info("↩️ 用户[%s] 续写消息 id=%s（已有 %d 字）",
                    user["username"], cont_msg["id"], len(cont_partial))

    # ── 权限 / 管理员说明 ──
    perm_note = "" if is_admin(user) else (
        "\n\n[权限说明] 当前账号不具备服务器操作权限：你没有终端（terminal）类工具，"
        "无法执行服务器命令，也无法修改服务器上的程序代码。"
        "当用户要求你执行这类操作时，请直接说明需要管理员账号（"
        + "、".join(sorted(ADMIN_USERS)) +
        "），不要尝试用其它工具变通，也不要假装已经完成。"
    )
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
                f"\n[云服务器工作空间] 当前文件空间根目录是 {SERVER_WS_ROOT}："
                "read_file / write_file / edit_file / list_directory / "
                f"search_files 可作用于该目录下的任意真实路径；你自己的代码在 {AI_CODE_DIR}。"
                "优先用它们读写文件，不要再用终端里的 cat/sed/echo 改代码。两点注意："
                "(1) 该根目录之外的路径（如 /etc、/var/log、/tmp）文件工具到不了，需用 terminal_run 跑命令；"
                "(2) 不要在根目录全盘递归搜索（会遍历大量文件），请指定具体子目录。"
            )

    # ── 历史上下文 ──
    history_messages = [{"role": "system", "content": SYSTEM_PROMPT + sys_note + perm_note + admin_note}]
    for m in recent_llm_messages(user["id"]):
        history_messages.append({"role": m["role"], "content": m["text"]})
    turn_note = (extra_note + img_note + cont_note).strip()
    _hist_win = history_window_info()

    lock = _chat_locks.setdefault(user["id"], asyncio.Lock())
    _spec = resolve_llm_spec(model_key)
    logger.info("💬 用户[%s] 提问 → 模型 %s(%s, thinking=%s)｜历史 %d 条(窗口 %s/%d轮)",
                user["username"], _spec["model"], _spec["key"], _spec["thinking"],
                max(0, len(history_messages) - 1), _hist_win["mode"], _hist_win["turns"])

    # ── 后台运行 ──
    run_id = secrets.token_hex(8)
    store.create_run(run_id, user["id"], query, _spec["label"] or _spec["model"],
                     cont_msg_id=int(cont_msg_id) if cont_msg_id else None)
    rstate: Dict[str, Any] = {
        "id": run_id, "user_id": user["id"], "seq": 0, "status": "running",
        "answer": "", "partial": "", "error": "", "msg_id": None,
        "events": [], "waiters": [], "task": None, "finished_at": None,
        "dbq": [], "flush_alive": True, "flusher": None,
    }
    RUNS[run_id] = rstate

    if _EVENT_MODE != "legacy":
        rstate["flusher"] = asyncio.create_task(_flush_run_events(run_id, rstate))

    def _emit(item: Dict[str, Any]) -> None:
        """定稿一条事件：分配 seq → 进内存流 → 排队落库 → 唤醒订阅者。"""
        rstate["seq"] += 1
        item["seq"] = rstate["seq"]
        rstate["events"].append(item)
        if len(rstate["events"]) > 4000:     # 长任务防内存膨胀：只丢最老的一半
            del rstate["events"][:2000]
        if _EVENT_MODE == "legacy":
            try:
                store.append_run_event(run_id, item["seq"], str(item.get("type") or ""),
                                       json.dumps(item, ensure_ascii=False))
            except Exception:
                logger.exception("⚠️ 后台任务事件落库失败 id=%s seq=%s", run_id, item["seq"])
        else:
            rstate["dbq"].append((item["seq"], str(item.get("type") or ""),
                                  json.dumps(item, ensure_ascii=False), time.time()))
        for w in list(rstate["waiters"]):
            w.set()

    _coalescer = _DeltaCoalescer(_EVENT_COALESCE_SEC)

    def _flush_delta() -> None:
        """把攒着的思考/回答增量合并成「一条」事件发出去（顺序、时间线不变）。"""
        for _t, _txt in _coalescer.flush():
            _emit({"type": _t, "delta": _txt})

    def _publish(ev: Dict[str, Any]) -> None:
        """写事件到内存（并排队落库），唤醒订阅者。SSE 流与轮询接口共用这一份。"""
        typ = str(ev.get("type") or "")
        if typ in _EVENT_DELTA_TYPES and ev.get("delta"):
            for _t, _txt in _coalescer.feed(typ, str(ev["delta"]), time.time()):
                _emit({"type": _t, "delta": _txt})
            return
        _flush_delta()                       # 非增量事件：先清空攒着的增量，保持顺序
        _emit(dict(ev))

    async def _run_worker() -> None:
        """真正的生成过程：与任何 HTTP 连接无关，跑到完成/出错为止。"""
        answer: str | None = None
        failed = False
        used_model = ""
        partial = ""
        status = "done"
        err_text = ""
        try:
            async with lock:
                async for ev in agent_loop_stream(
                    tool_name_to_session=tool_map,
                    openai_tools=openai_tools,
                    user_query=user_content,
                    history_messages=history_messages,
                    turn_note=turn_note,
                    model_key=model_key,
                ):
                    if ev.get("type") == "answer" and ev.get("delta"):
                        partial += ev["delta"]
                        rstate["partial"] = partial
                    if ev.get("type") == "done":
                        answer = ev.get("answer") or ""
                        used_model = _spec["label"] or ev.get("model") or _spec["model"]
                        _u = ev.get("usage") or {}
                        if _u:
                            logger.info("📊 用户[%s] %s 轮 本次用量 %s",
                                        user["username"], _u.get("rounds", "?"),
                                        _u.get("summary") or _u)
                    elif ev.get("type") == "error":
                        failed = True
                    _publish(ev)
        except asyncio.CancelledError:
            status = "stopped"
            err_text = "服务重启导致任务中断"
            rstate["error"] = err_text
            _publish({"type": "error", "message": err_text})
            raise
        except Exception as e:
            logger.exception("❌ 后台生成异常 id=%s", run_id)
            status = "error"
            failed = True
            err_text = str(e)
            _publish({"type": "error", "message": f"服务内部错误：{e}"})
        finally:
            _model_col = used_model or _spec["label"] or _spec["model"]
            msg_id = None
            try:
                if cont_msg is not None:
                    tail = (answer or "").strip()
                    if tail:
                        full = (cont_partial + "\n\n" + tail).strip() if cont_partial else tail
                        update_message(cont_msg["id"], user["id"], full,
                                       interrupted=0, model=_model_col or None)
                        msg_id = int(cont_msg["id"])
                        logger.info("✅ 续写完成并回写 id=%s（共 %d 字）", cont_msg["id"], len(full))
                elif answer is not None and answer.strip() and not failed:
                    add_message(user["id"], "user", query)
                    msg_id = add_message(user["id"], "assistant", answer.strip(), used_model)
                elif partial.strip():
                    add_message(user["id"], "user", query)
                    msg_id = add_message(user["id"], "assistant", partial.strip(),
                                         _model_col, interrupted=1)
                    if status == "done":
                        status = "interrupted"
                    logger.info("✂️ 回答未写完，已保存半截（%d 字）并标记可继续", len(partial.strip()))
            except Exception:
                logger.exception("❌ 后台任务落库失败 id=%s", run_id)

            rstate["status"] = status
            rstate["answer"] = (answer or partial or "").strip()
            rstate["error"] = err_text
            rstate["msg_id"] = msg_id
            rstate["finished_at"] = time.time()
            try:
                store.finish_run(run_id, status, rstate["answer"], err_text, msg_id)
            except Exception:
                logger.exception("⚠️ 后台任务状态落库失败 id=%s", run_id)
            _publish({"type": "end", "status": status, "msg_id": msg_id,
                      "interrupted": 1 if status in ("interrupted", "stopped", "error") else 0,
                      "chars": len(rstate["answer"])})
            rstate["flush_alive"] = False    # 让 flusher 写完最后一批后自行退出
            _run_keep_then_forget(run_id)

    rstate["task"] = asyncio.create_task(_run_worker())
    logger.info("🚀 后台任务启动 id=%s 用户[%s]（与连接解耦，断网继续跑）", run_id, user["username"])
    return run_id


async def _im_fetch_run(run_id: str, since: int = 0) -> dict:
    """IM 通道的 run 轮询注入点：不带用户过滤。"""
    mem = RUNS.get(run_id) or {}
    events = _run_events_for(run_id, since)
    status = str(mem.get("status") or "")
    if not status:
        status = "done"   # 内存里没有 → 已结束并被遗忘
    elif status in ("interrupted", "stopped"):
        status = "done"   # 半截/受停 → 让 follow 拿到已有内容后正常收尾
    return {
        "run_id": run_id,
        "status": status,
        "answer": (mem.get("answer") or mem.get("partial") or ""),
        "error": mem.get("error") or "",
        "events": events,
    }


@app.post("/api/chat")
async def chat(req: ChatRequest, request: Request, user: dict = Depends(require_user)):
    global shared_mcp
    if not shared_mcp or not shared_mcp.get("tool_name_to_session"):
        raise HTTPException(status_code=500, detail="MCP服务尚未就绪，请稍后再试")

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

    run_id = await _spawn_run(user, req.query, req.model,
                              extra_note=(ip_note + geo_note).strip(),
                              cont_msg_id=req.continue_msg_id)
    rstate = RUNS[run_id]

    async def event_gen():
        """本次连接只是「订阅者」：把后台任务的事件推给它；断开不取消任务。"""
        yield _sse({"type": "run", "run_id": run_id})
        # 用 seq 当游标，而不是列表下标：内存裁剪(del events[:2000])会挪动下标，
        # 用下标会导致长任务直播时静默漏推事件。
        sent = 0
        while True:
            for _e in list(rstate["events"]):
                if int(_e.get("seq", 0)) > sent:
                    sent = int(_e["seq"])
                    yield _sse(_e)
            if rstate["status"] != "running":
                break
            w = asyncio.Event()
            rstate["waiters"].append(w)
            try:
                await asyncio.wait_for(w.wait(), timeout=15.0)
            except asyncio.TimeoutError:
                yield ": ping\n\n"   # SSE 注释帧保活，前端解析器忽略
            finally:
                if w in rstate["waiters"]:
                    rstate["waiters"].remove(w)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",   # 避免中间代理缓冲拖慢实时性
        },
    )


def _live_run_status(row: dict, mem: dict) -> str:
    """判定任务真实状态。

    库里标 running、但内存里已经没有这个任务 → 它其实早死了（服务重启过、
    或进程被回收）。不修的话前端会一直挂着这个僵尸任务无限轮询：既卡页面，
    又每秒把上万条历史事件从库里重拉一遍。
    """
    st = str(mem.get("status") or row.get("status") or "done")
    if st == "running" and row.get("id") not in RUNS:
        return "interrupted"
    return st


# 挂载静态网页，static文件夹放在项目根目录，里面放index.html
@app.get("/api/run/active")
async def run_active(user: dict = Depends(require_user)):
    """页面重载/换设备回来：返回最近一次后台任务（含进行中的）与它的处理过程。"""
    row = store.latest_run(user["id"])
    if not row:
        return {"run": None}
    rid = row["id"]
    mem = RUNS.get(rid) or {}
    events = _run_events_for(rid, 0)
    status = _live_run_status(row, mem)
    return {"run": {
        "run_id": rid,
        "status": status,
        "query": row["query"],
        "answer": (mem.get("partial") or mem.get("answer") or row["answer"]) or "",
        "error": row["error"],
        "msg_id": row["msg_id"],
        "cont_msg_id": row["cont_msg_id"],
        "created_at": row["created_at"],
        "finished_at": mem.get("finished_at"),
        "seq": events[-1]["seq"] if events else 0,
        "events": events,
    }}


@app.get("/api/run/{run_id}")
async def run_poll(run_id: str, since: int = 0, user: dict = Depends(require_user)):
    """轮询增量事件：断线重连后，前端靠它把「处理过程」补齐。"""
    row = store.get_run(run_id, user["id"])
    if not row:
        raise HTTPException(status_code=404, detail="任务不存在")
    mem = RUNS.get(run_id) or {}
    events = _run_events_for(run_id, since)
    status = _live_run_status(row, mem)
    return {
        "run_id": run_id,
        "status": status,
        "answer": (mem.get("partial") or mem.get("answer") or row["answer"]) or "",
        "msg_id": row["msg_id"],
        "interrupted": 0 if status == "done" else 1,
        "seq": events[-1]["seq"] if events else int(since or 0),
        "events": events,
    }


app.mount("/", StaticFiles(directory="static", html=True), name="static")
