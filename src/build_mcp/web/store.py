"""
用户 / 邀请码 / 消息 的 SQLite 持久层（无第三方依赖）。

DB 默认放在 ~/build-mcp-data/app.db（项目目录之外）：
  - 避免被 terminal 工具（允许路径 ~/build-mcp）与 filesystem 工具读到；
  - 也避免误提交进 git。

所有函数每次操作独立连接，简单可靠；量级为本机单人/数人使用，无性能问题。

命令行工具（在项目根执行）：
  uv run python -m build_mcp.web.store invite <CODE> [备注]  # 新增邀请码
  uv run python -m build_mcp.web.store invites               # 列出邀请码(含使用状态)
  uv run python -m build_mcp.web.store note <CODE> <备注>    # 改备注
  uv run python -m build_mcp.web.store reset <CODE>          # 已使用的复原为未使用
  uv run python -m build_mcp.web.store revoke <CODE>         # 删除邀请码
  uv run python -m build_mcp.web.store users                 # 列出用户
"""
import argparse
import hashlib
import os
import re
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Optional

# ---------------- 路径配置 ----------------
DATA_DIR = Path(os.environ.get("MCP_WEB_DATA_DIR", str(Path.home() / "build-mcp-data")))
DB_PATH = DATA_DIR / "app.db"

# 每个用户的文件空间根目录：~/fs_workspace/users/u<id>_<用户名>/
FS_ROOT = Path(os.environ.get("MCP_WEB_FS_ROOT", str(Path.home() / "fs_workspace")))
USERS_DIR = FS_ROOT / "users"

USERNAME_RE = re.compile(r"^[A-Za-z0-9_\-\u4e00-\u9fa5]{2,24}$")
# 目录名里不允许的字符一律替换成下划线
_DIR_BAD = re.compile(r"[^\w\u4e00-\u9fa5\-]")

# 邀请码归一化：去首尾空白、剔除零宽/变体选择符（emoji 场景常见隐形字符）、统一大写。
# 存储与查询必须走同一函数，保证 "🐶" 与 "🐶\ufe0f" 视为同一个码。
_INVITE_INVISIBLE = re.compile(r"[\u200b\u200c\u200d\ufeff\ufe0e\ufe0f]")

def normalize_code(code: str) -> str:
    code = _INVITE_INVISIBLE.sub("", (code or "").strip()).upper()
    return code

def code_is_valid(code: str) -> bool:
    """2~32 个可见字符；不允许内部空白与控制字符（emoji、字母数字、连字符、_ 均可）。"""
    if not code or len(code) > 32:
        return False
    return not re.search(r"[\s\x00-\x1f\x7f]", code)

PBKDF2_ITER = 260_000


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FS_ROOT.mkdir(parents=True, exist_ok=True)
    USERS_DIR.mkdir(parents=True, exist_ok=True)


def _conn() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  username   TEXT NOT NULL UNIQUE,
  pass_salt  TEXT NOT NULL,
  pass_hash  TEXT NOT NULL,
  created_at REAL NOT NULL,
  last_seen_version TEXT NOT NULL DEFAULT '',
  ws_mode    TEXT NOT NULL DEFAULT 'local'
);
CREATE TABLE IF NOT EXISTS invite_codes(
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  code       TEXT NOT NULL UNIQUE,
  note       TEXT NOT NULL DEFAULT '',
  created_at REAL NOT NULL,
  used_by    INTEGER,
  used_at    REAL
);
CREATE TABLE IF NOT EXISTS messages(
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users(id),
  role    TEXT NOT NULL CHECK(role IN ('user','assistant')),
  text    TEXT NOT NULL,
  ts      REAL NOT NULL,
  model   TEXT NOT NULL DEFAULT '',
  interrupted INTEGER NOT NULL DEFAULT 0   -- 1=这轮回答没生成完（断网/关页面），可继续
);
CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id, id);

-- 后台运行(run)：把生成任务从 HTTP 连接里摘出来，断网/关页面也继续跑；
-- 过程事件(run_events)全部落库，用户回到页面能看到「它干了什么」。
CREATE TABLE IF NOT EXISTS runs(
  id          TEXT PRIMARY KEY,
  user_id     INTEGER NOT NULL REFERENCES users(id),
  query       TEXT NOT NULL,
  model       TEXT NOT NULL DEFAULT '',
  status      TEXT NOT NULL DEFAULT 'running',
  answer      TEXT NOT NULL DEFAULT '',
  error       TEXT NOT NULL DEFAULT '',
  cont_msg_id INTEGER,
  msg_id      INTEGER,
  created_at  REAL NOT NULL,
  updated_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS run_events(
  run_id TEXT NOT NULL,
  seq    INTEGER NOT NULL,
  type   TEXT NOT NULL DEFAULT '',
  data   TEXT NOT NULL DEFAULT '',
  ts     REAL NOT NULL,
  PRIMARY KEY(run_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_runs_user ON runs(user_id, created_at);

-- Riot（拳头）账号绑定：服务器 IP 不能直接密码登录（Riot 强制人机验证），
-- 由用户在浏览器里登录完成后回填 access_token，后台据此查瓦洛兰特每日商店。
-- ssid = 登录后浏览器里的长期 cookie，用它能在后台自动换新令牌（access_token 只有 1 小时）。
CREATE TABLE IF NOT EXISTS riot_bindings(
  user_id      INTEGER PRIMARY KEY REFERENCES users(id),
  region       TEXT NOT NULL DEFAULT 'ap',
  access_token TEXT NOT NULL DEFAULT '',
  ssid         TEXT NOT NULL DEFAULT '',
  puuid        TEXT NOT NULL DEFAULT '',
  game_name    TEXT NOT NULL DEFAULT '',
  tag_line     TEXT NOT NULL DEFAULT '',
  updated_at   REAL NOT NULL DEFAULT 0
);

-- IM（QQ 群/单聊）滚动摘要：超出「最近 10 条原文」窗口的旧消息，由后台任务
-- 异步压成一段摘要存在这里（不占回复路径，见 web/im_summary.py）。
CREATE TABLE IF NOT EXISTS im_summaries(
  chat_id    TEXT PRIMARY KEY,
  summary    TEXT NOT NULL DEFAULT '',
  lines      INTEGER NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL DEFAULT 0
);

-- IM 指令注入防御记账：谁试图给机器人植入指令（改口癖/系统攻击/JSON 劫持）。
-- 第一次 = 警告；第二次起 = 封禁 banned_until 之前的所有消息（不回复不调模型）。
CREATE TABLE IF NOT EXISTS im_abuses(
  sender_id    TEXT PRIMARY KEY,
  warnings     INTEGER NOT NULL DEFAULT 0,
  banned_until REAL NOT NULL DEFAULT 0,
  updated_at   REAL NOT NULL DEFAULT 0
);

"""


def _migrate(conn: sqlite3.Connection) -> None:
    """轻量迁移：给已有库补上新增列（老库可能缺 model / last_seen_version）。"""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(messages)")}
    if "model" not in cols:
        conn.execute("ALTER TABLE messages ADD COLUMN model TEXT NOT NULL DEFAULT ''")
    if "interrupted" not in cols:
        # interrupted=1：回答因断网/关页面未写完，前端据此在气泡下显示「继续」按钮
        conn.execute("ALTER TABLE messages ADD COLUMN interrupted INTEGER NOT NULL DEFAULT 0")

    ucols = {r[1] for r in conn.execute("PRAGMA table_info(users)")}
    if "last_seen_version" not in ucols:
        conn.execute("ALTER TABLE users ADD COLUMN last_seen_version TEXT NOT NULL DEFAULT ''")
    if "ws_mode" not in ucols:
        # local = 个人工作空间（默认）；server = 云服务器代码目录（仅管理员可选）
        conn.execute("ALTER TABLE users ADD COLUMN ws_mode TEXT NOT NULL DEFAULT 'local'")

    # Riot 绑定：老库没有 ssid 列 → 补上（ssid 用于长期免登录自动换令牌）
    rcols = {r[1] for r in conn.execute("PRAGMA table_info(riot_bindings)")}
    if rcols and "ssid" not in rcols:
        conn.execute("ALTER TABLE riot_bindings ADD COLUMN ssid TEXT NOT NULL DEFAULT ''")


def init_db(import_env_codes: str = ""):
    """建表，并把环境变量里的邀请码(逗号分隔，可带 :备注)导入。"""
    conn = _conn()
    try:
        conn.executescript(SCHEMA)
        _migrate(conn)
        for item in [c.strip() for c in (import_env_codes or "").split(",") if c.strip()]:
            code, _, note = item.partition(":")
            nc = normalize_code(code)
            if nc and code_is_valid(nc):
                conn.execute(
                    "INSERT OR IGNORE INTO invite_codes(code,note,created_at) VALUES(?,?,?)",
                    (nc, note.strip(), time.time()),
                )
        conn.commit()
    finally:
        conn.close()


# ---------------- 密码 ----------------
def hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), bytes.fromhex(salt), PBKDF2_ITER
    ).hex()
    return salt, digest


def verify_password(password: str, salt: str, expect_hash: str) -> bool:
    _, digest = hash_password(password, salt)
    return secrets.compare_digest(digest, expect_hash)


# ---------------- 用户 ----------------
def get_user_by_name(username: str) -> dict | None:
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM users WHERE username=?", (username,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_user_by_id(uid: int) -> dict | None:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def set_user_seen_version(uid: int, version: str) -> None:
    """记录该用户已看过的最新更新版本（用于"下次不再弹窗"）。"""
    conn = _conn()
    try:
        with conn:
            conn.execute(
                "UPDATE users SET last_seen_version=? WHERE id=?",
                ((version or "").strip(), uid),
            )
    finally:
        conn.close()


def set_user_ws_mode(uid: int, mode: str) -> None:
    """记录用户选择的服务器端文件空间模式（local=个人工作空间 / server=云服务器）。"""
    conn = _conn()
    try:
        with conn:
            conn.execute(
                "UPDATE users SET ws_mode=? WHERE id=?",
                ((mode or "local").strip().lower(), uid),
            )
    finally:
        conn.close()


def create_user(username: str, password: str) -> int:
    """创建用户并分配独立文件空间目录；两步同事务，任一步失败都回滚。"""
    salt, digest = hash_password(password)
    user_dir = user_filesystem_dir(username, None)
    conn = _conn()
    try:
        with conn:  # 事务
            cur = conn.execute(
                "INSERT INTO users(username,pass_salt,pass_hash,created_at) VALUES(?,?,?,?)",
                (username, salt, digest, time.time()),
            )
            uid = cur.lastrowid
            user_dir = user_filesystem_dir(username, uid)
            user_dir.mkdir(parents=True, exist_ok=False)
        return uid
    except Exception:
        # 目录若已创建则回滚删除，保持一致性
        try:
            if user_dir.exists():
                user_dir.rmdir()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def user_filesystem_dir(username: str, uid: int | None) -> Path:
    """用户文件空间 = USERS_DIR / u<id>_<sanitized_name>。uid 在注册前未知，先算好路径。"""
    safe = _DIR_BAD.sub("_", username or "user")
    return USERS_DIR / (f"u{uid}_{safe}" if uid is not None else f"u__{safe}")


# ---------------- 邀请码 ----------------
def find_invite(code: str) -> dict | None:
    """查未使用的邀请码（大小写不敏感）。"""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM invite_codes WHERE code=? AND used_by IS NULL",
            (normalize_code(code),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def consume_invite(invite_id: int, user_id: int) -> None:
    conn = _conn()
    try:
        with conn:
            conn.execute(
                "UPDATE invite_codes SET used_by=?, used_at=? WHERE id=? AND used_by IS NULL",
                (user_id, time.time(), invite_id),
            )
    finally:
        conn.close()


def list_invites() -> list[dict]:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT ic.*, u.username AS used_by_name "
            "FROM invite_codes ic LEFT JOIN users u ON ic.used_by=u.id "
            "ORDER BY ic.id DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def delete_invite(code: str) -> bool:
    """删除邀请码，返回是否删到了。"""
    conn = _conn()
    try:
        with conn:
            cur = conn.execute("DELETE FROM invite_codes WHERE code=?", (normalize_code(code),))
        return cur.rowcount > 0
    finally:
        conn.close()


def reset_invite(code: str) -> bool:
    """把一个已被使用的邀请码复原为“未使用”（找回/重新发给别人）。"""
    conn = _conn()
    try:
        with conn:
            cur = conn.execute(
                "UPDATE invite_codes SET used_by=NULL, used_at=NULL WHERE code=?",
                (normalize_code(code),),
            )
        return cur.rowcount > 0
    finally:
        conn.close()


def set_invite_note(code: str, note: str) -> bool:
    """修改邀请码备注。"""
    conn = _conn()
    try:
        with conn:
            cur = conn.execute(
                "UPDATE invite_codes SET note=? WHERE code=?",
                (note.strip(), normalize_code(code)),
            )
        return cur.rowcount > 0
    finally:
        conn.close()


def list_users() -> list[dict]:
    conn = _conn()
    try:
        rows = conn.execute("SELECT id,username,created_at FROM users ORDER BY id").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ---------------- 消息 ----------------
def add_message(user_id: int, role: str, text: str, model: str = "",
                interrupted: int = 0) -> int:
    """追加一条消息，返回新行 id。interrupted=1 表示这轮没答完（可继续）。"""
    conn = _conn()
    try:
        with conn:
            cur = conn.execute(
                "INSERT INTO messages(user_id,role,text,ts,model,interrupted) VALUES(?,?,?,?,?,?)",
                (user_id, role, text, time.time(), model or "", 1 if interrupted else 0),
            )
            return int(cur.lastrowid)
    finally:
        conn.close()


def list_messages(user_id: int, limit: int = 200) -> list[dict]:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT id,role,text,ts,model,interrupted FROM messages "
            "WHERE user_id=? ORDER BY id DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in reversed(rows)]
    finally:
        conn.close()


def get_message(msg_id: int, user_id: int) -> dict | None:
    """取单条消息（限本人），不存在或不属于该用户则返回 None。"""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT id,role,text,ts,model,interrupted FROM messages "
            "WHERE id=? AND user_id=?",
            (msg_id, user_id),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def update_message(msg_id: int, user_id: int, text: str,
                   interrupted: int | None = None, model: str | None = None) -> bool:
    """回写消息正文（续写补完后用）。interrupted 传 0 表示「已补完」。"""
    sets, vals = ["text=?"], [text]
    if interrupted is not None:
        sets.append("interrupted=?")
        vals.append(1 if interrupted else 0)
    if model is not None:
        sets.append("model=?")
        vals.append(model)
    vals += [msg_id, user_id]
    conn = _conn()
    try:
        with conn:
            cur = conn.execute(
                f"UPDATE messages SET {','.join(sets)} WHERE id=? AND user_id=?", vals
            )
            return cur.rowcount > 0
    finally:
        conn.close()


def prev_user_message(msg_id: int, user_id: int) -> dict | None:
    """取该消息之前最近的一条 user 消息（续写时用来还原「当初问的是什么」）。"""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT id,role,text,ts,model,interrupted FROM messages "
            "WHERE user_id=? AND id<? AND role='user' ORDER BY id DESC LIMIT 1",
            (user_id, msg_id),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ---------------- LLM 历史窗口 ----------------
# 两种取法（user+assistant 各一条计 1 轮）：
#
# slide 滑动窗口：永远取最近 N 轮。每来一轮窗口整体前移一条 → 历史前缀每轮都变，
#   而 DeepSeek 前缀缓存是「从第 0 个 token 起逐字节比对」，于是固定前缀之外的历史
#   永远按未命中价（命中价的 30 倍）计费。
#
# block 分块累积（默认）：窗口起点对齐到 N 轮的整数倍、并往回退一整块。块内每来一轮
#   只是在**尾部追加**，起点不动 → 之前发过的历史前缀逐轮复用，命中率显著提高；
#   攒满一整块才整体前移一次（即每 N 轮失效一次，而不是每轮）。
#   窗口长度落在 [N, 2N) 轮之间，**永远不小于**滑动窗口，最多多出一倍上下文；
#   多出来的部分走缓存命中价，因此总成本反而更低。
_HISTORY_TURNS = int(os.environ.get("MCP_WEB_HISTORY_TURNS", "8"))
_HISTORY_MODE = os.environ.get("MCP_WEB_HISTORY_MODE", "block").strip().lower()


def _history_window(total: int, turns: int, mode: str = "block") -> tuple[int, int]:
    """按消息总条数算出历史窗口的 (offset, limit)，offset = 跳过最老的多少条。

    - slide：最近 turns 轮，offset = max(0, total - 2*turns)。
    - block：起点 = floor(total / 2*turns) 的整数倍再往回退一整块，
      窗口长度落在 [2*turns, 4*turns) 条；块内 total 递增时 offset 保持不变。
    """
    if total <= 0:
        return 0, 0
    turns = max(1, int(turns))
    chunk = turns * 2                      # 一块 = turns 轮
    if mode == "slide":
        offset = max(0, total - chunk)
    else:
        offset = max(0, (total // chunk - 1) * chunk)
    return offset, total - offset


def history_window_info() -> dict:
    """当前历史窗口配置（供日志 / 排障）。"""
    return {"turns": _HISTORY_TURNS, "mode": _HISTORY_MODE}


def recent_llm_messages(user_id: int, turns: int | None = None,
                        mode: str | None = None) -> list[dict]:
    """取作为 LLM 上下文的历史消息（默认分块累积窗口，见上方说明）。"""
    turns = _HISTORY_TURNS if turns is None else turns
    mode = _HISTORY_MODE if mode is None else mode
    conn = _conn()
    try:
        total = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE user_id=?", (user_id,)
        ).fetchone()[0]
        offset, limit = _history_window(int(total or 0), turns, mode)
        if limit <= 0:
            return []
        rows = conn.execute(
            # 用 ASC + OFFSET：offset 即「跳过最老的多少条」，语义与 _history_window 一致。
            # （注意别用 DESC + OFFSET——那样 OFFSET 是从最新那头开始跳的，方向正好相反。）
            "SELECT id,role,text FROM messages WHERE user_id=? ORDER BY id ASC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def clear_messages(user_id: int) -> None:
    conn = _conn()
    try:
        with conn:
            conn.execute("DELETE FROM messages WHERE user_id=?", (user_id,))
    finally:
        conn.close()


# ── 后台运行(run)：生成任务与 HTTP 连接解耦后的落库 ──────────────────
def create_run(run_id: str, user_id: int, query: str, model: str = "",
               cont_msg_id: int | None = None) -> None:
    """新建一次后台运行。断网/关页面都不影响它继续跑。"""
    conn = _conn()
    try:
        with conn:
            now = time.time()
            conn.execute(
                "INSERT INTO runs(id,user_id,query,model,status,answer,error,"
                "cont_msg_id,msg_id,created_at,updated_at) "
                "VALUES(?,?,?,?,'running','','',?,NULL,?,?)",
                (run_id, user_id, query, model or "", cont_msg_id, now, now),
            )
    finally:
        conn.close()


def finish_run(run_id: str, status: str, answer: str = "", error: str = "",
               msg_id: int | None = None) -> None:
    """任务收尾：写状态 / 最终文本 / 落库后的消息 id。"""
    conn = _conn()
    try:
        with conn:
            conn.execute(
                "UPDATE runs SET status=?,answer=?,error=?,msg_id=?,updated_at=? WHERE id=?",
                (status, answer or "", error or "", msg_id, time.time(), run_id),
            )
    finally:
        conn.close()


def get_run(run_id: str, user_id: int) -> dict | None:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM runs WHERE id=? AND user_id=?",
                           (run_id, user_id)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def latest_run(user_id: int, fresh_seconds: float = 6 * 3600) -> dict | None:
    """最近一次运行(默认只看 6 小时内)，供页面重载/换设备后恢复显示。"""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM runs WHERE user_id=? AND created_at>=? "
            "ORDER BY created_at DESC LIMIT 1",
            (user_id, time.time() - fresh_seconds)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def append_run_events(run_id: str, rows: list) -> None:
    """批量追加过程事件：一次连接 + 一次事务，避免每条事件单独 fsync。

    rows 为 [(seq, type, data_json, ts), ...]。一次回答会产生上万条过程事件，
    若逐条各开一次连接与事务，每次 commit 都要 fsync 落盘，会直接把 asyncio
    事件循环拖死（表现就是网页很卡）。这里用 executemany 一次写完一批。
    """
    if not rows:
        return
    conn = _conn()
    try:
        with conn:
            conn.executemany(
                "INSERT OR REPLACE INTO run_events(run_id,seq,type,data,ts) VALUES(?,?,?,?,?)",
                [(run_id, int(s), str(t or ""), str(d or ""), float(ts))
                 for (s, t, d, ts) in rows],
            )
    finally:
        conn.close()


def append_run_event(run_id: str, seq: int, type_: str, data: str = "") -> None:
    """追加一条过程事件（旧接口，保留兼容；高频场景请用 append_run_events 批量写）。"""
    append_run_events(run_id, [(seq, type_, data, time.time())])


def run_events_since(run_id: str, since: int = 0, limit: int = 4000) -> list[dict]:
    """取 seq>since 的过程事件（升序），data 为 JSON 字符串。"""
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT seq,type,data FROM run_events WHERE run_id=? AND seq>? "
            "ORDER BY seq LIMIT ?",
            (run_id, int(since or 0), int(limit))).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def reap_stale_runs() -> int:
    """服务重启后：把库里仍标 running 的孤儿任务收成 interrupted（它其实已经死了）。"""
    conn = _conn()
    try:
        with conn:
            cur = conn.execute(
                "UPDATE runs SET status='interrupted', updated_at=? WHERE status='running'",
                (time.time(),))
            return int(cur.rowcount or 0)
    finally:
        conn.close()


def prune_runs(keep: int = 50) -> None:
    """只保留最近 keep 次运行及其事件，避免库无限膨胀。"""
    conn = _conn()
    try:
        with conn:
            ids = [r["id"] for r in conn.execute(
                "SELECT id FROM runs ORDER BY created_at DESC LIMIT -1 OFFSET ?",
                (int(keep),)).fetchall()]
            for rid in ids:
                conn.execute("DELETE FROM run_events WHERE run_id=?", (rid,))
                conn.execute("DELETE FROM runs WHERE id=?", (rid,))
    finally:
        conn.close()


# ---------------- CLI ----------------
def _main():
    parser = argparse.ArgumentParser(prog="store", description="管理用户与邀请码")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_inv = sub.add_parser("invite", help="新增一个邀请码")
    p_inv.add_argument("code", help="邀请码内容（不区分大小写）")
    p_inv.add_argument("note", nargs="?", default="", help="备注，如：赠予对象")

    sub.add_parser("invites", help="列出邀请码及使用状态")

    p_revoke = sub.add_parser("revoke", help="删除一个邀请码（立即失效）")
    p_revoke.add_argument("code", help="要删除的邀请码")

    p_reset = sub.add_parser("reset", help="把已使用的邀请码复原为未使用")
    p_reset.add_argument("code", help="要复原的邀请码")

    p_note = sub.add_parser("note", help="修改邀请码备注")
    p_note.add_argument("code", help="邀请码")
    p_note.add_argument("text", help="新备注内容")

    sub.add_parser("users", help="列出已注册用户")

    args = parser.parse_args()
    init_db()

    if args.cmd == "invite":
        code = normalize_code(args.code)
        if not code_is_valid(code):
            print("❌ 邀请码需为 2~32 个可见字符（emoji/字母/数字/连字符均可，不含空格）")
            raise SystemExit(1)
        conn = _conn()
        try:
            cur = conn.execute(
                "INSERT OR IGNORE INTO invite_codes(code,note,created_at) VALUES(?,?,?)",
                (code, args.note, time.time()),
            )
            conn.commit()
        finally:
            conn.close()
        if cur.rowcount:
            print(f"✅ 邀请码 {code} 已加入（{args.note or '无备注'}）")
        else:
            print(f"ℹ️  邀请码 {code} 已存在，未重复添加")
    elif args.cmd == "invites":
        rows = list_invites()
        if not rows:
            print("（暂无邀请码）")
        for ic in rows:
            used = f"已使用 by {ic['used_by_name']} @{time.strftime('%m-%d %H:%M', time.localtime(ic['used_at']))}" if ic["used_by"] else "未使用"
            print(f"  {ic['code']:<20} {ic['note'] or '':<16} {used}")
    elif args.cmd == "revoke":
        print("✅ 已删除" if delete_invite(args.code) else f"❌ 未找到邀请码 {args.code.upper()}")
    elif args.cmd == "reset":
        print("✅ 已复原为未使用" if reset_invite(args.code) else f"❌ 未找到邀请码 {args.code.upper()}")
    elif args.cmd == "note":
        print("✅ 备注已更新" if set_invite_note(args.code, args.text) else f"❌ 未找到邀请码 {args.code.upper()}")
    elif args.cmd == "users":
        for u in list_users():
            print(f"  #{u['id']:<3} {u['username']:<20} 注册于 {time.strftime('%Y-%m-%d %H:%M', time.localtime(u['created_at']))}")


# ── Riot（拳头）账号绑定 ─────────────────────────────────────────────────────

def save_riot_binding(user_id: int, region: str, access_token: str,
                      puuid: str = "", game_name: str = "", tag_line: str = "",
                      ssid: Optional[str] = None) -> None:
    """保存/覆盖某用户的 Riot 绑定。

    access_token 是浏览器登录换来的短期令牌（1 小时）；
    ssid 是长期 cookie，有它后台就能自动续令牌（不传/空 = 保留库里已有的，不会被清掉）。

    ⚠️ 两个坑（2026-09-15 实测踩到，别改回去）：
      · ssid 列是 NOT NULL → 不能直接往 VALUES 里塞 NULL，SQLite 会当场报
        `NOT NULL constraint failed`，而且 **ON CONFLICT 兜不住 NOT NULL**
        （NOT NULL 不是「冲突」，是立即中止）→ 接口 500。所以用 COALESCE(?,'')。
      · 原来的 `CASE WHEN excluded.ssid IS NULL` 是**死分支**（NULL 根本插不进来），
        要判的是空串 `''`。
    """
    conn = _conn()
    try:
        with conn:
            conn.execute(
                "INSERT INTO riot_bindings(user_id,region,access_token,ssid,puuid,game_name,tag_line,updated_at)"
                " VALUES(?,?,?,COALESCE(?,''),?,?,?,?)"
                " ON CONFLICT(user_id) DO UPDATE SET region=excluded.region,"
                " access_token=excluded.access_token,"
                " ssid=CASE WHEN excluded.ssid = '' THEN riot_bindings.ssid ELSE excluded.ssid END,"
                " puuid=excluded.puuid, game_name=excluded.game_name, tag_line=excluded.tag_line,"
                " updated_at=excluded.updated_at",
                (int(user_id), region or "ap", access_token, ssid, puuid, game_name, tag_line, time.time()),
            )
    finally:
        conn.close()


def update_riot_access_token(user_id: int, access_token: str, ssid: Optional[str] = None) -> None:
    """只刷新短期令牌（用 ssid 自动续期后调用），不动账号信息。

    ssid 传值时**整包覆盖**登录 cookie：Riot 续期会轮换 ssid，若只回写令牌不带
    新 cookie，下一次续期就会失效（「第一次能查、第二次说失效」）。
    """
    if not access_token:
        return
    conn = _conn()
    try:
        with conn:
            if ssid:
                conn.execute("UPDATE riot_bindings SET access_token=?, ssid=?, updated_at=?"
                             " WHERE user_id=?", (access_token, ssid, time.time(), int(user_id)))
            else:
                conn.execute("UPDATE riot_bindings SET access_token=?, updated_at=? WHERE user_id=?",
                             (access_token, time.time(), int(user_id)))
    finally:
        conn.close()


def get_riot_binding(user_id: int) -> Optional[dict]:
    """取某用户的 Riot 绑定（无则 None）。"""
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM riot_bindings WHERE user_id=?",
                           (int(user_id),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def clear_riot_binding(user_id: int) -> None:
    conn = _conn()
    try:
        with conn:
            conn.execute("DELETE FROM riot_bindings WHERE user_id=?", (int(user_id),))
    finally:
        conn.close()


def latest_riot_binding() -> Optional[dict]:
    """最近一次绑定的账号（给 IM 机器人用：主人绑一次，群里就能查）。"""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM riot_bindings WHERE access_token<>'' ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_im_summary(chat_id: str) -> dict:
    """取某会话（群 openid / 用户 openid）的滚动摘要；没有则返回空摘要。"""
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM im_summaries WHERE chat_id=?", (str(chat_id),)).fetchone()
        return dict(row) if row else {"chat_id": str(chat_id), "summary": "", "lines": 0,
                                      "updated_at": 0.0}
    finally:
        conn.close()


def put_im_summary(chat_id: str, summary: str, lines: int = 0) -> None:
    """写入/更新某会话的滚动摘要（后台任务调用，回复路径只读不写）。"""
    conn = _conn()
    try:
        with conn:
            conn.execute(
                "INSERT INTO im_summaries(chat_id,summary,lines,updated_at) VALUES(?,?,?,?)"
                " ON CONFLICT(chat_id) DO UPDATE SET summary=excluded.summary,"
                " lines=excluded.lines, updated_at=excluded.updated_at",
                (str(chat_id), summary or "", int(lines), time.time()),
            )
    finally:
        conn.close()


# ---------------- IM 指令注入防御记账 ----------------

def get_im_abuse(sender_id: str) -> Optional[dict]:
    """取某发送者的注入违规记录；没有返回 None。"""
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM im_abuses WHERE sender_id=?",
                           (str(sender_id),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def record_im_abuse(sender_id: str, ban_seconds: float = 0.0) -> dict:
    """违规次数 +1（ban_seconds>0 时同时写入封禁截止时间），返回最新记录。"""
    sid = str(sender_id)
    ban_until = time.time() + float(ban_seconds) if ban_seconds > 0 else 0.0
    conn = _conn()
    try:
        with conn:
            conn.execute(
                "INSERT INTO im_abuses(sender_id,warnings,banned_until,updated_at) "
                "VALUES(?,1,?,?)"
                " ON CONFLICT(sender_id) DO UPDATE SET"
                " warnings=im_abuses.warnings+1,"
                " banned_until=CASE WHEN ?>0 THEN excluded.banned_until"
                " ELSE im_abuses.banned_until END,"
                " updated_at=excluded.updated_at",
                (sid, ban_until, time.time(), ban_seconds),
            )
        row = conn.execute("SELECT * FROM im_abuses WHERE sender_id=?", (sid,)).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def is_im_banned(sender_id: str) -> bool:
    """该发送者当前是否处于封禁期（封禁截止时间未到）。"""
    rec = get_im_abuse(sender_id)
    return bool(rec) and float(rec.get("banned_until") or 0) > time.time()


if __name__ == "__main__":
    _main()
