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
  model   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id, id);
"""


def _migrate(conn: sqlite3.Connection) -> None:
    """轻量迁移：给已有库补上新增列（老库可能缺 model / last_seen_version）。"""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(messages)")}
    if "model" not in cols:
        conn.execute("ALTER TABLE messages ADD COLUMN model TEXT NOT NULL DEFAULT ''")

    ucols = {r[1] for r in conn.execute("PRAGMA table_info(users)")}
    if "last_seen_version" not in ucols:
        conn.execute("ALTER TABLE users ADD COLUMN last_seen_version TEXT NOT NULL DEFAULT ''")
    if "ws_mode" not in ucols:
        # local = 个人工作空间（默认）；server = 云服务器代码目录（仅管理员可选）
        conn.execute("ALTER TABLE users ADD COLUMN ws_mode TEXT NOT NULL DEFAULT 'local'")


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
def add_message(user_id: int, role: str, text: str, model: str = "") -> None:
    conn = _conn()
    try:
        with conn:
            conn.execute(
                "INSERT INTO messages(user_id,role,text,ts,model) VALUES(?,?,?,?,?)",
                (user_id, role, text, time.time(), model or ""),
            )
    finally:
        conn.close()


def list_messages(user_id: int, limit: int = 200) -> list[dict]:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT id,role,text,ts,model FROM messages WHERE user_id=? ORDER BY id DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in reversed(rows)]
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


if __name__ == "__main__":
    _main()
