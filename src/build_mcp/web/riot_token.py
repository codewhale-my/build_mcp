"""Riot 绑定链接的一次性令牌。

群里有人要查商店又没绑定时，机器人会回一条链接让他自己登录一次。
这条链接不带登录态，所以用 HMAC 签名一个「用户 id + 过期时间」当凭证：
  * 不需要注册/登录网页端也能打开绑定页（群友大多没有网页账号）；
  * 30 分钟自动失效，且只能绑到自己那个账号名下。
"""
import base64
import hashlib
import hmac
import json
import os
import time

from build_mcp.web.store import DATA_DIR

DEFAULT_TTL = 1800          # 30 分钟
_KEY_FILE = DATA_DIR / "riot_bind.key"
_FALLBACK = b"hjmcp-riot-bind-fallback-key"      # 兜底：密钥文件写不进去时不至于崩


def _key() -> bytes:
    try:
        if _KEY_FILE.exists():
            k = _KEY_FILE.read_bytes().strip()
            if len(k) >= 32:
                return k
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        k = base64.urlsafe_b64encode(os.urandom(32))
        _KEY_FILE.write_bytes(k)
        try:
            os.chmod(_KEY_FILE, 0o600)
        except Exception:                       # noqa: BLE001
            pass
        return k
    except Exception:                           # noqa: BLE001
        return _FALLBACK


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _b64d(txt: str) -> bytes:
    return base64.urlsafe_b64decode(txt + "=" * (-len(txt) % 4))


def _sign(payload: str) -> str:
    return _b64e(hmac.new(_key(), payload.encode(), hashlib.sha256).digest())[:32]


def make_token(user_id: int, ttl: int = DEFAULT_TTL) -> str:
    """给某用户签一条限时绑定令牌。"""
    payload = _b64e(json.dumps({"u": int(user_id), "e": int(time.time()) + int(ttl)}).encode())
    return f"{payload}.{_sign(payload)}"


def parse_token(token: str):
    """校验令牌并返回 user_id；失效/被改过返回 None。"""
    try:
        payload, sig = str(token or "").split(".", 1)
        if not hmac.compare_digest(sig, _sign(payload)):
            return None
        data = json.loads(_b64d(payload))
        if int(data.get("e", 0)) < time.time():
            return None
        return int(data["u"])
    except Exception:                           # noqa: BLE001
        return None
