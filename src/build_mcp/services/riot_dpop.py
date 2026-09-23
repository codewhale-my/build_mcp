"""Riot DPoP / OAuth 2.0（授权码 + PKCE + 令牌绑定）—— 「登录一次，之后永久免登」的绑定方式。

为什么要在老的「贴 ssid cookie」之外再加这条路
────────────────────────────────────────────
老路线的先天毛病（RIOT.md 里记了三条铁律，条条都是坑）：
  1. ssid 是**登录会话 cookie**：Riot 每次续期都会轮换它，而且会话本身大约三周就烂，
     用户迟早要重登；
  2. 换令牌必须带**整包** cookie（ssid/csid/asid/tdid/__cf_bm…），少一个就被 303 踢回
     登录页 —— 「ssid 明明对却说失效」的主因；UA 还必须装成拳头客户端；
  3. 哪天 Riot 收紧 cookie 策略，整条链路全废。

DPoP 路线换了个思路：首登走 OAuth 2.0 **授权码 + PKCE**（client_id=ritoplus），换令牌时
带上 **DPoP 证明**（RFC 9449，用本地 ES256 私钥签的 JWT）。Riot 把令牌和「我们本地公钥的
指纹」绑在一起（access_token 的 cnf.jkt 声明），此后**只要本地还握着那把私钥**，拿
refresh_token 就能一直换新令牌 —— 全程不需要 cookie，也不需要再开浏览器。

移植说明（源项目 github.com/0forzero/riot-auth-dpop-js，Node/MIT）
──────────────────────────────────────────────────────────────
真正值钱的只有四件事，本文件全部用标准库 + cryptography 重写：
  createDpopProof / jwkThumbprint / loginWithWebOAuth / refreshTokens（+ fetchEntitlements）。
**故意没有照搬它的 Playwright**：它是在服务器上开 Chromium，拦截
`http://localhost/redirect?code=…` 这一次跳转来抓授权码。我们这台服务器 1.6G 内存、
又是机房 IP（Riot 强制人机验证、不信任），既跑不动也不被信任。所以改成：
  ① 后台生成 PKCE + DPoP 私钥，拼出 authorize URL 交给用户；
  ② 用户在**自己的浏览器**里登录（人机验证由 Riot 页面自己处理）；
  ③ 浏览器跳去 http://localhost/redirect?code=… ；本机没服务会打不开，
     **但地址栏里留着 code** —— 用户把整条地址粘回来即可；
  ④ 后台拿 code + code_verifier + DPoP 证明换令牌入库。
交互和老的「③ 贴地址」一模一样，换来的是长期免登。

依赖
────
`cryptography`（服务器上已有，是 pyjwt 的传递依赖、写在 uv.lock 里，版本 50.0.1）。
**故意不写进 pyproject.toml**：那会让 `uv run` 重新解析依赖，而这台机器不该为装包联网。
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid as uuidlib
from typing import Any, Dict, Optional, Tuple

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric import utils as asym_utils

from build_mcp.common.logger import get_logger

logger = get_logger(name="riot_dpop")

# ── 常量：全部照抄参考实现（ritoplus = 拳头手机客户端；offline_access 才有 refresh_token）──
CLIENT_ID = "ritoplus"
AUTHORIZE_ENDPOINT = "https://auth.riotgames.com/authorize"
TOKEN_ENDPOINT = "https://auth.riotgames.com/token"
USERINFO_ENDPOINT = "https://auth.riotgames.com/userinfo"
REDIRECT_URI = "http://localhost/redirect"
SCOPE = ("lol summoner offline_access openid link ban account "
         "riot://riot.authenticator/session.auth")
RIOT_PRM = "alias:change game:play riot:interact chat:text chat:voice friend:add"
DPOP_UA = "RiotGamesApi/26.3.0.0 rso-auth (Android;15.00;AP4A.250105.002;) ritoplus/5.3.0"

# 和 valorant_sdk 一致：服务器上出网必须走本机 mihomo（auth.riotgames.com 被墙/需代理）。
# 这里不 import valorant_sdk —— 反过来是 valorant_sdk import 本模块，会成环。
_PROXY = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}

PENDING_TTL = 1800          # 发起登录到回贴授权码之间允许的最长时间（30 分钟）


# ════════════════════════ base64url / JSON 小工具 ════════════════════════

def b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def b64url_decode(txt: str) -> bytes:
    return base64.urlsafe_b64decode(str(txt) + "=" * (-len(str(txt)) % 4))


def b64url_json(obj: Any) -> str:
    """紧凑 JSON 再 base64url —— JWT 的签名输入必须字节级可复现，不能有多余空格。"""
    return b64url_encode(json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode())


def sha256_b64url(data: Any) -> str:
    """PKCE 的 code_challenge 和 DPoP 的 ath 都用它（SHA-256 后 base64url，无填充）。"""
    if isinstance(data, str):
        data = data.encode()
    return b64url_encode(hashlib.sha256(data).digest())


def _int_b64(value: int, size: int = 32) -> str:
    return b64url_encode(int(value).to_bytes(size, "big"))


# ════════════════════════ DPoP 密钥与证明 ════════════════════════

def new_dpop_key() -> Dict[str, str]:
    """生成一把 ES256（P-256）私钥，导出成 JWK（含私钥分量 d）。

    JWK 里 d/x/y 都是**定长 32 字节大端**再 base64url —— 这一点很要命：用
    `hex` 转出来的数字去掉前导零后长度就变了，Riot 会算出**不同**的指纹，
    于是「明明带着证明却报 invalid_dpop_proof」。所以统一走 _int_b64。
    """
    nums = ec.generate_private_key(ec.SECP256R1()).private_numbers()
    return {
        "kty": "EC",
        "crv": "P-256",
        "x": _int_b64(nums.public_numbers.x),
        "y": _int_b64(nums.public_numbers.y),
        "d": _int_b64(nums.private_value),
    }


def public_jwk(private_jwk: Dict[str, str]) -> Dict[str, str]:
    """公开部分：DPoP 证明的头部要塞它，绝不能带私钥分量 d。"""
    return {k: private_jwk[k] for k in ("kty", "crv", "x", "y")}


def jwk_thumbprint(pub: Dict[str, str]) -> str:
    """RFC 7638 JWK 指纹：只取 crv/kty/x/y，按**字典序**拼紧凑 JSON 再 SHA-256。

    Riot 会把同一个值写进 access_token 的 `cnf.jkt` —— 两边一致才说明绑定成功，
    这也是我们在绑定完成后能自查「到底绑上没绑上」的唯一凭据。
    """
    canonical = json.dumps({"crv": pub["crv"], "kty": pub["kty"],
                            "x": pub["x"], "y": pub["y"]},
                           separators=(",", ":"), sort_keys=True)
    return sha256_b64url(canonical)


def normalize_htu(raw_url: str) -> str:
    """DPoP 的 htu 声明：按 RFC 9449 4.2 去掉 query 与 fragment，只留 scheme://host/path。"""
    p = urllib.parse.urlsplit(raw_url)
    return urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, "", ""))


def _load_private_key(jwk: Dict[str, str]):
    """从 JWK 的 d 分量还原出可签名的私钥（不需要 x/y，derive 会自己算出来）。"""
    return ec.derive_private_key(int.from_bytes(b64url_decode(jwk["d"]), "big"), ec.SECP256R1())


def _sign_es256(jwk: Dict[str, str], data: bytes) -> str:
    """ES256 签名。

    ⚠️ 必须转成 **ieee-p1363（定长 r||s，各 32 字节）**：cryptography 默认输出 DER
    （带 SEQUENCE 头、长度可变），而 JOSE/JWT 要的是裸 r||s。直接塞 DER 出去就是
    「证明看起来合法但验不过」。
    """
    der = _load_private_key(jwk).sign(data, ec.ECDSA(hashes.SHA256()))
    r, s = asym_utils.decode_dss_signature(der)
    return b64url_encode(r.to_bytes(32, "big") + s.to_bytes(32, "big"))


def create_dpop_proof(private_jwk: Dict[str, str], method: str, url: str,
                      access_token: Optional[str] = None,
                      nonce: Optional[str] = None) -> str:
    """生成一条 DPoP 证明 JWT（RFC 9449）。

    - `htm`/`htu`：把证明绑死在「这一个方法 + 这一个地址」上，被截获也没法挪去别处；
    - `jti`：一次性编号，防重放；
    - `ath`：带 access_token 访问业务接口时才加（= SHA-256(access_token)），
      换令牌这一步不加；
    - `nonce`：Riot 有时会先回 400 use_dpop_nonce + `DPoP-Nonce` 响应头，要求
      下一条证明里带上它，见 _post_form 的自动重试。
    """
    header = {"typ": "dpop+jwt", "alg": "ES256", "jwk": public_jwk(private_jwk)}
    payload: Dict[str, Any] = {
        "htm": str(method).upper(),
        "htu": normalize_htu(url),
        "iat": int(time.time()),
        "jti": str(uuidlib.uuid4()),
    }
    if access_token:
        payload["ath"] = sha256_b64url(access_token)
    if nonce:
        payload["nonce"] = nonce
    signing_input = f"{b64url_json(header)}.{b64url_json(payload)}"
    return f"{signing_input}.{_sign_es256(private_jwk, signing_input.encode())}"


def decode_jwt(token: str) -> Dict[str, Any]:
    """不验签地读 JWT 的头/载荷 —— 用来确认 cnf.jkt / sub(puuid) / exp。"""
    parts = str(token or "").split(".")
    if len(parts) < 2:
        return {}
    try:
        return {"header": json.loads(b64url_decode(parts[0])),
                "payload": json.loads(b64url_decode(parts[1]))}
    except Exception:                                  # noqa: BLE001
        return {}


# ════════════════════════ PKCE 与登录地址 ════════════════════════

def new_pkce() -> Tuple[str, str]:
    """生成 (code_verifier, code_challenge)。

    verifier 是 32 字节随机数的 base64url（43 字符，符合 RFC 7636 的 43~128 要求）；
    challenge = base64url(SHA-256(verifier))，方法 S256。
    """
    verifier = b64url_encode(os.urandom(32))
    return verifier, sha256_b64url(verifier)


def build_authorize_url(code_challenge: str, nonce: str = "") -> str:
    """拼出给用户自己浏览器打开的登录地址（PKCE + S256）。"""
    query = {
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "response_type": "code",
        "scope": SCOPE,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "nonce": nonce or str(uuidlib.uuid4()),
    }
    return AUTHORIZE_ENDPOINT + "?" + urllib.parse.urlencode(query)


_CODE_RE = re.compile(r"[?&#]code=([^&\s\"'#]+)")


def parse_code(raw: str) -> str:
    """从用户粘贴的内容里取授权码。

    三种贴法都认：① 整条 `http://localhost/redirect?code=…` 地址（正常情况）；
    ② 带了别的参数/`#` 尾巴的地址；③ 只把 code 本身复制过来了。
    """
    s = (raw or "").strip().strip('"').strip("'")
    if not s:
        return ""
    m = _CODE_RE.search(s)
    if m:
        return urllib.parse.unquote(m.group(1))
    if re.fullmatch(r"[A-Za-z0-9_\-\.]{20,}", s):       # 裸 code（无 URL 特征）
        return s
    return ""


# ════════════════════════ HTTP 层 ════════════════════════

def _hdr(headers: Dict[str, str], name: str) -> str:
    """响应头大小写不定（Node 那边全是小写，HTTPMessage 保留原样）→ 统一小写比对。"""
    low = name.lower()
    for k, v in (headers or {}).items():
        if k.lower() == low:
            return v
    return ""


def _post_form_sync(url: str, form: Dict[str, Any], private_jwk: Dict[str, str],
                    timeout: float = 15.0) -> Tuple[int, str, Dict[str, str]]:
    """POST 表单 + DPoP 证明；遇到 use_dpop_nonce 自动带 nonce 重试一次。"""
    data = urllib.parse.urlencode({k: v for k, v in form.items() if v is not None}).encode()
    base_headers = {
        "User-Agent": DPOP_UA,
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    def _send(nonce: Optional[str] = None) -> Tuple[int, str, Dict[str, str]]:
        req = urllib.request.Request(
            url, data=data, method="POST",
            headers={**base_headers, "DPoP": create_dpop_proof(private_jwk, "POST", url,
                                                              nonce=nonce)})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler(dict(_PROXY)))
        try:
            resp = opener.open(req, timeout=timeout)
            return resp.status, resp.read().decode("utf-8", "replace"), dict(resp.headers)
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", "replace"), dict(e.headers or {})

    last = ""
    for attempt in (1, 2):                       # 网络/代理抖动 → 重试一次
        try:
            status, body, headers = _send()
            break
        except Exception as e:                   # noqa: BLE001
            last = f"{type(e).__name__}: {e}"
            if attempt == 1:
                time.sleep(1.0)
    else:
        return 0, last, {}

    if status == 400:
        try:
            err = (json.loads(body) or {}).get("error")
        except Exception:                        # noqa: BLE001
            err = ""
        nonce = _hdr(headers, "DPoP-Nonce")
        if err == "use_dpop_nonce" and nonce:
            logger.info("🔁 Riot 要求 DPoP nonce，带 nonce 重试一次")
            try:
                return _send(nonce)
            except Exception as e:               # noqa: BLE001
                return 0, f"{type(e).__name__}: {e}", {}
    return status, body, headers


def _get_json_sync(url: str, private_jwk: Dict[str, str], access_token: str,
                   timeout: float = 15.0) -> Tuple[int, str]:
    """带 DPoP 证明（含 ath）的 GET —— 参考实现里 userinfo 就是这么调的。"""
    def _send(nonce: Optional[str] = None) -> Tuple[int, str]:
        req = urllib.request.Request(url, method="GET", headers={
            "User-Agent": DPOP_UA, "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "DPoP": create_dpop_proof(private_jwk, "GET", url, access_token=access_token,
                                      nonce=nonce)})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler(dict(_PROXY)))
        try:
            resp = opener.open(req, timeout=timeout)
            return resp.status, resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", "replace")

    try:
        return _send()
    except Exception as e:                       # noqa: BLE001
        return 0, f"{type(e).__name__}: {e}"


async def _run(fn, *args, **kwargs):
    return await asyncio.get_event_loop().run_in_executor(
        None, lambda: fn(*args, **kwargs))


# ════════════════════════ 令牌交换 / 续期 ════════════════════════

def _friendly_error(status: int, body: str, action: str) -> str:
    """把 Riot 的 OAuth 错误翻成人话（区分「用户/环境问题」和「我们实现的问题」）。"""
    try:
        j = json.loads(body) or {}
    except Exception:                                # noqa: BLE001
        j = {}
    err = str(j.get("error") or "").strip()
    desc = str(j.get("error_description") or "").strip()
    if status == 0:
        return f"{action}失败：连不上 Riot（网络/代理异常）{body[:120]}"
    if err == "invalid_dpop_proof":
        # 这一条几乎一定是实现问题（签名编码/htu/指纹不对），不能甩给用户
        return (f"{action}失败：Riot 拒绝了 DPoP 证明（{desc or err}）。"
                "这是绑定服务自身的问题，请把这句话转告管理员")
    if err == "invalid_grant":
        return (f"{action}失败：凭证已失效（{desc or '一次性、有时效'}）。"
                "请从第 ① 步重新发起一次登录（授权码只能用一次，且几分钟就过期）")
    if err == "invalid_client":
        return f"{action}失败：客户端不被接受（{desc or err}），绑定服务需要更新"
    if err in ("use_dpop_nonce",):
        return f"{action}失败：DPoP nonce 协商没成功（{desc or err}），请再试一次"
    return f"{action}失败：Riot 返回 {status} {err} {desc}".strip()


def _token_result(status: int, body: str, action: str) -> Dict[str, Any]:
    """统一解析 /token 的响应；成功时把 exp/cnf.jkt 一起解出来方便自查。"""
    try:
        j = json.loads(body) or {}
    except Exception:                                # noqa: BLE001
        j = {}
    if status == 200 and j.get("access_token"):
        payload = (decode_jwt(j["access_token"]) or {}).get("payload") or {}
        return {
            "access_token": j["access_token"],
            "refresh_token": j.get("refresh_token") or "",
            "id_token": j.get("id_token") or "",
            "token_type": j.get("token_type") or "",
            "expires_at": time.time() + float(j.get("expires_in") or 3600),
            "puuid": payload.get("sub") or "",
            "jkt": ((payload.get("cnf") or {}).get("jkt")) or "",
        }
    return {"error": _friendly_error(status, body, action)}


async def exchange_code(private_jwk: Dict[str, str], code: str, code_verifier: str,
                        redirect_uri: str = REDIRECT_URI) -> Dict[str, Any]:
    """用授权码 + code_verifier + DPoP 证明换令牌（这一步只有一次机会：码是一次性的）。"""
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
        "client_id": CLIENT_ID,
        "riot_prm": RIOT_PRM,
    }
    status, body, _ = await _run(_post_form_sync, TOKEN_ENDPOINT, form, private_jwk)
    res = _token_result(status, body, "换取令牌")
    if res.get("access_token"):
        logger.info("🎮 DPoP 授权码兑换成功：puuid=%s… cnf.jkt=%s…",
                    str(res.get("puuid"))[:8], str(res.get("jkt"))[:10])
    else:
        logger.warning("⚠️ DPoP 授权码兑换失败：%s", res.get("error"))
    return res


async def refresh_tokens(private_jwk: Dict[str, str], refresh_token: str,
                         id_token: str = "") -> Dict[str, Any]:
    """用 refresh_token + DPoP 证明换新令牌 —— **全程无 cookie、无浏览器**，这就是长期免登。"""
    if not refresh_token:
        return {"error": "没有 refresh_token，请重新登录一次"}
    form = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "id_token": id_token or None,
        "riot_prm": RIOT_PRM,
    }
    status, body, _ = await _run(_post_form_sync, TOKEN_ENDPOINT, form, private_jwk)
    res = _token_result(status, body, "续期令牌")
    if not res.get("access_token"):
        logger.warning("⚠️ DPoP 续期失败：%s", res.get("error"))
    return res


async def userinfo(private_jwk: Dict[str, str], access_token: str) -> Dict[str, Any]:
    """带 DPoP 证明取账号信息（拿 puuid）。给 account_info 的兜底用。"""
    status, body = await _run(_get_json_sync, USERINFO_ENDPOINT, private_jwk, access_token)
    if status == 0:
        return {"error": f"连不上 Riot（网络/代理异常）：{body[:120]}"}
    if status != 200:
        return {"error": f"取账号信息失败（Riot 返回 {status}）"}
    try:
        j = json.loads(body)
    except Exception:                                # noqa: BLE001
        return {"error": "Riot 返回内容无法解析"}
    acct = j.get("acct") or {}
    return {
        "puuid": j.get("sub") or acct.get("puuid") or "",
        "game_name": j.get("gameName") or acct.get("game_name") or "",
        "tag_line": j.get("tagLine") or acct.get("tag_line") or "",
    }
