"""瓦洛兰特（国际服）SDK：每日商店查询 + 皮肤搜索。

数据来源：
- 皮肤图鉴/中文名：valorant-api.com（公开免费接口）
- 每日商店：Riot 官方内部接口（模拟客户端登录，需 Riot 账号凭据）
"""
import asyncio
import base64
import json
import re
import time
import uuid as uuidlib
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional

from build_mcp.common.logger import get_logger

logger = get_logger(name="valorant_sdk")

_UA = {"User-Agent": "Mozilla/5.0"}
_cache: Dict[str, Any] = {}

_CLIENT_PLATFORM = base64.b64encode(json.dumps({
    "platformType": "PC",
    "platformOS": "Windows",
    "platformOSVersion": "10.0.19042.1.256.64bit",
    "platformChipset": "Unknown",
}).encode()).decode()

_REGION_SHARD = {
    "ap": "ap", "na": "na", "eu": "eu", "kr": "kr", "latam": "latam", "br": "br",
}


MIHOMO_CTRL = "http://127.0.0.1:9090"
_PROXY = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}


def _proxy_reset() -> None:
    """把代理选择组切回自动择优（PICK → OUT）。

    排障/扫节点时会把 PICK 钉在某个具体节点上；一旦那个节点挂了，全网都不通，
    表现成"5 秒超时 + HTTP 000"，很容易被误判成"被 Riot 封了"。这里做自愈。
    """
    try:
        req = urllib.request.Request(f"{MIHOMO_CTRL}/proxies/PICK",
                                     data=json.dumps({"name": "OUT"}).encode(),
                                     method="PUT", headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5).read()
        logger.info("🩺 代理已切回自动择优组（PICK→OUT）")
    except Exception:                             # noqa: BLE001  控制器不可用就算了
        pass


async def _http(url: str, method: str = "GET", body: Optional[dict] = None,
                headers: Optional[Dict[str, str]] = None, timeout: float = 12.0) -> Any:
    """返回 (status, json/text)；连不上时代理自愈并重试一次。"""
    def _do():
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={**_UA, "Content-Type": "application/json", **(headers or {})})
        last = ""
        for attempt in (1, 2):
            try:
                _opener = urllib.request.build_opener(urllib.request.ProxyHandler(_PROXY))
                resp = _opener.open(req, timeout=timeout)
                return resp.status, resp.read().decode("utf-8", "replace")
            except urllib.error.HTTPError as e:
                return e.code, e.read().decode("utf-8", "replace")
            except Exception as e:                # noqa: BLE001
                last = f"{type(e).__name__}: {e}"
                if attempt == 1:                  # 自愈：切回择优组，稍等再试一次
                    _proxy_reset()
                    time.sleep(1.0)
        return 0, last
    return await asyncio.get_event_loop().run_in_executor(None, _do)


async def _cached(key: str, ttl: int, fetch):
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    val = await fetch()
    _cache[key] = (now, val)
    return val


# ---------- 皮肤图鉴（valorant-api，免登录） ----------

async def _skin_map(lang: str = "zh-CN") -> Dict[str, str]:
    """skinlevel uuid -> 中文名。缓存 1 天。"""
    async def _fetch():
        st, raw = await _http(f"https://valorant-api.com/v1/weapons/skinlevels?language={lang}")
        if st != 200:
            return {}
        data = json.loads(raw).get("data", [])
        return {x["uuid"]: x.get("displayName", x["uuid"]) for x in data}
    return await _cached(f"skinmap_{lang}", 86400, _fetch)


async def search_skins(keyword: str, limit: int = 8) -> Dict[str, Any]:
    """按关键词搜皮肤（中文名模糊匹配）。"""
    st, raw = await _http("https://valorant-api.com/v1/weapons/skinlevels?language=zh-CN")
    if st != 200:
        return {"error": f"valorant-api 不可达 status={st}"}
    data = json.loads(raw).get("data", [])
    kw = keyword.strip().lower()
    hits = [x for x in data if kw in x.get("displayName", "").lower()]
    return {
        "total": len(hits),
        "items": [{"name": x["displayName"], "uuid": x["uuid"],
                   "icon": x.get("displayIcon")} for x in hits[:limit]],
    }


# ---------- Riot 登录 + 每日商店 ----------

async def _riot_login(username: str, password: str) -> Dict[str, Any]:
    """模拟 play-valorant-web 客户端登录，返回 access_token/puuid/entitlements。"""
    auth_url = "https://auth.riotgames.com/api/v1/authorization"

    # 用 cookiejar 保证整个登录流程共享会话
    import http.cookiejar
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj),
        urllib.request.ProxyHandler({"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}))

    def _req(method, url, body=None, headers=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={**_UA, "Content-Type": "application/json", **(headers or {})})
        resp = opener.open(req, timeout=12)
        return resp.status, resp.read().decode("utf-8", "replace")

    def _sync_flow():
        st, raw = _req("POST", auth_url, body={
            "client_id": "play-valorant-web-prod", "nonce": "1",
            "redirect_uri": "https://playvalorant.com/opt_in",
            "response_type": "token id_token", "scope": "account openid"})
        if st != 200:
            raise RuntimeError(f"auth init {st}: {raw[:200]}")
        st, raw = _req("PUT", auth_url, body={
            "type": "auth", "username": username, "password": password, "remember": True})
        j = json.loads(raw)
        if st != 200:
            raise RuntimeError(f"login {st}: {j.get('type') or raw[:200]}")
        if j.get("type") == "multifactor":
            return {"need_2fa": True}
        uri = j.get("response", {}).get("parameters", {}).get("uri", "")
        if "access_token=" not in uri:
            raise RuntimeError(f"login failed: {j.get('error') or raw[:200]}")
        frag = uri.split("#", 1)[1]
        params = dict(p.split("=", 1) for p in frag.split("&") if "=" in p)
        access_token = params["access_token"]
        st, raw = _req("GET", "https://account.riotgames.com/api/userinfo",
                       headers={"Authorization": f"Bearer {access_token}"})
        uinfo = json.loads(raw)
        st, raw = _req("POST", "https://entitlements.auth.riotgames.com/api/token/entitlements",
                       headers={"Authorization": f"Bearer {access_token}"})
        ent = json.loads(raw)
        return {"access_token": access_token, "puuid": uinfo.get("sub"),
                "entitlements_token": ent.get("entitlements_token") or ent.get("token") or "",
                "game_name": uinfo.get("gameName", ""), "tag_line": uinfo.get("tagLine", "")}

    return await asyncio.get_event_loop().run_in_executor(None, _sync_flow)


async def daily_store(username: str, password: str, region: str = "ap") -> Dict[str, Any]:
    """查询账号当日商店（四件每日皮肤 + 价格 + 剩余刷新秒数）。"""
    region = region.lower()
    shard = _REGION_SHARD.get(region)
    if not shard:
        return {"error": f"未知 region: {region}（可用 ap/na/eu/kr/latam/br）"}

    login = await _riot_login(username, password)
    if login.get("error"):
        return login
    if login.get("need_2fa"):
        return {"error": "该账号开启了双重验证(2FA)，暂不支持，请用无 2FA 的账号或提供 cookie"}

    token = login["access_token"]
    puuid = login["puuid"]
    ent = login["entitlements_token"]

    # 客户端版本（valorant-api 公开）
    st, raw = await _http("https://valorant-api.com/v1/version")
    client_version = json.loads(raw)["data"]["riotClientVersion"] if st == 200 else "release-13.05-shipping-11-5350494"

    base_headers = {
        "Authorization": f"Bearer {token}",
        "X-Riot-Entitlements-JWT": ent,
        "X-Riot-ClientVersion": client_version,
        "X-Riot-ClientPlatform": _CLIENT_PLATFORM,
    }

    def _sync():
        cj_less = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}))
        def _g(url):
            req = urllib.request.Request(url, headers={**_UA, **base_headers})
            resp = cj_less.open(req, timeout=12)
            return json.loads(resp.read().decode("utf-8", "replace"))
        storefront = _g(f"https://pd.{shard}.a.pvp.net/store/v2/storefront/{puuid}")
        try:
            offers = _g(f"https://pd.{shard}.a.pvp.net/store/v3/offers")
        except Exception:
            offers = {}
        return storefront, offers

    try:
        storefront, offers = await asyncio.get_event_loop().run_in_executor(None, _sync)
    except Exception as e:
        return {"error": f"商店接口失败: {e}"}

    panel = storefront.get("SkinsPanelLayout", {})
    offer_uuids = panel.get("SingleItemOffers", []) or []
    remain = panel.get("SingleItemOffersRemainingDurationInSeconds",
                       panel.get("SingleItemOffersRemainingDurationSeconds", 0)) or 0

    # 价格表：offer uuid -> VP 价格
    price_map: Dict[str, int] = {}
    for o in (offers.get("Offers") or []):
        if o.get("IsDirectPurchase") is False:
            for cost in (o.get("Cost") or {}).values():
                price_map[o.get("OfferID")] = cost
    # v2 兜底
    for o in (storefront.get("SingleItemOffers") or []):
        pass

    skinmap = await _skin_map()
    items = []
    for u in offer_uuids:
        items.append({
            "name": skinmap.get(u, u),
            "uuid": u,
            "price_vp": price_map.get(u),
        })

    return {
        "player": f'{login.get("game_name","")}#{login.get("tag_line","")}',
        "region": region,
        "refresh_in_seconds": remain,
        "items": items,
    }


# ══════════════ 浏览器登录模式（绕过 Riot 对机房 IP 的强制人机验证）══════════════
# 服务器/VPS 的 IP 在 Riot 眼里是"风险 IP"，密码登录一律返回 auth_failure（要求 hCaptcha）。
# 因此：用户在【自己的浏览器】里打开下面这条官方授权页登录（人机验证在浏览器里完成），
# 登录成功后浏览器停在 https://playvalorant.com/opt_in#access_token=...&id_token=...
# 用户把整条地址粘回网页端 → 后台取出 access_token → 调 Riot 接口查商店。
RIOT_AUTH_URL = (
    "https://auth.riotgames.com/authorize"
    "?client_id=play-valorant-web-prod&nonce=1"
    "&redirect_uri=https%3A%2F%2Fplayvalorant.com%2Fopt_in"
    "&response_type=token%20id_token&scope=account%20openid"
)

_ACCESS_TOKEN_RE = re.compile(r"access_token=([^&\s\"'#]+)")
_JWT_RE = re.compile(r"^[A-Za-z0-9_\-\.]{80,}$")


def parse_access_token(raw: str) -> str:
    """从用户粘贴的内容里取出 access_token：支持整条 URL（#/query 都行）或裸令牌。"""
    s = (raw or "").strip().strip('"').strip("'")
    if not s:
        return ""
    m = _ACCESS_TOKEN_RE.search(s)
    if m:
        return urllib.parse.unquote(m.group(1))
    if _JWT_RE.match(s) and s.count(".") == 2:
        return s
    return ""


_SSID_RE = re.compile(r"ssid\s*[=:]\s*[\"']?([^;'\"\s\\]+)")


def extract_ssid(raw: str) -> str:
    """从用户粘贴的内容里取出 ssid cookie；取不到返回 ""。

    三种贴法都认：① 纯 ssid 值（eyJ… 长串）；② `ssid=xxx; 其它cookie=…` 整段 Cookie；
    ③ 开发者工具 Network 里的「Copy as cURL」整段文本。
    """
    s = (raw or "").strip().strip('"').strip("'")
    for _ in range(2):                             # 容错：整串可能是 URL 编码过的
        if not s:
            return ""
        m = _SSID_RE.search(s)
        if m:
            return urllib.parse.unquote(m.group(1)).strip()
        if len(s) >= 40 and not re.search(r"[\s;'\"]", s) and re.fullmatch(r"[A-Za-z0-9_\-\.]+", s):
            return s                               # 裸 ssid 值
        if "%" not in s:
            return ""
        s = urllib.parse.unquote(s)
    return ""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """禁止 urllib 自动跟随 3xx —— cookie 续期需要自己读 Location 头。"""
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        return None


async def cookie_login(ssid: str) -> Dict[str, Any]:
    """用 ssid cookie 在后台换一个新的 access_token（不触发人机验证）。

    access_token 只有 1 小时，ssid 是长期 cookie —— 有它就能"登录一次、以后一直查"。
    ssid 失效（改密码/长时间不用/被踢）时返回 error，需要用户重新登录一次。
    """
    ssid = (ssid or "").strip().strip('"').strip("'")
    if not ssid:
        return {"error": "没识别到 ssid，请重新复制"}
    # 常见误贴：tdid（设备标识 JWT，以 eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9 开头、两百多字符）
    if ssid.startswith("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"):
        return {"error": "你贴的这串是 tdid（设备标识），不是 ssid。请在 Cookies 列表里找"
                         "名字叫【ssid】的那一行，复制它的 Value（一般 100 字符以上）"}
    if len(ssid) < 32:
        return {"error": f"这串只有 {len(ssid)} 个字符，太短了不像 ssid。"
                         "请确认复制的是 Cookies 里【名字叫 ssid】那一行的完整 Value"}

    # Cookie 续期走 /authorize（同 SkinPeek redeemCookies）：GET + ssid cookie → 303，
    # Location 带 #access_token=... 为成功；Location 以 /login 开头 = ssid 无效。
    # ⚠️ 不要用 PUT /api/v1/authorization —— 那是密码登录入口，会无视 ssid 另开会话，永远 type=auth。
    auth_url = ("https://auth.riotgames.com/authorize?redirect_uri="
                "https%3A%2F%2Fplayvalorant.com%2Fopt_in&client_id=play-valorant-web-prod"
                "&response_type=token%20id_token&scope=account%20openid&nonce=1")

    def _sync():
        last = ""
        for attempt in (1, 2):
            req = urllib.request.Request(auth_url, headers={**_UA, "Cookie": f"ssid={ssid}"})
            opener = urllib.request.build_opener(
                _NoRedirect(), urllib.request.ProxyHandler(dict(_PROXY)))
            try:
                resp = opener.open(req, timeout=15)
                loc = resp.geturl()                       # 200 直落 = 没带上会话
                return {"type": "page", "url": loc}
            except urllib.error.HTTPError as e:
                loc = e.headers.get("Location", "") if e.headers else ""
                if e.code in (301, 302, 303, 307, 308):
                    return {"type": "redirect", "location": loc,
                            "set_cookie": (e.headers.get("Set-Cookie") or "") if e.headers else ""}
                return {"type": "http", "status": e.code}
            except Exception as e:                        # noqa: BLE001
                last = f"{type(e).__name__}: {e}"
                if attempt == 1:                          # 网络/代理抖动 → 自愈重试一次
                    _proxy_reset()
                    time.sleep(1.0)
        return {"type": "net_error", "detail": last}

    try:
        j = await asyncio.get_event_loop().run_in_executor(None, _sync)
    except Exception as e:                        # noqa: BLE001
        return {"error": f"续期失败（网络异常）：{e}"}

    if j.get("type") == "redirect":
        loc = j.get("location") or ""
        if loc.startswith("/login"):
            return {"error": "这个 ssid 已经失效（Riot 要它重新登录）：可能改过密码或太久没用，"
                             "请在群里让机器人再发一条绑定链接"}
        tok = parse_access_token(loc)
        if tok:
            out = {"access_token": tok}
            m = re.search(r"[; ]ssid=([^;\\s]+)", j.get("set_cookie") or "")
            if m:                                     # Riot 轮换了 ssid → 一并回传让调用方更新
                out["new_ssid"] = m.group(1)
            return out
        return {"error": "Riot 返回的重定向里没有令牌，请重新登录一次"}
    if j.get("type") == "net_error":
        return {"error": f"连不上 Riot（网络/代理异常）：{str(j.get('detail'))[:100]}"}
    if j.get("type") == "http" and j.get("status") == 403:
        return {"error": "Riot 拒绝了请求（403，可能是 Cloudflare/风控），稍后再试一次"}
    return {"error": "登录状态已失效，请在群里让机器人再发一条绑定链接"}


async def account_info(access_token: str) -> Dict[str, Any]:
    """用令牌取账号信息（puuid / 游戏名#Tag）。令牌无效或过期会返回 error。"""
    st, raw = await _http("https://auth.riotgames.com/userinfo",
                          headers={"Authorization": f"Bearer {access_token}"})
    if st == 0:
        return {"error": f"连不上 Riot（网络/代理异常，稍后重试）：{raw[:120]}"}
    if st != 200:
        return {"error": f"令牌无效或已过期（Riot 返回 {st}）"}
    try:
        j = json.loads(raw)
    except Exception:
        return {"error": "Riot 返回内容无法解析"}
    acct = j.get("acct") or {}
    return {
        "puuid": j.get("sub") or acct.get("puuid") or "",
        "game_name": j.get("gameName") or acct.get("game_name") or "",
        "tag_line": j.get("tagLine") or acct.get("tag_line") or "",
    }


async def _entitlements_token(access_token: str) -> str:
    """取 entitlements JWT —— pd.*.a.pvp.net 的商店接口必须带 X-Riot-Entitlements-JWT，
    缺了就是 403 MISSING_ENTITLEMENT。两个端点/键名都兼容（官方文档里混用过）。"""
    for url in ("https://entitlements.auth.riotgames.com/api/token/v1",
                "https://entitlements.auth.riotgames.com/api/token/entitlements"):
        st, raw = await _http(url, method="POST",
                              headers={"Authorization": f"Bearer {access_token}"})
        if st != 200:
            continue
        try:
            j = json.loads(raw)
        except Exception:                             # noqa: BLE001
            continue
        ent = j.get("entitlements_token") or j.get("token") or ""
        if ent:
            return ent
    return ""


async def store_with_token(access_token: str, region: str = "ap") -> Dict[str, Any]:
    """用已登录令牌查每日商店（不再需要密码，因此不触发人机验证）。"""
    region = (region or "ap").lower()
    shard = _REGION_SHARD.get(region)
    if not shard:
        return {"error": f"未知 region: {region}（可用 ap/na/eu/kr/latam/br）"}

    info = await account_info(access_token)
    if info.get("error"):
        return info
    puuid = info.get("puuid") or ""
    if not puuid:
        return {"error": "令牌里没有 puuid，请重新登录后再粘贴一次"}

    ent = await _entitlements_token(access_token)
    if not ent:
        return {"error": "拿不到 entitlements 令牌（Riot 接口异常），请在群里再要一次绑定链接重新登录"}

    st, raw = await _http("https://valorant-api.com/v1/version")
    client_version = "release-13.05-shipping-11-5350494"
    try:
        if st == 200:
            client_version = json.loads(raw)["data"]["riotClientVersion"] or client_version
    except Exception:
        pass

    hdrs = {
        "Authorization": f"Bearer {access_token}",
        "X-Riot-Entitlements-JWT": ent,
        "X-Riot-ClientVersion": client_version,
        "X-Riot-ClientPlatform": _CLIENT_PLATFORM,
    }
    # v3 storefront：必须 POST + 空 body {}（GET v2 已下线→404，GET v3→405，参照 SkinPeek 实现）
    st, raw = await _http(f"https://pd.{shard}.a.pvp.net/store/v3/storefront/{puuid}",
                          method="POST", body={}, headers=hdrs)
    if st == 0:
        return {"error": f"连不上 Riot 商店服务（网络/代理异常，稍后重试）：{raw[:120]}"}
    if st != 200:
        return {"error": f"商店接口失败（Riot 返回 {st}）：{raw[:160]}"}
    try:
        storefront = json.loads(raw)
    except Exception:
        return {"error": "商店接口返回无法解析"}

    panel = storefront.get("SkinsPanelLayout", {}) or {}
    uuids = panel.get("SingleItemOffers") or []
    remain = panel.get("SingleItemOffersRemainingDurationInSeconds",
                       panel.get("SingleItemOffersRemainingDurationSeconds", 0)) or 0

    # 价格直接在 storefront 响应里：SingleItemStoreOffers[*].{OfferID,Cost}
    price_map: Dict[str, int] = {}
    for o in (panel.get("SingleItemStoreOffers") or []):
        for cost in (o.get("Cost") or {}).values():
            try:
                price_map[o.get("OfferID")] = int(cost)
            except Exception:                             # noqa: BLE001
                pass
    if not price_map:                                     # 兜底：独立价格表端点
        st2, raw2 = await _http(f"https://pd.{shard}.a.pvp.net/store/v3/offers", headers=hdrs)
        if st2 == 200:
            try:
                for o in (json.loads(raw2).get("Offers") or []):
                    if o.get("IsDirectPurchase") is False:
                        for cost in (o.get("Cost") or {}).values():
                            price_map[o.get("OfferID")] = int(cost)
            except Exception:                             # noqa: BLE001
                pass
    skinmap = await _skin_map()
    items = [{"name": skinmap.get(u, u), "uuid": u, "price_vp": price_map.get(u)} for u in uuids]
    return {
        "player": f'{info.get("game_name","")}#{info.get("tag_line","")}'.strip("#"),
        "region": region,
        "refresh_in_seconds": remain,
        "items": items,
    }


async def bound_daily_store(region: str = "", uid: Any = None) -> Dict[str, Any]:
    """用已绑定的账号查每日商店。

    uid 指定时查那个用户自己的绑定（群里每个人各绑各的）；
    不指定则回退到最近一次绑定（兼容老调用）。
    """
    try:
        from build_mcp.web import store as _webstore
        row = (_webstore.get_riot_binding(int(uid)) if uid else None) \
            or _webstore.latest_riot_binding()
    except Exception as e:                                    # noqa: BLE001
        return {"error": f"读取绑定信息失败：{e}"}
    if not row:
        return {"error": "还没绑定 Riot 账号：请打开绑定链接登录一次"
                         "（Riot 对服务器 IP 强制人机验证，只能在浏览器里登录）"}
    region = (region or row.get("region") or "ap").lower()
    token = row.get("access_token") or ""
    ssid = row.get("ssid") or ""

    # 长期绑定：有 ssid 就每次换一张新令牌（access_token 只有 1 小时，不换必过期）
    if ssid:
        got = await cookie_login(ssid)
        if got.get("access_token"):
            token = got["access_token"]
            try:
                _webstore.update_riot_access_token(int(row["user_id"]), token)
            except Exception as e:                        # noqa: BLE001  刷新不影响本次查询
                logger.warning("刷新 Riot 令牌入库失败：%s", e)
        elif not token:
            return {"error": got.get("error") or "登录状态已失效，请重新绑定"}

    if not token:
        return {"error": "还没绑定 Riot 账号：请打开绑定链接登录一次"}
    res = await store_with_token(token, region)
    if res.get("error") and ("401" in str(res.get("error")) or "403" in str(res.get("error"))):
        res["error"] += ("（登录状态已失效，请在群里让机器人再发一条绑定链接）" if ssid
                         else "（如想长期免登录，请在绑定页粘贴一次 ssid）")
    return res
