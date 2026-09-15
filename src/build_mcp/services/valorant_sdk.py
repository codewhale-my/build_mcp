"""瓦洛兰特（国际服）SDK：每日商店查询 + 皮肤搜索。

数据来源：
- 皮肤图鉴/中文名：valorant-api.com（公开免费接口）
- 每日商店：Riot 官方内部接口（模拟客户端登录，需 Riot 账号凭据）
"""
import asyncio
import base64
import json
import time
import uuid as uuidlib
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


async def _http(url: str, method: str = "GET", body: Optional[dict] = None,
                headers: Optional[Dict[str, str]] = None, timeout: float = 12.0) -> Any:
    """返回 (status, json/text)。"""
    def _do():
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={**_UA, "Content-Type": "application/json", **(headers or {})})
        try:
            _proxy = urllib.request.ProxyHandler({"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"})
            _opener = urllib.request.build_opener(_proxy)
            resp = _opener.open(req, timeout=timeout)
            raw = resp.read().decode("utf-8", "replace")
            return resp.status, raw
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", "replace")
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
                "entitlements_token": ent.get("entitlements_token"),
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
        "X-Riot-Entitlements-Token": ent,
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
    remain = panel.get("SingleItemOffersRemainingDurationSeconds", 0)

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
