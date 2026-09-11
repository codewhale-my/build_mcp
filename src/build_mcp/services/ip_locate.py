# src/build_mcp/services/ip_locate.py
"""免费 IP 归属地查询（备用库，无需 API Key）。

为什么需要它：高德 /v3/ip 对部分网段没有覆盖（尤其运营商蜂窝出口，
如中国移动 39.144.0.0/16），会返回 status=1 但 province/city 全空。
这时依次尝试国内免费 IP 库，拿到省/市后用高德地理编码换成经纬度，
保证下游 search_nearby 依然可用。

接口（全部免 key、免注册，按顺序尝试，任一命中即返回）：
  1. pconline   https://whois.pconline.com.cn/ipJson.jsp   境内 IP 覆盖好、中文名，~0.1s
  2. ipinfo.io  https://ipinfo.io/{ip}/json                境外库，阿里云北京可达，~0.4s
  3. ipwho.is   https://ipwho.is/{ip}                      境外库，大陆机房常超时，放最后

注：实测过但被淘汰的接口——api.vore.top（服务端 Redis 故障返回 HTML）、
bilibili zone（忽略 ip 参数，永远返回调用方 IP，会造成"用服务器 IP 定位用户"的错误）、
ipapi.co（共享 IP 触发 429）。这些不要再加回来。
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Awaitable, Callable, Optional

import httpx

logger = logging.getLogger("ip_locate")

TIMEOUT = 4.0
# 单个库的最长等待：外部客户端的 timeout 可能是 10s，直接把链路拖死；
# 这里统一卡住上限，hanging 的库最多浪费 3.5s 就跳到下一个。
PER_PROVIDER_TIMEOUT = 3.5
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) hj-mcp/1.0",
    "Accept": "application/json,text/plain,*/*",
}

# 内网 / 非公网地址直接跳过
_PRIVATE_PREFIX = ("10.", "127.", "192.168.", "169.254.", "0.", "255.")
_PRIVATE_172 = tuple(f"172.{i}." for i in range(16, 32))


def is_public_ip(ip: Optional[str]) -> bool:
    """粗判是不是可用于查询的公网 IP（不追求完备，够用即可）。"""
    if not ip or not isinstance(ip, str):
        return False
    ip = ip.strip()
    if ip in ("unknown", "localhost", "::1"):
        return False
    if ip.startswith(_PRIVATE_PREFIX) or ip.startswith(_PRIVATE_172):
        return False
    if ":" in ip:  # IPv6 这些免费库基本不支持
        return False
    return True


def _first_str(d: dict, *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _clean(name: str) -> str:
    """把 '中国北京市' / '北京城区' 之类清一下，便于地理编码。"""
    if not name:
        return ""
    for junk in ("中国", "中华人民共和国"):
        if name.startswith(junk) and len(name) > len(junk):
            name = name[len(junk):]
    return name.strip()


def _decode(raw: bytes) -> str:
    """pconline 返回体可能是 GBK，按 utf-8 → gbk → gb18030 依次尝试。"""
    for enc in ("utf-8", "gbk", "gb18030"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


async def _via_pconline(client: httpx.AsyncClient, ip: str) -> Optional[dict]:
    r = await client.get(
        "https://whois.pconline.com.cn/ipJson.jsp",
        params={"ip": ip, "json": "true"},
        headers=_HEADERS,
    )
    if r.status_code != 200:
        return None
    text = _decode(r.content)
    if "{" not in text:
        return None
    try:
        data = json.loads(text[text.find("{"): text.rfind("}") + 1])
    except ValueError:
        # 偶尔会被限流/返回错误页，交给下一个库，不要抛异常刷日志
        logger.info("pconline 返回体不是 JSON（可能被限流）")
        return None
    if not isinstance(data, dict) or data.get("err"):
        return None
    prov = _clean(_first_str(data, "pro", "province"))
    city = _clean(_first_str(data, "city"))
    if not prov and not city:
        return None
    return {
        "province": prov,
        "city": city or prov,
        "isp": _first_str(data, "addr"),
        "source": "pconline",
    }


async def _via_ipwho(client: httpx.AsyncClient, ip: str) -> Optional[dict]:
    r = await client.get(f"https://ipwho.is/{ip}", params={"lang": "zh"}, headers=_HEADERS)
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except ValueError:
        logger.info("ipwho.is 返回体不是 JSON")
        return None
    if not isinstance(data, dict) or not data.get("success"):
        return None
    prov = _clean(_first_str(data, "region", "region_name"))
    city = _clean(_first_str(data, "city"))
    if not prov and not city:
        return None
    conn = data.get("connection") if isinstance(data.get("connection"), dict) else {}
    return {
        "province": prov,
        "city": city or prov,
        "isp": _first_str(conn, "isp"),
        "lnglat": _fmt_lnglat(data.get("longitude"), data.get("latitude")),
        "source": "ipwho.is",
    }


async def _via_ipinfo(client: httpx.AsyncClient, ip: str) -> Optional[dict]:
    r = await client.get(f"https://ipinfo.io/{ip}/json", headers=_HEADERS)
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except ValueError:
        logger.info("ipinfo.io 返回体不是 JSON")
        return None
    if not isinstance(data, dict) or data.get("error"):
        return None
    prov = _clean(_first_str(data, "region"))
    city = _clean(_first_str(data, "city"))
    if not prov and not city:
        return None
    loc = _first_str(data, "loc")           # "lat,lng"，要转成高德习惯的 "lng,lat"
    lnglat = None
    if "," in loc:
        lat, _, lng = loc.partition(",")
        lnglat = _fmt_lnglat(lng, lat)
    return {
        "province": prov,
        "city": city or prov,
        "isp": _first_str(data, "org"),
        "lnglat": lnglat,
        "source": "ipinfo.io",
    }


def _fmt_lnglat(lng: Any, lat: Any) -> Optional[str]:
    """把库给的经纬度统一成高德习惯的 "lng,lat"，非法值返回 None。"""
    try:
        f_lng, f_lat = float(lng), float(lat)
    except (TypeError, ValueError):
        return None
    if not (-180 <= f_lng <= 180 and -90 <= f_lat <= 90):
        return None
    return f"{f_lng:.6f},{f_lat:.6f}"


# 顺序有讲究：pconline 最快且中文名最准（境内机房 0.1s 级）；
# ipinfo.io 从阿里云北京可达（0.4s）；ipwho.is 从大陆机房经常超时，放到最后兜底。
_PROVIDERS: tuple[Callable[[httpx.AsyncClient, str], Awaitable[Optional[dict]]], ...] = (
    _via_pconline,
    _via_ipinfo,
    _via_ipwho,
)


async def locate(ip: str, client: httpx.AsyncClient = None) -> Optional[dict]:
    """依次尝试各免费 IP 库，返回 {"province","city","isp","lnglat","source"}；全失败返回 None。

    lnglat 为可选字段（"lng,lat"，高德习惯），只有库自带经纬度时才有。

    Args:
        ip: 待查询的公网 IP。
        client: 复用外部 httpx 客户端（可带代理配置）；为空时自建一个。
    """
    if not is_public_ip(ip):
        logger.info("跳过备用 IP 库：%s 不是可查询的公网 IP", ip)
        return None

    own = client is None
    c = client or httpx.AsyncClient(timeout=TIMEOUT, headers=_HEADERS, follow_redirects=True)
    try:
        for fn in _PROVIDERS:
            t0 = time.monotonic()
            try:
                res = await asyncio.wait_for(fn(c, ip), timeout=PER_PROVIDER_TIMEOUT)
            except asyncio.TimeoutError:
                logger.info("备用 IP 库 %s 超时(>%ss)，跳过", fn.__name__, PER_PROVIDER_TIMEOUT)
                continue
            except Exception as e:  # 单个库失败不影响下一个
                logger.warning("备用 IP 库 %s 查询失败(%s)：%s", fn.__name__, ip, e)
                continue
            logger.info("备用 IP 库 %s 耗时 %.2fs", fn.__name__, time.monotonic() - t0)
            if res and (res.get("province") or res.get("city")):
                logger.info("备用 IP 库命中 %s：%s → %s", fn.__name__, ip, res)
                return res
        logger.info("备用 IP 库均无 %s 的归属地数据", ip)
        return None
    finally:
        if own:
            await c.aclose()
