"""行情数据 SDK：A股/美股指数（腾讯）、汇率（ERAPI）、BTC/ETH（Gate.io）"""
import asyncio
import json
import re
import time
from typing import Any, Dict, Optional

import urllib.request

from build_mcp.common.logger import get_logger

logger = get_logger(name="quote_sdk")

_UA = {"User-Agent": "Mozilla/5.0"}
# 简单内存缓存，避免高频打源
_cache: Dict[str, Any] = {}


async def _http_get(url: str, timeout: float = 8.0, headers: Optional[Dict[str, str]] = None) -> str:
    def _do():
        req = urllib.request.Request(url, headers={**_UA, **(headers or {})})
        raw = urllib.request.urlopen(req, timeout=timeout).read()
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.decode("gbk", "replace")
    return await asyncio.get_event_loop().run_in_executor(None, _do)


async def _cached(key: str, ttl: int, fetch):
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    val = await fetch()
    _cache[key] = (now, val)
    return val


# ---------- 腾讯行情（A股 / 美股指数） ----------

def _parse_tencent(raw: str) -> Dict[str, Any]:
    """解析 qt.gtimg.cn 返回，字段以 ~ 分隔"""
    out: Dict[str, Any] = {}
    for m in re.finditer(r'v_(\w+)="([^"]*)"', raw):
        code, payload = m.group(1), m.group(2)
        f = payload.split("~")
        if len(f) < 10:
            continue
        item: Dict[str, Any] = {"name": f[1], "code": code}
        # 指数/股票通用：3=现价 4=昨收 5=今开 31=涨跌 32=涨跌幅%
        if len(f) > 32:
            item.update({
                "price": f[3], "prev_close": f[4], "open": f[5],
                "change": f[31], "change_pct": f[32],
            })
        # 美股带日期时间(字段30)与币种
        if code.startswith("us"):
            item["time"] = f[30] if len(f) > 30 else ""
            for i, v in enumerate(f):
                if v in ("USD", "CNY", "HKD"):
                    item["currency"] = v
                    break
        else:
            item["time"] = f[30] if len(f) > 30 and len(f[30]) >= 14 else ""
        out[code] = item
    return out


async def quote_tencent(codes: str, ttl: int = 15) -> Dict[str, Any]:
    async def _fetch():
        raw = await _http_get(f"https://qt.gtimg.cn/q={codes}")
        return _parse_tencent(raw)
    return await _cached(f"tx:{codes}", ttl, _fetch)


# ---------- 汇率（open.er-api.com） ----------

async def fx_rates(base: str = "USD", symbols: Optional[str] = None, ttl: int = 600) -> Dict[str, Any]:
    async def _fetch():
        raw = await _http_get(f"https://open.er-api.com/v6/latest/{base}")
        d = json.loads(raw)
        if d.get("result") != "success":
            raise RuntimeError(f"fx API error: {d}")
        return {
            "base": d.get("base_code", base),
            "rates": d.get("rates", {}),
            "updated": d.get("time_last_update_utc", ""),
        }
    data = await _cached(f"fx:{base}", ttl, _fetch)
    if symbols:
        want = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        data = {**data, "rates": {k: v for k, v in data["rates"].items() if k in want}}
    return data


# ---------- 加密货币（Gate.io） ----------

async def crypto(pair: str, ttl: int = 15) -> Dict[str, Any]:
    pair = pair.upper().replace("-", "_").replace("/", "_")
    async def _fetch():
        raw = await _http_get(f"https://api.gateio.ws/api/v4/spot/tickers?currency_pair={pair}")
        arr = json.loads(raw)
        if not arr:
            raise RuntimeError(f"crypto API empty: {pair}")
        t = arr[0]
        return {
            "pair": pair,
            "last": t.get("last"),
            "change_pct_24h": t.get("change_percentage"),
            "high_24h": t.get("high_24h"),
            "low_24h": t.get("low_24h"),
            "vol_base_24h": t.get("base_volume"),
            "vol_quote_24h": t.get("quote_volume"),
        }
    return await _cached(f"gate:{pair}", ttl, _fetch)
