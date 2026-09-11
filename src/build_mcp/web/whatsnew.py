"""更新说明（What's New）数据加载与比对。

数据源：与本模块同目录的 ``whatsnew.json``，结构::

    {
      "entries": [
        {"version": "2026.09.11.2", "date": "2026-09-11",
         "title": "...", "points": ["...", "..."]},
        ...
      ]
    }

约定：
- ``entries`` 按 **新 → 旧** 排列，第一条即最新版本。
- 判断"用户是否看过"只做 ``version`` 字符串相等比较，不做版本号语义解析，
  所以发布新版本时只要换一个新的 version 字符串（如日期加序号）并插到数组最前面即可。
- 文件按 mtime 缓存，改完文件无需重启服务（下次请求即生效）。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("web.whatsnew")

_DATA_FILE = Path(__file__).with_name("whatsnew.json")

_cache: dict[str, Any] = {"mtime": None, "entries": []}


def _normalize(raw: Any) -> list[dict]:
    """只保留结构正确的条目，避免脏数据把前端渲染搞崩。"""
    entries: list[dict] = []
    if not isinstance(raw, dict):
        return entries
    for item in raw.get("entries") or []:
        if not isinstance(item, dict):
            continue
        version = str(item.get("version") or "").strip()
        if not version:
            continue
        points = [str(p).strip() for p in (item.get("points") or []) if str(p).strip()]
        entries.append(
            {
                "version": version,
                "date": str(item.get("date") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "points": points,
            }
        )
    return entries


def load_entries() -> list[dict]:
    """读取全部更新条目（新→旧）；文件缺失或损坏时返回空列表并记日志。"""
    try:
        mtime = _DATA_FILE.stat().st_mtime
    except OSError:
        if _cache["entries"]:
            logger.warning("更新说明文件已消失：%s", _DATA_FILE)
        _cache["mtime"], _cache["entries"] = None, []
        return []
    if _cache["mtime"] == mtime:
        return _cache["entries"]
    try:
        raw = json.loads(_DATA_FILE.read_text(encoding="utf-8"))
        entries = _normalize(raw)
    except Exception as e:  # 解析失败不能让接口 500，退化成"没有更新"
        logger.error("更新说明解析失败 %s：%s", _DATA_FILE, e)
        entries = []
    _cache["mtime"], _cache["entries"] = mtime, entries
    return entries


def latest_version() -> str:
    entries = load_entries()
    return entries[0]["version"] if entries else ""


def payload_for(seen_version: str | None) -> dict:
    """给某个用户的更新说明载荷。

    Args:
        seen_version: 该用户上次看过的版本号；空串/None 表示从未看过。

    Returns:
        {
          "latest": 最新版本号,
          "should_show": 是否需要自动弹窗,
          "unseen": 需要展示的新条目（新→旧；全新用户只给最新一条）,
          "entries": 全部条目（供"更新日志"入口翻看）,
        }
    """
    entries = load_entries()
    if not entries:
        return {"latest": "", "should_show": False, "unseen": [], "entries": []}

    latest = entries[0]["version"]
    seen = (seen_version or "").strip()
    if seen == latest:
        unseen: list[dict] = []
    elif not seen:
        unseen = entries[:1]  # 全新用户只看最新一条，避免一上来刷屏
    else:
        idx = next((i for i, e in enumerate(entries) if e["version"] == seen), None)
        # 见过的版本已不在列表里（比如被清理）→ 只给最新一条
        unseen = entries[:idx] if idx else entries[:1]

    return {
        "latest": latest,
        "should_show": bool(unseen),
        "unseen": unseen,
        "entries": entries,
    }
