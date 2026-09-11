# tests/test_gd_sdk_real.py
import logging
import os

import pytest
import pytest_asyncio
from build_mcp.common.config import load_config

from build_mcp.services.gd_sdk import GdSDK

_config = load_config("config.yaml")
API_KEY = _config["api_key"]


@pytest_asyncio.fixture
async def sdk():
    config = {
        "base_url": "https://restapi.amap.com",
        "api_key": API_KEY,
        "max_retries": 2,
    }
    async with GdSDK(config, logger=logging.getLogger("GdSDK")) as client:
        yield client


@pytest.mark.asyncio
async def test_locate_ip(sdk):
    result = await sdk.locate_ip("223.5.5.5")
    print(result)
    assert result is not None, "locate_ip 返回 None"
    assert result.get("status") == "1", f"locate_ip 调用失败: {result}"
    assert "province" in result, "locate_ip 返回中不包含 province"


@pytest.mark.asyncio
async def test_search_nearby(sdk):
    result = await sdk.search_nearby(
        location="104.066513,30.657034",
        keywords="咖啡店",
        radius=3000
    )
    print(result)
    assert result is not None, "search_nearby 返回 None"
    assert result.get("status") == "1", f"search_nearby 调用失败: {result}"
    assert "pois" in result, "search_nearby 返回中不包含 pois"