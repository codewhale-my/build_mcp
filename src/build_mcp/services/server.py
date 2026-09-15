import os
from typing import Annotated
from typing import Any, Dict, Generic, Optional, TypeVar

from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel
from pydantic import Field

from build_mcp.common.config import load_config
from build_mcp.common.logger import get_logger
from build_mcp.services.gd_sdk import GdSDK
from build_mcp.services import quote_sdk

# 优先从环境变量里读取API_KEY，如果没有则从配置文件读取
env_api_key = os.getenv("API_KEY")
_config = load_config("config.yaml")
API_KEY = _config["api_key"]

config = {
        "base_url": "https://restapi.amap.com",
        "api_key": API_KEY,
        "max_retries": 2,
    }

# 初始化 FastMCP 服务
mcp = MCPServer("amap‑maps", description="高德地图 MCP 服务", version="1.0.0")
sdk = GdSDK(config=config, logger=get_logger(name="gd_sdk"))
logger = get_logger(name="amap-maps")

# 定义通用的 API 响应模型
T = TypeVar("T")


class ApiResponse(BaseModel, Generic[T]):
  success: bool
  data: Optional[T] = None
  error: Optional[str] = None
  meta: Optional[Dict[str, Any]] = None

  @classmethod
  def ok(cls, data: T, meta: Dict[str, Any] = None) -> "ApiResponse[T]":
    return cls(success=True, data=data, meta=meta)

  @classmethod
  def fail(cls, error: str, meta: Dict[str, Any] = None) -> "ApiResponse[None]":
    return cls(success=False, error=error, meta=meta)


# 定义 Prompt
@mcp.prompt(name="assistant", description="高德地图智能导航助手，支持IP定位、周边POI查询等")
def amap_assistant(query: str) -> str:
  return (
    "你是高德地图智能导航助手，精通 IP 定位 和 周边POI查询。请你根据用户的需求获取调取工具，获取用户需要的相关信息。\n"
    "## 调用工具的步骤：\n"
    "1. 调用 `locate_ip` 工具到获取用户的经纬度（结果里的 location 字段就是 'lng,lat'）。\n"
    "2. 若成功获取经纬度，使用该经纬度调用 `search_nearby` 工具，结合搜索关键词进行周边信息的搜索。\n"
    "## 注意事项：\n"
    "- 不要主动要求用户提供经纬度信息，直接使用 `locate_ip` 工具获取。\n"
    "- 如果用户的需求中包含经纬度信息，可以直接使用该信息进行周边搜索。\n"
    "- 需要把经纬度说成地址时用 `regeo`（逆地理编码）。\n"
    f"用户的需求为：\n\n {query}。\n"
  )


@mcp.tool(name="locate_ip", description="IP 定位：根据 IP 返回省、市、区县和经纬度。data.location 是可直接喂给 search_nearby 的 'lng,lat' 中心点；data.source 标明结果来自 amap 还是备用 IP 库。传 ip 参数按该 IP 定位；不传则按服务端出口 IP（通常是机房地址，定位不准）。")
async def locate_ip(ip: Annotated[Optional[str], Field(description="要定位的公网 IP；务必传用户真实公网 IP")] = None) -> ApiResponse:
  """
  根据 IP 地址定位位置。

  Args:
      ip (str): 要定位的 IP 地址。

  Returns:
      dict: 包含定位结果的字典（province/city/location/source）。
  """
  logger.info(f"Locating IP: {ip}")
  try:
    result = await sdk.locate_ip(ip)
    if not result:
      return ApiResponse.fail("IP 定位失败：高德接口与备用 IP 库均无该 IP 的归属地数据。", meta={"ip": ip})
    logger.info(f"Locate IP result: {result}")
    return ApiResponse.ok(data=result, meta={"ip": ip, "source": result.get("source", "amap")})
  except Exception as e:
    logger.error(f"Error locating IP {ip}: {e}")
    return ApiResponse.fail(str(e))


@mcp.tool(name="regeo", description="逆地理编码：把经纬度（'lng,lat'）换算成文字地址（省市区+街道门牌）。用于把用户精确定位坐标变成人类可读地址。")
async def regeo(location: Annotated[str, Field(description="经纬度，格式 'lng,lat'，如 '116.397128,39.916527'")]) -> ApiResponse:
  """
  逆地理编码：经纬度 → 结构化地址。

  Args:
      location (str): "lng,lat"。

  Returns:
      dict: regeocode 结果（formatted_address、addressComponent）。
  """
  logger.info(f"Regeo: location={location}")
  try:
    result = await sdk.regeo(location)
    if not result:
      return ApiResponse.fail("逆地理编码失败，请检查经纬度格式是否为 'lng,lat'。", meta={"location": location})
    return ApiResponse.ok(data=result, meta={"location": location})
  except Exception as e:
    logger.error(f"Error regeo {location}: {e}")
    return ApiResponse.fail(str(e))


@mcp.tool(name="search_nearby", description="根据经纬度和关键词进行周边搜索，返回指定半径内的 POI 列表。")
async def search_nearby(
        location: Annotated[str, Field(description="中心点经纬度，格式为 'lng,lat'，如 '116.397128,39.916527'")],
        keywords: Annotated[str, Field(description="搜索关键词，例如: '餐厅'。", min_length=0)] = "",
        types: Annotated[str, Field(description="POI分类数字编码，多个用逗号分隔；**禁止传入中文**，中文搜索词请填到keywords参数。例：咖啡厅对应编码050500")] = "",
        radius: Annotated[int, Field(description="搜索半径（米），最大50000", ge=0, le=50000)] = 1000,
        page_num: Annotated[int, Field(description="页码，从1开始", ge=1)] = 1,
        page_size: Annotated[int, Field(description="每页数量，最大25", ge=1, le=25)] = 20,
) -> ApiResponse:
  """
   周边搜索。

   Args:
       location (str): 中心点经纬度，格式为 "lng,lat"。
       keywords (str, optional): 搜索关键词，默认为空。
       types (str, optional): POI 分类，默认为空。
       radius (int, optional): 搜索半径（米），最大 50000，默认为 1000。
       page_num (int, optional): 页码，默认为 1。
       page_size (int, optional): 每页数量，最大 25，默认为 10。

   Returns:
       dict: 包含搜索结果的字典。
  """
  logger.info(f"Searching nearby: location={location}, keywords={keywords}, types={types}, radius={radius}, page_num={page_num}, page_size={page_size}")
  try:
    result = await sdk.search_nearby(location=location, keywords=keywords, types=types, radius=radius, page_num=page_num, page_size=page_size)
    logger.info(f"search_nearby 高德接口原始返回 result={result}")
    if not result:
      return ApiResponse.fail("搜索结果为空，请检查日志，系统异常请检查相关日志。")
    logger.info(f"Search nearby result: {result}")
    return ApiResponse.ok(data=result, meta={
      "location": location,
      "keywords": keywords,
      "types": types,
      "radius": radius,
      "page_num": page_num,
      "page_size": page_size
    })
  except Exception as e:
    logger.error(f"Error searching nearby: {e}")
    return ApiResponse.fail(str(e))


@mcp.tool(name="market_quote", description="实时行情查询：A股指数/个股、美股指数、日元/美元等汇率、BTC/ETH 价格。market 取值：a= A股（codes 如 sh000001 上证指数、sz399001 深证成指、sh600519 贵州茅台）；us= 美股指数（codes 如 usIXIC 纳斯达克、usDJI 道琼斯、usSPX 标普500）；fx= 汇率（base 默认 USD，symbols 如 JPY,CNY，返回兑各货币汇率）；crypto= 加密货币（codes 如 BTC_USDT、ETH_USDT）。返回最新价、涨跌幅、更新时间。")
async def market_quote(
    market: Annotated[str, Field(description="市场类型：a / us / fx / crypto")],
    codes: Annotated[Optional[str], Field(description="代码，逗号分隔：a/us 用 sh000001,usIXIC 等；crypto 用 BTC_USDT,ETH_USDT")],
    base: Annotated[str, Field(description="fx 用：基准货币，默认 USD")] = "USD",
    symbols: Annotated[Optional[str], Field(description="fx 用：目标货币列表，如 JPY,CNY")] = None,
) -> ApiResponse:
  """实时行情查询：A股、美股指数、汇率、加密货币。"""
  logger.info(f"market_quote market={market} codes={codes} base={base} symbols={symbols}")
  try:
    m = (market or "").strip().lower()
    if m in ("a", "us"):
      if not codes:
        codes = "sh000001,sz399001" if m == "a" else "usIXIC,usDJI,usSPX"
      return ApiResponse.ok(data=await quote_sdk.quote_tencent(codes), meta={"market": m})
    if m == "fx":
      return ApiResponse.ok(data=await quote_sdk.fx_rates(base=base or "USD", symbols=symbols), meta={"market": "fx"})
    if m == "crypto":
      pairs = [c.strip() for c in (codes or "BTC_USDT,ETH_USDT").split(",") if c.strip()]
      data = {p: await quote_sdk.crypto(p) for p in pairs}
      return ApiResponse.ok(data=data, meta={"market": "crypto"})
    return ApiResponse.fail(f"未知 market: {market}，支持 a / us / fx / crypto")
  except Exception as e:
    logger.error(f"Error market_quote: {e}")
    return ApiResponse.fail(str(e))

# ================= 瓦洛兰特（国际服） =================
from build_mcp.services import valorant_sdk


@mcp.tool(name="valorant_daily_store", description="查询瓦洛兰特（国际服）账号的每日商店：四件每日皮肤、VP 价格、刷新倒计时。默认用网页端已绑定的账号（主人账号）查询，不用填账号密码；填了才走密码登录（服务器 IP 会被人机验证拦下，通常失败）。region: ap/na/eu/kr/latam/br（国服不支持）。")
async def valorant_daily_store(
        username: Annotated[str, Field(description="Riot 账号用户名（留空 = 用已绑定的账号，推荐）")] = "",
        password: Annotated[str, Field(description="Riot 账号密码（留空即可）")] = "",
        region: Annotated[str, Field(description="服务器区域：ap(亚太)/na(北美)/eu(欧洲)/kr(韩国)/latam/br")] = "ap",
        bind_key: Annotated[str, Field(description="绑定账号标识（系统会在对话里给出，形如 qq:12345；查「这个人」自己的商店时原样填上）")] = "",
) -> ApiResponse:
  logger.info(f"valorant_daily_store bind_key={bind_key} pwd_login={bool(username.strip())} region={region}")
  try:
    if bind_key and not username.strip():
        uid = None
        try:
            uid = int(str(bind_key).split(":")[-1])
        except Exception:
            uid = None
        result = await valorant_sdk.bound_daily_store(region, uid=uid)
    elif not username.strip():
        result = await valorant_sdk.bound_daily_store(region)
    else:
        result = await valorant_sdk.daily_store(username, password, region)
    if result.get("error"):
      return ApiResponse.fail(result["error"], meta=result)
    return ApiResponse.ok(data=result)
  except Exception as e:
    logger.error(f"Error valorant_daily_store: {e}")
    return ApiResponse.fail(str(e))


@mcp.tool(name="valorant_skin_search", description="按关键词搜索瓦洛兰特皮肤（中文名，免费公开数据，无需登录），返回名称/UUID/图标。")
async def valorant_skin_search(
        keyword: Annotated[str, Field(description="皮肤关键词，如 '爆裂''幻象''狂徒'")],
        limit: Annotated[int, Field(description="返回条数上限", ge=1, le=20)] = 8,
) -> ApiResponse:
  logger.info(f"valorant_skin_search kw={keyword}")
  try:
    result = await valorant_sdk.search_skins(keyword, limit)
    if result.get("error"):
      return ApiResponse.fail(result["error"])
    return ApiResponse.ok(data=result)
  except Exception as e:
    logger.error(f"Error valorant_skin_search: {e}")
    return ApiResponse.fail(str(e))
