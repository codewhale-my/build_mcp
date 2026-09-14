"""消息通道适配层 —— 把 IM（QQ / 微信）接到同一个 agent 核心上。

三条设计约束（都是被现实逼出来的）：

1. **零新增第三方依赖**：只用 httpx + websockets（pyproject 里已有），
   不引入 botpy/aiohttp —— 少一个包就少一份内存和一次 uv.lock 冲突。
2. **不反向依赖 web 层**：本包不 import web.main / web.store，
   起 run 与取事件都靠构造时注入，因此可离线自测、也不与另一个 agent 的改动互踩。
3. **复用「断网照跑」**：入站消息只负责起一个后台 run，通道只是订阅者。
   于是 IM 侧天然继承：掉线/切后台仍继续生成、处理过程全程可见、结果落库可回看。

当前通道：
  * qq_official —— QQ 官方机器人（WebSocket 长连接，**不需要域名/备案**）
  * wecom      —— 企业微信群机器人 webhook（单向推送，零门槛）
"""

__all__ = ["core", "qq_official", "wecom"]
