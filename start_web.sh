#!/usr/bin/env bash
# ============================================================
# MCP Web 服务 一键启动（只启 Web，不碰 cpolar 隧道）
# 用法: ./start_web.sh [端口]    默认 8000
# 已在运行则跳过，绝不重复拉起
# 需要公网时另跑 ./start_tunnel.sh（隧道不重启，域名不变）
# ============================================================
set -u
cd "$(dirname "$0")" || exit 1
PORT="${1:-8000}"

EXIST_PID=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
if [ -n "$EXIST_PID" ]; then
  echo "ℹ️  Web 服务已在运行 (pid=$EXIST_PID, 端口 $PORT)，跳过启动。"
  echo "   本机: http://localhost:$PORT"
  exit 0
fi

# 邀请码（逗号分隔，可带 :备注）。首次注册必须使用有效邀请码；
# 已有账号登录不需要。务必把默认码 MCP2026 换掉！
INVITES="${MCP_WEB_INVITE_CODES:-MCP2026}"
echo "🚀 启动 MCP Web 服务 (端口 $PORT) ..."
MCP_WEB_INVITE_CODES="$INVITES" setsid nohup uv run uvicorn build_mcp.web.main:app \
  --host 0.0.0.0 --port "$PORT" >> log/web.log 2>&1 < /dev/null &

OK=0
for i in $(seq 1 25); do
  sleep 1
  if curl -s -o /dev/null "http://127.0.0.1:$PORT/"; then OK=1; break; fi
done

if [ "$OK" = 1 ]; then
  NEW_PID=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
  echo "✅ Web 服务就绪 (pid=$NEW_PID): http://localhost:$PORT"
  echo "   邀请码: ${INVITES}  (自定义: MCP_WEB_INVITE_CODES='A:备注,B' ./start_web.sh)"
  echo "   生成更多邀请码: uv run python -m build_mcp.web.store invite <CODE> [备注]"
  echo "   如需公网访问: ./start_tunnel.sh $PORT"
else
  echo "❌ Web 25 秒内未就绪，检查: tail -f $(pwd)/log/web.log"
  exit 1
fi
