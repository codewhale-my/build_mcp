#!/usr/bin/env bash
# ============================================================
# MCP Web + 公网隧道 一键启动（防多实例）
# 用法:
#   ./start_web.sh            → 启动 Web + cpolar 公网隧道,打印域名
#   ./start_web.sh --local    → 只启动本地 Web,不开公网
#   ./start_web.sh 8001       → 自定义端口
# Web 与隧道均检测"已在运行"则跳过,绝不重复拉起
# ============================================================
set -u
cd "$(dirname "$0")" || exit 1
PORT=8000
MODE=public
for a in "$@"; do
  case "$a" in
    --local|-l) MODE=local ;;
    *) PORT="$a" ;;
  esac
done

TUNNEL_PATTERN="cpolar -log stdout http://127.0.0.1:$PORT"
# cpolar 启动后 argv 会改写成 "cpolar: master ..."，按启动命令匹配不到；
# 其进程名(comm)始终为 cpolar，故检测/停止统一用 pgrep -x cpolar
TUNNEL_LOG="$HOME/cpolar_tunnel.log"

cpolar_running() { pgrep -x cpolar >/dev/null 2>&1; }

get_tunnel_url() {
  grep "Tunnel established" "$TUNNEL_LOG" 2>/dev/null | tail -1 \
    | grep -oP 'https?://[^\s"]+' | head -1
}

# ---------- 1. Web 服务 ----------
EXIST_PID=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
if [ -n "$EXIST_PID" ]; then
  echo "ℹ️  Web 服务已在运行 (pid=$EXIST_PID, 端口 $PORT)，跳过启动。"
else
  PASSWORD="${MCP_WEB_PASSWORD:-yhj139802}"
  echo "🚀 启动 MCP Web 服务 (端口 $PORT) ..."
  MCP_WEB_PASSWORD="$PASSWORD" setsid nohup uv run uvicorn build_mcp.web.main:app \
    --host 0.0.0.0 --port "$PORT" >> log/web.log 2>&1 < /dev/null &
  OK=0
  for i in $(seq 1 25); do
    sleep 1
    if curl -s -o /dev/null "http://127.0.0.1:$PORT/"; then OK=1; break; fi
  done
  if [ "$OK" = 1 ]; then
    NEW_PID=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
    echo "✅ Web 服务就绪 (pid=$NEW_PID): http://localhost:$PORT"
  else
    echo "❌ Web 25 秒内未就绪，检查: tail -f $(pwd)/log/web.log"
    exit 1
  fi
fi

# ---------- 2. 公网隧道（默认开） ----------
if [ "$MODE" = public ]; then
  if cpolar_running; then
    URL=$(get_tunnel_url)
    echo "ℹ️  cpolar 隧道已在运行${URL:+，当前公网地址: $URL}"
  else
    echo "🚀 启动 cpolar 公网隧道 (本地端口 $PORT) ..."
    [ -f "$TUNNEL_LOG" ] && mv -f "$TUNNEL_LOG" "$TUNNEL_LOG.old" 2>/dev/null
    setsid nohup cpolar -log stdout "http://127.0.0.1:$PORT" > "$TUNNEL_LOG" 2>&1 < /dev/null &
    URL=""
    for i in $(seq 1 30); do
      sleep 1
      URL=$(get_tunnel_url)
      [ -n "$URL" ] && break
    done
    if [ -n "$URL" ]; then
      echo "✅ 公网隧道已建立"
    else
      echo "❌ 隧道 30 秒内未建立，检查: tail -f $TUNNEL_LOG"
    fi
  fi
  URL=${URL:-$(get_tunnel_url)}
  if [ -n "$URL" ]; then
    echo ""
    echo "================ 访问地址 ================"
    echo "  本机: http://localhost:$PORT"
    echo "  公网: $URL"
    echo "  密码: ${MCP_WEB_PASSWORD:-yhj139802}"
    echo "=========================================="
  fi
else
  echo ""
  echo "================ 访问地址 ================"
  echo "  本机: http://localhost:$PORT   (--local 模式, 未开公网)"
  echo "=========================================="
fi
