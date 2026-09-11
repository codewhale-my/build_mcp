#!/usr/bin/env bash
# ============================================================
# cpolar 公网隧道 一键启动（只启隧道，不碰 Web 服务）
# 用法: ./start_tunnel.sh [本地端口]    默认 8000
# 隧道已在运行则跳过，绝不重复拉起
# 注意: 免费版隧道每次重启域名都可能变化；隧道不重启则域名不变
# ============================================================
set -u
cd "$(dirname "$0")" || exit 1
PORT="${1:-8000}"
TUNNEL_LOG="$HOME/cpolar_tunnel.log"

# cpolar 启动后 argv 会改写成 "cpolar: master ..."，按启动命令匹配不到；
# 其进程名(comm)始终为 cpolar，故检测/停止统一用 pgrep -x cpolar
cpolar_running() { pgrep -x cpolar >/dev/null 2>&1; }

get_tunnel_url() {
  grep "Tunnel established" "$TUNNEL_LOG" 2>/dev/null | tail -1 \
    | grep -oP 'https?://[^\s"]+' | head -1
}

if cpolar_running; then
  URL=$(get_tunnel_url)
  echo "ℹ️  cpolar 隧道已在运行${URL:+，当前公网地址: $URL}"
  echo "   提示: 如需更换域名，请先 ./stop_tunnel.sh 再重新执行本脚本"
  exit 0
fi

echo "🚀 启动 cpolar 公网隧道 (本地端口 $PORT) ..."
# 轮转日志：保留旧日志便于回溯上次域名，新连接信息写入新日志
[ -f "$TUNNEL_LOG" ] && mv -f "$TUNNEL_LOG" "$TUNNEL_LOG.old" 2>/dev/null
setsid nohup cpolar -log stdout "http://127.0.0.1:$PORT" > "$TUNNEL_LOG" 2>&1 < /dev/null &

URL=""
for i in $(seq 1 30); do
  sleep 1
  URL=$(get_tunnel_url)
  [ -n "$URL" ] && break
done

if [ -n "$URL" ]; then
  echo "✅ 公网隧道已建立: $URL"
  echo "   提示: 隧道进程不重启，此域名保持不变"
else
  echo "❌ 隧道 30 秒内未建立，检查: tail -f $TUNNEL_LOG"
  exit 1
fi
