#!/usr/bin/env bash
# ============================================================
# cpolar 公网隧道 一键停止（只停隧道，不碰 Web 服务）
# 用法: ./stop_tunnel.sh
# 没有隧道在运行则自动跳过
# ============================================================
set -u

if ! pgrep -x cpolar >/dev/null 2>&1; then
  echo "ℹ️  没有在运行的 cpolar 隧道。"
  exit 0
fi

echo "🛑 停止 cpolar 隧道 ..."
pkill -x cpolar 2>/dev/null

STOPPED=0
for i in $(seq 1 8); do
  sleep 1
  if ! pgrep -x cpolar >/dev/null 2>&1; then STOPPED=1; break; fi
done

if [ "$STOPPED" = 1 ]; then
  echo "✅ 隧道已停止。"
else
  echo "⚠️  隧道未在 8 秒内退出，强制结束..."
  pkill -9 -x cpolar 2>/dev/null
  sleep 1
  echo "✅ 已强制停止。"
fi
echo "   提示: 下次 ./start_tunnel.sh 会分配新域名（免费版特性）"
