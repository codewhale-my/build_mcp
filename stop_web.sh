#!/usr/bin/env bash
# ============================================================
# MCP Web 服务 一键停止（只停 Web，不碰 cpolar 隧道）
# 用法: ./stop_web.sh [端口]    默认 8000
# 端口上没有 Web 服务则自动跳过
# ============================================================
set -u
PORT="${1:-8000}"

PID=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
if [ -z "$PID" ]; then
  echo "ℹ️  端口 $PORT 上没有 Web 服务在运行。"
  exit 0
fi

echo "🛑 停止 Web 服务 (pid=$PID, 端口 $PORT) ..."
kill "$PID" 2>/dev/null

for i in $(seq 1 10); do
  sleep 1
  if ! ss -tln 2>/dev/null | grep -q ":$PORT "; then
    echo "✅ Web 服务已停止，端口 $PORT 已释放。"
    exit 0
  fi
done

echo "⚠️  进程未在 10 秒内退出，强制结束..."
kill -9 "$PID" 2>/dev/null
sleep 1
if ! ss -tln 2>/dev/null | grep -q ":$PORT "; then
  echo "✅ 已强制停止。"
else
  echo "❌ 仍无法停止，请手动检查: ps aux | grep uvicorn"
  exit 1
fi
