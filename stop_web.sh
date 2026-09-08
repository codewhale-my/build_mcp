#!/usr/bin/env bash
# ============================================================
# MCP Web + 公网隧道 一键停止
# 用法: ./stop_web.sh [端口]    默认 8000
# 先停 cpolar 隧道,再停 Web 服务;进程不存在则自动跳过
# ============================================================
set -u
PORT="${1:-8000}"
# cpolar 启动后 argv 会改写成 "cpolar: master ..."，进程名(comm)恒为 cpolar，
# 检测/停止统一用 pgrep/pkill -x cpolar（按名精确匹配，不会误杀其它进程）
CPOLAR_MATCH="-x cpolar"

# ---------- 1. 停 cpolar 隧道 ----------
if pgrep $CPOLAR_MATCH >/dev/null 2>&1; then
  echo "🛑 停止 cpolar 隧道 ..."
  pkill $CPOLAR_MATCH 2>/dev/null
  STOPPED=0
  for i in $(seq 1 8); do
    sleep 1
    if ! pgrep $CPOLAR_MATCH >/dev/null 2>&1; then STOPPED=1; break; fi
  done
  if [ "$STOPPED" = 1 ]; then
    echo "✅ 隧道已停止。"
  else
    echo "⚠️  隧道未在 8 秒内退出，强制结束..."
    pkill -9 $CPOLAR_MATCH 2>/dev/null
    echo "✅ 已强制停止。"
  fi
else
  echo "ℹ️  没有在运行的 cpolar 隧道。"
fi

# ---------- 2. 停 Web 服务 ----------
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
