#!/usr/bin/env bash
# ==============================================================================
# 一键部署：本机(WSL) → 阿里云服务器
#
# 为什么不用服务器上的 git pull？
#   阿里云北京节点访问 github.com:443 会超时（实测 134s 无响应），
#   所以改由本机（能正常访问 GitHub）把代码直接推过去，绕开 GitHub。
#
# 用法:
#   ./deploy.sh                    # 只同步代码 + 重启 + 健康检查（默认 admin@47.108.234.194）
#   ./deploy.sh --config           # 额外同步 config.yaml（去掉本机代理行；服务器旧配置备份为 .bak）
#   ./deploy.sh --restart          # 只重启服务，不同步代码
#   ./deploy.sh admin@1.2.3.4      # 指定其他服务器（可与上面参数组合）
#
# 首次使用建议先做免密（否则每条命令都要输一次服务器密码）:
#   ssh-copy-id admin@47.108.234.194
# ==============================================================================
set -euo pipefail

HOST=""
ONLY_RESTART=0
WITH_CONFIG=0
for arg in "$@"; do
  case "$arg" in
    --restart|-r) ONLY_RESTART=1 ;;
    --config|-c)  WITH_CONFIG=1 ;;
    -* ) echo "未知参数: $arg"; exit 1 ;;
    *  ) HOST="$arg" ;;
  esac
done
HOST="${HOST:-admin@47.108.234.194}"

RUSER="${HOST%@*}"
REMOTE_DIR="/home/${RUSER}/build-mcp"
WEB_URL="${WEB_URL:-http://${HOST#*@}:8000}"
SRC="$(cd "$(dirname "$0")" && pwd)"

step() { printf '\n\033[1;36m▶ %s\033[0m\n' "$1"; }

if [ "${ONLY_RESTART}" = "0" ]; then
  step "同步代码  $SRC  →  ${HOST}:${REMOTE_DIR}"
  echo "   本机版本: $(git -C "$SRC" log --oneline -1 2>/dev/null || echo '(非 git 仓库)')"
  # 用 tar over ssh，服务器无需安装 rsync
  # 排除: .git / 虚拟环境 / 日志 / 缓存 / config.yaml(见 --config 说明)
  tar czf - \
    --exclude-vcs \
    --exclude='./.venv' \
    --exclude='./log' \
    --exclude='*.log' \
    --exclude='*.pyc' \
    --exclude='*__pycache__*' \
    --exclude='./src/build_mcp/config.yaml' \
    -C "$SRC" . \
  | ssh "$HOST" "mkdir -p '${REMOTE_DIR}' && tar xzf - -C '${REMOTE_DIR}'"
  echo "   已同步（config.yaml / 日志 / 虚拟环境保持服务器原样）"

  if [ "${WITH_CONFIG}" = "1" ]; then
    step "同步配置 config.yaml"
    echo "   · 去掉本机专用代理行(proxy: http://127.0.0.1:...)，服务器上没有这个代理"
    echo "   · 服务器旧配置首次会备份为 config.yaml.bak"
    ssh "$HOST" "test -f '${REMOTE_DIR}/src/build_mcp/config.yaml' && cp -n '${REMOTE_DIR}/src/build_mcp/config.yaml' '${REMOTE_DIR}/src/build_mcp/config.yaml.bak' || true"
    grep -v -E '^[[:space:]]*proxy[[:space:]]*:' "$SRC/src/build_mcp/config.yaml" \
      | ssh "$HOST" "cat > '${REMOTE_DIR}/src/build_mcp/config.yaml'"
    echo "   已写入 ${REMOTE_DIR}/src/build_mcp/config.yaml"
  fi
else
  step "跳过代码同步（--restart 模式）"
fi

step "重启 systemd 服务"
ssh -t "$HOST" 'sudo systemctl restart hjmcp'

step "健康检查  ${WEB_URL}"
for i in 1 2 3 4 5; do
  code="$(curl -s -o /tmp/_hj_deploy.html -w '%{http_code}' -m 8 "$WEB_URL/" || true)"
  if [ "$code" = "200" ]; then
    printf '   HTTP 200 ✅  页面OK\n'
    grep -o 'HJ_MCP Agent\|fileInput\|modelBtn\|新建工作空间' /tmp/_hj_deploy.html | sort -u | sed 's/^/   命中: /'
    rm -f /tmp/_hj_deploy.html
    printf '\n\033[1;32m✅ 部署完成\033[0m\n'
    exit 0
  fi
  printf '   第 %s 次探测: HTTP %s，等 3 秒重试…\n' "$i" "${code:-失败}"
  sleep 3
done
rm -f /tmp/_hj_deploy.html
printf '\n\033[1;31m❌ 服务未在预期时间内就绪，请在服务器上看日志:\033[0m\n'
echo "   ssh $HOST 'sudo journalctl -u hjmcp -n 50 --no-pager'"
exit 1
