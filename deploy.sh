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
# 公网自检优先走 HTTPS（nginx 443 → 127.0.0.1:8000），失败再退回裸 8000
WEB_URL="${WEB_URL:-https://${HOST#*@}}"
FALLBACK_URL="${FALLBACK_URL:-http://${HOST#*@}:8000}"
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
# ssh -t 会打印 "Connection to ... closed."，那是正常收尾，不是错误
ssh -t "$HOST" 'sudo systemctl restart hjmcp' || true

# 服务重启后 uv 要重建包并依次拉起 3 个 MCP 子进程，实测需 10~30 秒才开始监听。
# 所以这里耐心轮询「服务器本机 127.0.0.1:8000」，与本机网络/代理/安全组无关，最可靠。
step "等待服务就绪（通常 10~30 秒，最多等 90 秒）"
READY=0
for i in $(seq 1 45); do
  code="$(ssh -o BatchMode=yes -o ConnectTimeout=8 "$HOST" \
          "curl -s -o /dev/null -w '%{http_code}' -m 5 http://127.0.0.1:8000/" 2>/dev/null || true)"
  if [ "$code" = "200" ]; then
    READY=1
    printf '   就绪 ✅ HTTP 200（第 %s 次探测）\n' "$i"
    break
  fi
  printf '   第 %s 次: HTTP %s，2 秒后重试…\n' "$i" "${code:-失败}"
  sleep 2
done
if [ "$READY" = "0" ]; then
  printf '\n\033[1;31m❌ 服务未在 90 秒内就绪，请查看日志:\033[0m\n'
  echo "   ssh $HOST 'sudo journalctl -u hjmcp -n 80 --no-pager'"
  exit 1
fi

# 顺带回报 MCP 装载情况，便于确认 3 个服务都在
ssh -o BatchMode=yes "$HOST" \
  "sudo journalctl -u hjmcp --no-pager -n 300 2>/dev/null | grep -E 'MCP服务启动完成|✅MCP|❌MCP|启动失败' | tail -5" \
  2>/dev/null | sed 's/^/   /' || true

step "公网健康检查  ${WEB_URL}"
# --noproxy '*'：避免本机 curl 走代理导致误报 000
code="$(curl -s --noproxy '*' -o /tmp/_hj_deploy.html -w '%{http_code}' -m 10 "$WEB_URL/" || true)"
if [ "$code" != "200" ]; then
  printf '   HTTPS 探测返回 %s，退回裸 8000 再试一次…\n' "${code:-失败}"
  WEB_URL="$FALLBACK_URL"
  code="$(curl -s --noproxy '*' -o /tmp/_hj_deploy.html -w '%{http_code}' -m 10 "$WEB_URL/" || true)"
  if [ "$code" = "200" ]; then
    printf '\033[1;33m   ⚠️  只有 8000 通、443 不通 —— 多半是证书过期（IP 证书 6 天一次，看 GitHub Actions 有没有跑失败）\033[0m\n'
  fi
fi
if [ "$code" = "200" ]; then
  printf '   HTTP 200 ✅  页面OK  (%s)\n' "$WEB_URL"
  grep -o 'HJ_MCP Agent\|fileInput\|modelBtn\|geoBtn\|新建工作空间' /tmp/_hj_deploy.html | sort -u | sed 's/^/   命中: /'
  rm -f /tmp/_hj_deploy.html
  printf '\n\033[1;32m✅ 部署完成\033[0m   （服务端 token 是内存态，浏览器请刷新页面重新登录）\n'
  exit 0
fi
rm -f /tmp/_hj_deploy.html
printf '\n\033[1;31m❌ 公网探测返回 HTTP %s\033[0m\n' "${code:-失败}"
echo "   服务本机是通的，公网不通多半是安全组/防火墙拦截，或本机网络问题："
echo "   curl -v --noproxy '*' ${WEB_URL}/"
exit 1
