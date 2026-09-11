#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# certbot http-01 验证 hook —— 把 challenge 文件写到「被签发的服务器」的 nginx webroot
#
# 为什么需要它：
#   阿里云北京这台机器被定向阻断了 Let's Encrypt 的 ACME 接口（0/10 可达），
#   而 LE 签发 IP 证书时只允许 http-01 / tls-alpn-01（没有域名可用 DNS 验证）。
#   所以做法是：在「能连 LE 的机器」（本机 / GitHub Actions runner）上跑 ACME，
#   由这个 hook 通过 SSH 把 challenge 文件写到服务器的 webroot，LE 再来服务器
#   的 80 端口取走它完成验证。
#
# 不需要手动调用，certbot 会带着 CERTBOT_* 环境变量自动执行：
#   DEPLOY_HOST=<服务器IP> DEPLOY_USER=admin DEPLOY_KEY=~/.ssh/id_ed25519 \
#   certbot certonly --manual --preferred-challenges http \
#     --manual-auth-hook  .github/scripts/acme-auth.sh \
#     --manual-cleanup-hook .github/scripts/acme-cleanup.sh ...
# ---------------------------------------------------------------------------
set -euo pipefail

DEPLOY_HOST="${DEPLOY_HOST:?环境变量 DEPLOY_HOST 未设置（被签发服务器 IP）}"
DEPLOY_USER="${DEPLOY_USER:-admin}"
DEPLOY_KEY="${DEPLOY_KEY:-$HOME/.ssh/id_ed25519}"
WEBROOT="${WEBROOT:-/var/www/letsencrypt}"
CHALLENGE_DIR="${WEBROOT%/}/.well-known/acme-challenge"

TOKEN="${CERTBOT_TOKEN:-}"
VALIDATION="${CERTBOT_VALIDATION:-}"
if [ -z "$TOKEN" ] || [ -z "$VALIDATION" ]; then
  echo "[acme-auth] 错误：缺少 CERTBOT_TOKEN / CERTBOT_VALIDATION（是否用的 http-01？）" >&2
  exit 1
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
printf '%s' "$VALIDATION" >"$tmp"

if [ "$DEPLOY_HOST" = "127.0.0.1" ] || [ "$DEPLOY_HOST" = "localhost" ]; then
  # hook 直接跑在被签发服务器上时的分支
  sudo mkdir -p "$CHALLENGE_DIR"
  sudo install -m 644 "$tmp" "$CHALLENGE_DIR/$TOKEN"
else
  remote_tmp="/tmp/acme-${TOKEN}"
  scp -q -i "$DEPLOY_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    -o ConnectTimeout=20 "$tmp" "${DEPLOY_USER}@${DEPLOY_HOST}:${remote_tmp}"
  ssh -i "$DEPLOY_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    -o ConnectTimeout=20 "${DEPLOY_USER}@${DEPLOY_HOST}" \
    "sudo mkdir -p '${CHALLENGE_DIR}' && sudo install -m 644 '${remote_tmp}' '${CHALLENGE_DIR}/${TOKEN}' && rm -f '${remote_tmp}'"
fi

echo "[acme-auth] 验证文件已就位: ${CHALLENGE_DIR}/${TOKEN} (domain=${CERTBOT_DOMAIN:-?})"
