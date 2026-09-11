#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# certbot http-01 清理 hook —— 验证完成后删掉写进服务器 webroot 的 challenge 文件
# 与 acme-auth.sh 配套使用；失败不影响签发结果，所以这里不用 set -e。
# ---------------------------------------------------------------------------
set -uo pipefail

DEPLOY_HOST="${DEPLOY_HOST:-}"
DEPLOY_USER="${DEPLOY_USER:-admin}"
DEPLOY_KEY="${DEPLOY_KEY:-$HOME/.ssh/id_ed25519}"
WEBROOT="${WEBROOT:-/var/www/letsencrypt}"
CHALLENGE_DIR="${WEBROOT%/}/.well-known/acme-challenge"
TOKEN="${CERTBOT_TOKEN:-}"

if [ -z "$TOKEN" ]; then
  echo "[acme-cleanup] 没有 CERTBOT_TOKEN，跳过"
  exit 0
fi

if [ "$DEPLOY_HOST" = "127.0.0.1" ] || [ "$DEPLOY_HOST" = "localhost" ]; then
  sudo rm -f "$CHALLENGE_DIR/$TOKEN"
else
  ssh -i "$DEPLOY_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    -o ConnectTimeout=20 "${DEPLOY_USER}@${DEPLOY_HOST}" \
    "sudo rm -f '${CHALLENGE_DIR}/${TOKEN}'" || true
fi

echo "[acme-cleanup] 已清理: $TOKEN"
