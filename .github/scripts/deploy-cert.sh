#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 把签发好的证书推送到服务器、装进 /etc/nginx/ssl/ 并 reload nginx
#
# 本机手动用：
#   DEPLOY_HOST=47.108.234.194 DEPLOY_USER=admin \
#   CERT_FILE=~/https-attempt/certbot-prod/config/live/47.108.234.194/fullchain.pem \
#   KEY_FILE=~/https-attempt/certbot-prod/config/live/47.108.234.194/privkey.pem \
#   .github/scripts/deploy-cert.sh
#
# GitHub Actions 里由 renew-ip-cert.yml 调用（环境变量来自 repo secrets）。
# ---------------------------------------------------------------------------
set -euo pipefail

DEPLOY_HOST="${DEPLOY_HOST:?环境变量 DEPLOY_HOST 未设置}"
DEPLOY_USER="${DEPLOY_USER:-admin}"
DEPLOY_KEY="${DEPLOY_KEY:-$HOME/.ssh/id_ed25519}"
CERT_FILE="${CERT_FILE:?环境变量 CERT_FILE 未设置（fullchain.pem）}"
KEY_FILE="${KEY_FILE:?环境变量 KEY_FILE 未设置（privkey.pem）}"
SSL_DIR="${SSL_DIR:-/etc/nginx/ssl}"
CRT_NAME="${CRT_NAME:-hjmcp-ip.crt}"
KEY_NAME="${KEY_NAME:-hjmcp-ip.key}"
# 自检地址：默认拿证书里的 IP 走 https
CHECK_URL="${CHECK_URL:-https://${DEPLOY_HOST}/}"

[ -f "$CERT_FILE" ] || {
  echo "[deploy-cert] 找不到证书: $CERT_FILE" >&2
  exit 1
}
[ -f "$KEY_FILE" ] || {
  echo "[deploy-cert] 找不到私钥: $KEY_FILE" >&2
  exit 1
}

SSH_OPTS=(-i "$DEPLOY_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=20)
remote="${DEPLOY_USER}@${DEPLOY_HOST}"

echo "[deploy-cert] 证书信息:"
openssl x509 -in "$CERT_FILE" -noout -subject -issuer -dates -ext subjectAltName | sed 's/^/  /'

echo "[deploy-cert] 上传到 ${DEPLOY_HOST} ..."
scp -q "${SSH_OPTS[@]}" "$CERT_FILE" "${DEPLOY_USER}@${DEPLOY_HOST}:/tmp/hjmcp-fullchain.pem"
scp -q "${SSH_OPTS[@]}" "$KEY_FILE" "${DEPLOY_USER}@${DEPLOY_HOST}:/tmp/hjmcp-privkey.pem"

echo "[deploy-cert] 安装并 reload nginx ..."
# shellcheck disable=SC2029
ssh "${SSH_OPTS[@]}" "$remote" "
  set -e
  sudo mkdir -p '${SSL_DIR}'
  sudo install -m 644 /tmp/hjmcp-fullchain.pem '${SSL_DIR}/${CRT_NAME}'
  sudo install -m 600 /tmp/hjmcp-privkey.pem  '${SSL_DIR}/${KEY_NAME}'
  rm -f /tmp/hjmcp-fullchain.pem /tmp/hjmcp-privkey.pem
  sudo nginx -t
  sudo systemctl reload nginx
  echo '[deploy-cert] nginx 已 reload'
"

echo "[deploy-cert] 等 nginx 生效后自检 $CHECK_URL ..."
ok=0
for i in 1 2 3 4 5; do
  code="$(curl -s -o /dev/null -m 15 -w '%{http_code}' "$CHECK_URL" || true)"
  if [ "$code" = "200" ] || [ "$code" = "401" ] || [ "$code" = "302" ]; then
    echo "[deploy-cert] 自检通过: HTTP $code"
    ok=1
    break
  fi
  echo "[deploy-cert] 第 ${i} 次自检: HTTP ${code}，重试..."
  sleep 3
done
if [ "$ok" != "1" ]; then
  echo "[deploy-cert] 自检未通过，请检查服务器" >&2
  exit 1
fi

# 顺带把线上证书的到期时间打出来，方便看还有几天
echo "[deploy-cert] 线上证书到期时间:"
echo | openssl s_client -connect "${DEPLOY_HOST}:443" -servername "${DEPLOY_HOST}" 2>/dev/null |
  openssl x509 -noout -dates 2>/dev/null | sed 's/^/  /' || true
