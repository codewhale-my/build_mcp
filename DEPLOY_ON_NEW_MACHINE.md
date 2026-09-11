# 在新机器 WSL 上部署 build-mcp（完整指南）

> 适用场景：把「MCP Web 聊天服务 + 多用户鉴权 + 每用户文件沙箱 + cpolar 公网隧道」整套从现机器迁移到**另一台 Windows 的 WSL(Ubuntu)**。
> 写于 2026-09-08，以当时运行环境为准（uv 0.12、Python 3.12、Node 22、cpolar 3.3.12）。

---

## 0. 这套系统由哪些部分组成（迁移地图）

| 部件 | 位置 | 说明 |
|---|---|---|
| 代码仓库 | `~/build-mcp/` | git 仓库，remote 为 **SSH over 443**（见 §2） |
| Python 依赖 | `~/build-mcp/.venv/` + `uv.lock` | `uv sync` 一键还原，Python 版本按 `.python-version`=3.12 自动下载 |
| Node 运行库 | 系统 node / npm（≥22） | 三个 npx 型 MCP 工具靠它，首次使用自动下载 |
| 核心配置 | `~/build-mcp/src/build_mcp/config.yaml` | **含明文 LLM/高德 key,不入库**(历史已清洗);clone 后不存在,需自行准备(见 §4) |
| 用户/邀请码/历史数据 | `~/build-mcp-data/app.db`（**项目目录外**） | SQLite；不迁移则新机器从零开始 |
| 用户文件沙箱 | `~/fs_workspace/users/u<id>_<用户名>/` | 每个用户一个目录（服务自动创建） |
| 隧道日志/域名 | `~/cpolar_tunnel.log` | 看域名用 `grep "Tunnel established" ~/cpolar_tunnel.log \| tail -1` |
| 项目日志 | `~/build-mcp/log/web.log` | 启动失败先看这里 |
| 进程 | uvicorn(:8000) + cpolar | 4 个启动/停止脚本管理 |

MCP 工具构成：**共享** amap（`uv run build_mcp`）、websearch（`npx open-websearch@latest`，默认 DuckDuckGo 免 key）、terminal（`npx mcp-server-terminal --headless`）；**每用户独立** filesystem（`npx -y @modelcontextprotocol/server-filesystem <该用户目录>`，由后端按用户懒启动，天然沙箱互不可见）。

---

## 1. 准备 WSL 与基础软件

在 Windows 上装好 WSL + Ubuntu（本机为 Ubuntu，用户名 administrator），进入 WSL 后：

```bash
# 系统工具
sudo apt update && sudo apt install -y git curl unzip

# 1) uv（Python 包管理器；版本管理器，会自动下载 .python-version 指定的 3.12）
curl -LsSf https://astral.sh/uv/install.sh | sh
# 重新打开 shell 后验证
uv --version        # 本机 0.12.x

# 2) Node.js（需要 npx；apt 自带版本太旧，建议装 20+）
#    本机用 22。最快方式：
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt install -y nodejs
node --version && npm --version && npx --version
```

---

## 2. 拿到代码（推荐 git clone）

现仓库 remote 已固化为 **SSH over 443**（HTTPS git 流量在本网络环境被掐断，勿改回）：

```bash
# 1) 生成 SSH 密钥（新机器没有时）
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519 -C "your-email"
cat ~/.ssh/id_ed25519.pub
#    复制输出，粘贴到 https://github.com/settings/ssh/new（codewhale-my 账号）

# 2) 预置 host key（否则首次 ssh 会卡在 yes/no 确认）
ssh-keyscan -p 443 ssh.github.com >> ~/.ssh/known_hosts 2>/dev/null

# 3) 验证认证
ssh -T -p 443 git@ssh.github.com   # 看到 Hi codewhale-my! 即成功

# 4) 克隆
git clone ssh://git@ssh.github.com:443/codewhale-my/build_mcp.git ~/build-mcp
```

> 备选：直接把现机器 `~/build-mcp` 整目录打包拷过来也行（跳过 git）。但后续 push/pull 仍需按上面配 SSH。
> 若也想把旧用户数据带过来，先看 §6（拷 `app.db` 与 `fs_workspace/users`）。

---

## 3. 换行符坑（重要，本机刚踩过）

现机器 git 全局设了 `core.autocrlf=true`。仓库文件在 Linux 侧是 LF，**clone 到新机器时 `.sh` 等文件可能被写成 CRLF**，直接执行会报 `$'\r': command not found` 之类错误。

```bash
cd ~/build-mcp
# 让 git 在本仓库内不做 CRLF 转换（Linux 侧规范）
git config core.autocrlf input
# 重新按 LF 检出全部文件（把可能的 CRLF 洗掉）
git rm --cached -r . >/dev/null 2>&1; git reset --hard
# 验证：不应输出 CRLF
file start_web.sh && bash -n start_web.sh && echo OK
```

**长期根治**：在仓库根加 `.gitattributes`（提交后所有机器统一行为）：

```
* text=auto
*.sh  text eol=lf
*.py  text eol=lf
*.yaml text eol=lf
*.html text eol=lf
*.js  text eol=lf
*.md  text eol=lf
*.log -text
```

（`*.log -text` 让日志不再被跟踪/转换——当前 `log/*.log` 已被历史跟踪，详见 §9。）

---

## 4. 安装依赖并核对配置

```bash
cd ~/build-mcp
uv sync          # 按 uv.lock 装依赖，自动下载 Python 3.12（无需手动装 python）
```

> ⚠️ `config.yaml` **不进 git 仓库**（含敏感 key，已从历史清洗）。clone 后该文件不存在，需从原机器拷贝一份，或按下表新建；它已被 `.gitignore` 忽略，不会误提交。

核对 `src/build_mcp/config.yaml`：

| 项 | 现机器值（示例） | 新机器要做什么 |
|---|---|---|
| `llm_base_url` / `llm_api_key` / `llm_model` | deepseek | key 若随仓库公开过，**去 DeepSeek 后台轮换新 key 再填** |
| `api_key` | 高德 key | 确认有效；失效则去高德控制台换 |
| `proxy` | `http://127.0.0.1:10809` | ⚠️ 这是**现机器本地代理**。新机器没有就改成 `proxy: null`（直连；高德/DeepSeek 国内直连即可）。不删键、留 `null`，代码里 `httpx.AsyncClient(proxy=None)` 才合法 |
| `log_dir` | `./log` | 保持相对路径即可 |

`conversation.py` 里的 `WEBSEARCH_ENV / TERMINAL_ENV` 无需改（open-websearch 默认免 key）。

可选预热（首次对话会自动下载，这里先装好避免首问超时）：

```bash
npx --yes open-websearch@latest --help >/dev/null 2>&1; echo $?
npx --yes mcp-server-terminal --help >/dev/null 2>&1; echo $?
```

---

## 5. 首次启动与验证

```bash
cd ~/build-mcp
chmod +x *.sh

# 强烈建议先换掉内置默认邀请码再启动
MCP_WEB_INVITE_CODES='DEPLOY-001:管理员' ./start_web.sh
# 不带环境变量则默认码为 MCP2026（谁拿到都能注册！）
```

验证：

```bash
# 本机存活
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/    # 200
# 注册一个新账号（用户名/密码/邀请码）
curl -s -X POST http://127.0.0.1:8000/api/auth -H "Content-Type: application/json" \
  -d '{"username":"me","password":"pass123456","invite":"DEPLOY-001"}'
# 无邀请码注册应被拒(403)；已有账号仅验密码登录
```

浏览器开 `http://localhost:8000` 注册两个号互测：文件空间、历史消息彼此不可见即 OK。

日常管理（详细见 store CLI）：

```bash
uv run python -m build_mcp.web.store invites          # 查邀请码
uv run python -m build_mcp.web.store invite CODE 备注  # 新增
uv run python -m build_mcp.web.store note CODE 备注    # 改备注
uv run python -m build_mcp.web.store reset CODE        # 已用→未使用
uv run python -m build_mcp.web.store revoke CODE       # 删除
uv run python -m build_mcp.web.store users             # 用户列表
```

停止：`./stop_web.sh`。

---

## 6. 迁移旧数据（把现机器的用户/历史/文件带过去）

想让新机器无缝继承现机器账号，就迁移这两个目录（**两个都要，且保持相对关系**，目录名里的 `u<id>_` 与 DB 中的 id 对应，勿改名/拆开）：

```bash
# 旧机器上打包（停服后再打，保证一致）
cd ~/build-mcp && ./stop_web.sh
tar czf ~/buildmcp-data.tgz ~/build-mcp-data ~/fs_workspace/users
# 拷到新机器（内网 scp / U盘 / 网盘 / /mnt/c 共享都行），然后：
cd ~
tar xzf ~/buildmcp-data.tgz -C ~     # 还原出 ~/build-mcp-data 与 ~/fs_workspace/users
```

新机器上先 `./start_web.sh` 再验证：旧账号能直接登录、历史与文件都在。

> 注意：cpolar 隧道域名、服务端 token 不随迁移（重启即失效，重登即可）；`app.db` 里邀请码使用状态一并迁走，到新机器后发码要发"未使用"的。

---

## 7. 公网访问（cpolar）

```bash
# 1) 安装 cpolar（3.3.12，与现机器同版本；二进制放用户目录免 sudo）
curl -L https://www.cpolar.com/static/downloads/releases/3.3.12/cpolar-stable-linux-amd64.zip -o /tmp/cpolar.zip
unzip -o /tmp/cpolar.zip -d ~/.local/bin && chmod +x ~/.local/bin/cpolar && rm /tmp/cpolar.zip
cpolar version

# 2) 绑定账号：登录 https://www.cpolar.com 后台复制 authtoken
cpolar authtoken <你的token>          # 写入 ~/.cpolar/cpolar.yml

# 3) 起隧道
./start_tunnel.sh                    # 默认把 8000 映射到公网

# 4) 查域名
grep "Tunnel established" ~/cpolar_tunnel.log | tail -1
```

公网回归（换成你自己的域名）：

```bash
curl -s -o /dev/null -w "首页 %{http_code}\n" https://<域名>/
curl -s -X POST https://<域名>/api/auth -H "Content-Type: application/json" -d '{"username":"t","password":"12345678","invite":""}' -o /dev/null -w "无邀请码注册 %{http_code}\n"   # 期望 403
```

⚠️ 免费版**每次重启隧道域名都可能变**；日常改代码只重启 Web（`./stop_web.sh && ./start_web.sh`）就不会碰隧道、域名不变。想固定域名需 cpolar 付费套餐。

---

## 8. 常见问题速查

| 症状 | 处理 |
|---|---|
| `./start_web.sh` 报 `\r` 相关错 | §3 换行符问题 |
| Web 起不来 | `tail -f log/web.log` 看真实报错 |
| 端口被占 | `ss -tlnp \| grep 8000` |
| 地图工具报网络错 | §4 的 `proxy`（新机器没本地代理就设 `null`） |
| 首次提问很慢/超时 | npx 在后台下载 open-websearch/server-filesystem，跑一次 §4 预热即可 |
| 隧道起不来 | `tail -f ~/cpolar_tunnel.log`；确认已 `cpolar authtoken` |
| 用户目录没生成 | filesystem 是懒加载，该用户第一次调用文件工具才建目录 |
| git push 失败(GnuTLS/TLS 掐断) | 本仓库已切 SSH over 443，§2 配置好即不会再走 HTTPS |

---

## 9. 安全提醒（建议尽快处理）

1. `config.yaml` 曾短暂进入 git 历史并推送过 GitHub，现已用 `git filter-repo` 重写历史**彻底清除**（2026-09-08，远端现 HEAD=`002e9b3`）。但 key 在公开窗口期内理论上可被看到，**仍建议去 DeepSeek / 高德控制台轮换 key**，并将仓库设为私有。旧历史备份在 `~/buildmcp-git-backup-20260908.tgz`（含敏感内容，妥善保管或删除）。
2. `log/*.log` 也被历史跟踪（运行时产物本不该入库，虽被 `.gitignore` 覆盖但 `-f` 强加过）。后续每次提交都建议 `git rm --cached log/*.log`，并把 §3 的 `.gitattributes` 加上。
3. 登录用户经「终端」工具运行在 uvicorn 的 cwd（`~/build-mcp`），可读项目内文件 —— 演示版设计。生产环境请把 LLM key 挪到环境变量、收缩终端工作目录并做操作审计。
4. 默认邀请码 `MCP2026` 公开后等于开放注册，启动前务必用 `MCP_WEB_INVITE_CODES` 换成自己的码。

---

## 附：现机器与本仓库的关键差异备忘

- git remote：`ssh://git@ssh.github.com:443/codewhale-my/build_mcp.git`
- 现机器 `core.autocrlf=true`（全局+仓库）→ 新机器 WSL 内务必改 `input` 或加 `.gitattributes`
- Web 启动用 `uv run uvicorn build_mcp.web.main:app`（FastAPI，端口 8000），DB 在项目外 `~/build-mcp-data/app.db`

---

## 10. 更新线上服务器（阿里云，2026-09-10 起）

⚠️ **不要指望在服务器上 `git pull`**：阿里云北京节点访问 `github.com:443` 会直接超时
（实测 `Failed to connect to github.com port 443 after 134255 ms`）。
改用项目根目录的 `deploy.sh`——由本机（能正常访问 GitHub）把代码推过去：

```bash
# 本机 WSL，首次建议先免密
ssh-copy-id admin@47.108.234.194

cd /home/administrator/build-mcp
./deploy.sh              # 同步代码 → 重启 hjmcp → 健康检查
./deploy.sh --config     # 额外同步 config.yaml（自动去掉本机代理行，旧配置备份为 .bak）
./deploy.sh --restart    # 只重启服务
```

脚本行为：`tar` 打包（走 ssh，服务器无需装 rsync）→ 排除 `.git/.venv/log/*.log/__pycache__/src/build_mcp/config.yaml`
→ `ssh -t` 重启 `hjmcp` → curl 健康检查并核对页面关键标记。

排障：
- `./deploy.sh --restart` 时 sudo 需要 tty，脚本已用 `ssh -t`；若报密码错误检查免密是否配好。
- 服务器日志：`ssh admin@47.108.234.194 'sudo journalctl -u hjmcp -n 50 --no-pager'`
- 改完 `config.yaml` 必须重启才生效（`--config` 已包含重启）。

---

## 11. 定位能力说明（2026-09-10）

两条链路，**精确定位优先**：

| 链路 | 触发条件 | 精度 | 依赖 |
|---|---|---|---|
| 浏览器精确定位 | ① 用户点「开启定位」授权后；② **问题含位置意图时自动开启**（见下）。每轮对话带 `geo{lat,lng,acc}` | GPS/WiFi 级（米级） | ⚠️ **必须 HTTPS**，浏览器才给定位权限 |
| IP 定位 | 未拿到精确定位时，服务端把用户公网 IP 注入上下文 | 城市级 | 高德 `/v3/ip` + 备用免费库 |

**位置意图自动开启**（`static/index.html` 的 `GEO_INTENT_RE` / `needsGeoLocation(q)`）：
发送前对问题做一次关键词匹配，命中就自动开启定位并取坐标再发出去，用户不用先点按钮。
覆盖「我在哪 / 附近 / 周边 / 最近的 / 怎么走 / 导航 / 打车 / 天气 / 限行 /
找一家火锅店 / near me / how far」等中英文说法；不命中的普通问题完全不碰定位。
等待时长按 `navigator.permissions.query({name:'geolocation'})` 的状态决定：
首次待授权 15s、已授权 7s、已被拒绝 1.2s；拿不到就照常发送并回落 IP 定位。
直播区会显示「正在获取精确定位…」一行，结束时打勾并标注精度，或打叉标注"改用 IP 定位"。

✅ **2026-09-11 起已上 HTTPS**：正式地址 `https://47.108.234.194`（nginx 443 → 127.0.0.1:8000），
浏览器安全上下文成立，前端「开启定位」按钮可直接授权使用，见 §12。
原 `http://47.108.234.194:8000` 仍保留为退路（仅建议调试时用：登录密码是明文传输的）。

IP 定位兜底链（`src/build_mcp/services/ip_locate.py`，全部免 key）：
`pconline`（0.1s，中文名最准）→ `ipinfo.io`（0.4s，阿里云北京可达）→ `ipwho.is`（大陆机房常超时，放最后）。
每个库最多等 3.5s（`PER_PROVIDER_TIMEOUT`），避免一个 hanging 的库拖死整条链路。
高德查到的省市会用 `/v3/geocode/geo` 换成 `location`（`"lng,lat"`），保证 `search_nearby` 直接可用。

自测（服务器侧，不依赖前端）：
```bash
ssh admin@47.108.234.194 'cd ~/build-mcp && PYTHONPATH=src .venv/bin/python - <<PY
import asyncio, json
from build_mcp.common.config import load_config
from build_mcp.services.gd_sdk import GdSDK
cfg = load_config("config.yaml")
s = GdSDK(config={"base_url": "https://restapi.amap.com", "api_key": cfg["api_key"], "max_retries": 1})
print(json.dumps(asyncio.run(s.locate_ip("39.144.137.222")), ensure_ascii=False))
PY'
# 期望：source=pconline, province=四川省, city=成都市, location=104.066301,30.572961
```

---

## 12. HTTPS（纯 IP，2026-09-11 上线）

### 现状
- 正式地址：**https://47.108.234.194**（nginx 443 → `127.0.0.1:8000`），浏览器绿锁、可用于 Geolocation。
- 证书：**Let's Encrypt 的 IP 地址证书**（`certbot --ip-address` + `--preferred-profile shortlived`），
  **有效期只有 160 小时（≈6.7 天）**，由 GitHub Actions 自动续期。
- 80 端口：只服务 `/.well-known/acme-challenge/`，其余 301 跳 HTTPS。
- `http://47.108.234.194:8000` 保留为退路（**登录密码是明文传输的**，平时别用）。

### 为什么续期要放在 GitHub Actions 上（关键背景）
Let's Encrypt 从 2026-01 起对 IP 地址签发证书，但 IP 证书**必须**用 shortlived profile，
且**只能用 http-01 / tls-alpn-01 验证**（IP 没有 DNS 记录，无法用 dns-01）。

而本服务器访问 `acme-v02.api.letsencrypt.org` 被**定向阻断**：实测 0/10 全部超时，
换 6 个 Cloudflare IP、IP 直连、staging 全失败；但同在 Cloudflare 的 `get.acme.sh` 是 200 / 0.88s。
→ **签发和续期必须由一台能连上 LE 的机器完成**，这里选 GitHub 的 runner（不依赖任何人的电脑开机）。

### 整体链路
```
GitHub Actions runner（能连 LE）
  ├─ 1. certbot --manual --preferred-challenges http --ip-address 47.108.234.194
  ├─ 2. auth hook 经 SSH 把 challenge 文件写进服务器 /var/www/letsencrypt/.well-known/acme-challenge/
  ├─ 3. LE 从公网访问 http://47.108.234.194/.well-known/... 完成验证
  └─ 4. 证书推回服务器 /etc/nginx/ssl/hjmcp-ip.{crt,key}，nginx -t 后 reload
```

### 相关文件
| 文件 | 作用 |
|---|---|
| `.github/workflows/renew-ip-cert.yml` | 每天 02:17 UTC 检查 + 可手动触发；证书剩余 <3 天才真正签发；失败 GitHub 会发邮件 |
| `.github/scripts/acme-auth.sh` | certbot http-01 验证 hook：把 challenge 写到服务器 webroot |
| `.github/scripts/acme-cleanup.sh` | 验证完成后清理 challenge 文件 |
| `.github/scripts/deploy-cert.sh` | 推送证书 + `nginx -t` + reload + curl 自检（本机也能手动跑） |
| `deploy/nginx-hjmcp.conf` | 80 验证+301 / 443 TLS 反代（WebSocket Upgrade、`XFF=$remote_addr`、`proxy_buffering off`） |

### 仓库 Secret（一次性配置）
`DEPLOY_SSH_KEY` = 能免密登录服务器的私钥；对应公钥已加进服务器 `~/.ssh/authorized_keys`（备注 `github-actions-ip-cert`）。
位置：仓库 → Settings → Secrets and variables → Actions → New repository secret。

### 手动操作
```bash
# 1) 手动触发续期：仓库 → Actions → renew-ip-cert → Run workflow
#    勾选 force 可跳过"剩余 >3 天"检查，强制重签（首次验证全链路时用）

# 2) 本机手动签发 + 部署（Actions 出问题时的兜底；签发端需能连 LE）
cd ~/build-mcp
export DEPLOY_HOST=47.108.234.194 DEPLOY_USER=admin
export CERT_FILE=~/https-attempt/certbot-prod/config/live/47.108.234.194/fullchain.pem
export KEY_FILE=~/https-attempt/certbot-prod/config/live/47.108.234.194/privkey.pem
./.github/scripts/deploy-cert.sh

# 3) 改完 nginx 配置后
ssh admin@47.108.234.194 "sudo tee /etc/nginx/sites-available/hjmcp > /dev/null" < deploy/nginx-hjmcp.conf
ssh admin@47.108.234.194 "sudo nginx -t && sudo systemctl reload nginx"
```

### 验证
```bash
curl -sI http://47.108.234.194/                                                    # 期望 301 → https
curl -s -o /dev/null -w '%{http_code} tls=%{ssl_verify_result}\n' https://47.108.234.194/   # 期望 200 tls=0
echo | openssl s_client -connect 47.108.234.194:443 2>/dev/null | openssl x509 -noout -dates -ext subjectAltName
# 期望 subjectAltName 里有 IP Address:47.108.234.194
```

### ⚠️ 注意事项
- **不要开 HSTS**：IP 证书只有 6 天，一旦某次续期失败，HSTS 会让用户连"继续访问"的机会都没有。
- **GitHub 定时任务有 60 天不活动限制**：仓库若连续 60 天没有任何 push，scheduled workflow 会被自动停用。
  长期不提交代码时，记得去 Actions 页面确认任务还在跑。
- 续期失败时 GitHub 会给仓库 owner 发邮件；也可以随时 `curl -sI https://47.108.234.194/` 看证书是否正常。
- 域名 + ICP 备案下来后换成域名证书（阿里云免费 DV，90 天）：nginx 里只改 `server_name` 和证书路径，
  还能顺手开 HSTS，那时就不再需要这套"6 天一续"的机制了。

### 回滚（退回纯 8000）
```bash
ssh admin@47.108.234.194 "sudo rm -f /etc/nginx/sites-enabled/hjmcp && sudo systemctl disable --now nginx"
# 8000 完全不受影响；如需彻底清掉 80 端口配置，再删 /etc/nginx/sites-available/hjmcp
```

---

## 13. 更新说明弹窗（What's New，2026-09-11）

用户登录后，若存在**没看过的更新**，会自动弹一次说明弹窗；关掉后不再重复提醒。
顶栏 ✨ 按钮（有未读时带小红点）可随时重开，没有未读时展示全部历史更新。

### 相关文件
| 文件 | 作用 |
|---|---|
| `src/build_mcp/web/whatsnew.json` | **更新数据源**（唯一需要改的文件） |
| `src/build_mcp/web/whatsnew.py` | 按 mtime 缓存加载 + `payload_for(seen)` 比对 |
| `src/build_mcp/web/store.py` | `users.last_seen_version` 列（含 `_migrate`）+ `set_user_seen_version()` |
| `src/build_mcp/web/main.py` | `GET /api/whatsnew`、`POST /api/whatsnew/seen` |
| `static/index.html` | `loadWhatsNew()` / `showWhatsNew()` / `closeWhatsNew()` + `#updOverlay` 弹窗 |

### 怎么发一条新更新
编辑 `whatsnew.json`，把新条目**插到 `entries` 数组最前面**，并换一个**新的 `version` 字符串**即可：

```json
{
  "entries": [
    { "version": "2026.09.12.1", "date": "2026-09-12", "title": "一句话概括",
      "points": ["做了什么改动，一条一句", "最多三四条"] }
  ]
}
```
- 判断"是否看过"只做 **version 字符串相等比较**，不做版本号语义解析，所以用 `日期.序号` 这种写法最省事。
- 文件按 mtime 缓存，**改完不用重启服务**（但线上要 `./deploy.sh` 把文件推上去）。
- 文件缺失或 JSON 写坏时接口退化为"没有更新"，不会 500，也不会把前端搞崩。

### 行为细节
- **已读记在用户维度**（`users.last_seen_version`）：换设备、清浏览器缓存都不会重复弹。
- 全新用户只展示**最新一条**，不会一上来刷屏；老用户只展示上次看过的版本之后的条目。
- 关闭方式：点「知道了」/ 点遮罩空白处 / 按 Esc，三种都会上报已读。
- 万一 `POST /api/whatsnew/seen` 失败（网络抖动），下次登录会再弹一次，不会静默丢失。

### 验证
```bash
# 用临时账号走一遍：首次 should_show=true → POST seen → 再查 should_show=false
# 完整脚本思路见 §11/§12 的"临时邀请码 + curl"套路
ssh admin@47.108.234.194 "cd ~/build-mcp && /home/admin/.local/bin/uv run python -m build_mcp.web.store invite UPDTEST 临时"
curl -s -X POST https://47.108.234.194/api/auth -H 'Content-Type: application/json' \
  -d '{"username":"updtest1","password":"updtest123456","invite":"UPDTEST"}'
curl -s https://47.108.234.194/api/whatsnew -H "Authorization: Bearer <token>"
```

---

## 14. 管理员能力与云服务器工作空间（2026-09-11）

由站点内置 agent 开发，本次一并入库。

- **管理员名单**：环境变量 `MCP_WEB_ADMINS`（逗号分隔，默认 `yanghj`）。用户名**精确匹配、区分大小写**
  （`"YANGHJ"`、`"yanghj "` 这类仿冒名不会被误判为管理员，否则等于提权）。
  启动时 `_check_admin_accounts()` 会核对名单里每个账号是否已注册，未注册打 WARNING。
- **权限差异**（`is_admin()`）：
  - 管理员：共享工具保留 terminal 全套，另挂一个组合工具 `terminal_run`；可把文件空间切到「云服务器·整机」。
  - 非管理员：按「工具归属哪个 MCP 会话」剔除 terminal 工具集（不靠名字前缀猜，terminal 换实现也不会漏），
    并在提示词里明确告知无权操作服务器，避免模型反复试探或假装已完成。
- **云服务器工作空间**：`POST /api/workspace {"mode":"local"|"server"}`，状态存 `users.ws_mode`（自动迁移补列）。
  切到 `server` 后 filesystem MCP 的根变成 `MCP_WEB_SERVER_ROOT`（默认 `/`），AI 可直接读改整机文件。
  - 护栏 `_FsGuardShim` 只挡 `/proc`、`/sys`、`/dev`、`/run` 与「从 `/` 全盘递归」——不是收权限
    （管理员本就该有整机权限），是防止一次 `list_directory("/")` / `search_files("/")` 把服务拖死。
  - HTTP 侧 `_safe_user_file()` 改为多根白名单：个人工作空间恒可访问（上传文件落点），server 模式下追加整机根。
- **`terminal_run`**：一次调用完成「建/复用会话 → 发送 → 等待结束 → 读输出」并带回退出码，
  `session_id` 可复用（保留 cwd / 环境变量 / 已激活 venv）。交互式程序（vim/htop、需要确认的提示）仍走原生 `terminal_*`。
- **工具输出截断**：`conversation.truncate_tool_output()`，上限 `MCP_WEB_TOOL_OUTPUT_LIMIT`（默认 4000 字符，
  头 HEAD 2400 + 尾 TAIL 1400），避免终端整屏输出被后续每一轮请求重复计入。

## 15. 成本：前缀缓存与用量日志（2026-09-11）

- **背景**：DeepSeek 自 2026-08-17 起改峰谷定价（高峰=北京 9:00-12:00 / 14:00-18:00），
  **缓存命中与未命中的单价固定差 30 倍**（V4-Flash 高峰 ¥0.10 vs ¥3.00 每百万 token，空闲时段减半）。
  前缀缓存只比对「从第 0 个 token 起完全相同」的部分——system 里改一个字节，整段历史就全部按未命中计费。
- **已做的三件事**：
  1. **system 只放稳定内容**：`SYSTEM_PROMPT + sys_note + perm_note + admin_note`；
     随轮变化的部分（公网 IP / GPS 坐标 / 图片说明）由 `conversation._compose_user_message()`
     挂到最后一条用户消息（那里本来每轮就不同，吃掉它不影响缓存）。
     ⚠️ **后续新增提示词时，凡「每轮可能不同」的一律走 `turn_note`，不要塞进 system。**
  2. **用量落日志**：请求带 `stream_options={"include_usage": True}`（流式下 usage 只在最后一帧且该帧
     `choices` 为空，必须在 `continue` 之前取）；每次请求打印一行，整轮结束再用 logger 带用户名打一行。
     服务端不认 `stream_options` 时会自动去掉重试一次（自愈）。
- **实测**（2026-09-11 16:42，临时账号，两次请求 GPS 坐标不同）：固定前缀 **3968 tok** 稳定命中，
  两次命中率均 **90%**；单次输入成本从「全未命中」的 ¥0.0132 降到 ¥0.0017（约 **7.9 倍**）。
- **单价可覆盖**（换模型/调价不用改代码）：`MCP_WEB_PRICE_CACHE_HIT` / `MCP_WEB_PRICE_CACHE_MISS` /
  `MCP_WEB_PRICE_OUTPUT`（默认 0.10 / 3.00 / 9.00）。
- **下一层优化**：历史窗口已改为「分块累积」，见下节。

### 历史窗口：为什么用「分块累积」而不是滑动窗口

- **问题**：`recent_llm_messages(turns=8)` 原为滑动窗口，永远取最近 8 轮。每来一轮窗口整体前移一条，
  而缓存前缀必须逐字节相同 → **固定前缀之外的历史每轮都按未命中计费**（差价 30 倍）。
- **做法**（`store._history_window(total, turns, mode)` 纯函数，便于单测）：
  窗口起点对齐到 `2*turns` 条消息的整数倍、并**往回退一整块**。
  块内消息只在尾部追加、起点不动 → 已发过的历史前缀逐轮复用；攒满一整块才整体前移一次
  （每 8 轮失效 1 次，而不是每轮）。窗口长度稳定落在 **[N, 2N) 轮**，恒 ≥ 滑动窗口，
  最多多出一倍上下文，多出的部分走命中价。
- **开关**：`MCP_WEB_HISTORY_MODE=block|slide`（默认 `block`，出问题可立刻回落旧行为）、
  `MCP_WEB_HISTORY_TURNS`（默认 8）。每轮日志带 `｜历史 N 条(窗口 block/8轮)`。
- **⚠️ 踩坑**：首版写成 `ORDER BY id DESC ... LIMIT ? OFFSET ?` —— **DESC 下 OFFSET 是从「最新」
  那头开始跳的**，与「跳过最老的 N 条」方向正好相反，会返回最老的一批消息。现用 `ASC + OFFSET`。
- **实测 A/B**（服务器真实 API，两模式用不同文案避免互蹭缓存，每轮约 110 tok）：

  | 模式 | 命中轮数 | 累计命中 | 累计未命中 | 命中占比 |
  |---|---|---|---|---|
  | slide | 5/26 | 1152 | 11616 | 9.0% |
  | block | 21/26 | 10496 | 5783 | **64.5%** |

  线上真实链路 12 轮（临时账号，已清理）：第 1 轮 0 命中，第 2 轮起 **4096→4352 tok 随轮增长**，
  累计命中占比 **88.1%**，第 9 轮起 **96%**。同样位置滑动窗口会掉回「只剩固定前缀可命中」。


---

## 16. 与「服务器侧 agent」协作：合并流程与防覆盖闸门（2026-09-11）

### 为什么会互相覆盖
站点的内置 agent 直接在**服务器** `~/build-mcp` 上改代码（它的工作目录就是那儿）。
这些改动一开始**不在 git 里**（服务器 HEAD 停在旧提交，`git status` 一堆 M 里还混着行尾噪音），
而 `deploy.sh` 是「本机仓库 → tar → 覆盖服务器工作区」的单向同步 ——
于是在服务器上写的代码会被下一次部署整份盖掉（2026-09-11 真实发生过一次）。

### 合并流程（每次要从服务器取回改动时照这个走）
1. 拉回服务器工作区（排除 .git/.venv/log/__pycache__/*.bak*）：
   `ssh admin@... "cd ~/build-mcp && tar czf - --exclude=.git --exclude=.venv --exclude=log --exclude=__pycache__ --exclude='*.bak*' ." | tar xzf - -C /tmp/srv_tree`
2. **全树对比**（务必先做，别只看几个文件）：
   - A 服务器有、仓库没有（新增文件；注意 `*.orig` 这类合并副本不要入库）
   - B 仓库有、服务器没有（部署未覆盖的文件）
   - C 两侧都有但内容不同
3. **逐文件判定方向**：`diff <(tr -d '\r' < repo/f) <(tr -d '\r' < srv/f)`。
   行尾一定要归一化（本机经 UNC 编辑会写成 CRLF），否则 8000+ 行差异里八成是 `\r` 噪音。
4. **用「关键标记」双向核对**，确认取哪一侧不会丢功能。本项目常用标记：
   - `conversation.py`：`_tool_result_to_text`、`turn_note`、`_compose_user_message`、
     `_usage_numbers`、`include_usage`、`truncate_tool_output`、`_dedup_key`
   - `static/index.html`：`tblwrap`、`GEO_INTENT_RE`、`geoForSend`、`updOverlay`、
     `netErrMsg`、`fmtUsage`、`curRun`
5. 逐条看 `git diff -U0 | grep '^-'` 的**删除行**，确认是「同一段代码的重构替换」而不是功能被删。
6. 验证后 commit + push；**再** deploy（此时闸门会放行，因为两侧一致）。

### 防覆盖闸门（`deploy.sh` 默认开启）
部署前对 `static/index.html`、`web/store.py`、`web/main.py`、`client/conversation.py`、
`web/whatsnew.json` 逐个比对「本机仓库」与「服务器工作区」的哈希（行尾归一化后）。不一致时：

- 先把服务器版本备份到 `~/build-mcp-backups/<时间戳>/<原相对路径>`；
- 打印差异清单并**中止部署**（退出码 1），提示合并进 git 或 `--force`。

实测（2026-09-11）：在服务器 `main.py` 尾部加一行注释 → 部署被拦下、备份生成、内容确认在备份里；
去掉该行后重新部署打印「一致 ✅」并正常完成。

**注意**：闸门只比对这 5 个文件。若新增了源码文件，记得加进 `deploy.sh` 的 `GUARD_FILES`。

### 一句话规矩
**服务器上的改动必须先合并进 git 再部署。** `deploy.sh` 现在会替你拦住那次覆盖，
但拦下来的目的是让你合并，不是让你 `--force`。

### 另外两条工程纪律（本次踩出来的）
- 同一文件在一条消息里发多个 Edit 会**互相覆盖**（后写覆盖先写）；务必一次只改一处、改完立即 grep 验证。
- 写脚本文件偶发掉字符（`err` → `er`）；辅助脚本写到 D: 盘再用 `/mnt/d/...` 跑，且跑前先 `ast.parse`/`node --check` 过一遍。
