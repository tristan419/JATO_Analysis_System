# JATO Fullstack 腾讯云 Ubuntu 部署手册

本文对应当前 Fullstack 方案，不走 Docker。生产形态固定为：

- React + Vite 前端先构建成静态文件
- nginx 负责静态文件和反向代理
- FastAPI 后端通过 systemd 常驻
- 服务器更新通过 SSH 拉代码并执行发布脚本

如果你想尽量少输命令，当前推荐直接使用：`bash 03_Scripts/tencent_fullstack_bootstrap.sh`。

## 1. 目标目录与端口

- 仓库目录：`/opt/JATO_Analysis_System`
- 后端服务：`jato-fullstack-backend@8000`
- 后端监听：`127.0.0.1:8000`
- 前端静态目录：`/opt/JATO_Analysis_System/06_AppPlatform/frontend/dist`
- 外部入口：nginx `80`

## 2. 服务器前置依赖

推荐以 Ubuntu 22.04 或 24.04 为基础镜像。

如果你直接使用一键脚本，这一节的安装动作会自动完成，可以跳到第 3 节。

```bash
sudo apt-get update
sudo apt-get install -y git curl nginx python3 python3-venv python3-pip

curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs

node -v
npm -v
python3 --version
```

要求：

- Node.js 不低于 20.19.0
- Python 建议 3.10 及以上

## 3. 首次拉取代码

如果腾讯云上直接访问 GitHub 不稳定，先用镜像克隆：

```bash
sudo mkdir -p /opt
sudo chown "$USER":"$USER" /opt

cd /opt
REPO_REMOTE_URL="${REPO_REMOTE_URL:-https://gitclone.com/github.com/tristan419/JATO_Analysis_System.git}"
git clone "$REPO_REMOTE_URL"
cd /opt/JATO_Analysis_System

python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
pip install -r 06_AppPlatform/backend/requirements.txt
```

如果服务器不是直接使用你的个人账号，先把仓库目录 owner 调整给实际部署用户。

## 3.1 一键初始化、部署并启动

完成仓库拉取后，直接执行：

```bash
cd /opt/JATO_Analysis_System
bash 03_Scripts/tencent_fullstack_bootstrap.sh
```

常用覆盖参数：

```bash
cd /opt/JATO_Analysis_System
REPO_REMOTE_URL=https://gitclone.com/github.com/tristan419/JATO_Analysis_System.git \
SERVER_NAME=your.domain.com \
BACKEND_PORT=8000 \
APP_AUTH_TOKEN='你自己的token' \
VITE_API_BASE=/v1 \
bash 03_Scripts/tencent_fullstack_bootstrap.sh
```

这个脚本会自动做完：

- 检查 sudo 权限
- 安装 Python、Node.js、nginx 等基础依赖
- 创建 `.venv`
- 渲染 `/etc/jato-fullstack/backend.env`
- 安装并启用 `jato-fullstack-backend@8000`
- 安装前端依赖并构建 `dist`
- 安装 nginx 配置并重启服务
- 做本地健康检查

如果你已经有仓库副本，但 `origin` 还是指向 GitHub，把 `REPO_REMOTE_URL` 设成镜像地址后再跑一键脚本，脚本会把当前远端自动切到镜像。

如果脚本失败，终端会自动打印一段以 `BEGIN JATO FULLSTACK DIAGNOSTICS` 开头的诊断块。你把整段复制给我就行。

## 4. 配置后端环境变量

如果你已经跑过一键脚本，这一节通常已经自动完成；只在你需要手动改 token 或数据路径时再编辑。

复制模板并编辑：

```bash
sudo mkdir -p /etc/jato-fullstack
sudo cp 03_Scripts/deploy/systemd/jato-fullstack-backend.env.example /etc/jato-fullstack/backend.env
sudo nano /etc/jato-fullstack/backend.env
```

至少确认这些值：

- `APP_AUTH_ENABLED=true`
- `APP_AUTH_TOKEN=你自己的强口令`
- `JATO_PARQUET_PATH=/opt/JATO_Analysis_System/04_Processed_data/jato_full_archive.parquet`
- `JATO_PARTITIONED_PATH=/opt/JATO_Analysis_System/04_Processed_data/partitioned_dataset_v1`
- `APP_CRUD_DATA_PATH=/opt/JATO_Analysis_System/04_Processed_data/app_entities.json`

如果你暂时只允许内网访问，也可以把 `APP_AUTH_ENABLED=false`，但不建议长期这样跑公网。

## 5. 安装 systemd 后端服务

```bash
sudo cp 03_Scripts/deploy/systemd/jato-fullstack-backend@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now jato-fullstack-backend@8000
sudo systemctl status jato-fullstack-backend@8000 --no-pager
```

健康检查：

```bash
curl -fsS http://127.0.0.1:8000/healthz
```

## 6. 首次构建与发布前端

仓库已经提供统一发布脚本：`03_Scripts/deploy_fullstack_server.sh`。

首次执行前，确认：

- `.venv` 已创建
- `npm` 和 `node` 已可用
- `jato-fullstack-backend@8000` 已存在

执行：

```bash
cd /opt/JATO_Analysis_System
VITE_API_BASE=/v1 \
VITE_USER_ROLE=viewer \
VITE_USER_NAME=anonymous \
bash 03_Scripts/deploy_fullstack_server.sh
```

说明：

- `VITE_API_BASE=/v1` 代表前端走同域 API，不把后端地址写死到构建产物里
- `VITE_AUTH_TOKEN` 默认不建议写进前端构建产物，建议用户首次登录后在页面 Access Control 里填写 token

如果你只是想重新部署代码，不想重复初始化，直接运行这一节的 `03_Scripts/deploy_fullstack_server.sh` 即可。

## 7. 安装 nginx

```bash
cd /opt/JATO_Analysis_System
sudo chmod +x 03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh
sudo SERVER_NAME=_ BACKEND_PORT=8000 FRONTEND_ROOT=/opt/JATO_Analysis_System/06_AppPlatform/frontend/dist \
  bash 03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh
```

如果你已经有域名，把 `SERVER_NAME=_` 换成真实域名，并在 DNS 中把 A 记录指向腾讯云服务器公网 IP。

## 8. 防火墙与安全组

腾讯云安全组至少放通：

- `22/tcp` 用于 SSH
- `80/tcp` 用于 nginx
- 如果后续要加 HTTPS，再放通 `443/tcp`

如果服务器本机启用了 UFW：

```bash
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw enable
sudo ufw status
```

## 9. 验证项

```bash
curl -fsS http://127.0.0.1:8000/healthz
curl -I http://127.0.0.1/
curl -I http://127.0.0.1/assets/
```

如果要验证带鉴权接口：

```bash
curl -fsS http://127.0.0.1/v1/metadata/columns \
  -H 'X-Auth-Token: 你的token' \
  -H 'X-User-Role: admin' \
  -H 'X-User-Name: deploy-check'
```

浏览器检查：

- 首页能正常打开
- Dashboard `Load Overview` 成功
- CRUD 页面能正常分页
- Network 中 `/v1/...` 请求返回 200

## 9.1 一键打印诊断信息

任何时候只要你想把服务器当前状态贴给我，直接运行：

```bash
cd /opt/JATO_Analysis_System
bash 03_Scripts/print_fullstack_server_diagnostics.sh
```

请从 `BEGIN JATO FULLSTACK DIAGNOSTICS` 到 `END JATO FULLSTACK DIAGNOSTICS` 整段复制。

## 10. 后续自动部署

仓库新增 workflow：`.github/workflows/deploy-fullstack-tencent.yml`。

首次上线后，再配置这些 GitHub Secrets / Variables：

- Secrets: `SSH_HOST`, `SSH_USER`, `SSH_PRIVATE_KEY`
- Variables: `DEPLOY_REPO_DIR`
- Variables: `FULLSTACK_BACKEND_SERVICE_NAME`
- Variables: `DEPLOY_BRANCH`
- Variables: `FULLSTACK_VITE_API_BASE`
- Variables: `FULLSTACK_VITE_USER_ROLE`
- Variables: `FULLSTACK_VITE_USER_NAME`
- Environment / deploy script: `REPO_REMOTE_URL`

推荐默认值：

- `DEPLOY_REPO_DIR=/opt/JATO_Analysis_System`
- `FULLSTACK_BACKEND_SERVICE_NAME=jato-fullstack-backend@8000`
- `DEPLOY_BRANCH=main`
- `FULLSTACK_VITE_API_BASE=/v1`
- `REPO_REMOTE_URL=https://gitclone.com/github.com/tristan419/JATO_Analysis_System.git`

## 11. 回滚

查看最近提交：

```bash
cd /opt/JATO_Analysis_System
git log --oneline -5
```

回滚到指定提交后重新发布：

```bash
cd /opt/JATO_Analysis_System
git reset --hard <commit_sha>
bash 03_Scripts/deploy_fullstack_server.sh
```

这是服务器侧回滚命令，只建议在明确知道目标提交的情况下执行。