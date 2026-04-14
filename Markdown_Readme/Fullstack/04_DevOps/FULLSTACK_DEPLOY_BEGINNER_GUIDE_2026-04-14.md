# Fullstack Deploy Beginner Guide

这份文档是给新手看的。目标只有三个：

1. 看懂这套发布链路现在是怎么工作的
2. 知道以后 `push main` 时服务器上会发生什么
3. 知道在 GitHub 网页里去哪里补 `DEPLOY_CERTBOT_EMAIL` 和手动触发 `workflow_dispatch`

---

## 1. 现在的部署架构

当前生产环境是腾讯云 Ubuntu，一台机器上跑两层：

- 前端：React/Vite 构建产物，由 nginx 对外提供静态页面
- 后端：FastAPI，由 systemd 管理进程

关键路径：

- 仓库部署目录：`/opt/JATO_Analysis_System-main`
- 前端 dist：`/opt/JATO_Analysis_System-main/06_AppPlatform/frontend/dist`
- 后端服务名：`jato-fullstack-backend@8000`
- 域名：`ojeur.cloud`、`www.ojeur.cloud`

对外访问路径：

- `https://ojeur.cloud/`
- `https://www.ojeur.cloud/`
- `https://www.ojeur.cloud/market-scan`

---

## 2. 这次我实际做了什么

这次不是只“把页面跑起来”，而是把整条部署链路补完整了。

### 2.1 先解决了线上代码和本地代码不一致

之前腾讯云线上还停在旧版本，所以页面只显示到 `01/02/03`。现在已经把线上仓库切到新提交，并确认页面能看到 `04/05/06/07`。

### 2.2 把远端脏工作树问题收敛进发布脚本

发布脚本现在会在构建前，清理一小部分“明确安全”的 untracked 残留，例如：

- `04_Processed_data/.refresh_backups/pre-sync-*`
- `Markdown_Readme/Fullstack/*.md`
- `Markdown_Readme/Streamlit/*.md`

这避免了以后发布时又被远端临时文件卡住。

### 2.3 给 nginx 加了 HTTPS 自动化

新增了一个脚本：

- `03_Scripts/deploy/nginx/enable_jato_fullstack_https.sh`

它负责：

1. 先确保 HTTP nginx 配置存在
2. 安装 `certbot` 和 `python3-certbot-nginx`
3. 给域名申请 Let's Encrypt 证书
4. 自动把 `80 -> 443` 跳转配好
5. 验证本机 HTTPS 是否可用

### 2.4 防止后续发布把 HTTPS 冲掉

原来的 nginx 安装脚本会直接重写站点配置。这个逻辑对“首次 HTTP 部署”没问题，但对已经接入 Certbot 的站点有风险。

所以现在：

- `03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh`
- 检测到 nginx 配置里有 `managed by Certbot`
- 默认跳过覆盖

这意味着以后再发版，不会把证书配置冲没。

### 2.5 GitHub Actions 现在会在发版后自动对账 ingress

workflow 文件：

- `.github/workflows/deploy-fullstack-tencent.yml`

发布完成后，它还会再做一步：

- 如果配置的是 HTTP，就补 HTTP ingress
- 如果配置的是 HTTPS，就走 `enable_jato_fullstack_https.sh`
- 如果 nginx 已经是 Certbot 管理的配置，就保留现状，不粗暴重写

所以后续正常情况下：

- 你 `push main`
- GitHub Actions 自动发布
- 域名和 HTTPS 也会一起被维持住

---

## 3. 以后一次正常发布会发生什么

你本地执行：

```bash
git push JATO_Analysis_System main
```

然后 GitHub Actions 做这些事：

1. checkout 仓库代码
2. 打包部署归档
3. 上传到腾讯云服务器
4. 解压到 `/opt/JATO_Analysis_System-main`
5. 执行 `03_Scripts/deploy_fullstack_server.sh`
6. 重新构建前端
7. 重启后端服务
8. 检查 `/healthz`
9. 再检查 nginx / 域名 / HTTPS 入口是否还正确

如果这一切都正常，你就不需要手工 SSH 上去重新配域名或证书。

---

## 4. 这条链路里最重要的文件

建议先只记下面 5 个：

### 4.1 自动部署入口

- `.github/workflows/deploy-fullstack-tencent.yml`

这是 GitHub Actions 主入口。

### 4.2 统一发布脚本

- `03_Scripts/deploy_fullstack_server.sh`

这是“部署代码、安装依赖、构建前端、重启后端”的核心脚本。

### 4.3 首次 HTTP nginx 配置脚本

- `03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh`

这是把 nginx 基础站点配好的脚本。

### 4.4 HTTPS / 证书脚本

- `03_Scripts/deploy/nginx/enable_jato_fullstack_https.sh`

这是申请证书并接入 443 的脚本。

### 4.5 首次整机 bootstrap 脚本

- `03_Scripts/tencent_fullstack_bootstrap.sh`

这是“从一台新机器开始初始化”的脚本。

---

## 5. 为什么还要补 `DEPLOY_CERTBOT_EMAIL`

Let's Encrypt 建议给证书注册留一个邮箱。这样有两个好处：

1. 证书快过期时能收到通知
2. 不再使用 `--register-unsafely-without-email`

当前 workflow 已经支持读取：

- Secret 名称：`DEPLOY_CERTBOT_EMAIL`

只要这个 secret 被补上，以后 GitHub Actions 在需要补证书或校验 HTTPS 时，就会自动带上邮箱。

---

## 6. 怎么在 GitHub 里补 `DEPLOY_CERTBOT_EMAIL` secret

我这边现在不能直接替你写这个 secret，原因有两个：

1. 你还没把要写入的邮箱值给我
2. 当前环境没有可直接管理 GitHub repository secrets 的能力

你自己在网页里补，步骤很短：

1. 打开仓库 GitHub 页面
2. 点击顶部 `Settings`
3. 左侧点 `Secrets and variables`
4. 点击 `Actions`
5. 点击右上角 `New repository secret`
6. Name 填：`DEPLOY_CERTBOT_EMAIL`
7. Secret 填：你的邮箱，例如你希望接收证书通知的邮箱
8. 点击 `Add secret`

填完后，后续 workflow 就能读取到它。

---

## 7. `workflow_dispatch` 在哪里点击

`workflow_dispatch` 的意思就是：这个 workflow 不光能靠 `push` 自动触发，也能让你在 GitHub 网页里手动点一次运行。

点击路径：

1. 打开仓库 GitHub 页面
2. 点击顶部 `Actions`
3. 左侧工作流列表里找到：`deploy-fullstack-tencent`
4. 点进去后，在页面右上角找 `Run workflow`
5. Branch 选择 `main`
6. 点击绿色按钮确认运行

如果你没看到 `Run workflow`，通常就看这几项：

- 当前看的不是默认分支上的 workflow 文件
- GitHub Actions 没启用
- 你没有足够的仓库权限

---

## 8. 新手最实用的排查顺序

如果以后你怀疑“发版没成功”，不要一上来就改代码。按这个顺序看：

### 8.1 先看 GitHub Actions

看 `deploy-fullstack-tencent` 是不是绿色。

### 8.2 再看后端健康检查

服务器上跑：

```bash
curl -fsS http://127.0.0.1:8000/healthz
```

### 8.3 再看 nginx 和 HTTPS

服务器上跑：

```bash
ss -ltn | grep -E '(:80|:443)\b'
sudo nginx -t
```

### 8.4 最后看公网访问

本地跑：

```bash
curl -I https://ojeur.cloud/
curl -I https://www.ojeur.cloud/market-scan
```

---

## 9. 你现在可以怎么用这套流程

### 场景 A：你只是改了前后端代码

直接：

```bash
git push JATO_Analysis_System main
```

然后去 GitHub Actions 看部署结果。

### 场景 B：你想强制再跑一次发布

去 GitHub 网页点一次 `workflow_dispatch`。

### 场景 C：你换了域名或第一次接 HTTPS

服务器上手动执行：

```bash
cd /opt/JATO_Analysis_System-main
sudo SERVER_NAME="你的域名 你的www域名" \
  CERTBOT_EMAIL='你的邮箱' \
  bash 03_Scripts/deploy/nginx/enable_jato_fullstack_https.sh
```

### 场景 D：你怀疑 GitHub Actions 的 SSH 又坏了

先不要急着改 workflow，先对照这组已经验证过的值：

- `SSH_HOST=150.158.141.14`
- `SSH_PORT=22`
- `SSH_USER=ubuntu`
- `SSH_PRIVATE_KEY=~/.ssh/tencent_lh.pem` 的完整内容

这次排障最后已经证明：服务器 SSH 服务本身没问题，真正出错的是 GitHub Secrets 里那组 SSH 配置。只要它们保持正确，正常发布就不需要你再手工上传归档。

---

## 10. 以后是不是走这一套就够了

对，日常发版以后默认就走这一套，够用了：

1. 本地改代码
2. `git push JATO_Analysis_System main`
3. 看 GitHub Actions 里的 `deploy-fullstack-tencent` 是否成功

只有三种情况通常需要额外手工操作：

1. 新机器首次初始化
2. 更换域名或首次接 HTTPS
3. 紧急回滚或系统级故障排查

---

## 11. 一句话记忆版

这套链路现在可以这样记：

- 代码发布靠 GitHub Actions
- 应用重启靠 `deploy_fullstack_server.sh`
- 域名和 HTTP 入口靠 nginx 安装脚本
- HTTPS 和证书靠 `enable_jato_fullstack_https.sh`
- 证书配置不会再被后续发布覆盖
