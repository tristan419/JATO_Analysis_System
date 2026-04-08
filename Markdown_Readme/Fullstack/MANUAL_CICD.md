# JATO Fullstack 手动 CI/CD 手册

这份文档对应自动化还未完全接管之前的发布流程。目标是把前后端都纳入同一套人工检查和人工发布步骤，避免只更新前端或只更新后端。

## 1. 手动 CI：本地或开发机先过关

发布前至少执行下面三组检查。

### 1.1 Python 烟测

```bash
cd /path/to/JATO_Analysis_System
python 03_Scripts/ci_smoke_check.py
```

### 1.2 Fullstack 后端检查

```bash
cd /path/to/JATO_Analysis_System
. .venv/bin/activate
pip install -r 06_AppPlatform/backend/requirements.txt
python -m compileall 06_AppPlatform/backend/app
```

### 1.3 Fullstack 前端检查

```bash
cd /path/to/JATO_Analysis_System/06_AppPlatform/frontend
npm ci
npx tsc --noEmit
VITE_API_BASE=/v1 VITE_AUTH_TOKEN=ci-token VITE_USER_ROLE=admin VITE_USER_NAME=manual-ci npm run build
```

## 2. 手动 Push 到 GitHub

只把你确认过的文件加入提交，不要把本地日志、缓存、临时环境一起推上去。

```bash
cd /path/to/JATO_Analysis_System
REMOTE_NAME="$(git remote | head -n 1)"
git status --short
git add .
git commit -m "publish fullstack app and Tencent deploy assets"
git push "$REMOTE_NAME" main
```

如果当前工作区还有不准备上库的内容，不要直接 `git add .`，而是改成按路径选择性 `git add`。

## 3. 手动 CD：腾讯云服务器更新

登录服务器：

```bash
ssh <user>@<tencent-cloud-ip>
```

执行发布脚本：

```bash
cd /opt/JATO_Analysis_System
VITE_API_BASE=/v1 \
VITE_USER_ROLE=viewer \
VITE_USER_NAME=manual-deploy \
bash 03_Scripts/deploy_fullstack_server.sh
```

这个脚本会自动做这些事：

- 拉取最新 main
- 安装后端依赖
- `npm ci` 并重新构建前端
- 重启 `jato-fullstack-backend@8000`
- 如果 nginx 已在运行，则自动 reload nginx

## 4. 手动验收

发布后至少检查：

```bash
curl -fsS http://127.0.0.1:8000/healthz
curl -I http://127.0.0.1/
```

再从浏览器手工验证：

- Dashboard 可以加载 overview
- 年度与月度图表正常
- 高级分析页面能出图
- CRUD 页面列表可以打开

## 5. 失败回退

### 5.1 先看日志

```bash
sudo journalctl -u jato-fullstack-backend@8000 -n 100 --no-pager
sudo nginx -t
```

### 5.2 回退代码并重新发布

```bash
cd /opt/JATO_Analysis_System
git log --oneline -5
git reset --hard <commit_sha>
bash 03_Scripts/deploy_fullstack_server.sh
```

### 5.3 快速确认回退成功

```bash
curl -fsS http://127.0.0.1:8000/healthz
curl -I http://127.0.0.1/
```

## 6. 何时切到自动化

满足下面条件后，就可以把这套人工流程切到 GitHub Actions：

- 服务器首发已经成功
- `/opt/JATO_Analysis_System` 路径固定
- `jato-fullstack-backend@8000` 服务名固定
- GitHub Secrets 已经配置好 SSH 连接信息
- 你确认前端构建应该统一使用 `VITE_API_BASE=/v1`

自动化 workflow 文件：`.github/workflows/deploy-fullstack-tencent.yml`
