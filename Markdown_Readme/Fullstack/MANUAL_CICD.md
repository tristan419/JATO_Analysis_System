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

## 7. 自动化 CI/CD 已知问题与修复记录

### 7.1 scp-action 归档路径问题（2026-04-08）

**现象**：Step 7（Upload archive to server）成功，但 Step 8（Deploy on server via SSH）tar 解包失败，提示找不到 `/tmp/JATO_deploy.tar.gz`。

**原因**：`appleboy/scp-action@v0.1.7` 内部把源文件用 tar 打包后传到远端再解包。在 Actions 容器中，`RUNNER_TEMP` 被挂载在 `/github/runner_temp/`。source 设为 `/github/runner_temp/JATO_deploy.tar.gz` 时，tar 内部存储路径为 `github/runner_temp/JATO_deploy.tar.gz`（2 层目录前缀）。

- `strip_components: 0` → 文件落在 `/tmp/github/runner_temp/JATO_deploy.tar.gz`（**错误路径**）
- `strip_components: 2` → 文件落在 `/tmp/JATO_deploy.tar.gz`（**正确路径**）

**修复**：将 workflow 中 `strip_components` 从 0 改为 2。

### 7.2 Node.js 版本检查过于严格（2026-04-08）

**现象**：deploy_fullstack_server.sh 的 Node.js 版本检查要求 20.19+，但服务器安装的可能是 20.15 或 20.17 等更早版本，导致部署脚本在版本校验处直接 exit 1。

**修复**：放宽检查条件，只要求 Node.js 20.10+ 或 22.x+。

### 7.3 deploy archive slim 化（2026-04-08）

**现象**：deploy 归档过大（>700MB），导致上传耗时超过 10 分钟，CI/CD 经常因数据传输超时失败。

**原因**：归档中包含了 `01_RAW_DATA`（~700MB 原始 Excel）、`04_Processed_data`（~61MB parquet）等只需要在服务器本地存在的数据文件。

**修复**：tar 打包时添加如下 `--exclude`：
```
01_RAW_DATA, 04_Processed_data, *.ipynb, data_wangler, Markdown_Readme, .git, node_modules, .venv, __pycache__
```
归档从 700MB+ 缩减到约 50MB。服务器上已有的数据目录不受影响。

### 7.4 前端构建产物白屏或加载失败

**现象**：浏览器打开页面白屏，控制台报 `ERR_CONNECTION_REFUSED` 请求 `http://127.0.0.1:8000/v1`。

**原因**：`06_AppPlatform/frontend/src/api/client.ts` 中 API_BASE 被硬编码为 `http://127.0.0.1:8000/v1`，打包进了前端产物。用户浏览器无法连接服务器本地回环地址。

**修复**：
1. `client.ts` 改为 `const API_BASE = import.meta.env.VITE_API_BASE ?? "/v1";`
2. `.env.production` 设 `VITE_API_BASE=/v1`（走 nginx 同域代理）
3. 构建时通过环境变量注入，不在代码中写死地址

### 7.5 Plotly 导致首屏卡死

**现象**：dashbaord 首屏加载 >3MB 的 Plotly 库，在弱网/低端机上白屏甚至超时。

**修复**：
1. `vite.config.ts` 中通过 `manualChunks` 把 plotly 分到独立 chunk `plotly-vendor`
2. `PlotlyChart.tsx` 改为 `React.lazy(() => import("react-plotly.js"))` 按需加载
3. `ExportPanel.tsx` 中 PNG 导出改为动态 `import("plotly.js-dist-min")`
4. `DashboardPage.tsx` 整个仪表板组件 lazy-load

**效果**：首屏 gzip 压缩后约 118KB，最大单文件加载 115ms。Plotly 只在用户真正打开图表时才下载。
