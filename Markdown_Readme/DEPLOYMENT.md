# Dashboard 部署模板（v1）

> 文档定位：Dashboard 部署方式、容量建议与上线前检查。
> 返回总览：[ROADMAP（总览导航）](./ROADMAP.md)

## 1) 标准启动模板

项目启动入口：`05_DashBoard/app.py`

本地运行：

```bash
cd 05_DashBoard
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

依赖安装（根目录）：

```bash
pip install -r requirements.txt
```

---

## 2) 环境模板

### A. Streamlit Community Cloud（免费）

- Repository Root：仓库根目录
- Main file path：`05_DashBoard/app.py`
- Python version：建议 `3.12`
- Secrets：在平台 Secrets 面板按 `.streamlit/secrets.toml.example` 填写

### B. 自建 VM（推荐 50 人访问）

- 建议规格（起步）：`2 vCPU / 8GB RAM`
- 进程管理：`systemd` 或 `supervisor`
- 反向代理：`nginx`（启用 gzip、keep-alive）
- 启动命令：

```bash
streamlit run 05_DashBoard/app.py --server.port 8501 --server.address 0.0.0.0
```

### C. 当前默认路径（2026-03-11）

- 当前项目默认采用 `EC2 + systemd + nginx + GitHub Actions SSH 自动更新`。
- 自动更新工作流：`.github/workflows/deploy-ec2-auto-update.yml`。
- 默认发布目标：`/opt/JATO_Analysis_System` 上的 `jato-dashboard@8501`。
- `ECS` 保留为后续容器化升级路径，不再作为当前默认自动发布方案。
- 若选择 EC2 路径，建议仅保留 EC2 工作流自动触发；ECS 工作流改为手动触发，避免一次 push 触发两套部署。

---

## 3) 并发容量估算（简版）

估算公式（经验值）：

`并发用户上限 ≈ 可用内存 / 单会话峰值内存`

建议先用基准脚本测读取耗时与内存：

```bash
PYTHONPATH=05_DashBoard python 03_Scripts/benchmark_dashboard_load.py --repeats 3
```

容量建议：

- 单机 8GB 内存：建议先按 20~40 活跃会话压测
- 若目标 50+ 活跃会话，优先：
  - 开启大数据模式（列裁剪 + 下推）
  - 关闭不必要的全列明细读取
  - 采用对象存储 + 分区数据

---

## 4) 回归检查清单（部署前）

- [ ] `03_Scripts/elt_worker.py` 产出 `jato_full_archive.parquet`
- [ ] `03_Scripts/build_partitioned_dataset.py --overwrite` 成功
- [ ] `partitioned_dataset_v1/manifest.json` 存在
- [ ] Dashboard 首屏可加载，状态栏显示读取耗时
- [ ] 高级图表（9 图）均可打开无报错
- [ ] 明细预览下载 CSV 正常

---

## 5) 50 并发访问方案论证（2026-03-09）

### 5.1 论证输入（当前基线）

来自现网数据集（`partitioned_dataset_v1`，724,501 行）与项目脚本实测：

- `benchmark_dashboard_load.py --repeats 3`
  - 侧边栏读取 `avg=0.578s`
  - 分析投影读取 `avg=1.224s`
  - 分析全列读取 `avg=4.669s`
  - 投影相对全列加速 `3.82x`
- `benchmark_time_transform_pipeline.py --repeats 2`
  - `sum` 模式：`95.70x` 加速
  - `group:powertrain` 模式：`21.74x` 加速
  - 口径一致性：`Parity check: PASS`
- 内存画像（脚本实测）
  - 全量无筛选：投影约 `205.15MB`，全列约 `1364.43MB`
  - 热门国家（德国，117,338 行）：投影约 `32.53MB`，全列约 `219.74MB`

结论：50 并发场景下，瓶颈不是时间变换，而是“全列读取 + 明细路径”的内存与 I/O 放大。

### 5.2 方案对比

| 方案 | 架构 | 50 并发可行性 | 风险 |
| --- | --- | --- | --- |
| A | 单机 `2 vCPU / 8GB` | 不建议作为 50 并发目标 | 高峰时易触发内存与响应抖动 |
| B | 单机 `4 vCPU / 16GB` | 可作为过渡方案（需强约束加载策略） | 无高可用，单点故障 |
| C | 双实例 `2 x (4 vCPU / 16GB)` + 负载均衡 | 推荐 | 成本略增，但稳定性明显提升 |

### 5.3 推荐方案（目标：稳定 50 并发）

推荐采用 `方案 C`：

- 计算层：2 个应用实例（每实例 `4 vCPU / 16GB`）
- 接入层：`nginx` 或云负载均衡，建议会话亲和（sticky）
- 数据层：分区数据集优先（后续可切对象存储）
- 运行策略：默认开启大数据模式（列裁剪 + 下推）

关键约束（必须满足）：

1. 生产默认禁用“明细预览按需全列”的自动开启，避免每次交互触发全列读取。
2. 高基数字段筛选优先走下推，避免无筛选全量读取路径。
3. 维持 CI smoke + nightly 性能门禁，阈值不低于当前文档标准。

### 5.4 验收口径（上线前）

建议按 50 并发压测（至少 10 分钟稳态）满足：

- 首屏加载 `P95 <= 8s`
- 分析读取 `P95 <= 3s`（投影模式）
- 错误率 `< 1%`
- 实例内存长期 `< 80%`

若不达标，优先顺序：

1. 收紧全列读取入口（仅明确需要时触发）
2. 降低单实例缓存压力（缓存项上限与 TTL）
3. 增加实例数（横向扩容）

### 5.5 实施建议（两阶段）

- 阶段 1（快速落地）：先上 `4 vCPU / 16GB` 单实例验证约束与压测脚本。
- 阶段 2（目标形态）：切到双实例 + 负载均衡，完成 50 并发稳态验收后再对外承诺容量。

---

## 6) 可执行模板与落地步骤

已提供模板文件：

- `03_Scripts/deploy/nginx/jato_dashboard.conf.example`
- `03_Scripts/deploy/systemd/jato-dashboard@.service`
- `03_Scripts/deploy/loadtest/k6_dashboard_50vus.js`

### 6.1 systemd 启动策略（默认单实例，按需双实例）

1. 修改 `03_Scripts/deploy/systemd/jato-dashboard@.service` 中的工作目录与 Python 路径：

`WorkingDirectory=/opt/JATO_Analysis_System`

`ExecStart=/opt/JATO_Analysis_System/.venv/bin/python ...`

1. 安装服务模板：

```bash
sudo cp 03_Scripts/deploy/systemd/jato-dashboard@.service /etc/systemd/system/
sudo systemctl daemon-reload
```

1. 默认建议先启单实例（8501）：

```bash
sudo systemctl disable --now jato-dashboard@8502
sudo systemctl enable --now jato-dashboard@8501
```

1. 机器内存足够时（建议 `>= 16GB`）再启双实例：

```bash
sudo systemctl enable --now jato-dashboard@8501
sudo systemctl enable --now jato-dashboard@8502
```

1. 检查状态：

```bash
sudo systemctl status jato-dashboard@8501 --no-pager
sudo systemctl status jato-dashboard@8502 --no-pager
```

### 6.2 nginx 反向代理与会话亲和

1. 安装并启用配置：

```bash
sudo cp 03_Scripts/deploy/nginx/jato_dashboard.conf.example /etc/nginx/conf.d/jato_dashboard.conf
sudo nginx -t
sudo systemctl reload nginx
```

1. 快速健康检查：

```bash
curl -sS http://127.0.0.1/healthz
```

若返回 `404 Not Found (nginx)`，通常是默认站点仍在生效，可执行：

```bash
sudo rm -f /etc/nginx/sites-enabled/default /etc/nginx/conf.d/default.conf
sudo nginx -t
sudo systemctl reload nginx
curl -sS http://127.0.0.1/healthz
```

### 6.3 50 并发压测（k6）

1. 安装 k6（按目标系统官方方式安装）。

1. 执行压测（默认 2+3+5+2 分钟分阶段）：

```bash
k6 run -e BASE_URL=http://127.0.0.1 03_Scripts/deploy/loadtest/k6_dashboard_50vus.js
```

1. 验收结论按第 `5.4` 节阈值判定：

`p(95) <= 8000ms`

`http_req_failed < 1%`

### 6.4 发布当天建议执行顺序

1. 先跑 `03_Scripts/ci_smoke_check.py`
1. 先启动单实例 + nginx 验证稳定
1. 再按资源情况切到双实例
1. 执行 k6 压测并记录 P95/失败率
1. 通过后再开放真实流量

---

## 7) AWS EC2 从 0 到上线（当前推荐路径）

### 7.1 资源建议（目标 50 并发）

1. 区域：就近选择（示例 `ap-southeast-1` 或 `ap-northeast-1`）。
1. 计算：2 台 EC2（每台 `4 vCPU / 16GB`，建议 `m6i.xlarge` 或同级）。
1. 负载均衡：1 个 ALB（Application Load Balancer）。
1. 存储：EBS `gp3`（建议 100GB 起步），后续可切 S3 承载数据。
1. 监控：CloudWatch（CPU、内存、HTTP 错误率、响应时延）。

### 7.2 网络与安全组

1. ALB 安全组：

`入站 80/443: 0.0.0.0/0`

`出站: 全放通`

1. EC2 安全组：

`入站 22: 你的办公出口 IP`

`入站 80: 仅 ALB 安全组`

`出站: 全放通`

1. 不要直接对公网开放 EC2 的 `8501/8502` 端口。

### 7.3 服务器初始化（Ubuntu）

1. 上传项目到服务器（git clone 或 scp）。
1. 执行一键初始化脚本：

```bash
cd /opt/JATO_Analysis_System
sudo bash 03_Scripts/deploy/aws/bootstrap_ubuntu.sh /opt/JATO_Analysis_System
```

说明：

- 该脚本默认启动单实例（`8501`）并停用 `8502`。
- 若检测到机器内存 `< 8GB` 且无 swap，会按磁盘可用空间自动创建 swap（优先 4GB，不足时降级为 3GB/2GB/1GB）；若磁盘空间仍不足会告警并继续部署，不会中断。
- 若历史残留了未启用的 `/swapfile`（例如上次创建中断），脚本会在 `apt update` 前自动清理，避免 `No space left on device`。
- 若你已是高内存机并要双实例，可在执行时显式开启：

```bash
cd /opt/JATO_Analysis_System
sudo JATO_ENABLE_SECONDARY_INSTANCE=true \
  bash 03_Scripts/deploy/aws/bootstrap_ubuntu.sh /opt/JATO_Analysis_System
```

1. 验证本机健康检查：

```bash
curl -sS http://127.0.0.1/healthz
```

### 7.3.1 GitHub Actions 自动更新（push main 自动部署）

当前仓库默认自动部署走 EC2，工作流文件：`.github/workflows/deploy-ec2-auto-update.yml`。

工作流行为：

1. 监听 `main` 分支 push。
1. GitHub Actions 通过 SSH 登录 EC2。
1. 在服务器执行：`git fetch`、`git reset --hard origin/main`、`pip install -r requirements.txt`。
1. 通过 `systemd` 重启 `jato-dashboard@8501`。
1. 访问 `/_stcore/health` 做健康检查。

GitHub 仓库 `Secrets`（必填）：

- `EC2_HOST`
- `EC2_USER`
- `EC2_SSH_KEY`

GitHub 仓库 `Variables`（可选，未填写则走默认值）：

- `EC2_REPO_DIR`，默认 `/opt/JATO_Analysis_System`
- `SYSTEMD_SERVICE_NAME`，默认 `jato-dashboard@8501`
- `DASHBOARD_PORT`，默认 `8501`

服务器一次性准备：

1. 确保 EC2 上项目目录固定为 `/opt/JATO_Analysis_System`（或与 `EC2_REPO_DIR` 保持一致）。
1. 确保 `ubuntu` 用户可以 SSH 登录并具备仓库读权限。
1. 建议服务以非 `root` 用户运行，避免端口占用、日志和 `__pycache__` 权限混乱。
1. 给部署用户开放受限 sudo 权限，仅允许重启和查看 dashboard 服务：

```bash
sudo bash -c 'cat >/etc/sudoers.d/jato-dashboard-deploy <<EOF
ubuntu ALL=(ALL) NOPASSWD:/bin/systemctl restart jato-dashboard@8501,/bin/systemctl status jato-dashboard@8501
EOF'
sudo chmod 440 /etc/sudoers.d/jato-dashboard-deploy
sudo visudo -cf /etc/sudoers.d/jato-dashboard-deploy
```

建议验证：

```bash
sudo systemctl status jato-dashboard@8501 --no-pager
curl -sS http://127.0.0.1:8501/_stcore/health
```

### 7.3.2 出现 `502` / `connecting streamlit server` 的快速判定

1. 若 `nginx access.log` 中出现 `/_stcore/stream 101` 但 `/_stcore/health` 间歇 `502`，优先怀疑应用进程被杀。
1. 用以下命令确认是否 OOM：

```bash
sudo journalctl -u jato-dashboard@8501 -n 120 --no-pager
```

重点看是否出现：

`Failed with result 'oom-kill'`

`A process of this unit has been killed by the OOM killer`

1. 立即止血：保持单实例、确认 `/healthz` 稳定 200：

```bash
sudo systemctl disable --now jato-dashboard@8502
sudo systemctl restart jato-dashboard@8501
curl -i http://127.0.0.1/healthz
```

1. 若是小规格机（如 `t3.medium`），建议升级到 `4 vCPU / 16GB` 或继续保持单实例并收紧筛选范围。

### 7.4 ALB 绑定与健康检查

1. 创建 Target Group（HTTP，端口 80，实例目标）。
1. Health Check Path 配置为 `/healthz`。
1. ALB Listener（80/443）转发到该 Target Group。
1. 验证 ALB DNS 可访问首页和健康检查。

### 7.5 上线前验收（必须）

1. 应用侧：`03_Scripts/ci_smoke_check.py` 全通过。
1. 压测侧：

```bash
k6 run -e BASE_URL=http://<ALB_DNS> 03_Scripts/deploy/loadtest/k6_dashboard_50vus.js
```

1. 指标满足第 `5.4` 节阈值后再对外发布。

### 7.6 运维建议（首月）

1. 每日巡检：实例状态、Target Group 健康数、错误率。
1. 每周巡检：成本、日志量、慢请求分布。
1. 每次数据刷新后：抽样执行 `ci_smoke_check.py` + 小规模 k6 回归。

---

## 8) 本地 -> Docker -> GitHub Actions -> AWS（ECS，后续升级路径）

本节给出容器化升级路径，适合后续需要更强发布标准化、滚动发布与多实例弹性时再启用。

### 8.1 本地（Local）

1. 本地先通过 smoke：

```bash
python 03_Scripts/ci_smoke_check.py
```

1. 构建镜像：

```bash
docker build -t jato-dashboard:local .
```

1. 挂载本地数据目录运行容器（推荐）：

```bash
docker run --rm -p 8501:8501 \
  -e JATO_PARTITIONED_PATH=/data/04_Processed_data/partitioned_dataset_v1 \
  -e JATO_PARQUET_PATH=/data/04_Processed_data/jato_full_archive.parquet \
  -v "$PWD/04_Processed_data:/data/04_Processed_data:ro" \
  jato-dashboard:local
```

1. 本地健康检查：

```bash
curl -sS http://127.0.0.1:8501/_stcore/health
```

### 8.2 GitHub Actions（CI/CD）

已提供工作流：`.github/workflows/deploy-aws-ecs.yml`

说明：当前默认自动发布走 EC2，因此该 ECS 工作流建议保留为 `workflow_dispatch` 手动触发，用于容器化演练、灰度验证或后续迁移。

该工作流会执行：

1. `ci_smoke_check.py`
1. Docker build
1. 推送到 ECR（镜像 tag 用 `commit SHA`）
1. 更新 ECS task definition 并发布

GitHub 仓库配置项（必须）：

1. `Secrets`

`AWS_ROLE_TO_ASSUME`：GitHub OIDC 假设角色 ARN

1. `Variables`

`AWS_REGION`

`ECR_REPOSITORY`

`ECS_CLUSTER`

`ECS_SERVICE`

`ECS_TASK_DEFINITION`

`ECS_CONTAINER_NAME`

### 8.3 AWS（ECR + ECS + ALB）

1. ECR：创建仓库（如 `jato-dashboard`）。
1. ECS：创建 Cluster + Service（建议最少 2 个 Task 实例）。
1. Task 定义中容器端口使用 `8501`。
1. ALB Target Group 健康检查路径使用 `/_stcore/health`。
1. Service 绑定 ALB，对外仅开放 `80/443`。

### 8.4 常见错误与纠正

1. 错误：把生产跑在个人电脑。

纠正：个人电脑只做开发/验证，生产放 AWS 常开环境。

1. 错误：把 `04_Processed_data` 打进 Docker 镜像。

纠正：数据通过挂载卷/EFS/S3 同步提供，镜像保持轻量。

1. 错误：GitHub Actions 里放长期 AK/SK。

纠正：使用 GitHub OIDC + `AWS_ROLE_TO_ASSUME` 临时凭证。

1. 错误：只用 `latest` 标签发布。

纠正：使用 `commit SHA` 不可变标签，支持精确回滚。

1. 错误：直接开放 `8501/8502` 到公网。

纠正：仅 ALB 对公网开放，应用容器放私网/受限安全组。

### 8.5 GitHub OIDC + IAM（必须先做）

模板文件：

- `03_Scripts/deploy/aws/iam-github-oidc-trust-policy.json`
- `03_Scripts/deploy/aws/iam-github-ecs-deploy-policy.json`

执行顺序：

1. 在 AWS 账户中创建 OIDC Provider：`token.actions.githubusercontent.com`。
1. 创建 IAM Role，信任策略使用 `iam-github-oidc-trust-policy.json`。
1. 给该 Role 绑定权限策略 `iam-github-ecs-deploy-policy.json`。
1. 将 Role ARN 写入 GitHub Secret：`AWS_ROLE_TO_ASSUME`。

### 8.6 ECS Task Definition 模板

模板文件：

- `03_Scripts/deploy/aws/ecs-taskdef.template.json`

关键点：

1. 替换模板中的占位符（账号、区域、角色、EFS 等）。
1. 容器健康检查与 ALB 保持一致：`/_stcore/health`。
1. 环境变量使用：

`JATO_PARTITIONED_PATH`

`JATO_PARQUET_PATH`

1. 首次发布前，先手工注册一次 task definition，再让 GitHub Actions 持续更新镜像。

### 8.7 配置检查（避免踩坑）

`deploy-aws-ecs.yml` 已内置必填项预检：

- Secret：`AWS_ROLE_TO_ASSUME`
- Variables：`AWS_REGION`、`ECR_REPOSITORY`、`ECS_CLUSTER`、`ECS_SERVICE`、`ECS_TASK_DEFINITION`、`ECS_CONTAINER_NAME`

若缺失，工作流会在部署前直接失败并提示缺少的配置名。

### 8.8 一键 AWS CLI 引导（推荐）

脚本：`03_Scripts/deploy/aws/aws_cli_setup_ci_cd.sh`

最短执行示例：

```bash
export AWS_REGION="ap-southeast-1"
export AWS_ACCOUNT_ID="123456789012"
export GITHUB_ORG="your-org"
export GITHUB_REPO="JATO_Analysis_System"
export ECR_REPOSITORY="jato-dashboard"
export GITHUB_DEPLOY_ROLE_NAME="github-actions-ecs-deploy"
export TASK_EXECUTION_ROLE_NAME="ecsTaskExecutionRole"
export TASK_ROLE_NAME="jatoDashboardTaskRole"
export EFS_FILE_SYSTEM_ID="fs-xxxxxxxx"
export ECS_CLUSTER="jato-dashboard-cluster"
export ECS_SERVICE="jato-dashboard-service"
export ECS_CONTAINER_NAME="jato-dashboard"

bash 03_Scripts/deploy/aws/aws_cli_setup_ci_cd.sh
```

脚本会完成：

1. 校验 GitHub OIDC Provider 是否存在
1. 创建/更新 GitHub 部署 IAM Role（含信任策略）
1. 注入 ECS/ECR 部署权限策略
1. 确保 ECR 仓库存在
1. 渲染 ECS task definition（可选：若镜像 tag 存在则自动注册）
1. 输出应写入 GitHub 的 Secret/Variables

### 8.9 从 0 到上线 20 步清单

1. [AWS Console] 选定目标 Region（与后续 CLI、GitHub Variables 保持一致）。
1. [AWS Console] 确认账户已创建 GitHub OIDC Provider：`token.actions.githubusercontent.com`。
1. [AWS Console] 创建或确认 ECR 仓库（例如 `jato-dashboard`）。
1. [AWS Console] 创建或确认 ECS Cluster（例如 `jato-dashboard-cluster`）。
1. [AWS Console] 创建或确认 ECS Service（例如 `jato-dashboard-service`）。
1. [AWS Console] 创建或确认 ALB + Target Group，健康检查路径设为 `/_stcore/health`。
1. [AWS Console] 确认 ALB 仅对公网开放 `80/443`，不要开放应用容器端口到公网。
1. [AWS Console] 确认 Task Execution Role 与 Task Role 已存在。
1. [AWS Console] 若使用 EFS 挂载数据，确认 EFS 文件系统与挂载目标可用。
1. [Local Terminal] 在项目根目录执行一次本地 smoke：`python 03_Scripts/ci_smoke_check.py`。
1. [Local Terminal] 准备并检查环境变量：`AWS_REGION`、`AWS_ACCOUNT_ID`、`GITHUB_ORG`、`GITHUB_REPO`、`ECR_REPOSITORY`、`GITHUB_DEPLOY_ROLE_NAME`、`TASK_EXECUTION_ROLE_NAME`、`TASK_ROLE_NAME`、`EFS_FILE_SYSTEM_ID`、`ECS_CLUSTER`、`ECS_SERVICE`、`ECS_CONTAINER_NAME`。
1. [Local Terminal] 执行引导脚本：`bash 03_Scripts/deploy/aws/aws_cli_setup_ci_cd.sh`。
1. [Local Terminal] 记录脚本输出的 Role ARN 与 GitHub 配置项。
1. [GitHub Repo Settings] 填入 Secret：`AWS_ROLE_TO_ASSUME`。
1. [GitHub Repo Settings] 填入 Variables：`AWS_REGION`、`ECR_REPOSITORY`、`ECS_CLUSTER`、`ECS_SERVICE`、`ECS_TASK_DEFINITION`、`ECS_CONTAINER_NAME`。
1. [Local Terminal] 提交并推送当前代码到 `main`，触发 `deploy-aws-ecs.yml`。
1. [GitHub Actions] 检查工作流执行顺序：smoke -> build image -> push ECR -> render taskdef -> deploy ECS。
1. [AWS Console] 观察 ECS Service 发布状态，确认新 Task 进入 healthy。
1. [Local Terminal] 使用 ALB 地址执行 k6 压测：`k6 run -e BASE_URL=http://<ALB_DNS> 03_Scripts/deploy/loadtest/k6_dashboard_50vus.js`。
1. [验收] 满足阈值后对外发布：`P95 <= 8s`、`http_req_failed < 1%`、实例内存长期 `< 80%`。

### 8.10 新手版（从未用过 AWS）

> 目标：先把服务成功上线，再逐步升级到 50 并发稳定架构。

#### 第 1 阶段：先跑起来（最小可用）

1. 注册并登录 AWS，选定一个固定 Region（例如 `ap-southeast-1`）。
1. 在 Billing 中确认支付方式可用，并开启 MFA。
1. 创建 1 台 EC2：

`Ubuntu 22.04/24.04`

`m6i.xlarge (4 vCPU / 16GB)`

1. 配置安全组：

`22` 端口仅允许你的公网 IP

`80` 端口允许 `0.0.0.0/0`

（`443` 可后续加域名时启用）

1. 本地下载并保存 Key Pair（`.pem`）。
1. 本地终端连接服务器：

```bash
chmod 400 <your-key>.pem
ssh -i <your-key>.pem ubuntu@<EC2_PUBLIC_IP>
```

1. 在服务器安装 git：

```bash
sudo apt-get update -y
sudo apt-get install -y git
```

1. 拉取项目代码到服务器：

```bash
sudo mkdir -p /opt
cd /opt
sudo git clone <your-repo-url> JATO_Analysis_System
sudo chown -R ubuntu:ubuntu /opt/JATO_Analysis_System
cd /opt/JATO_Analysis_System
```

1. 运行一键部署脚本（默认单实例 `8501`，会停用 `8502`）：

```bash
sudo bash 03_Scripts/deploy/aws/bootstrap_ubuntu.sh /opt/JATO_Analysis_System
```

1. 健康检查：

```bash
curl -sS http://127.0.0.1/healthz
```

1. 本地浏览器访问：`http://<EC2_PUBLIC_IP>`。

到这一步，服务已经可访问。

#### 第 2 阶段：升级到 50 并发稳定形态

1. 再创建 1 台同规格 EC2，重复第 1 阶段部署。
1. 创建 ALB + Target Group，健康检查路径使用 `/_stcore/health`。
1. 将两台 EC2 挂到同一 Target Group。
1. 对外入口切到 ALB DNS，不再直连单台 EC2。

#### 第 3 阶段：接入自动部署（GitHub Actions -> AWS）

1. 使用模板和脚本完成 OIDC/IAM/ECR/ECS 对接：

`03_Scripts/deploy/aws/aws_cli_setup_ci_cd.sh`

1. 在 GitHub 配置：

Secret：`AWS_ROLE_TO_ASSUME`

Variables：`AWS_REGION`、`ECR_REPOSITORY`、`ECS_CLUSTER`、`ECS_SERVICE`、`ECS_TASK_DEFINITION`、`ECS_CONTAINER_NAME`

1. 推送 `main`，触发 `.github/workflows/deploy-aws-ecs.yml` 自动部署。

#### 第 4 阶段：压测与发布

1. 执行压测：

```bash
k6 run -e BASE_URL=http://<ALB_DNS> 03_Scripts/deploy/loadtest/k6_dashboard_50vus.js
```

1. 验收阈值：

`P95 <= 8s`

`http_req_failed < 1%`

`实例内存长期 < 80%`

1. 达标后正式对外发布。
