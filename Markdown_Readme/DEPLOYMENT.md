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
