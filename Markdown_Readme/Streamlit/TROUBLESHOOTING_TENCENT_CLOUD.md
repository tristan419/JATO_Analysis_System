# 腾讯云部署问题排查指南

## 环境信息
- 服务器：腾讯云 Ubuntu Linux
- 项目路径：`/var/www/JATO_Analysis_System`
- Python 环境：venv 虚拟环境

## 问题1: 主题颜色差异

### 现象
- 本地运行：蓝色主题
- 腾讯云：显示为红色主题

### 排查步骤

**1. 激活虚拟环境并检查配置**
```bash
cd /var/www/JATO_Analysis_System
source venv/bin/activate
python 03_Scripts/check_theme_config.py
```

**2. 验证配置文件内容**
```bash
cat .streamlit/config.toml
```

应该看到：
```toml
[theme]
primaryColor = "#2563EB"  # 蓝色
```

**3. 如果颜色不对，重新拉取配置**
```bash
git pull origin main
git checkout .streamlit/config.toml
```

---

## 问题2: 性能差异

### 现象
- 本地加载：0.2s
- 腾讯云：2.4s

### 诊断步骤

**1. 运行性能诊断**
```bash
cd /var/www/JATO_Analysis_System
source venv/bin/activate
python 03_Scripts/diagnose_performance.py
```

**2. 分析输出**
关注以下指标：
- 首次加载时间
- 缓存命中时间
- 加速比

### 优化方案

**方案A: 启用持久化缓存（推荐）**

详细步骤请参考：[性能优化：启用磁盘持久化缓存](./PERFORMANCE_OPTIMIZATION_CACHE.md)

简要说明：修改 `05_DashBoard/dashboard/data.py`，在所有 `@st.cache_data` 装饰器中添加 `persist="disk"`：

```python
@st.cache_data(ttl=3600, persist="disk")
```

**方案B: 预热缓存**

创建预热脚本 `03_Scripts/warmup_cache.py`：
```python
import sys
sys.path.insert(0, '05_DashBoard')
from dashboard.data import load_sidebar_data
load_sidebar_data()
```

然后在启动前运行：
```bash
python 03_Scripts/warmup_cache.py
```

**方案C: 检查数据位置**
```bash
# 确保数据在本地磁盘，不在网络存储
ls -lh 04_Processed_data/
df -h 04_Processed_data/
```

---

## 快速检查清单

- [ ] 配置文件存在且正确
- [ ] 使用正确的启动脚本
- [ ] 数据文件在本地磁盘
- [ ] venv 虚拟环境已激活
- [ ] 端口未被占用
