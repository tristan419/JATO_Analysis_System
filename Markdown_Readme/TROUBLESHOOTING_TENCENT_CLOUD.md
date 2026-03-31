# 腾讯云部署问题排查指南

## 问题1: 主题颜色差异

### 现象
- 本地运行：蓝色主题
- 腾讯云：显示为红色主题

### 排查步骤

**1. 检查配置文件**
```powershell
# 在腾讯云 Windows Server 上执行
cd C:\path\to\JATO_Analysis_System
python 03_Scripts\check_theme_config.py
```

**2. 验证配置文件内容**
```powershell
type .streamlit\config.toml
```

应该看到：
```toml
[theme]
primaryColor = "#2563EB"  # 蓝色
```

**3. 如果颜色不对，重新拉取配置**
```powershell
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
```powershell
cd C:\path\to\JATO_Analysis_System
python 03_Scripts\diagnose_performance.py
```

**2. 分析输出**
关注以下指标：
- 首次加载时间
- 缓存命中时间
- 加速比

### 优化方案

**方案A: 启用持久化缓存（推荐）**

修改 `05_DashBoard/dashboard/data.py`：
```python
# 在所有 @st.cache_data 装饰器中添加 persist="disk"
@st.cache_data(ttl=3600, persist="disk")
def load_sidebar_data():
    ...
```

**方案B: 预热缓存**

在启动脚本中添加预热：
```python
# 创建 03_Scripts/warmup_cache.py
import sys
sys.path.insert(0, '05_DashBoard')
from dashboard.data import load_sidebar_data
load_sidebar_data()
```

**方案C: 检查数据位置**
```powershell
# 确保数据在本地磁盘，不在网络存储
dir 04_Processed_data
```

---

## 快速检查清单

- [ ] 配置文件存在且正确
- [ ] 使用正确的启动脚本
- [ ] 数据文件在本地磁盘
- [ ] Python 环境正确
- [ ] 端口未被占用
