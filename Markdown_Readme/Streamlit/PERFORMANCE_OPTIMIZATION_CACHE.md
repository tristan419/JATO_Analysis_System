# Dashboard 性能优化：启用磁盘持久化缓存

## 问题描述

腾讯云服务器上 Dashboard 加载慢（2.4s），诊断发现缓存使用的是内存缓存，每次重启都会丢失。

## 诊断结果

```
首次加载 sidebar：2.388s
首次加载 analysis：7.895s
缓存命中后：0.537s
警告：No runtime found, using MemoryCacheStorageManager
```

## 解决方案

在 `05_DashBoard/dashboard/data.py` 的所有 `@st.cache_data` 装饰器中添加 `persist="disk"` 参数。

## 具体修改步骤

### 1. load_full_data 函数

**修改前：**
```python
@st.cache_data(ttl=3600)
def load_full_data():
```

**修改后：**
```python
@st.cache_data(ttl=3600, persist="disk")
def load_full_data():
```

### 2. load_column_names 函数

**修改前：**
```python
@st.cache_data(ttl=3600)
def load_column_names(data_path: str, dataset_version: str) -> List[str]:
```

**修改后：**
```python
@st.cache_data(ttl=3600, persist="disk")
def load_column_names(data_path: str, dataset_version: str) -> List[str]:
```

### 3. _load_dataset_slice_sidebar_cached 函数

**修改前：**
```python
@st.cache_data(ttl=3600)
def _load_dataset_slice_sidebar_cached(
```

**修改后：**
```python
@st.cache_data(ttl=3600, persist="disk")
def _load_dataset_slice_sidebar_cached(
```

### 4. _load_dataset_slice_analysis_cached 函数

**修改前：**
```python
@st.cache_data(ttl=3600)
def _load_dataset_slice_analysis_cached(
```

**修改后：**
```python
@st.cache_data(ttl=3600, persist="disk")
def _load_dataset_slice_analysis_cached(
```

### 5. _load_dataset_slice_detail_cached 函数

**修改前：**
```python
@st.cache_data(ttl=3600)
def _load_dataset_slice_detail_cached(
```

**修改后：**
```python
@st.cache_data(ttl=3600, persist="disk")
def _load_dataset_slice_detail_cached(
```

### 6. _load_distinct_options_sidebar_cached 函数

**修改前：**
```python
@st.cache_data(ttl=3600)
def _load_distinct_options_sidebar_cached(
```

**修改后：**
```python
@st.cache_data(ttl=3600, persist="disk")
def _load_distinct_options_sidebar_cached(
```

### 7. _load_filtered_row_count_sidebar_cached 函数

**修改前：**
```python
@st.cache_data(ttl=3600)
def _load_filtered_row_count_sidebar_cached(
```

**修改后：**
```python
@st.cache_data(ttl=3600, persist="disk")
def _load_filtered_row_count_sidebar_cached(
```

## 应用修改

### 在腾讯云服务器上操作

```bash
cd /var/www/JATO_Analysis_System
source venv/bin/activate

# 编辑文件
nano 05_DashBoard/dashboard/data.py

# 或使用 vim
vim 05_DashBoard/dashboard/data.py
```

使用编辑器的查找替换功能：
- 查找：`@st.cache_data(ttl=3600)`
- 替换为：`@st.cache_data(ttl=3600, persist="disk")`

### 重启 Dashboard

```bash
bash 03_Scripts/restart_dashboard.sh
```

## 验证效果

重启后再次运行诊断：

```bash
python 03_Scripts/diagnose_performance.py
```

预期结果：
- 首次加载仍需 2-3s
- 重启后缓存命中，加载时间降至 0.2-0.5s
- 不再显示 "MemoryCacheStorageManager" 警告

## 预期性能提升

- 首次启动：2.4s（不变）
- 重启后加载：0.2-0.5s（提升 5-10倍）
- 用户体验：接近本地性能
