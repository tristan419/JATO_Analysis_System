#!/usr/bin/env python3
"""
性能诊断脚本 - 用于分析数据加载时间
"""
import time
import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "05_DashBoard"))


def measure_load_time():
    """测量数据加载时间"""
    print("=" * 60)
    print("JATO Dashboard 性能诊断")
    print("=" * 60)
    
    # 测试1: 导入模块时间
    print("\n[1/5] 测试模块导入时间...")
    start = time.time()
    from dashboard.data import (
        load_column_names,
        load_dataset_slice,
        get_dataset_version_token,
        get_project_root
    )
    import_time = time.time() - start
    print(f"✓ 模块导入耗时: {import_time:.3f}s")
    
    # 获取数据路径
    data_path = str(get_project_root() / "04_Processed_data" / "partitioned_dataset_v1")
    dataset_version = get_dataset_version_token(data_path)
    
    # 测试2: 加载列名（schema）
    print("\n[2/5] 测试加载列名...")
    start = time.time()
    columns = load_column_names(data_path, dataset_version)
    schema_time = time.time() - start
    print(f"✓ 列名加载耗时: {schema_time:.3f}s")
    print(f"  - 列数量: {len(columns)}")
    
    # 测试3: 首次加载数据（sidebar范围）
    print("\n[3/5] 测试首次加载数据（sidebar范围）...")
    start = time.time()
    df_sidebar = load_dataset_slice(
        data_path,
        columns=tuple(columns[:10]),
        cache_scope="sidebar",
        dataset_version=dataset_version
    )
    sidebar_time = time.time() - start
    print(f"✓ Sidebar数据加载耗时: {sidebar_time:.3f}s")
    print(f"  - 数据行数: {len(df_sidebar)}")
    
    # 测试4: 第二次加载（测试缓存）
    print("\n[4/5] 测试缓存命中...")
    start = time.time()
    df_sidebar = load_dataset_slice(
        data_path,
        columns=tuple(columns[:10]),
        cache_scope="sidebar",
        dataset_version=dataset_version
    )
    cache_time = time.time() - start
    print(f"✓ 缓存命中耗时: {cache_time:.3f}s")
    if cache_time > 0:
        print(f"  - 加速比: {sidebar_time/cache_time:.1f}x")
    
    # 测试5: 加载分析数据
    print("\n[5/5] 测试分析数据加载...")
    start = time.time()
    df_analysis = load_dataset_slice(
        data_path,
        cache_scope="analysis",
        dataset_version=dataset_version
    )
    analysis_time = time.time() - start
    print(f"✓ Analysis数据加载耗时: {analysis_time:.3f}s")
    print(f"  - 数据行数: {len(df_analysis)}")
    
    # 总结
    print("\n" + "=" * 60)
    print("性能总结:")
    print(f"  首次加载总耗时: {sidebar_time + analysis_time:.3f}s")
    print(f"  缓存后总耗时: {cache_time:.3f}s")
    print("=" * 60)


if __name__ == "__main__":
    measure_load_time()
