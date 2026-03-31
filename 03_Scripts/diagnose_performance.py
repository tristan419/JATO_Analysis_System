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
    print("\n[1/4] 测试模块导入时间...")
    start = time.time()
    from dashboard.data import load_sidebar_data, load_analysis_data
    import_time = time.time() - start
    print(f"✓ 模块导入耗时: {import_time:.3f}s")
    
    # 测试2: 首次加载侧边栏数据
    print("\n[2/4] 测试首次加载侧边栏数据...")
    start = time.time()
    sidebar_data = load_sidebar_data()
    sidebar_time = time.time() - start
    print(f"✓ 侧边栏数据加载耗时: {sidebar_time:.3f}s")
    print(f"  - 国家数量: {len(sidebar_data['countries'])}")
    print(f"  - 品牌数量: {len(sidebar_data['brands'])}")
    
    # 测试3: 第二次加载（测试缓存）
    print("\n[3/4] 测试缓存命中...")
    start = time.time()
    sidebar_data = load_sidebar_data()
    cache_time = time.time() - start
    print(f"✓ 缓存命中耗时: {cache_time:.3f}s")
    print(f"  - 加速比: {sidebar_time/cache_time:.1f}x")
    
    # 测试4: 加载分析数据
    print("\n[4/4] 测试分析数据加载...")
    start = time.time()
    analysis_data = load_analysis_data(
        countries=sidebar_data['countries'][:1],
        brands=None,
        years=None
    )
    analysis_time = time.time() - start
    print(f"✓ 分析数据加载耗时: {analysis_time:.3f}s")
    print(f"  - 数据行数: {len(analysis_data)}")
    
    # 总结
    print("\n" + "=" * 60)
    print("性能总结:")
    print(f"  首次加载总耗时: {sidebar_time + analysis_time:.3f}s")
    print(f"  缓存后总耗时: {cache_time + analysis_time:.3f}s")
    print("=" * 60)


if __name__ == "__main__":
    measure_load_time()
