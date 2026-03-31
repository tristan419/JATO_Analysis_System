#!/usr/bin/env python3
"""
主题配置检查脚本 - 检查和修复 Streamlit 主题配置
"""
import os
from pathlib import Path


def check_theme_config():
    """检查主题配置"""
    print("=" * 60)
    print("Streamlit 主题配置检查")
    print("=" * 60)
    
    project_root = Path(__file__).parent.parent
    config_file = project_root / ".streamlit" / "config.toml"
    
    print(f"\n配置文件路径: {config_file}")
    print(f"文件存在: {config_file.exists()}")
    
    if config_file.exists():
        print("\n当前配置内容:")
        print("-" * 60)
        with open(config_file, 'r', encoding='utf-8') as f:
            content = f.read()
            print(content)
        print("-" * 60)
        
        # 检查主题颜色
        if '#2563EB' in content:
            print("\n✓ 主题颜色: #2563EB (蓝色)")
        elif 'primaryColor' in content:
            import re
            match = re.search(r'primaryColor\s*=\s*"([^"]+)"', content)
            if match:
                color = match.group(1)
                print(f"\n✓ 主题颜色: {color}")
        else:
            print("\n⚠ 未找到主题颜色配置")
    else:
        print("\n⚠ 配置文件不存在")
    
    # 检查环境变量
    print("\n" + "=" * 60)
    print("环境变量检查:")
    print("=" * 60)
    env_vars = [
        'STREAMLIT_THEME_PRIMARY_COLOR',
        'STREAMLIT_THEME_BACKGROUND_COLOR',
        'STREAMLIT_THEME_SECONDARY_BACKGROUND_COLOR',
        'STREAMLIT_THEME_TEXT_COLOR'
    ]
    
    for var in env_vars:
        value = os.environ.get(var)
        if value:
            print(f"✓ {var} = {value}")
        else:
            print(f"✗ {var} = (未设置)")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    check_theme_config()
