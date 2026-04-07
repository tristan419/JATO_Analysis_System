"""
预聚合模块：在数据刷新时预先计算常见的分组汇总结果。
目的：大幅减少前端传输数据量，从 70 万行降至几千行。
"""

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as ds


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_analysis_data(parquet_path: str) -> pd.DataFrame:
    """从 parquet 加载分析数据。"""
    if Path(parquet_path).is_dir():
        dataset = ds.dataset(parquet_path, format="parquet")
        return dataset.to_table().to_pandas()
    else:
        return pd.read_parquet(parquet_path)


def compute_country_summary(df: pd.DataFrame) -> pd.DataFrame:
    """按国家聚合：计数、平均值等。"""
    if "国家" not in df.columns:
        return pd.DataFrame()

    numeric_cols = (df.select_dtypes(include=['number'])
                    .columns.tolist())

    agg_dict = {"国家": "count"}
    for col in numeric_cols:
        if col not in ["国家"]:
            agg_dict[col] = ["mean", "min", "max", "count"]

    summary = df.groupby("国家").agg(agg_dict).reset_index()
    cols = ["_".join(col).strip("_") if col[1] else col[0]
            for col in summary.columns.values]
    summary.columns = cols
    return summary


def compute_year_month_summary(df: pd.DataFrame) -> pd.DataFrame:
    """按年月聚合：逐月数据。"""
    month_cols = [col for col in df.columns
                  if col.startswith("20") and len(col) > 4]

    if not month_cols:
        return pd.DataFrame()

    monthly_data = []
    for col in sorted(month_cols):
        if col in df.columns:
            monthly_data.append({
                "yearMonth": col,
                "totalCount": int(df[col].sum()),
                "avgValue": float(df[col].mean()),
            })

    return pd.DataFrame(monthly_data)


def compute_powertrain_summary(df: pd.DataFrame) -> pd.DataFrame:
    """按功率系统（燃油、混动、纯电等）聚合。"""
    if ("动力系统" not in df.columns
            and "Powertrain" not in df.columns):
        return pd.DataFrame()

    pt_col = ("动力系统" if "动力系统" in df.columns
              else "Powertrain")

    numeric_cols = (df.select_dtypes(include=['number'])
                    .columns.tolist())
    agg_dict = {pt_col: "count"}
    for col in numeric_cols[:5]:  # Only first 5 metrics
        agg_dict[col] = ["mean", "count"]

    summary = df.groupby(pt_col).agg(agg_dict).reset_index()
    cols = ["_".join(col).strip("_") if col[1] else col[0]
            for col in summary.columns.values]
    summary.columns = cols
    return summary


def compute_segment_summary(df: pd.DataFrame) -> pd.DataFrame:
    """按车形分类聚合。"""
    if ("车形分类" not in df.columns
            and "Segment" not in df.columns):
        return pd.DataFrame()

    seg_col = ("车形分类" if "车形分类" in df.columns
               else "Segment")

    numeric_cols = (df.select_dtypes(include=['number'])
                    .columns.tolist())
    agg_dict = {seg_col: "count"}
    for col in numeric_cols[:5]:
        agg_dict[col] = ["mean"]

    summary = df.groupby(seg_col).agg(agg_dict).reset_index()
    cols = ["_".join(col).strip("_") if col[1] else col[0]
            for col in summary.columns.values]
    summary.columns = cols
    return summary


def compute_top_makes_summary(df: pd.DataFrame,
                               top_n: int = 20) -> pd.DataFrame:
    """计算最热门的 N 个品牌。"""
    if ("品牌" not in df.columns
            and "Make" not in df.columns):
        return pd.DataFrame()

    make_col = ("品牌" if "品牌" in df.columns
                else "Make")
    numeric_cols = (df.select_dtypes(include=['number'])
                    .columns.tolist())

    agg_dict = {make_col: "count"}
    for col in numeric_cols[:3]:
        agg_dict[col] = "mean"

    summary = df.groupby(make_col).agg(agg_dict)
    summary = (summary.sort_values(
        by=list(agg_dict.keys()),
        ascending=False).head(top_n))
    summary = summary.reset_index()
    cols = ["_".join(col).strip("_") if col[1] else col[0]
            for col in summary.columns.values]
    summary.columns = cols
    return summary


def save_summary_tables(
    summaries: dict[str, pd.DataFrame],
    output_dir: str,
) -> dict[str, str]:
    """保存预聚合表到 Parquet 和 CSV。"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    saved_files = {}
    for name, df in summaries.items():
        if df.empty:
            continue
        
        csv_path = output_path / f"{name}_summary.csv"
        parquet_path = output_path / f"{name}_summary.parquet"
        
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        df.to_parquet(parquet_path, index=False)
        
        saved_files[name] = {
            "csv": str(csv_path),
            "parquet": str(parquet_path),
            "rows": len(df),
            "columns": len(df.columns),
        }
    
    return saved_files


def generate_summaries_manifest(
    saved_files: dict[str, dict],
    original_row_count: int,
) -> dict[str, Any]:
    """生成预聚合清单。"""
    total_summary_rows = sum(
        item.get("rows", 0)
        for item in saved_files.values()
    )
    
    bandwidth_reduction = (1.0 -
                           (total_summary_rows /
                            max(original_row_count, 1)))
    
    return {
        "generatedAt": pd.Timestamp.now().isoformat(),
        "originalRowCount": original_row_count,
        "totalSummaryRows": total_summary_rows,
        "bandwidthReduction": f"{bandwidth_reduction * 100:.1f}%",
        "summaries": saved_files,
    }


def precompute_all_summaries(
    parquet_path: str,
    output_dir: str = "04_Processed_data/summaries",
) -> dict[str, Any]:
    """
    主入口：加载数据并预聚合所有统计汇总。
    返回清单信息。
    """
    print(f"📊 开始预聚合：读取 {parquet_path}...")
    df = load_analysis_data(parquet_path)
    original_rows = len(df)
    print(f"   ✓ 加载成功，共 {original_rows} 行")
    
    print("📊 计算国家汇总...")
    country_summary = compute_country_summary(df)
    print(f"   ✓ {len(country_summary)} 个国家")
    
    print("📊 计算年月汇总...")
    year_month_summary = compute_year_month_summary(df)
    print(f"   ✓ {len(year_month_summary)} 个时间节点")
    
    print("📊 计算功率类型汇总...")
    powertrain_summary = compute_powertrain_summary(df)
    print(f"   ✓ {len(powertrain_summary)} 个功率类型")
    
    print("📊 计算车形分类汇总...")
    segment_summary = compute_segment_summary(df)
    print(f"   ✓ {len(segment_summary)} 个车形分类")
    
    print("📊 计算热门品牌（Top 20）...")
    top_makes_summary = compute_top_makes_summary(df, top_n=20)
    print(f"   ✓ {len(top_makes_summary)} 个品牌")
    
    summaries = {
        "country": country_summary,
        "yearMonth": year_month_summary,
        "powertrain": powertrain_summary,
        "segment": segment_summary,
        "topMakes": top_makes_summary,
    }
    
    print(f"\n💾 保存预聚合表到 {output_dir}...")
    saved_files = save_summary_tables(summaries, output_dir)
    
    manifest = generate_summaries_manifest(
        saved_files=saved_files,
        original_row_count=original_rows,
    )
    
    manifest_path = Path(output_dir) / "summaries_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    
    print(f"   ✓ 清单已保存: {manifest_path}")
    print(f"\n📈 汇总：原始 {original_rows} 行 → 汇总 {manifest['totalSummaryRows']} 行")
    print(f"📈 带宽降低：{manifest['bandwidthReduction']}")
    
    return manifest


if __name__ == "__main__":
    project_root = get_project_root()
    parquet_path = str(project_root / "04_Processed_data" / "fullParquetV1.parquet")
    output_dir = str(project_root / "04_Processed_data" / "summaries")
    
    manifest = precompute_all_summaries(parquet_path, output_dir)
    print(f"\n✅ 预聚合完成！清单中有 {len(manifest['summaries'])} 个汇总表")
