import pandas as pd
import time
from pathlib import Path

def convert_jato_to_parquet():
    input_file = "JATO-2026.1.xlsx"
    output_dir = Path("04_Processed_data")
    output_file = output_dir / "jato_full_archive.parquet"

    output_dir.mkdir(exist_ok=True)

    print(f"🚀 开始转换: {input_file}")
    start_time = time.time()

    # 1) 读取 Excel
    df = pd.read_excel(input_file, sheet_name="Data Export", engine="calamine")
    t1 = time.time()
    print(f"📥 读取 Excel 耗时: {t1 - start_time:.2f} 秒, shape={df.shape}")

    # 2) 基础清洗
    df.columns = [str(c).strip() for c in df.columns]

    # 3) 统一 object 列类型（关键修复）
    print("🧹 统一 object 列为 string 类型...")
    object_cols = df.select_dtypes(include=["object"]).columns
    for col in object_cols:
        # 使用 pandas StringDtype，保留缺失值为 <NA>，避免 'nan' 字面字符串
        df[col] = df[col].astype("string")

    # 4) 写入 Parquet
    print("💾 正在写入 Parquet...")
    df.to_parquet(output_file, engine="pyarrow", compression="snappy", index=False)

    t2 = time.time()
    print(f"✅ 转换完成: {output_file}")
    print(f"⏱️ 总耗时: {t2 - start_time:.2f} 秒")

if __name__ == "__main__":
    convert_jato_to_parquet()