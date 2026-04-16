import pandas as pd
from pathlib import Path

path = next(Path("04_Processed_data").rglob("*.parquet"), None)
if path:
    df = pd.read_parquet(path)
    print("Columns:", list(df.columns))
else:
    print("No parquet found")

