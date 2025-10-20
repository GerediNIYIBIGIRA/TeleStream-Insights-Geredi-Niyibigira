"""
Write parquet files to a local lake path (a directory). This is a stand-in for delta
or an object store writer.
"""
from pathlib import Path
import pandas as pd


def write_parquet_to_lake(df: pd.DataFrame, lake_path: str, filename: str = "data.parquet") -> str:
    p = Path(lake_path)
    p.mkdir(parents=True, exist_ok=True)
    out = p / filename
    df.to_parquet(out, index=False)
    return str(out)