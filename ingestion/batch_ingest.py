"""
A simple batch ingestion script that reads CSV files from a directory, does basic validation,
and writes a combined parquet file to a staging area. This is intended as a scaffold and
example of how batch ingestion can be implemented.
"""

from pathlib import Path
import argparse
import pandas as pd


def ingest_csvs(input_dir: str, output_path: str):
    input_path = Path(input_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"Input directory {input_dir} does not exist")

    csv_files = list(input_path.glob("*.csv"))
    if not csv_files:
        print("No CSV files found to ingest")
        return

    dfs = []
    for f in csv_files:
        print(f"Reading {f}")
        df = pd.read_csv(f)
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    # Basic validation example
    if combined.empty:
        print("Combined dataframe is empty after ingestion")
    else:
        combined.to_parquet(output_path, index=False)
        print(f"Wrote ingested data to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Batch CSV ingestion to parquet staging"
    )
    parser.add_argument(
        "--input-dir", required=True, help="Directory containing CSV files"
    )
    parser.add_argument("--output-path", required=True, help="Output parquet path")
    args = parser.parse_args()
    ingest_csvs(args.input_dir, args.output_path)
