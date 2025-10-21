"""
A minimal example of loading data into a relational warehouse using SQLAlchemy. This
is a template: in production you'd use bulk COPY / loaders provided by your warehouse.
"""

from sqlalchemy import create_engine
import pandas as pd


def load_dataframe_to_table(
    df: pd.DataFrame, table_name: str, connection_string: str, if_exists: str = "append"
):
    engine = create_engine(connection_string)
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--conn", required=True, help="SQLAlchemy connection string")
    args = parser.parse_args()

    df = pd.read_parquet(args.input)
    load_dataframe_to_table(df, args.table, args.conn)
    print("Loaded dataframe to warehouse")
