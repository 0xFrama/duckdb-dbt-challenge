import polars as pl

df = pl.read_parquet("/home/xf4/projects/duckdb-dbt-challenge/data/raw/yellow_tripdata_2024-01.parquet")
with pl.Config(tbl_cols=-1):
    print(df.describe())