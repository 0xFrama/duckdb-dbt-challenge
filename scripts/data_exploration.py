import polars as pl

df = pl.read_parquet("/home/xf4/projects/duckdb-dbt-challenge/data/raw/yellow_tripdata_2024-01.parquet")
#null_counts = {col: df[col] < 0 for col in df.columns}

# Displaying results
#for col, count in null_counts.items():
#    print(f"Column: {col}: {count}")

print(df.columns)
print(df.select('PULocationID').unique())

#lzdf=pl.scan_parquet("/home/xf4/projects/duckdb-dbt-challenge/data/raw/yellow_tripdata_2024-01.parquet")
#row_num = lzdf.select(pl.len()).collect().item()
#print(row_num) # 2964624
