# Quick Start Guide

## Prerequisites

1. **Virtual Environment**

    Ensure you have installed and activated the virtual environment as described in the original repository's README.md.

    ```shell
    $ uv venv
    $ source .venv/bin/activate
    ```
2. **Dependecies**

    Install the required dependecies inside the virtual environment:

    ```shell
    $ uv sync
    $ wget https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip && unzip duckdb_cli-linux-amd64.zip && mv duckdb .venv/bin/
    $ uv run dbt deps 
    ```
    
3. **Dataset**
    
    Ensure the dataset has been downloaded inside the folder `data/raw/`.
    ```shell
    $ wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
    
## How to run the project

You can now run the project:
    
```shell
$ uv run dbt run
```
**Result**

This command will create:
- 5 tables (1 staging table and 4 marts tables)
- 1 view (staging)

These objects will be created in your DuckDB database file (`data/db/yellow_tripdata.duckdb`).

1. **Querying the data**

    To explore the newly created tables or views, you can run SQL queries directly against the DuckDB database.

    For example:
    ```shell
    $ uv run duckdb data/db/yellow_tripdata.duckdb "SELECT * FROM staging_yellow_tripdata LIMIT 10"
    ```

    Adjust the query as needed to inspect other tables or apply filters.
    
    After running the project - as shown above - these are the available tables (T)/ views (V) in the duckdb database:
    - (V) `stg_yellow_tripdata__cleaned_sources`
    - (T) `trips_by_time_of_day`
    - (T) `top_5_pickup_zones`
    - (T) `rate_performance`
    - (T) `distance_analysis`

2. **Running tests**

    To validate your models, run:
    ```shell
    $ uv run dbt test 
    ```
    **Result**

    This will execute all tests defined in `staging/schema.yaml`.