### Repo qick start

Once installed the required virtualenv - as shown in the orginal repo's README.md file - you can run the following steps: 

1. To run the project, type:
```shell
$ uv run dbt run
```
This will create 5 tables (1 staging, 4 marts) and 1 view (staging), which can be queried as, for example:
```shell
$ uv run duckdb data/db/yellow_tripdata.duckdb "select  * from staging_yellow_tripdata limit 10"
```
3. To run tests, type: 
```shell
$ uv run dbt test
```
This command is going to run all tests defined in the```staging/schema.yaml``` file.

---


