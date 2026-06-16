# OneHouse Streaming Lakehouse

OneHouse is a local Docker lab for near real-time telecom monitoring. It follows
the requested flow:

`Simulator -> Kafka/Redpanda -> Spark Structured Streaming -> MinIO + Iceberg REST Catalog -> Trino -> dbt -> Airflow -> Superset`

The repo starts from raw BTS/telecom events, lands them as append-only Iceberg
Bronze data, enriches and validates them with dbt, then serves Gold KPI tables
for a heatmap dashboard.

## What is included

- Kafka-compatible ingestion with Redpanda.
- Continuous telecom log simulator for CDR and mobile data events.
- Spark Structured Streaming micro-batch job with de-duplication and Iceberg writes.
- MinIO object storage plus Iceberg REST catalog.
- Trino Iceberg catalog for OLAP SQL access.
- dbt project with Staging, Silver, Gold incremental KPI models, docs, lineage, and tests.
- Airflow DAG scheduled every 15 minutes with dbt quality gates.
- Superset service with a seeded Trino connection and dashboard bootstrap script.

## Ports

| Service | URL |
| --- | --- |
| Trino | http://localhost:8080 |
| Airflow | http://localhost:8082 |
| Redpanda Console | http://localhost:8084 |
| Superset | http://localhost:8088 |
| MinIO API | http://localhost:9000 |
| MinIO Console | http://localhost:9001 |
| Iceberg REST Catalog | http://localhost:8181 |

Default UI credentials are `admin/admin` for Airflow, Superset, and MinIO.

## Quick start on Windows PowerShell

```powershell
Copy-Item .env.example .env
.\scripts\onehouse.ps1 start
.\scripts\onehouse.ps1 dbt
.\scripts\onehouse.ps1 orchestration
.\scripts\onehouse.ps1 visualization
.\scripts\onehouse.ps1 smoke
```

The first run pulls large Spark, Trino, Airflow, and Superset images. Expect it
to take time and several GB of disk space.

## Manual commands

Start the core streaming lakehouse:

```powershell
docker compose up -d --build redpanda redpanda-console minio minio-init iceberg-rest trino simulator spark-streaming
```

Run dbt transformations and tests:

```powershell
docker compose --profile tools run --rm dbt seed --full-refresh
docker compose --profile tools run --rm dbt run
docker compose --profile tools run --rm dbt test
docker compose --profile tools run --rm dbt docs generate
```

Start Airflow and Superset:

```powershell
docker compose --profile orchestration up -d --build airflow-init airflow-webserver airflow-scheduler
docker compose --profile visualization up -d --build superset
```

Query Gold data from Trino:

```powershell
docker exec -it onehouse-trino trino --catalog iceberg --schema gold
```

Example SQL:

```sql
select *
from iceberg.gold.gold_cell_heatmap
order by cell_load_score desc
limit 20;
```

## Data model

- `iceberg.bronze.telecom_events`: append-only parsed Kafka events written by Spark.
- `iceberg.reference.bts_metadata`: dbt seed with BTS metadata and coordinates.
- `iceberg.staging.stg_telecom_events`: normalized typed source view.
- `iceberg.silver.silver_cell_events`: de-duplicated and BTS-enriched events.
- `iceberg.gold.gold_network_kpis`: 15-minute incremental KPI aggregate.
- `iceberg.gold.gold_cell_heatmap`: latest per-cell serving view for Superset.

## Quality gates

The dbt project validates:

- Gold tables are not empty before reporting.
- BTS coordinates stay within Vietnam bounding ranges.
- drop call rate, QoE issue rate, and load score stay between 0 and 100.
- cell IDs in Silver map to reference BTS metadata.
- dashboard severity values are constrained to `NORMAL`, `WARNING`, `CRITICAL`.

Airflow stops the pipeline when `dbt test --select tag:gold` fails.

## Cleanup

```powershell
docker compose --profile tools --profile orchestration --profile visualization down
docker volume rm onehouse_minio_data onehouse_spark_checkpoints onehouse_airflow_postgres_data onehouse_airflow_logs onehouse_superset_home
```

## Reference docs used for stack wiring

- Apache Iceberg Spark quickstart: https://apache.github.io/iceberg/spark-quickstart/
- Trino Iceberg REST catalog configuration: https://trino.io/docs/current/object-storage/metastores.html#rest-catalog
- Trino S3/MinIO filesystem configuration: https://trino.io/docs/current/object-storage/file-system-s3.html
- dbt-trino package: https://pypi.org/project/dbt-trino/
