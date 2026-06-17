param(
    [Parameter(Position = 0)]
    [ValidateSet("start", "dbt", "orchestration", "visualization", "smoke", "logs", "stop")]
    [string]$Action = "start"
)

$ErrorActionPreference = "Stop"

function Ensure-EnvFile {
    if (-not (Test-Path ".env") -and (Test-Path ".env.example")) {
        Copy-Item ".env.example" ".env"
    }
}

switch ($Action) {
    "start" {
        Ensure-EnvFile
        docker compose up -d --build redpanda redpanda-console minio minio-init iceberg-rest trino simulator spark-streaming
    }
    "dbt" {
        docker compose --profile tools run --rm dbt seed --full-refresh
        docker compose --profile tools run --rm dbt run
        docker compose --profile tools run --rm dbt test
        docker compose --profile tools run --rm dbt docs generate
    }
    "orchestration" {
        docker compose --profile orchestration up -d --build airflow-init airflow-webserver airflow-scheduler
    }
    "visualization" {
        docker compose --profile visualization up -d --build superset
    }
    "smoke" {
        Get-Content -Raw "scripts/trino_smoke.sql" | docker exec -i onehouse-trino trino --catalog iceberg --schema gold
    }
    "logs" {
        docker compose logs -f simulator spark-streaming trino
    }
    "stop" {
        docker compose --profile tools --profile orchestration --profile visualization down
    }
}
