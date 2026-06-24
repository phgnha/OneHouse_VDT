# OneHouse Streaming Lakehouse — Viettel Digital Twin (VDT)

Nền tảng Streaming Lakehouse giám sát trải nghiệm khách hàng và lưu lượng mạng viễn thông Viettel theo thời gian thực (Near Real-time), kết hợp luồng **Telemetry** (dữ liệu hiệu năng mạng) và **CDC** (dữ liệu thay đổi từ hệ thống thanh toán) trên cùng một Data Platform.

## Tổng quan kiến trúc

```
┌────────────────────────────────────────────────────────────────────────────┐
│                       DUAL INGESTION PIPELINE                            │
│                                                                          │
│  ┌───────────────┐     ┌──────────────┐     ┌────────────────────────┐   │
│  │  Telecom      │     │              │     │  Spark Standalone      │   │
│  │  Simulator    │────▶│   Redpanda   │────▶│  ┌──────────────────┐  │   │
│  │  (Kafka JSON) │     │   (Kafka)    │     │  │ stream_to_       │  │   │
│  └───────────────┘     │              │     │  │ iceberg.py       │  │   │
│                        │              │     │  │ (Append)         │  │   │
│  ┌───────────────┐     │              │     │  └──────────────────┘  │   │
│  │  PostgreSQL   │     │              │     │  ┌──────────────────┐  │   │
│  │  Billing DB   │     │              │     │  │ cdc_billing_     │  │   │
│  │      │        │     │              │     │  │ stream.py        │  │   │
│  │  Debezium ────│────▶│              │────▶│  │ (MERGE INTO)     │  │   │
│  │  (CDC WAL)    │     │              │     │  └──────────────────┘  │   │
│  └───────────────┘     └──────────────┘     └────────────────────────┘   │
│                                                        │                 │
│                              ┌──────────────────────────┘                │
│                              ▼                                           │
│                   ┌─────────────────────┐                                │
│                   │  MinIO + Iceberg    │                                │
│                   │  (viettel catalog)  │                                │
│                   └─────────────────────┘                                │
│                              │                                           │
│            ┌─────────────────┼─────────────────────┐                    │
│            ▼                 ▼                     ▼                    │
│     ┌────────────┐   ┌─────────────┐   ┌──────────────────┐            │
│     │   Trino    │   │     dbt     │   │     Airflow      │            │
│     │  (Query)   │◀──│  (ELT)     │◀──│  (Orchestrator)  │            │
│     └────────────┘   └─────────────┘   └──────────────────┘            │
│            │                                                            │
│            ▼                                                            │
│     ┌─────────────┐                                                     │
│     │  Superset   │                                                     │
│     │ (Dashboard) │                                                     │
│     └─────────────┘                                                     │
└────────────────────────────────────────────────────────────────────────────┘
```

## Thành phần hệ thống

| Tầng | Service | Container | Vai trò |
|------|---------|-----------|---------|
| **Ingestion** | Redpanda | `onehouse-redpanda` | Message broker tương thích Kafka, tiếp nhận cả telecom events và CDC events |
| **Ingestion** | Telecom Simulator | `onehouse-simulator` | Sinh liên tục CDR và log Internet di động 4G/5G (~20 events/giây) |
| **CDC** | PostgreSQL Billing | `onehouse-postgres-billing` | CSDL OLTP chứa bảng `billing_plans` (gói cước) và `subscribers` (thuê bao) |
| **CDC** | Billing Generator | `onehouse-billing-generator` | Sinh liên tục các thay đổi OLTP (INSERT/UPDATE) trên bảng billing |
| **CDC** | Debezium | `onehouse-debezium` | Kafka Connect đọc WAL logical replication, chuyển đổi thành CDC events |
| **CDC** | Debezium Init | `onehouse-debezium-init` | Tự động đăng ký connector khi khởi động |
| **Processing** | Spark Master | `onehouse-spark-master` | Quản lý Spark Standalone Cluster |
| **Processing** | Spark Worker | `onehouse-spark-worker` | Thực thi các task tính toán từ Master |
| **Processing** | Spark Streaming | `onehouse-spark-streaming` | Job #1: Đọc telecom events → Append vào Iceberg bronze |
| **Processing** | Spark CDC | `onehouse-spark-cdc` | Job #2: Đọc CDC events → MERGE INTO (Upsert) Iceberg bronze |
| **Storage** | MinIO | `onehouse-minio` | Object Storage S3-compatible lưu trữ Iceberg data files (Parquet) |
| **Catalog** | Iceberg REST | `onehouse-iceberg-rest` | Metadata catalog chia sẻ giữa Spark và Trino |
| **Compute** | Trino | `onehouse-trino` | MPP SQL Engine cho phân tích OLAP |
| **Transform** | dbt | `onehouse-dbt` | ELT framework: staging → silver → gold, tests, docs, lineage |
| **Orchestrate** | Airflow | `onehouse-airflow-*` | Lập lịch chạy dbt mỗi 15 phút với quality gates |
| **Visualize** | Superset | `onehouse-superset` | Dashboard bản đồ nhiệt giám sát mạng viễn thông |

## Ports

| Service | URL |
|---------|-----|
| Trino | http://localhost:8080 |
| Airflow | http://localhost:8082 |
| Debezium Connect | http://localhost:8083 |
| Redpanda Console | http://localhost:8084 |
| Spark Master UI | http://localhost:8085 |
| Spark Worker UI | http://localhost:8086 |
| Superset | http://localhost:8088 |
| MinIO API | http://localhost:9000 |
| MinIO Console | http://localhost:9001 |
| Iceberg REST Catalog | http://localhost:8181 |
| PostgreSQL Billing | localhost:5432 |

Tài khoản mặc định `admin/admin` cho Airflow, Superset, và MinIO.

## Luồng dữ liệu

### Luồng 1: Telemetry (Append-only)

```
Telecom Simulator → Redpanda (topic: telecom.raw_logs) → Spark stream_to_iceberg.py
    → viettel.bronze.telecom_events (Append, Iceberg Format V2)
    → dbt: stg_telecom_events → silver_cell_events → gold_network_kpis → gold_cell_heatmap
```

### Luồng 2: CDC Billing (Upsert / MERGE INTO)

```
PostgreSQL (WAL) → Debezium → Redpanda (topics: billing.public.*)
    → Spark cdc_billing_stream.py (foreachBatch + MERGE INTO)
    → viettel.bronze.billing_plans, viettel.bronze.subscribers (Upsert, Iceberg Format V2)
    → dbt: stg_billing_plans, stg_subscribers → gold_customer_360 → gold_plan_revenue_impact
```

### Luồng 3: Cross-domain (Customer 360)

```
gold_cell_heatmap (Telemetry) + stg_subscribers + stg_billing_plans (CDC)
    → JOIN trên home_cell_id / plan_id
    → gold_customer_360: customer_impact_score, churn_risk_tier
    → gold_plan_revenue_impact: revenue_at_risk_pct, plan_health_status
```

## Data Model (Medallion Architecture)

### Bronze (Dữ liệu thô)
| Bảng | Nguồn | Ghi chú |
|------|-------|---------|
| `viettel.bronze.telecom_events` | Kafka Append | Phân vùng theo `days(event_ts), network_type` |
| `viettel.bronze.billing_plans` | CDC MERGE INTO | Iceberg Format V2, merge-on-read |
| `viettel.bronze.subscribers` | CDC MERGE INTO | Phân vùng theo `status` |

### Reference (Dữ liệu tham chiếu)
| Bảng | Nguồn | Ghi chú |
|------|-------|---------|
| `viettel.reference.bts_metadata` | dbt seed (CSV) | 8 trạm BTS: Hà Nội, HCM, Đà Nẵng, Hải Phòng, Cần Thơ, Quảng Ninh |

### Staging (Chuẩn hóa)
| View | Nguồn | Ghi chú |
|------|-------|---------|
| `viettel.staging.stg_telecom_events` | `bronze.telecom_events` | Ép kiểu, chuẩn hóa, tính `total_mb`, `is_call_failure`, `has_qoe_issue` |
| `viettel.staging.stg_billing_plans` | `bronze.billing_plans` | Lọc soft-delete, chuẩn hóa `plan_type` |
| `viettel.staging.stg_subscribers` | `bronze.subscribers` | Lọc soft-delete, uppercase `home_cell_id` cho JOIN |

### Silver (Làm sạch & Làm giàu)
| Bảng | Nguồn | Ghi chú |
|------|-------|---------|
| `viettel.silver.silver_cell_events` | `stg_telecom_events` + `bts_metadata` | Khử trùng lặp bằng `event_id`, JOIN metadata BTS (tỉnh, huyện, toạ độ) |

### Gold (KPI & Analytics)
| Bảng | Nguồn | Ghi chú |
|------|-------|---------|
| `viettel.gold.gold_network_kpis` | `silver_cell_events` | Incremental MERGE 15 phút, tổng hợp theo `cell_id + network_type` |
| `viettel.gold.gold_cell_heatmap` | `gold_network_kpis` | View: latest-per-cell, severity = NORMAL/WARNING/CRITICAL |
| `viettel.gold.gold_customer_360` | `stg_subscribers` + `stg_billing_plans` + `gold_cell_heatmap` | Cross-domain JOIN, `customer_impact_score`, `churn_risk_tier` |
| `viettel.gold.gold_plan_revenue_impact` | `gold_customer_360` | Tổng hợp theo gói cước, `revenue_at_risk_pct`, `plan_health_status` |

## KPI Definitions

| KPI | Công thức | Đơn vị |
|-----|-----------|--------|
| Drop Call Rate | `dropped_calls / total_calls × 100` | % |
| QoE Issue Rate | `qoe_issue_events / total_events × 100` | % |
| Data Traffic | `sum(download_mb + upload_mb) / 1024` | GB |
| Cell Load Score | Traffic pressure + call failures × 2 + QoE issues × 1.5 | 0–100 |
| Customer Impact Score | `(monthly_fee / 100K) × (cell_load / 100) × severity_weight × 100` | 0–∞ |
| Revenue at Risk | `total_revenue_at_risk / total_monthly_revenue × 100` | % |

## Quality Gates (dbt Tests)

- Bảng Gold không được rỗng (`not_empty`)
- Toạ độ BTS nằm trong biên giới Việt Nam (lat: 8.0–23.5, lng: 102.0–110.5)
- `drop_call_rate_pct`, `qoe_issue_rate_pct`, `cell_load_score` ∈ [0, 100]
- `cell_id` trong Silver phải tồn tại trong `bts_metadata` (referential integrity)
- `severity` ∈ {`NORMAL`, `WARNING`, `CRITICAL`}
- `churn_risk_tier` ∈ {`HIGH_RISK`, `MEDIUM_RISK`, `LOW_RISK`}
- `plan_health_status` ∈ {`CRITICAL`, `WARNING`, `HEALTHY`}
- `subscriber_id` và `plan_id` unique trong các bảng CDC staging
- `monthly_fee` ∈ [0, 10,000,000] VNĐ

Airflow ngắt pipeline nếu `dbt test --select tag:gold` thất bại.

## Quick Start

```powershell
# 1. Chuẩn bị .env
Copy-Item .env.example .env

# 2. Khởi động core stack (Telemetry pipeline)
.\scripts\onehouse.ps1 start

# 3. Khởi động CDC pipeline
.\scripts\onehouse.ps1 cdc

# 4. Chạy dbt transformations
.\scripts\onehouse.ps1 dbt

# 5. Khởi động Airflow (tự động chạy dbt mỗi 15 phút)
.\scripts\onehouse.ps1 orchestration

# 6. Khởi động Superset dashboard
.\scripts\onehouse.ps1 visualization

# 7. Kiểm tra smoke test
.\scripts\onehouse.ps1 smoke
```

## Manual Commands

### Core Streaming Lakehouse

```powershell
docker compose up -d --build redpanda redpanda-console minio minio-init iceberg-rest trino simulator spark-master spark-worker spark-streaming
```

### CDC Pipeline

```powershell
docker compose up -d --build postgres-billing billing-generator debezium debezium-init spark-cdc
```

### dbt

```powershell
docker compose --profile tools run --rm dbt seed --full-refresh
docker compose --profile tools run --rm dbt run
docker compose --profile tools run --rm dbt test
docker compose --profile tools run --rm dbt docs generate
```

### Truy vấn dữ liệu

```powershell
# Truy vấn gold Telemetry
docker exec -it onehouse-trino trino --catalog viettel --schema gold

# Truy vấn Customer 360
docker exec -i onehouse-trino trino --catalog viettel --schema gold --execute "
  SELECT subscriber_id, full_name, plan_name, monthly_fee,
         home_cell_severity, churn_risk_tier, customer_impact_score
  FROM gold_customer_360
  WHERE churn_risk_tier = 'HIGH_RISK'
  ORDER BY customer_impact_score DESC
  LIMIT 10
"
```

## Cấu trúc thư mục

```
OneHouse_VDT/
├── airflow/dags/                  # Airflow DAG (dbt pipeline 15-min)
├── configs/trino/                 # Trino catalog & config
├── dbt_onehouse/                  # dbt project
│   ├── models/
│   │   ├── staging/               # Sources + staging views
│   │   ├── silver/                # De-dup & enrichment
│   │   └── gold/                  # KPI aggregates & Customer 360
│   ├── seeds/                     # bts_metadata.csv
│   ├── tests/generic/             # Custom test macros
│   └── macros/                    # generate_schema_name override
├── dbt_profiles/                  # Trino connection profile
├── debezium/                      # Debezium connector config
├── docker/
│   ├── spark/                     # Custom Spark 3.5.3 image + entrypoint
│   └── dbt/                       # dbt Docker image
├── docs/                          # Architecture & data contract docs
├── postgresql/                    # Billing DB init SQL + WAL publication
├── scripts/                       # PowerShell helpers & smoke tests
├── simulator/                     # Telecom + Billing data generators
├── spark/jobs/                    # Spark streaming jobs
│   ├── stream_to_iceberg.py       # Telemetry: Kafka → Iceberg (Append)
│   └── cdc_billing_stream.py      # CDC: Kafka → Iceberg (MERGE INTO)
├── superset/                      # Superset bootstrap script
├── docker-compose.yml             # 16 services orchestration
└── .env.example                   # Environment template
```

## Cleanup

```powershell
docker compose --profile tools --profile orchestration --profile visualization down
docker volume rm onehouse_minio_data onehouse_postgres_billing_data onehouse_airflow_postgres_data onehouse_airflow_logs onehouse_superset_home
```

## Tech Stack

| Công nghệ | Phiên bản | Vai trò |
|-----------|-----------|---------|
| Redpanda | v24.3.7 | Kafka-compatible message broker |
| Apache Spark | 3.5.3 | Distributed stream processing (Master-Worker) |
| Apache Iceberg | 1.7.1 | Open table format (Format V2, MERGE INTO) |
| MinIO | RELEASE.2024-11-07 | S3-compatible object storage |
| Trino | 457 | Distributed SQL query engine |
| dbt-trino | 1.8.x | ELT transformation framework |
| Apache Airflow | 2.10.4 | Workflow orchestration |
| Apache Superset | 4.1.1 | BI dashboard |
| PostgreSQL | 16-alpine | OLTP billing database (CDC source) |
| Debezium | 2.7 | Change Data Capture (Kafka Connect) |

## Reference Docs

- Apache Iceberg Spark quickstart: https://apache.github.io/iceberg/spark-quickstart/
- Trino Iceberg REST catalog: https://trino.io/docs/current/object-storage/metastores.html#rest-catalog
- Trino S3/MinIO filesystem: https://trino.io/docs/current/object-storage/file-system-s3.html
- Debezium PostgreSQL connector: https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- dbt-trino package: https://pypi.org/project/dbt-trino/
