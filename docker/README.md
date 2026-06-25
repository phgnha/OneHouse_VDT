# Docker — Custom Images

## Tổng quan

Thư mục này chứa các **Dockerfile tùy chỉnh** cho các service trong hệ thống. Mỗi image được build riêng để bổ sung dependencies, JARs, hoặc driver mà image gốc không có sẵn — giúp container khởi động nhanh hơn và không phụ thuộc vào network khi runtime.

## Cấu trúc thư mục

```
docker/
├── spark/
│   ├── Dockerfile       # Custom Spark 3.5.3 image + pre-baked JARs
│   └── entrypoint.sh    # Multi-mode entrypoint (master/worker/submit)
├── dbt/
│   └── Dockerfile       # Python image + dbt-trino
├── airflow/
│   └── Dockerfile       # Extends apache/airflow:2.10.4
└── superset/
    └── Dockerfile       # Extends apache/superset:4.1.1 + trino driver
```

## Chi tiết từng image

### 1. `spark/Dockerfile` — Spark 3.5.3 Custom Image

| Thuộc tính | Giá trị |
|------------|---------|
| Base image | `python:3.12-slim-bookworm` |
| Java | OpenJDK **17** |
| Spark version | **3.5.3** |

**Pre-baked JARs** (6 JARs, không cần `--packages` khi runtime):

| JAR | Phiên bản | Mục đích |
|-----|-----------|----------|
| `iceberg-spark-runtime` | 1.7.1 | Iceberg table format support |
| `iceberg-aws-bundle` | 1.7.1 | S3/MinIO integration |
| `spark-sql-kafka` | 3.5.3 | Kafka/Redpanda connector |
| `kafka-clients` | — | Kafka protocol client |
| `commons-pool2` | — | Connection pooling |

> **Tại sao pre-bake JARs?** Việc download JARs qua `--packages` tại runtime gây chậm khởi động và dễ fail nếu mất mạng. Bake sẵn vào image đảm bảo **offline-ready** và **reproducible**.

### 2. `spark/entrypoint.sh` — Multi-mode Entrypoint

Hỗ trợ 3 chế độ chạy qua biến môi trường `$SPARK_MODE`:

| Mode | Mô tả |
|------|--------|
| `master` | Khởi động Spark Master |
| `worker` | Khởi động Spark Worker, kết nối tới Master |
| `submit` | Submit job script (chỉ định qua `$SPARK_JOB_SCRIPT`) |

### 3. `dbt/Dockerfile` — dbt-trino

| Thuộc tính | Giá trị |
|------------|---------|
| Base image | Python slim |
| Package | `dbt-trino` |

Cài đặt `dbt-trino` adapter để dbt có thể kết nối và thực thi SQL trên Trino.

### 4. `airflow/Dockerfile` — Apache Airflow

| Thuộc tính | Giá trị |
|------------|---------|
| Base image | `apache/airflow:2.10.4` |

Extends image chính thức, bổ sung dependencies cần thiết cho các DAG của project.

### 5. `superset/Dockerfile` — Apache Superset

| Thuộc tính | Giá trị |
|------------|---------|
| Base image | `apache/superset:4.1.1` |
| Driver bổ sung | `trino` driver |

Cài thêm Trino driver (`trino[sqlalchemy]`) để Superset có thể query trực tiếp dữ liệu từ Trino.

## Tích hợp với các thành phần khác

Tất cả các image được tham chiếu trong `docker-compose.yml` ở thư mục gốc:

```yaml
# Ví dụ trong docker-compose.yml
services:
  spark-master:
    build: ./docker/spark
  dbt:
    build: ./docker/dbt
  airflow:
    build: ./docker/airflow
  superset:
    build: ./docker/superset
```

## Build thủ công (nếu cần)

```bash
# Build từng image riêng lẻ
docker build -t onehouse-spark ./docker/spark
docker build -t onehouse-dbt ./docker/dbt
docker build -t onehouse-airflow ./docker/airflow
docker build -t onehouse-superset ./docker/superset
```
