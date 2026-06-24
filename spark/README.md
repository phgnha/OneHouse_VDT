# Apache Spark — Streaming Ingestion

## Tổng quan

Thư mục này chứa các **Spark Structured Streaming job** chịu trách nhiệm ingest dữ liệu real-time từ Redpanda vào tầng **Bronze** của Iceberg Lakehouse. Có hai job riêng biệt phục vụ hai luồng dữ liệu: **Telemetry** (append-only) và **CDC Billing** (upsert via MERGE INTO).

## Cấu trúc thư mục

```
spark/
└── jobs/
    ├── stream_to_iceberg.py      # Job #1 — Telemetry streaming pipeline
    └── cdc_billing_stream.py     # Job #2 — CDC billing pipeline
```

## Chi tiết các job

### Job #1: `stream_to_iceberg.py` — Telemetry Pipeline

Đọc streaming data từ Redpanda, parse JSON, loại bỏ bản ghi trùng lặp và ghi vào Iceberg.

| Tham số | Giá trị |
|---------|---------|
| Source topic | `telecom.raw_logs` |
| Target table | `viettel.bronze.telecom_events` |
| Write mode | **Append-only** |
| Deduplication | Watermark trên `event_ts` |
| Trigger | Micro-batch mỗi **20 giây** |

**Luồng xử lý**:
```
Redpanda → Read Stream → Parse JSON → Watermark Dedup → Append to Iceberg
```

### Job #2: `cdc_billing_stream.py` — CDC Pipeline

Đọc CDC events từ Debezium (qua Redpanda), thực hiện upsert vào Iceberg sử dụng `MERGE INTO`.

| Tham số | Giá trị |
|---------|---------|
| Source topics | `billing.public.billing_plans`, `billing.public.subscribers` |
| Target tables | `viettel.bronze.billing_plans`, `viettel.bronze.subscribers` |
| Write mode | **Upsert** (MERGE INTO) |
| Table format | Iceberg Format **V2** (row-level deletes) |
| Soft-delete | Xử lý qua flag `__deleted` |
| Trigger | Micro-batch mỗi **30 giây** |

**Luồng xử lý**:
```
Redpanda (Debezium) → Read Stream → foreachBatch → SQL MERGE INTO → Iceberg V2
```

> **Lưu ý**: Iceberg Format V2 là bắt buộc để hỗ trợ row-level MERGE INTO. Debezium đã flatten CDC envelope thành plain JSON (nhờ SMT `ExtractNewRecordState`), nên Spark đọc trực tiếp các field mà không cần unwrap.

## Cấu hình chung

| Tham số | Giá trị |
|---------|---------|
| Catalog name | `viettel` |
| Catalog type | Hive Metastore (Iceberg) |
| Checkpoint path | `s3a://warehouse/checkpoints/` |
| Docker image | Build từ `docker/spark/Dockerfile` |

## Tích hợp với các thành phần khác

```
┌────────────┐     ┌───────────┐     ┌─────────────────┐     ┌──────────┐
│  Redpanda  │────▶│   Spark   │────▶│  Iceberg Bronze │────▶│  Trino   │
│  (Topics)  │     │ Streaming │     │    (MinIO/S3)   │     │  (Query) │
└────────────┘     └───────────┘     └─────────────────┘     └──────────┘
```

- **Redpanda**: Nguồn streaming data (Kafka-compatible protocol).
- **Iceberg + MinIO**: Spark ghi dữ liệu dạng Iceberg table trên S3-compatible storage.
- **Trino / dbt**: Downstream — đọc dữ liệu Bronze để transform lên Silver/Gold.
- **Checkpoint**: Lưu trên `s3a://warehouse/checkpoints/` để đảm bảo exactly-once semantics khi restart.
