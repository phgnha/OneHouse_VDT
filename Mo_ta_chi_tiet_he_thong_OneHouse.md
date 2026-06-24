# TÀI LIỆU KIẾN TRÚC & MÔ TẢ CHI TIẾT HỆ THỐNG ONEHOUSE
## NỀN TẢNG STREAMING LAKEHOUSE GIÁM SÁT TRẢI NGHIỆM KHÁCH HÀNG VÀ LƯU LƯỢNG MẠNG VIỄN THÔNG REAL-TIME

---

## 1. TỔNG QUAN DỰ ÁN (PROJECT OVERVIEW)

* **Tên dự án:** OneHouse
* **Mục tiêu:** Xây dựng nền tảng Streaming Lakehouse giám sát trải nghiệm khách hàng và lưu lượng mạng viễn thông Viettel theo thời gian thực (Near Real-time), kết hợp luồng **Telemetry** (hiệu năng mạng) và **CDC** (dữ liệu thanh toán) trên cùng một Data Platform.
* **Ứng viên thực hiện:** Nguyễn Phong Nhã (MSV: B22DCCN573 - Học viện Công nghệ Bưu chính Viễn thông)
* **Đơn vị chủ trì:** Tổng Công ty Giải pháp Doanh nghiệp Viettel (VTS)
* **Mentor hướng dẫn:** Nguyễn Đức Anh (anhnd178@viettel.com.vn)

### Bài toán Nghiệp vụ
Hệ thống giải quyết **hai bài toán nghiệp vụ song song**:

1. **Giám sát hiệu năng mạng viễn thông (Telemetry):** Xử lý dữ liệu khổng lồ từ các trạm phát sóng (BTS) bao gồm log cước (CDR), log truy cập Internet di động 4G/5G và dữ liệu trải nghiệm người dùng (QoE). Giúp đội ngũ kỹ thuật phản ứng nhanh với các "điểm đen" về sóng, tối ưu hóa hạ tầng mạng.

2. **Phân tích tác động doanh thu (CDC Billing):** Thu nhận thay đổi từ hệ thống OLTP thanh toán (gói cước, thuê bao) qua Change Data Capture (CDC), kết hợp với dữ liệu chất lượng mạng để tạo góc nhìn **Customer 360** — phát hiện khách hàng VIP đang bị ảnh hưởng bởi trạm BTS kém chất lượng, ước tính doanh thu rủi ro.

---

### Output
- Hệ thống vận hành: **Dual Ingestion Pipeline** (Telemetry + CDC) từ Ingestion đến Visualization chạy tự động với độ trễ thấp.
- Quản trị dữ liệu: Toàn bộ logic nghiệp vụ được mã hóa (Code-based) trong dbt, có sơ đồ Lineage và tài liệu tự động.
- Chất lượng dữ liệu: 100% dữ liệu lớp Gold được kiểm tra qua các bộ test tự động của dbt trước khi lên báo cáo.
- Sản phẩm bàn giao:
   + 01 Dashboard giám sát bản đồ nhiệt lưu lượng mạng và lỗi trải nghiệm khách hàng.
   + 01 Dashboard Customer 360 phân tích tác động doanh thu theo chất lượng mạng.
   + 01 Repo mã nguồn (dbt project, Airflow DAGs, Spark jobs, CDC pipeline) chuẩn hóa quy trình.

---

## 2. KIẾN TRÚC TỔNG THỂ HỆ THỐNG (ARCHITECTURE OVERVIEW)

Hệ thống được thiết kế theo mô hình **Modern Data Stack (MDS)** trên nền tảng **Lakehouse Architecture**, với **Dual Ingestion Pipeline** xử lý đồng thời hai nguồn dữ liệu khác nhau.

```
┌────────────────────────────────────────────────────────────────────────────┐
│                       DUAL INGESTION PIPELINE                            │
│                                                                          │
│  ┌───────────────┐     ┌──────────────┐     ┌────────────────────────┐   │
│  │  Telecom      │     │              │     │  Spark Standalone      │   │
│  │  Simulator    │────▶│   Redpanda   │────▶│  ┌──────────────────┐  │   │
│  │  (CDR/Data)   │     │   (Kafka)    │     │  │ Job #1:          │  │   │
│  └───────────────┘     │              │     │  │ Telemetry Stream │  │   │
│                        │              │     │  │ (Append)         │  │   │
│  ┌───────────────┐     │              │     │  └──────────────────┘  │   │
│  │  PostgreSQL   │     │              │     │  ┌──────────────────┐  │   │
│  │  Billing DB   │     │              │     │  │ Job #2:          │  │   │
│  │      │        │     │              │     │  │ CDC Stream       │  │   │
│  │  Debezium ────│────▶│              │────▶│  │ (MERGE INTO)     │  │   │
│  │  (CDC WAL)    │     │              │     │  └──────────────────┘  │   │
│  └───────────────┘     └──────────────┘     └────────────────────────┘   │
│                                                        │                 │
│                   ┌────────────────────────────────────┘                 │
│                   ▼                                                      │
│        ┌─────────────────────┐                                          │
│        │  MinIO + Iceberg    │  (viettel catalog, Format V2)            │
│        └─────────────────────┘                                          │
│                   │                                                      │
│     ┌─────────────┼──────────────────────┐                              │
│     ▼             ▼                      ▼                              │
│  ┌────────┐  ┌─────────┐  ┌──────────────────┐                         │
│  │ Trino  │  │  dbt    │  │    Airflow       │                         │
│  │(Query) │◀─│ (ELT)  │◀─│ (Orchestrator)   │                         │
│  └────────┘  └─────────┘  └──────────────────┘                         │
│       │                                                                  │
│       ▼                                                                  │
│  ┌──────────┐                                                           │
│  │ Superset │                                                           │
│  │(Heatmap) │                                                           │
│  └──────────┘                                                           │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. THÀNH PHẦN CHI TIẾT & DATA STACK SPECS

### 3.1. Tầng Thu nạp Dữ liệu (Data Ingestion Layer)

#### 3.1.1. Luồng Telemetry (Streaming Append)
* **Công nghệ:** Redpanda (Kafka-compatible) & Apache Spark Structured Streaming 3.5.3.
* **Cơ chế hoạt động:**
    * **Redpanda:** Đóng vai trò là Distributed Message Broker tương thích Kafka, tiếp nhận luồng log viễn thông từ simulator với tốc độ ~20 events/giây qua topic `telecom.raw_logs`.
    * **Spark Streaming (Job #1 — `stream_to_iceberg.py`):** Thực hiện micro-batching (chu kỳ 20 giây) để liên tục consume dữ liệu từ Redpanda. Spark thực hiện parse JSON, khử trùng lặp (Watermark-based dedup theo `event_id`), và **Append-only** xuống Iceberg bronze table.

#### 3.1.2. Luồng CDC Billing (Change Data Capture → MERGE INTO)
* **Công nghệ:** PostgreSQL 16 (WAL Logical Replication) + Debezium 2.7 (Kafka Connect) + Spark Structured Streaming.
* **Cơ chế hoạt động:**
    * **PostgreSQL Billing:** CSDL OLTP chứa 2 bảng — `billing_plans` (10 gói cước Viettel) và `subscribers` (200+ thuê bao). WAL level được cấu hình `logical` để Debezium theo dõi thay đổi.
    * **Billing Generator:** Script Python chạy liên tục, mô phỏng nghiệp vụ OLTP: 70% UPDATE thuê bao (đổi gói cước, đổi trạng thái, di chuyển BTS), 20% INSERT thuê bao mới, 10% UPDATE gói cước (thay đổi phí, quota).
    * **Debezium:** Kafka Connect connector đọc WAL, sử dụng `ExtractNewRecordState` SMT để flatten CDC envelope thành plain JSON. Cơ chế `delete.handling.mode: rewrite` thêm cờ `__deleted` để xử lý xóa.
    * **Spark CDC (Job #2 — `cdc_billing_stream.py`):** Sử dụng `foreachBatch` + SQL `MERGE INTO` (yêu cầu Iceberg Format V2) để thực hiện **Upsert** — INSERT bản ghi mới, UPDATE bản ghi thay đổi, DELETE bản ghi bị xóa. Khác biệt cơ bản với Job #1 (Append-only).

### 3.2. Tầng Xử lý Phân tán (Distributed Processing Layer)
* **Công nghệ:** Apache Spark 3.5.3 Standalone Cluster (Custom Docker Image).
* **Kiến trúc:**
    * **Spark Master:** Quản lý cluster, cấp phát tài nguyên cho worker. Web UI tại `:8085`.
    * **Spark Worker:** Thực thi các task tính toán. Cấu hình qua `SPARK_WORKER_CORES` (mặc định 2) và `SPARK_WORKER_MEMORY` (mặc định 2GB).
    * **Custom Image:** Base `python:3.12-slim-bookworm`, Java 17, Spark 3.5.3 với **6 JAR pre-baked** (Iceberg 1.7.1 runtime + AWS bundle, Kafka connector, commons-pool2). Loại bỏ hoàn toàn `--packages` runtime download.
    * **Multi-mode Entrypoint:** Biến `SPARK_MODE` chọn chế độ: `master`, `worker`, hoặc `submit`. Script path cấu hình qua `$SPARK_JOB_SCRIPT`.
    * **Checkpoints:** Lưu trên MinIO S3 (`s3a://warehouse/checkpoints/`) thay vì Docker volume local, đảm bảo fault-tolerance khi container restart.

### 3.3. Tầng Lưu trữ & Quản lý Siêu dữ liệu (Storage & Catalog Layer)
* **Công nghệ:** MinIO Object Storage + Apache Iceberg Format V2 + Iceberg REST Catalog.
* **Cơ chế hoạt động:**
    * **MinIO:** Hạ tầng Object Storage S3-compatible, lưu trữ toàn bộ data files (Parquet) và metadata files của Iceberg.
    * **Apache Iceberg Format V2:** Hỗ trợ cả **Append** (telemetry) lẫn **MERGE INTO** (CDC billing). Format V2 bật tính năng row-level deletes và merge-on-read mode, cho phép Upsert hiệu quả.
    * **Iceberg REST Catalog (`viettel`):** Trung tâm quản lý metadata, chia sẻ giữa Spark (ghi) và Trino (đọc). Loại bỏ bottleneck của Hive Metastore khi có nhiều luồng ghi đồng thời.

### 3.4. Tầng Tính toán & Biến đổi Dữ liệu (Compute & Transformation Layer)
* **Công nghệ:** Trino 457 + dbt-trino.
* **Cơ chế hoạt động:**
    * **Trino:** MPP SQL Query Engine, truy vấn trực tiếp Iceberg tables trên MinIO qua catalog `viettel`.
    * **dbt:** Định nghĩa toàn bộ logic nghiệp vụ bằng SQL, push-down execution xuống Trino.

**Mô hình Medallion Architecture (Mở rộng):**

1. **Bronze (Lớp thô):**
   - *Telemetry:* `bronze.telecom_events` — Append-only, phân vùng theo `days(event_ts), network_type`
   - *CDC:* `bronze.billing_plans` và `bronze.subscribers` — MERGE INTO (Upsert), merge-on-read

2. **Staging (Chuẩn hóa):**
   - `stg_telecom_events` — Ép kiểu, chuẩn hóa, tính `total_mb`, `is_call_failure`, `has_qoe_issue`
   - `stg_billing_plans` — Lọc soft-delete, chuẩn hóa `plan_type`
   - `stg_subscribers` — Lọc soft-delete, uppercase `home_cell_id` cho tương thích JOIN

3. **Silver (Tích hợp & Làm giàu):**
   - `silver_cell_events` — Khử trùng lặp theo `event_id`, JOIN metadata BTS (tỉnh, huyện, toạ độ, vendor, capacity)

4. **Gold (KPI & Analytics) — 4 bảng:**
   - `gold_network_kpis` — Incremental MERGE, cửa sổ 15 phút, tổng hợp Drop Call Rate, QoE Issue Rate, Cell Load Score
   - `gold_cell_heatmap` — View latest-per-cell, severity classification (NORMAL/WARNING/CRITICAL)
   - `gold_customer_360` — **Cross-domain JOIN** giữa billing (CDC) và telemetry → `customer_impact_score`, `churn_risk_tier`
   - `gold_plan_revenue_impact` — Tổng hợp theo gói cước → `revenue_at_risk_pct`, `plan_health_status`

### 3.5. Tầng Tự động hóa & Kiểm soát Chất lượng (Orchestration & Data Quality Layer)
* **Công nghệ:** Apache Airflow 2.10.4 + dbt Tests.
* **Cơ chế hoạt động:**
    * **dbt Tests:** Bộ quy tắc ràng buộc chất lượng dữ liệu tự động, bao gồm:
      - `not_empty` — Bảng Gold không được rỗng
      - `vietnam_coordinate_range` — Toạ độ GPS BTS trong biên giới Việt Nam
      - `accepted_range` — Drop Call Rate, QoE Issue Rate, Cell Load Score ∈ [0, 100]
      - `relationships` — cell_id trong Silver tồn tại trong bts_metadata
      - `accepted_values` — severity, churn_risk_tier, plan_health_status có giá trị hợp lệ
      - `unique` — subscriber_id, plan_id unique trong CDC staging
    * **Apache Airflow:** DAG `onehouse_dbt_medallion_pipeline` chạy mỗi 15 phút:
      1. `wait_for_trino` → 2. `dbt_debug` → 3. `dbt_seed` → 4. `dbt_run staging silver` → 5. `dbt_run gold` → 6. `dbt_test tag:gold` → 7. `dbt_docs_generate`

### 3.6. Tầng Trực quan hóa (Serving & Visualization Layer)
* **Công nghệ:** Apache Superset 4.1.1.
* **Cơ chế hoạt động:** Kết nối trực tiếp vào Trino Engine để khai thác dữ liệu Gold. Xây dựng:
    - **Dashboard bản đồ nhiệt (Geographical Heatmap):** Dựa trên toạ độ BTS, hiển thị severity và cell_load_score.
    - **Dashboard Customer 360:** Hiển thị khách hàng rủi ro cao, phân tích tác động doanh thu theo gói cước.

---

## 4. MA TRẬN LUỒNG DỮ LIỆU ĐẦU CUỐI (END-TO-END DATA FLOW)

| Tầng Dữ Liệu | Công Nghệ | Logic / Cơ Chế | Đầu Ra |
|:---|:---|:---|:---|
| **Ingestion (Telemetry)** | Redpanda, Spark Streaming | Micro-batching 20s, Parse JSON, Dedup, Append-only | `bronze.telecom_events` |
| **Ingestion (CDC)** | PostgreSQL, Debezium, Spark CDC | WAL → Kafka Connect → foreachBatch → MERGE INTO | `bronze.billing_plans`, `bronze.subscribers` |
| **Staging** | Trino, dbt Views | Ép kiểu, chuẩn hóa, lọc soft-delete, tính computed columns | 3 staging views |
| **Silver** | Trino, dbt Table | Khử trùng lặp, JOIN BTS metadata, enrichment | `silver_cell_events` |
| **Gold (Telemetry)** | Trino, dbt Incremental | Delta processing 15 phút, Aggregate KPIs, severity | `gold_network_kpis`, `gold_cell_heatmap` |
| **Gold (Cross-domain)** | Trino, dbt Table | JOIN billing × telemetry, Impact Score, Churn Risk | `gold_customer_360`, `gold_plan_revenue_impact` |
| **Automation** | Airflow, dbt Tests | DAG 15-min, Quality Gates, ngắt mạch khi test fail | Pipeline tự động, Lineage docs |
| **Serving** | Superset | OLAP Direct Query via Trino | Heatmap + Customer 360 Dashboards |

---

## 5. ĐIỂM NHẤN CÔNG NGHỆ

1. **Kiến trúc Dual Ingestion (MDS Lakehouse):** Kết hợp đồng thời Streaming Append (Telemetry) và CDC MERGE INTO (Billing) trên cùng một Iceberg catalog, sử dụng chung Spark cluster và Trino engine. Đây là kiến trúc **Hybrid Ingestion** chuẩn Enterprise.

2. **Cross-domain Analytics (Customer 360):** Khả năng JOIN dữ liệu từ hai domain hoàn toàn khác nhau (mạng viễn thông × thanh toán) để tạo ra insight mà từng domain riêng lẻ không thể có — ví dụ: *"Khách hàng VIP đang trả 990K/tháng nhưng ở trạm BTS có drop call rate 8%"*.

3. **Iceberg Format V2 + MERGE INTO:** Tận dụng row-level deletes và merge-on-read mode để thực hiện Upsert hiệu quả cho CDC, trong khi vẫn giữ Append-only cho Telemetry — hai pattern ghi khác nhau trên cùng một table format.

4. **Spark Standalone Cluster (Custom Image):** Loại bỏ hoàn toàn `--packages` runtime download bằng 6 JAR pre-baked. Multi-mode entrypoint cho phép chạy đồng thời nhiều Spark jobs trên cùng cluster.

5. **Tối ưu hóa Hiệu năng (Incremental Compute):** dbt Incremental Models chỉ quét delta 15 phút gần nhất, kết hợp Hidden Partitioning của Iceberg, giải quyết triệt để bài toán Big Data quy mô doanh nghiệp.

6. **Quản trị Dữ liệu Chuẩn Enterprise:** 100% logic nghiệp vụ mã hóa tường minh (Code-based), quản lý phiên bản qua Git, Data Lineage tự động, Quality Gates ngắt mạch pipeline khi phát hiện dữ liệu lỗi.

---

## 6. DANH SÁCH SERVICES (16 CONTAINERS)

| # | Service | Container | Image | Port |
|---|---------|-----------|-------|------|
| 1 | Redpanda | onehouse-redpanda | redpandadata/redpanda:v24.3.7 | 19092, 19644 |
| 2 | Redpanda Console | onehouse-redpanda-console | redpandadata/console:v2.8.3 | 8084 |
| 3 | MinIO | onehouse-minio | minio/minio:RELEASE.2024-11-07 | 9000, 9001 |
| 4 | MinIO Init | onehouse-minio-init | minio/mc:latest | — |
| 5 | Iceberg REST | onehouse-iceberg-rest | tabulario/iceberg-rest:1.6.0 | 8181 |
| 6 | Trino | onehouse-trino | trinodb/trino:457 | 8080 |
| 7 | Simulator | onehouse-simulator | custom (python:3.11-slim) | — |
| 8 | Spark Master | onehouse-spark-master | custom (python:3.12-slim + Spark 3.5.3) | 7077, 8085 |
| 9 | Spark Worker | onehouse-spark-worker | custom (same as Master) | 8086 |
| 10 | Spark Streaming | onehouse-spark-streaming | custom (same as Master) | 4040 |
| 11 | PostgreSQL Billing | onehouse-postgres-billing | postgres:16-alpine | 5432 |
| 12 | Billing Generator | onehouse-billing-generator | custom (same as Simulator) | — |
| 13 | Debezium | onehouse-debezium | debezium/connect:2.7 | 8083 |
| 14 | Debezium Init | onehouse-debezium-init | curlimages/curl:latest | — |
| 15 | Spark CDC | onehouse-spark-cdc | custom (same as Master) | — |
| 16 | dbt | onehouse-dbt | custom (python + dbt-trino) | — |
| * | Airflow (3 containers) | onehouse-airflow-* | apache/airflow:2.10.4 | 8082 |
| * | Superset | onehouse-superset | apache/superset:4.1.1 | 8088 |
