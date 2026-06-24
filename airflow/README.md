# Apache Airflow — Pipeline Orchestration

## Tổng quan

Thư mục này chứa các **DAG** (Directed Acyclic Graph) của Apache Airflow, đóng vai trò **điều phối toàn bộ pipeline dbt** theo kiến trúc Medallion (Bronze → Silver → Gold). DAG chạy tự động mỗi **15 phút**, đảm bảo dữ liệu được transform liên tục từ tầng staging đến tầng gold, đồng thời thực thi quality gate trước khi publish kết quả.

## Cấu trúc thư mục

```
airflow/
└── dags/
    └── onehouse_dbt_pipeline.py   # DAG chính của pipeline
```

| File | Mô tả |
|------|--------|
| `dags/onehouse_dbt_pipeline.py` | Định nghĩa DAG `onehouse_dbt_medallion_pipeline` với chuỗi task thực thi dbt thông qua `DockerOperator` |

## Chuỗi Task trong DAG

DAG `onehouse_dbt_medallion_pipeline` bao gồm các task theo thứ tự:

```
wait_for_trino → dbt_debug → dbt_seed → dbt_run (staging, silver)
    → dbt_run (gold) → dbt_test (tag:gold) → dbt_docs_generate
```

| Task | Chức năng |
|------|-----------|
| `wait_for_trino` | Sensor chờ Trino (`trino:8080`) sẵn sàng trước khi bắt đầu |
| `dbt_debug` | Kiểm tra kết nối dbt ↔ Trino |
| `dbt_seed` | Nạp seed data (dữ liệu tham chiếu tĩnh) |
| `dbt_run` (staging, silver) | Transform các model tầng staging và silver |
| `dbt_run` (gold) | Transform các model tầng gold (aggregation, metrics) |
| `dbt_test` (tag:gold) | **Quality gate** — pipeline dừng nếu test thất bại |
| `dbt_docs_generate` | Sinh tài liệu dbt tự động |

> **Quality Gate**: Nếu `dbt test --select tag:gold` fail, pipeline **dừng ngay lập tức** — dữ liệu gold không đạt chất lượng sẽ không được publish.

## Cấu hình

| Tham số | Giá trị |
|---------|---------|
| Schedule interval | `*/15 * * * *` (mỗi 15 phút) |
| Catalog | `viettel` |
| Trino host | `trino:8080` |
| Docker image | Build từ `docker/airflow/Dockerfile` (extends `apache/airflow:2.10.4`) |
| Webserver port | `http://localhost:8082` |
| Default credentials | `admin` / `admin` |

## Tích hợp với các thành phần khác

```
┌─────────────┐     ┌──────────┐     ┌──────────────┐
│   Airflow    │────▶│   dbt    │────▶│    Trino      │
│  (Scheduler) │     │ (Docker) │     │ (Query Engine)│
└─────────────┘     └──────────┘     └──────────────┘
                                            │
                                     ┌──────▼──────┐
                                     │   Iceberg    │
                                     │  (Lakehouse) │
                                     └─────────────┘
```

- **dbt**: Airflow gọi dbt thông qua `DockerOperator`, sử dụng image từ `docker/dbt/`.
- **Trino**: dbt kết nối tới Trino để thực thi SQL transform trên các bảng Iceberg.
- **Iceberg / MinIO**: Dữ liệu cuối cùng được lưu trữ dưới dạng Iceberg tables trên S3 (MinIO).

## Truy cập

```bash
# Airflow Web UI
http://localhost:8082
# Login: admin / admin
```
