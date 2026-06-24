# Data Simulator — Bộ sinh dữ liệu giả lập

## Tổng quan

Thư mục này chứa **hai script Python** sinh dữ liệu giả lập cho hệ thống, đóng vai trò nguồn dữ liệu đầu vào (data source) của toàn bộ pipeline. Cả hai script chia sẻ cùng một Docker image và được chọn thông qua biến môi trường `$GENERATOR_SCRIPT`.

## Cấu trúc thư mục

```
simulator/
├── telecom_log_simulator.py   # Sinh sự kiện viễn thông → Redpanda
├── billing_generator.py       # Sinh thay đổi OLTP → PostgreSQL
├── Dockerfile                 # Image Python 3.11-slim dùng chung
└── requirements.txt           # kafka-python, psycopg2-binary
```

## Chi tiết các script

### 1. `telecom_log_simulator.py` — Streaming Data Generator

Sinh các sự kiện **CDR** (Call Detail Record) và **mobile data** rồi gửi tới Redpanda topic `telecom.raw_logs` với tốc độ mặc định **~20 events/giây**.

| Tham số | Giá trị mặc định |
|---------|-------------------|
| Target topic | `telecom.raw_logs` |
| Throughput | ~20 events/giây |
| Số BTS cells | 8 cells mô phỏng |
| Duplicate ratio | 1% |
| Bad record ratio | 0.5% |

**Vùng phủ sóng mô phỏng**: Hà Nội, TP. Hồ Chí Minh, Đà Nẵng, Hải Phòng, Cần Thơ, Quảng Ninh — mỗi cell có **risk factor** riêng để tạo dữ liệu bất thường phục vụ phát hiện anomaly.

### 2. `billing_generator.py` — CDC Source Generator

Sinh các thay đổi **OLTP** (INSERT/UPDATE) vào PostgreSQL billing database, làm nguồn cho Debezium CDC.

| Thao tác | Tỷ lệ |
|----------|--------|
| UPDATE subscribers | 70% |
| INSERT subscriber mới | 20% |
| UPDATE billing plans | 10% |

- Khởi tạo **200 subscribers** ban đầu (seed), sau đó chạy **vòng lặp liên tục** INSERT/UPDATE.
- Sử dụng thư viện `psycopg2` để kết nối PostgreSQL.

## Cấu hình qua biến môi trường

| Biến môi trường | Mô tả | Mặc định |
|-----------------|--------|----------|
| `GENERATOR_SCRIPT` | Script Python được chạy khi container khởi động | `telecom_log_simulator.py` |
| `SIMULATOR_EVENTS_PER_SECOND` | Số event sinh mỗi giây (telecom simulator) | `20` |
| `SIMULATOR_DUPLICATE_RATIO` | Tỷ lệ bản ghi trùng lặp | `0.01` |
| `SIMULATOR_BAD_RECORD_RATIO` | Tỷ lệ bản ghi lỗi | `0.005` |
| `BATCH_SIZE` | Kích thước batch mỗi chu kỳ (billing generator) | — |
| `INTERVAL_SEC` | Khoảng thời gian giữa các chu kỳ (billing generator) | — |

## Docker

```dockerfile
# Base: python:3.11-slim
# Chọn script qua biến $GENERATOR_SCRIPT
CMD ["python", "-u", "$GENERATOR_SCRIPT"]
```

## Tích hợp với các thành phần khác

```
telecom_log_simulator ──▶ Redpanda (telecom.raw_logs) ──▶ Spark Streaming ──▶ Iceberg Bronze
billing_generator ──────▶ PostgreSQL ──▶ Debezium CDC ──▶ Redpanda ──▶ Spark CDC ──▶ Iceberg Bronze
```

- **Redpanda**: `telecom_log_simulator` ghi trực tiếp event vào Kafka-compatible topic.
- **PostgreSQL**: `billing_generator` ghi vào DB, Debezium bắt các thay đổi qua WAL.
- **Spark**: Downstream consumer đọc từ cả hai luồng dữ liệu.
