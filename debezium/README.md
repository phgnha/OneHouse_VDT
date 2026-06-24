# Debezium — Change Data Capture (CDC)

## Tổng quan

Thư mục này chứa **cấu hình connector** của Debezium, chịu trách nhiệm capture mọi thay đổi dữ liệu (INSERT/UPDATE/DELETE) từ PostgreSQL billing database và đẩy thành các event vào Redpanda. Debezium sử dụng **logical replication** của PostgreSQL để đảm bảo không bỏ sót bất kỳ thay đổi nào.

## Cấu trúc thư mục

```
debezium/
└── register_billing.json    # Cấu hình PostgreSQL source connector
```

## Chi tiết cấu hình connector

File `register_billing.json` định nghĩa một **PostgreSQL source connector** với các thiết lập quan trọng:

### Thông số kết nối

| Tham số | Giá trị |
|---------|---------|
| Connector class | `io.debezium.connector.postgresql.PostgresConnector` |
| Plugin | `pgoutput` |
| Replication slot | `debezium_slot` |
| Publication | `billing_publication` |
| Database | `viettel_billing` |

### Single Message Transform (SMT)

| SMT | Mô tả |
|-----|--------|
| `ExtractNewRecordState` | Flatten CDC envelope thành **plain JSON** (loại bỏ `before`/`after` wrapper) |
| Delete handling | Mode `rewrite` — thêm field `__deleted` thay vì xóa message |

> **Tại sao dùng `rewrite` mode?** Thay vì tạo tombstone record (null value), Debezium thêm field `__deleted: true/false` vào mỗi message. Điều này giúp Spark CDC job xử lý soft-delete dễ dàng hơn khi thực hiện `MERGE INTO`.

### Output Topics

| Topic | Nguồn bảng |
|-------|-----------|
| `billing.public.billing_plans` | Bảng `billing_plans` |
| `billing.public.subscribers` | Bảng `subscribers` |

## Cấu hình Docker

| Tham số | Giá trị |
|---------|---------|
| Image | `debezium/connect:2.7` |
| Port | `http://localhost:8083` |
| Auto-register | Container `debezium-init` (`curlimages/curl`) tự động POST connector config khi khởi động |

### Cơ chế tự động đăng ký

Container `debezium-init` sử dụng `curl` để gửi request POST tới Debezium Connect REST API:

```bash
curl -X POST http://debezium:8083/connectors \
  -H "Content-Type: application/json" \
  -d @register_billing.json
```

## Tích hợp với các thành phần khác

```
┌────────────┐     ┌──────────┐     ┌──────────┐     ┌───────────┐
│ PostgreSQL │────▶│ Debezium │────▶│ Redpanda │────▶│   Spark   │
│   (WAL)    │     │ Connect  │     │ (Topics) │     │ CDC Job   │
└────────────┘     └──────────┘     └──────────┘     └───────────┘
```

- **PostgreSQL**: Đọc WAL thông qua `debezium_slot` + `billing_publication`.
- **Redpanda**: Ghi CDC events (đã flatten) vào Kafka-compatible topics.
- **Spark**: Job `cdc_billing_stream.py` consume các topic này và MERGE INTO Iceberg.

## Lệnh hữu ích

```bash
# Kiểm tra trạng thái connector
curl http://localhost:8083/connectors/billing-connector/status

# Liệt kê tất cả connectors
curl http://localhost:8083/connectors

# Xóa connector (nếu cần tạo lại)
curl -X DELETE http://localhost:8083/connectors/billing-connector
```
