# PostgreSQL — Billing Database (CDC Source)

## Tổng quan

Thư mục này chứa các **SQL init script** để khởi tạo cơ sở dữ liệu billing trên PostgreSQL. Database này đóng vai trò **nguồn OLTP** cho pipeline CDC (Change Data Capture) — mọi thay đổi dữ liệu sẽ được Debezium capture và đẩy vào Redpanda.

## Cấu trúc thư mục

```
postgresql/
├── init_billing.sql          # Tạo bảng và seed data
└── create_publication.sql    # Cấu hình logical replication cho CDC
```

## Chi tiết các file

### `init_billing.sql` — Khởi tạo schema và dữ liệu

Tạo hai bảng chính trong database `viettel_billing`:

| Bảng | Mô tả |
|------|--------|
| `billing_plans` | Danh sách gói cước Viettel (seed 10 gói) |
| `subscribers` | Thông tin thuê bao đăng ký |

**Dữ liệu seed `billing_plans`** — 10 gói cước mô phỏng Viettel:

| Gói cước | Loại | Giá (VND) |
|----------|------|-----------|
| ECO30 | prepaid | 30,000 |
| ... | ... | ... |
| VIP Unlimited | postpaid | 990,000 |

Phân loại gói cước: **prepaid** (trả trước) và **postpaid** (trả sau), giá dao động từ 30K đến 990K VND.

### `create_publication.sql` — Cấu hình CDC

Tạo các thành phần cần thiết cho Debezium CDC:

| Thành phần | Giá trị | Mô tả |
|------------|---------|--------|
| Replication slot | `debezium_slot` | Logical replication slot cho Debezium |
| Publication | `billing_publication` | Publish tất cả bảng (`FOR ALL TABLES`) |

> **Quan trọng**: PostgreSQL phải được cấu hình `wal_level = logical` để hỗ trợ logical replication. Điều này được thiết lập qua `docker-compose.yml` command flag.

## Cấu hình Docker

| Tham số | Giá trị |
|---------|---------|
| Image | `postgres:16-alpine` |
| Port | `localhost:5432` |
| User | `viettel` |
| Database | `viettel_billing` |
| WAL level | `logical` (set qua docker-compose command) |

## Tích hợp với các thành phần khác

```
┌─────────────┐     ┌────────────┐     ┌──────────┐     ┌───────────┐
│  Billing    │────▶│ PostgreSQL │────▶│ Debezium │────▶│  Redpanda │
│  Generator  │     │   (WAL)    │     │  (CDC)   │     │  (Topics) │
└─────────────┘     └────────────┘     └──────────┘     └───────────┘
```

- **Simulator** (`billing_generator.py`): Liên tục INSERT/UPDATE dữ liệu vào các bảng.
- **Debezium**: Đọc WAL logs thông qua logical replication slot, capture mọi thay đổi.
- **Redpanda**: Debezium ghi CDC events vào topics `billing.public.billing_plans` và `billing.public.subscribers`.

## Lệnh hữu ích

```bash
# Kết nối vào database
docker exec -it postgres psql -U viettel -d viettel_billing

# Kiểm tra replication slot
SELECT * FROM pg_replication_slots;

# Kiểm tra publication
SELECT * FROM pg_publication_tables;
```
