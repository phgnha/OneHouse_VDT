# Helper Scripts

## Tổng quan

Thư mục `scripts/` chứa các script tiện ích hỗ trợ vận hành hệ thống OneHouse. File chính là `onehouse.ps1` — một PowerShell script đóng vai trò **orchestration entry point**, cho phép khởi động, kiểm tra, và dừng toàn bộ các thành phần của hệ thống chỉ bằng một lệnh duy nhất.

## Cấu trúc thư mục

```
scripts/
├── onehouse.ps1       # Script điều phối chính (PowerShell)
└── trino_smoke.sql    # Câu truy vấn kiểm tra nhanh Trino
```

## `onehouse.ps1` — Script điều phối

**Cú pháp:** `.\scripts\onehouse.ps1 <action>`

Script hỗ trợ **8 action** tương ứng với các giai đoạn vận hành:

| Action | Mô tả | Docker Services |
|--------|--------|-----------------|
| `start` | Khởi động hạ tầng cốt lõi (Data Lake + Streaming) | `redpanda`, `minio`, `iceberg-rest`, `trino`, `simulator`, `spark-master`, `spark-worker`, `spark-streaming` |
| `cdc` | Khởi động pipeline Change Data Capture | `postgres-billing`, `billing-generator`, `debezium`, `debezium-init`, `spark-cdc` |
| `dbt` | Chạy thủ công toàn bộ dbt pipeline | `dbt seed` → `dbt run` → `dbt test` → `dbt docs generate` |
| `orchestration` | Khởi động Airflow (điều phối tự động) | `airflow-init`, `airflow-webserver`, `airflow-scheduler` |
| `visualization` | Khởi động Apache Superset (BI/Dashboard) | `superset` |
| `smoke` | Chạy smoke test trên Trino | Đọc `trino_smoke.sql` → thực thi trong container Trino |
| `logs` | Theo dõi log realtime từ các service chính | `docker compose logs -f` |
| `stop` | Dừng và gỡ bỏ toàn bộ container | Shutdown tất cả profiles |

### Thứ tự khởi động khuyên dùng

```powershell
.\scripts\onehouse.ps1 start          # 1. Hạ tầng cốt lõi
.\scripts\onehouse.ps1 cdc            # 2. CDC pipeline (tùy chọn)
.\scripts\onehouse.ps1 dbt            # 3. Data transformation
.\scripts\onehouse.ps1 orchestration  # 4. Airflow (tùy chọn)
.\scripts\onehouse.ps1 visualization  # 5. Superset (tùy chọn)
.\scripts\onehouse.ps1 smoke          # 6. Kiểm tra kết quả
```

## `trino_smoke.sql` — Smoke Test

File SQL chứa các câu truy vấn kiểm tra nhanh để xác nhận hệ thống hoạt động đúng:

| Câu truy vấn | Mục đích |
|---------------|----------|
| `SHOW CATALOGS` | Xác nhận catalog `viettel` đã được đăng ký |
| `SHOW SCHEMAS FROM viettel` | Kiểm tra các schema (staging, silver, gold) tồn tại |
| `SHOW TABLES FROM viettel.gold` | Liệt kê các bảng gold layer |
| `SELECT ... FROM gold_cell_heatmap` | Truy vấn top 20 trạm BTS có tải cao nhất — kiểm tra dữ liệu thực tế |

### Chạy smoke test

```powershell
# Qua script
.\scripts\onehouse.ps1 smoke

# Hoặc chạy trực tiếp
Get-Content -Raw "scripts/trino_smoke.sql" | docker exec -i onehouse-trino trino --catalog viettel --schema gold
```

## Kết nối với các thành phần khác

- **docker-compose.yml**: Script gọi `docker compose` để quản lý lifecycle các service
- **`.env` / `.env.example`**: Action `start` và `cdc` tự động copy `.env.example` → `.env` nếu chưa có
- **Trino container**: Action `smoke` thực thi SQL trực tiếp bên trong container `onehouse-trino`
