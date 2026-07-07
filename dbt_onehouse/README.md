# dbt Project — OneHouse Transformation Layer

## Tổng quan

Thư mục `dbt_onehouse/` chứa toàn bộ dbt project thực hiện **data transformation** theo kiến trúc **Medallion** (Staging → Silver → Gold). Project kết nối đến Trino query engine tại `trino:8080`, sử dụng catalog ``, và xử lý dữ liệu telemetry viễn thông cùng billing CDC để tạo ra các bảng phân tích sẵn sàng cho BI/Dashboard.

Profile: `onehouse` | Biến chính: `gold_delta_minutes: 30`

## Cấu trúc thư mục

```
dbt_onehouse/
├── dbt_project.yml                     # Cấu hình project
├── models/
│   ├── staging/                        # Layer 1: Raw sources → Views
│   │   ├── sources.yml                 # Source definition — telemetry events
│   │   ├── sources_billing.yml         # Source definition — CDC billing
│   │   ├── stg_telecom_events.sql      # Staging view: sự kiện viễn thông
│   │   ├── stg_billing_plans.sql       # Staging view: gói cước
│   │   └── stg_subscribers.sql         # Staging view: thuê bao
│   ├── silver/
│   │   └── silver_cell_events.sql      # Layer 2: Dedup + BTS enrichment
│   ├── gold/
│   │   ├── gold_network_kpis.sql       # KPI mạng (incremental merge)
│   │   ├── gold_cell_heatmap.sql       # Bản đồ nhiệt tải trạm (view)
│   │   ├── gold_customer_360.sql       # Chân dung khách hàng (cross-domain)
│   │   └── gold_plan_revenue_impact.sql # Phân tích doanh thu gói cước
│   └── schema.yml                      # Test definitions cho tất cả models
├── seeds/
│   └── bts_metadata.csv                # Master data: 8 trạm BTS Việt Nam
├── tests/generic/
│   ├── accepted_range.sql              # Kiểm tra giá trị trong khoảng
│   ├── not_empty.sql                   # Kiểm tra bảng không rỗng
│   └── vietnam_coordinate_range.sql    # Kiểm tra tọa độ Việt Nam hợp lệ
└── macros/
    └── generate_schema_name.sql        # Override schema name convention
```

## Kiến trúc Medallion

| Layer | Schema | Materialization | Mô tả |
|-------|--------|-----------------|--------|
| **Staging** | `staging` | `view` | Đọc raw data từ Iceberg tables, chuẩn hóa kiểu dữ liệu và tên cột |
| **Silver** | `silver` | `table` | Loại bỏ bản ghi trùng lặp (dedup), làm giàu dữ liệu với BTS metadata |
| **Gold** | `gold` | `incremental` / `table` / `view` | Bảng phân tích cuối cùng phục vụ BI và báo cáo |

## Gold Models chi tiết

| Model | Materialization | Mô tả |
|-------|-----------------|--------|
| `gold_network_kpis` | `incremental` (merge) | KPI mạng viễn thông: drop call rate, throughput, QoE — sử dụng `gold_delta_minutes` để xử lý incremental |
| `gold_cell_heatmap` | `view` | Bản đồ nhiệt tải trạm BTS với tọa độ GPS, severity level |
| `gold_customer_360` | `table` | Chân dung khách hàng 360° kết hợp telemetry + billing (cross-domain) |
| `gold_plan_revenue_impact` | `table` | Phân tích hiệu quả doanh thu theo gói cước |

## Seeds & Tests

**Seed data** (`bts_metadata.csv`): Chứa thông tin 8 trạm BTS trên toàn quốc với các trường: `cell_id`, `province`, `district`, `latitude`, `longitude`, `region`, `vendor`, `band`, `site_capacity_gbps`, `active_from`.

**Custom generic tests**:
- `accepted_range`: Kiểm tra giá trị số nằm trong khoảng min/max cho phép
- `not_empty`: Đảm bảo model có ít nhất 1 bản ghi sau khi chạy
- `vietnam_coordinate_range`: Xác nhận tọa độ latitude/longitude nằm trong lãnh thổ Việt Nam

## Kết nối với các thành phần khác

- **Trino**: dbt gửi SQL đến Trino (`trino:8080`, catalog ``) để thực thi transformation
- **Iceberg tables**: Staging models đọc raw data từ Iceberg tables được tạo bởi Spark Streaming và Spark CDC
- **Superset**: Đọc trực tiếp các bảng Gold để tạo dashboard
- **Airflow**: Điều phối việc chạy `dbt run` / `dbt test` theo lịch

## Lệnh thường dùng

```powershell
# Chạy qua Docker (khuyên dùng)
docker compose --profile tools run --rm dbt seed --full-refresh
docker compose --profile tools run --rm dbt run
docker compose --profile tools run --rm dbt test
docker compose --profile tools run --rm dbt docs generate
```
