# Superset — Dashboard & Visualization

Apache Superset kết nối với Trino để truy vấn các bảng Iceberg Gold và hiển thị KPIs giám sát mạng viễn thông Viettel dưới dạng dashboard trực quan.

## Vai trò

- Trực quan hóa **bản đồ nhiệt** (Geographical Heatmap) theo toạ độ BTS
- Hiển thị **Drop Call Rate**, **QoE Issue Rate**, **Cell Load Score** theo trạm
- Monitor **Customer 360** — khách hàng rủi ro cao và tác động doanh thu theo gói cước
- Kết nối trực tiếp vào Trino Engine — không cache dữ liệu riêng

## Cấu trúc thư mục

```
superset/
├── README.md               ← Tài liệu này
├── bootstrap.sh             ← Script khởi tạo tự động (init DB, tạo admin, đăng ký Trino)
├── create_dashboard.py      ← Tạo dashboard mẫu "OneHouse Network Experience" qua API
└── superset_config.py       ← Config Superset (SECRET_KEY, session timeout)
```

## Khởi tạo tự động

Container startup (`bootstrap.sh`) thực hiện tuần tự:

1. **Migrate DB** — `superset db upgrade`
2. **Tạo admin user** — từ biến môi trường `SUPERSET_ADMIN_*`
3. **Đăng ký Trino** — URI `trino://admin@trino:8080/viettel/gold`
4. **Tạo dashboard mẫu** — chạy `create_dashboard.py` để seed dashboard

## Cấu hình

| Biến môi trường | Mặc định | Mô tả |
|-----------------|----------|-------|
| `SUPERSET_SECRET_KEY` | `change-me-in-real-deployments` | Secret key cho session |
| `SUPERSET_ADMIN_USERNAME` | `admin` | Username admin |
| `SUPERSET_ADMIN_PASSWORD` | `admin` | Password admin |
| `SUPERSET_ADMIN_EMAIL` | `admin@example.com` | Email admin |
| `SUPERSET_TRINO_URI` | `trino://admin@trino:8080/viettel/gold` | URI kết nối Trino |

## Port

| Port | Mô tả |
|------|-------|
| `8088` | Superset Web UI |

## Datasets gợi ý

Sau khi Trino có dữ liệu Gold, thêm các datasets:

| Dataset | Domain | Mô tả |
|---------|--------|-------|
| `gold.gold_cell_heatmap` | Telemetry | Bản đồ nhiệt hiệu năng mạng theo BTS |
| `gold.gold_network_kpis` | Telemetry | KPI 15 phút: Drop Call Rate, QoE, Traffic |
| `gold.gold_customer_360` | Cross-domain | Customer 360: billing × network quality |
| `gold.gold_plan_revenue_impact` | Billing | Tác động doanh thu theo gói cước |

## Kiểm tra

Truy cập UI: [http://localhost:8088](http://localhost:8088)
- Username: `admin`
- Password: `admin`

Kiểm tra kết nối Trino:
```
Settings → Database Connections → onehouse-trino → Test Connection
```

## Lưu ý

- Dữ liệu được dbt cập nhật mỗi 15 phút (qua Airflow). Bấm **Refresh** trên dashboard để xem dữ liệu mới nhất.
- Nếu bootstrap API thay đổi giữa các phiên bản Superset, service vẫn khởi động bình thường. Tạo dashboard thủ công từ các datasets ở trên.
- Docker image: `apache/superset:4.1.1` + `trino[sqlalchemy]` driver (xem `docker/superset/Dockerfile`).
