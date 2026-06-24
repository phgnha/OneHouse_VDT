# Tài liệu dự án

## Tổng quan

Thư mục `docs/` chứa tài liệu kỹ thuật chi tiết của hệ thống OneHouse — bao gồm kiến trúc tổng thể và các data contract. Đây là nguồn tham khảo chính giúp hiểu thiết kế hệ thống, luồng dữ liệu, và quy ước về cấu trúc dữ liệu giữa các thành phần.

## Cấu trúc thư mục

```
docs/
├── architecture.md    # Kiến trúc hệ thống
└── data_contract.md   # Hợp đồng dữ liệu (Data Contracts)
```

## `architecture.md` — Kiến trúc hệ thống

Tài liệu mô tả toàn bộ kiến trúc **Data Lakehouse** với các nội dung chính:

| Phần | Nội dung |
|------|----------|
| **System Architecture** | Sơ đồ Mermaid tổng quan luồng dữ liệu end-to-end: Simulator → Kafka → Spark → Iceberg → Trino → Superset |
| **Component Roles** | Bảng liệt kê vai trò của từng thành phần: Redpanda (message broker), MinIO (object storage), Spark (stream processing), Trino (SQL engine), dbt (transformation), v.v. |
| **Medallion Layers** | Giải thích 3 layer dữ liệu: Bronze (raw Iceberg tables), Silver (deduplicated + enriched), Gold (aggregated analytics) |
| **Spark Cluster** | Kiến trúc Spark master-worker: streaming job xử lý telemetry realtime, CDC job xử lý change data capture từ PostgreSQL |
| **CDC Pipeline** | Luồng Change Data Capture: PostgreSQL → Debezium → Redpanda → Spark CDC → Iceberg tables |

### Các sơ đồ Mermaid có trong tài liệu

- Sơ đồ luồng dữ liệu tổng thể (flowchart)
- Kiến trúc Spark cluster
- Pipeline CDC end-to-end

## `data_contract.md` — Hợp đồng dữ liệu

Tài liệu định nghĩa **data contract** cho tất cả các nguồn dữ liệu và KPI đầu ra:

| Phần | Nội dung |
|------|----------|
| **Telemetry Events** | Định nghĩa **25 trường dữ liệu** của sự kiện viễn thông: `event_id`, `cell_id`, `subscriber_id`, `signal_strength`, `throughput_mbps`, `latency_ms`, v.v. |
| **CDC Billing Events** | Cấu trúc dữ liệu thay đổi từ hệ thống billing: thông tin gói cước, thuê bao, trạng thái thanh toán |
| **Simulator Parameters** | Tham số của bộ mô phỏng dữ liệu: tần suất gửi event, phân bố giá trị, biên độ nhiễu |
| **KPI Definitions** | Định nghĩa các chỉ số KPI với **ngưỡng (thresholds)** và **công thức tính (formulas)**: drop call rate, throughput trung bình, QoE score, cell load score |

### Ví dụ KPI Definitions

Data contract bao gồm các KPI quan trọng như:

- **Drop Call Rate (%)**: Tỷ lệ cuộc gọi bị rớt — ngưỡng cảnh báo khi > 2%
- **Throughput (Mbps)**: Tốc độ truyền tải dữ liệu trung bình
- **QoE Issue Rate (%)**: Tỷ lệ trải nghiệm người dùng dưới mức chấp nhận
- **Cell Load Score**: Điểm tải tổng hợp của trạm BTS

## Kết nối với các thành phần khác

- **dbt models**: Các model trong `dbt_onehouse/models/` được xây dựng dựa trên data contract đã định nghĩa
- **Simulator**: Tham số mô phỏng trong `data_contract.md` tương ứng với cấu hình trong thư mục `simulator/`
- **Superset dashboards**: Các KPI definitions làm cơ sở cho việc thiết kế dashboard
- **schema.yml**: Test definitions trong dbt tham chiếu đến ngưỡng và ràng buộc từ data contract

## Đối tượng sử dụng

| Vai trò | Tài liệu tham khảo |
|---------|---------------------|
| Data Engineer | `architecture.md` — hiểu luồng dữ liệu và cách deploy |
| Data Analyst | `data_contract.md` — hiểu ý nghĩa các trường và KPI |
| DevOps | `architecture.md` — hiểu các thành phần cần vận hành |
| Stakeholder | `data_contract.md` — hiểu ngưỡng KPI và business logic |
