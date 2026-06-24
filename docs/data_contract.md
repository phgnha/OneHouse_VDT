# OneHouse Data Contract

## Luồng 1: Telemetry — Kafka Event Fields

Topic: `telecom.raw_logs`

| Field | Type | Ghi chú |
|-------|------|---------|
| event_id | string (UUID) | Định danh duy nhất event |
| event_ts | timestamp string (ISO 8601) | Thời gian event phát sinh (UTC) |
| subscriber_id | string | Số điện thoại thuê bao giả lập (849XXXXXXXX) |
| cell_id | string | Mã trạm BTS (VD: `HN_CAU_GIAY_001`) |
| event_type | string | `VOICE` (32%) hoặc `DATA` (68%) |
| network_type | string | `4G` (62%) hoặc `5G` (38%) |
| band | string | Dải băng tần LTE/NR (VD: `n78`, `B3`, `B7`) |
| device_type | string | `android` / `ios` / `iot_router` / `mifi` |
| call_status | string | `COMPLETED` / `DROPPED` / `FAILED` (chỉ VOICE) |
| call_duration_sec | integer | Thời lượng cuộc gọi 1–900 giây (chỉ VOICE) |
| drop_reason | string | `radio_link_failure` / `handover_failure` / `congestion` / `core_timeout` |
| download_mb | double | Lưu lượng tải xuống (chỉ DATA) |
| upload_mb | double | Lưu lượng tải lên (chỉ DATA) |
| latency_ms | double | Độ trễ mạng, 5–650ms |
| jitter_ms | double | Biến động độ trễ, 0.1–180ms |
| packet_loss_pct | double | Tỷ lệ mất gói, 0–30% |
| rsrp | double | Cường độ tín hiệu vô tuyến, -140 đến -60 dBm |
| sinr | double | Chất lượng tín hiệu, -10 đến 35 dB |
| source | string | `"onehouse-simulator"` |

### Simulator Parameters

| Biến môi trường | Mặc định | Mô tả |
|-----------------|----------|-------|
| SIMULATOR_EVENTS_PER_SECOND | 20 | Tốc độ sinh events |
| SIMULATOR_DUPLICATE_RATIO | 0.01 | Tỷ lệ bản ghi trùng lặp |
| SIMULATOR_BAD_RECORD_RATIO | 0.005 | Tỷ lệ bản ghi lỗi (null cell_id/event_ts/event_type) |

### BTS Risk Factors

| Cell ID | Tỉnh | Risk | Ghi chú |
|---------|------|------|---------|
| HN_CAU_GIAY_001 | Hà Nội | 8% | Trạm đông đúc |
| HCM_Q1_001 | TP. HCM | 9% | Trạm rủi ro cao nhất |
| QNH_HA_LONG_001 | Quảng Ninh | 7% | Trạm du lịch |
| HCM_THU_DUC_002 | TP. HCM | 6% | Khu công nghệ |
| HN_HOAN_KIEM_002 | Hà Nội | 5% | Trung tâm lịch sử |
| CT_NINH_KIEU_001 | Cần Thơ | 5% | Trung tâm ĐBSCL |
| DN_HAI_CHAU_001 | Đà Nẵng | 4% | Trung tâm miền Trung |
| HP_LE_CHAN_001 | Hải Phòng | 3% | Trạm ổn định nhất |

Trạm có risk ≥ 7% được chọn với trọng số 1.6× (nhiều events hơn).

---

## Luồng 2: CDC Billing — Debezium Event Fields

### Topic: `billing.public.billing_plans`

| Field | Type | Ghi chú |
|-------|------|---------|
| plan_id | string | Mã gói cước (VD: `ST90`, `V200`, `VIP`) |
| plan_name | string | Tên gói cước |
| plan_type | string | `prepaid` hoặc `postpaid` |
| monthly_fee | double | Phí hàng tháng (VNĐ) |
| data_quota_gb | double | Hạn mức data (GB), NULL = unlimited |
| voice_minutes | integer | Phút gọi, NULL = unlimited hoặc data-only |
| created_at | timestamp string | Thời gian tạo |
| updated_at | timestamp string | Thời gian cập nhật gần nhất |
| __deleted | string | Cờ xóa từ Debezium rewrite mode (`"true"` / `"false"`) |

### Topic: `billing.public.subscribers`

| Field | Type | Ghi chú |
|-------|------|---------|
| subscriber_id | string | MSISDN số điện thoại (khóa chính) |
| full_name | string | Họ và tên thuê bao |
| plan_id | string | Khóa ngoại → `billing_plans.plan_id` |
| home_cell_id | string | Mã trạm BTS gần nhà thuê bao |
| status | string | `active` / `suspended` / `terminated` |
| activated_at | timestamp string | Thời gian kích hoạt |
| updated_at | timestamp string | Thời gian cập nhật gần nhất |
| __deleted | string | Cờ xóa từ Debezium rewrite mode |

### Billing Generator Parameters

| Biến môi trường | Mặc định | Mô tả |
|-----------------|----------|-------|
| BATCH_SIZE | 10 | Số lượng event mỗi chu kỳ |
| INTERVAL_SEC | 5 | Chu kỳ sinh dữ liệu (giây) |

### Event Distribution (mỗi chu kỳ)
- 70% UPDATE subscribers (đổi gói cước, đổi status, di chuyển cell, đổi tên)
- 20% INSERT subscribers mới
- 10% UPDATE billing_plans (thay đổi phí hoặc quota)

### Seed Data: 10 Gói Cước Viettel

| plan_id | Tên | Loại | Phí/tháng | Data | Voice |
|---------|-----|------|-----------|------|-------|
| ECO30 | Economy 30 | prepaid | 30,000 | 1.5 GB | 100 phút |
| ST90 | ST90 - Data 90K | prepaid | 90,000 | 4 GB | — |
| ST120 | ST120 - Data 120K | prepaid | 120,000 | 7 GB | — |
| V200 | V200 - Combo 200K | prepaid | 200,000 | 10 GB | 500 phút |
| V300 | V300 - Combo 300K | prepaid | 300,000 | 20 GB | 1000 phút |
| MAX90 | MaxSpeed 90 | postpaid | 90,000 | 8 GB | — |
| MAX200 | MaxSpeed 200 | postpaid | 200,000 | 30 GB | — |
| BIZ300 | Business 300 | postpaid | 300,000 | 50 GB | 2000 phút |
| BIZ500 | Business 500 | postpaid | 500,000 | 100 GB | 5000 phút |
| VIP | Viettel VIP Unlimited | postpaid | 990,000 | Unlimited | Unlimited |

---

## KPI Definitions

### Telemetry KPIs (Gold Network)

| KPI | Công thức | Đơn vị | Ngưỡng |
|-----|-----------|--------|--------|
| Drop Call Rate | `dropped_calls / total_calls × 100` | % | CRITICAL ≥ 5%, WARNING ≥ 2% |
| QoE Issue Rate | `qoe_issue_events / total_events × 100` | % | CRITICAL ≥ 18%, WARNING ≥ 10% |
| Data Traffic | `sum(download_mb + upload_mb) / 1024` | GB | — |
| Cell Load Score | `(traffic/capacity × 100) + failures × 2 + qoe_issues × 1.5` | 0–100 | CRITICAL ≥ 85, WARNING ≥ 65 |
| Avg Latency | `avg(latency_ms)` | ms | — |
| Avg RSRP | `avg(rsrp)` | dBm | — |
| Avg SINR | `avg(sinr)` | dB | — |

### QoE Issue Detection

Một event được đánh dấu `has_qoe_issue = true` khi thoả bất kỳ điều kiện:
- `latency_ms > 120`
- `packet_loss_pct > 2.0`
- `rsrp < -110`
- `sinr < 5`

### Billing KPIs (Gold Customer 360)

| KPI | Công thức | Mô tả |
|-----|-----------|-------|
| Customer Impact Score | `(fee/100K) × (load/100) × severity_weight × 100` | Ưu tiên chăm sóc khách VIP ở trạm lỗi |
| Churn Risk Tier | Rule-based trên severity + monthly_fee | HIGH_RISK / MEDIUM_RISK / LOW_RISK |
| Revenue at Risk % | `total_revenue_at_risk / total_monthly_revenue × 100` | Phần trăm doanh thu đang bị đe doạ |
| Plan Health Status | Dựa trên critical_revenue / total_revenue | CRITICAL (>20%) / WARNING (>15%) / HEALTHY |

### Severity Weights (cho Customer Impact Score)
| Severity | Weight |
|----------|--------|
| CRITICAL | 3.0 |
| WARNING | 1.5 |
| NORMAL / NULL | 0.5 |

### Churn Risk Rules
| Điều kiện | Tier |
|-----------|------|
| Cell CRITICAL + monthly_fee ≥ 300,000 | HIGH_RISK |
| Cell CRITICAL hoặc monthly_fee ≥ 500,000 | MEDIUM_RISK |
| Cell WARNING + monthly_fee ≥ 200,000 | MEDIUM_RISK |
| Còn lại | LOW_RISK |
