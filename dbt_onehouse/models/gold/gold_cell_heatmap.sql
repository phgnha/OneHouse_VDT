-- Cấu hình tạo bảng view cho Dashboard
{{ config(materialized='view', schema='gold', tags=['gold', 'dashboard']) }}

-- Dùng hàm Window đánh số thứ tự xếp hạng (ranking) cho mỗi trạm BTS
-- Nhằm mục đích chỉ lấy dòng dữ liệu mới nhất (dựa trên chu kỳ thời gian window_start)
with ranked as (
    select
        kpis.*,
        row_number() over (
            partition by cell_id
            order by window_start desc
        ) as recency_rank
    from {{ ref('gold_network_kpis') }} as kpis
),

-- Lọc ra bản ghi có recency_rank = 1 (Tức là trạng thái gần đây nhất của từng cell)
latest as (
    select *
    from ranked
    where recency_rank = 1
)

-- Trả ra báo cáo Heatmap toàn mạng với các ngưỡng cảnh báo độ nghiêm trọng
select
    window_start,
    window_end,
    cell_id,
    province,
    district,
    region,
    latitude,
    longitude,
    vendor,
    band,
    network_type,
    total_events,
    total_calls,
    total_data_sessions,
    dropped_calls,
    qoe_issue_events,
    data_traffic_gb,
    avg_latency_ms,
    avg_jitter_ms,
    avg_packet_loss_pct,
    avg_rsrp,
    avg_sinr,
    coalesce(drop_call_rate_pct, 0.0) as drop_call_rate_pct,
    coalesce(qoe_issue_rate_pct, 0.0) as qoe_issue_rate_pct,
    coalesce(cell_load_score, 0.0) as cell_load_score,
    -- Đánh giá mức độ nghiêm trọng dựa trên các ngưỡng của từng chỉ số KPI
    case
        -- NGUY HIỂM (CRITICAL): Rớt cuộc gọi >= 5% hoặc Trải nghiệm tồi >= 18% hoặc Quá tải >= 85%
        when coalesce(drop_call_rate_pct, 0.0) >= 5.0
          or coalesce(qoe_issue_rate_pct, 0.0) >= 18.0
          or coalesce(cell_load_score, 0.0) >= 85.0
            then 'CRITICAL'
        -- CẢNH BÁO (WARNING): Rớt cuộc gọi >= 2% hoặc Trải nghiệm tồi >= 10% hoặc Quá tải >= 65%
        when coalesce(drop_call_rate_pct, 0.0) >= 2.0
          or coalesce(qoe_issue_rate_pct, 0.0) >= 10.0
          or coalesce(cell_load_score, 0.0) >= 65.0
            then 'WARNING'
        -- BÌNH THƯỜNG (NORMAL): Cho các trường hợp còn lại
        else 'NORMAL'
    end as severity,
    updated_at
from latest
