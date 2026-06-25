{{
  -- Cấu hình tạo model dạng bảng gia tăng (incremental)
  -- dbt sẽ chỉ chạy MERGE INTO phần dữ liệu mới thay vì ghi lại toàn bộ bảng
  config(
    materialized='incremental',
    -- Khóa chính dùng để update khi có dữ liệu trùng lặp (chống duplicate)
    unique_key=['window_start', 'cell_id', 'network_type'],
    incremental_strategy='merge',
    -- Tự động đồng bộ nếu schema có sự thay đổi (thêm/bớt cột)
    on_schema_change='sync_all_columns'
  )
}}

-- Đọc từ bảng silver (đã được làm sạch và join với master data)
with base_events as (
    select *
    from {{ ref('silver_cell_events') }}
    -- Nếu đang chạy chế độ incremental (chỉ load dữ liệu mới),
    -- thì chỉ lấy các event phát sinh trong khoảng thời gian `gold_delta_minutes` (mặc định 30 phút) đổ lại
    {% if is_incremental() %}
      where event_ts >= cast(localtimestamp(6) as timestamp(6)) - interval '{{ var("gold_delta_minutes", 30) }}' minute
    {% endif %}
),

-- Nhóm dữ liệu (Bucketing) theo các khoảng thời gian (windows) 15 phút (900 giây)
bucketed as (
    select
        -- Thuật toán lấy phần nguyên của phép chia cho 900 để đưa timestamp về đầu khung giờ 15 phút
        cast(from_unixtime(floor(to_unixtime(event_ts) / 900) * 900) as timestamp(6)) as window_start,
        cell_id,
        province,
        district,
        region,
        latitude,
        longitude,
        vendor,
        band,
        network_type,
        site_capacity_gbps,
        event_type,
        call_status,
        total_mb,
        latency_ms,
        jitter_ms,
        packet_loss_pct,
        rsrp,
        sinr,
        is_call_failure,
        has_qoe_issue
    from base_events
)

-- Tổng hợp tính toán (Aggregate) các chỉ số KPI theo nhóm trạm BTS và khung giờ
select
    window_start,                                 -- Thời gian bắt đầu chu kỳ 15 phút
    window_start + interval '15' minute as window_end, -- Thời gian kết thúc chu kỳ
    cell_id,
    province,
    district,
    region,
    latitude,
    longitude,
    vendor,
    band,
    network_type,
    count(*) as total_events,                     -- Tổng số lượng kết nối mạng (cả thoại và data)
    count_if(event_type = 'VOICE') as total_calls,-- Tổng số cuộc gọi
    count_if(event_type = 'DATA') as total_data_sessions, -- Tổng số phiên kết nối internet
    count_if(is_call_failure) as dropped_calls,   -- Tổng số lượng cuộc gọi bị lỗi/rớt
    count_if(has_qoe_issue) as qoe_issue_events,  -- Tổng số phiên dùng mạng bị trải nghiệm kém
    round(coalesce(sum(total_mb), 0.0) / 1024.0, 4) as data_traffic_gb, -- Đổi tổng lưu lượng từ MB sang GB
    round(avg(latency_ms), 2) as avg_latency_ms,  -- Độ trễ trung bình
    round(avg(jitter_ms), 2) as avg_jitter_ms,    -- Độ dao động tín hiệu trung bình
    round(avg(packet_loss_pct), 3) as avg_packet_loss_pct, -- Tỷ lệ mất gói tin trung bình
    round(avg(rsrp), 2) as avg_rsrp,              -- Cường độ sóng trung bình
    round(avg(sinr), 2) as avg_sinr,              -- Tỷ lệ tín hiệu trên nhiễu trung bình
    -- KPI: Tỷ lệ rớt cuộc gọi (%)
    round(
        100.0 * count_if(is_call_failure) / nullif(count_if(event_type = 'VOICE'), 0),
        3
    ) as drop_call_rate_pct,
    -- KPI: Tỷ lệ người dùng bị trải nghiệm tồi (%)
    round(
        100.0 * count_if(has_qoe_issue) / nullif(count(*), 0),
        3
    ) as qoe_issue_rate_pct,
    -- Tính điểm quá tải (Cell Load Score) theo công thức kết hợp lưu lượng data và số lỗi thoại
    -- Điểm tối đa được khống chế ở mức 100 bằng hàm least
    round(
        least(
            100.0,
            (
                coalesce(sum(total_mb), 0.0) / greatest(site_capacity_gbps * 1024.0, 1.0) * 100.0
            )
            + count_if(is_call_failure) * 2.0
            + count_if(has_qoe_issue) * 1.5
        ),
        3
    ) as cell_load_score,
    cast(localtimestamp(6) as timestamp(6)) as updated_at
from bucketed
group by
    window_start,
    cell_id,
    province,
    district,
    region,
    latitude,
    longitude,
    vendor,
    band,
    network_type,
    site_capacity_gbps
