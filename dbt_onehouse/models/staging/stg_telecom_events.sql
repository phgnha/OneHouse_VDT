-- Đọc dữ liệu thô từ Kafka Stream đã được Spark đẩy vào Iceberg Bronze
with source_events as (
    select *
    from {{ source('bronze', 'telecom_events') }}
),

-- Bước làm sạch và chuẩn hóa kiểu dữ liệu (ép kiểu rõ ràng, đồng bộ hóa case)
normalized as (
    select
        cast(event_id as varchar) as event_id,             -- Khóa ngoại của event
        cast(event_ts as timestamp(6)) as event_ts,        -- Thời điểm phát sinh sự kiện
        cast(subscriber_id as varchar) as subscriber_id,   -- Số thuê bao của người dùng
        upper(cast(cell_id as varchar)) as cell_id,        -- Đảm bảo mã trạm BTS viết hoa (để dễ map với master data)
        upper(cast(event_type as varchar)) as event_type,  -- Loại sự kiện: VOICE hay DATA
        upper(cast(network_type as varchar)) as network_type,-- Loại mạng: 4G hay 5G
        upper(cast(band as varchar)) as band,              -- Dải tần số (B3, B7, n78...)
        lower(cast(device_type as varchar)) as device_type,-- Loại thiết bị (android, ios, mifi...), viết thường
        upper(cast(call_status as varchar)) as call_status,-- Trạng thái gọi: COMPLETED, FAILED, DROPPED
        cast(call_duration_sec as integer) as call_duration_sec, -- Thời lượng gọi bằng giây
        lower(cast(drop_reason as varchar)) as drop_reason,-- Nguyên nhân rớt cuộc gọi
        coalesce(cast(download_mb as double), 0.0) as download_mb, -- Khối lượng tải xuống, nếu null gán 0
        coalesce(cast(upload_mb as double), 0.0) as upload_mb,     -- Khối lượng tải lên, nếu null gán 0
        cast(latency_ms as double) as latency_ms,          -- Độ trễ mạng (Ping)
        cast(jitter_ms as double) as jitter_ms,            -- Mức độ dao động độ trễ (Jitter)
        cast(packet_loss_pct as double) as packet_loss_pct,-- Tỷ lệ thất thoát gói tin
        cast(rsrp as double) as rsrp,                      -- Sức mạnh tín hiệu (Signal Power)
        cast(sinr as double) as sinr,                      -- Tỷ lệ nhiễu tín hiệu (Signal-to-Noise Ratio)
        cast(source as varchar) as source,                 -- Nguồn phát sinh log
        cast(kafka_key as varchar) as kafka_key,           -- Khóa dùng trong Kafka Partition
        cast(kafka_timestamp as timestamp(6)) as kafka_timestamp,-- Dấu thời gian khi đưa vào Kafka broker
        cast(raw_payload as varchar) as raw_payload,       -- Payload gốc chưa parse
        cast(ingested_at as timestamp(6)) as ingested_at,  -- Thời điểm được spark ghi nhận xuống data lake
        cast(event_date as date) as event_date,            -- Trường ngày phục vụ partition query
        cast(event_hour as integer) as event_hour          -- Trường giờ phục vụ phân tích theo múi giờ
    from source_events
    -- Yêu cầu tối thiểu để được tính là một record hợp lệ
    where event_id is not null
      and event_ts is not null
)

-- Thêm các cột dẫn xuất (derived columns) phục vụ phân tích sâu hơn
select
    *,
    -- Tính toán tổng lưu lượng Data
    download_mb + upload_mb as total_mb,
    
    -- Cột đánh dấu logic (Boolean flag): True nếu cuộc gọi bị lỗi hoặc bị rớt
    event_type = 'VOICE' and call_status in ('DROPPED', 'FAILED') as is_call_failure,
    
    -- Cột đánh dấu logic (Boolean flag): Phát hiện vấn đề về chất lượng mạng (Quality of Experience - QoE)
    -- Được định nghĩa là: Độ trễ quá cao (>120ms), mất mạng (>2%), sóng quá yếu (<-110) hoặc quá nhiễu (<5)
    (
        coalesce(latency_ms, 0.0) > 120
        or coalesce(packet_loss_pct, 0.0) > 2.0
        or coalesce(rsrp, 0.0) < -110
        or coalesce(sinr, 99.0) < 5
    ) as has_qoe_issue
from normalized
