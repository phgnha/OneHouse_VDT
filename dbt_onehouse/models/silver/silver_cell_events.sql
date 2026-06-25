-- Đánh số thứ tự các dòng dữ liệu bị trùng lặp dựa trên event_id
with ranked_events as (
    select
        events.*,
        -- Dùng hàm Window function (row_number) để xếp hạng các sự kiện có chung event_id
        -- Dòng nào được nạp vào hồ dữ liệu muộn nhất (ingested_at desc) sẽ có row_num = 1
        row_number() over (
            partition by event_id
            order by ingested_at desc
        ) as row_num
    from {{ ref('stg_telecom_events') }} as events
    -- Các điều kiện lọc dữ liệu (Data Cleaning) ban đầu
    where cell_id is not null                   -- Trạm BTS không được trống
      and event_type in ('VOICE', 'DATA')       -- Chỉ lấy luồng thoại hoặc Data
      and network_type in ('4G', '5G')          -- Lọc các chuẩn mạng hợp lệ
      -- Xóa bỏ các dòng event gửi đến từ tương lai bị sai lệch giờ hệ thống (vượt quá hiện tại 5 phút)
      and event_ts <= cast(localtimestamp(6) as timestamp(6)) + interval '5' minute
),

-- CTE dùng để lọc bỏ trùng lặp triệt để
deduplicated as (
    select *
    from ranked_events
    -- Giữ lại đúng 1 dòng mới nhất cho mỗi event_id
    where row_num = 1
),

-- CTE làm nhiệm vụ kết nối (join) và bổ sung (enrich) thông tin từ Master Data
enriched as (
    select
        events.event_id,
        events.event_ts,
        events.subscriber_id,
        events.cell_id,
        bts.province,             -- Bổ sung Tỉnh/Thành
        bts.district,             -- Bổ sung Quận/Huyện
        bts.region,               -- Bổ sung Khu vực
        bts.latitude,             -- Bổ sung Tọa độ Vĩ tuyến
        bts.longitude,            -- Bổ sung Tọa độ Kinh tuyến
        bts.vendor,               -- Bổ sung Nhà cung cấp thiết bị (Nokia, Ericsson...)
        -- Nếu event không có band, lấy band mặc định cấu hình trên trạm (dùng COALESCE)
        coalesce(events.band, upper(bts.band)) as band,
        bts.site_capacity_gbps,   -- Bổ sung sức chứa (Capacity) lý thuyết của trạm
        events.event_type,
        events.network_type,
        events.device_type,
        events.call_status,
        events.call_duration_sec,
        events.drop_reason,
        events.download_mb,
        events.upload_mb,
        events.total_mb,
        events.latency_ms,
        events.jitter_ms,
        events.packet_loss_pct,
        events.rsrp,
        events.sinr,
        events.is_call_failure,
        events.has_qoe_issue,
        events.source,
        events.kafka_timestamp,
        events.ingested_at,
        events.event_date,
        events.event_hour
    from deduplicated as events
    -- INNER JOIN với bảng dữ liệu meta của trạm BTS (seed data đã được dbt đưa vào)
    inner join {{ ref('bts_metadata') }} as bts
        on events.cell_id = upper(bts.cell_id)
)

-- Trả ra tập kết quả làm sạch để tạo bảng Silver
select *
from enriched
