-- Cấu hình tạo model dưới dạng view ảo (view) đặt tại schema 'staging'
{{
  config(
    materialized='view',
    schema='staging'
  )
}}

-- CTE đọc dữ liệu từ bảng người dùng gốc
with source_subscribers as (
    select *
    -- Tham chiếu đến bảng 'subscribers' tại dữ liệu thô (bronze)
    from {{ source('bronze_billing', 'subscribers') }}
    -- Bỏ qua các người dùng đã bị xóa (soft-delete qua Debezium CDC)
    where is_deleted = false
),

-- CTE chuẩn hóa và ép kiểu dữ liệu an toàn
normalized as (
    select
        cast(subscriber_id as varchar) as subscriber_id, -- Mã định danh thuê bao (số điện thoại)
        cast(full_name as varchar)     as full_name,     -- Họ và tên đầy đủ
        cast(plan_id as varchar)       as plan_id,       -- Khóa ngoại nối tới gói cước
        upper(cast(home_cell_id as varchar)) as home_cell_id, -- Mã trạm BTS gốc, chuẩn hóa thành VIẾT HOA
        lower(cast(status as varchar)) as status,        -- Trạng thái thuê bao, chuẩn hóa thành viết thường
        cast(activated_at as timestamp(6)) as activated_at, -- Thời gian kích hoạt
        cast(updated_at as timestamp(6))   as updated_at,   -- Thời gian thay đổi gần nhất
        cast(ingested_at as timestamp(6))  as ingested_at   -- Thời điểm dữ liệu cập bến Iceberg
    from source_subscribers
    -- Lọc bỏ các dòng thiếu thông tin định danh và giới hạn các trạng thái hợp lệ
    where subscriber_id is not null
      and status in ('active', 'suspended', 'terminated')
),

-- Khử trùng lặp: Chỉ giữ lại bản ghi mới nhất cho mỗi thuê bao
deduped as (
    select *,
        row_number() over (partition by subscriber_id order by updated_at desc) as rn
    from normalized
)

-- Trả ra output đã qua các bước làm sạch ban đầu
select 
    subscriber_id,
    full_name,
    plan_id,
    home_cell_id,
    status,
    activated_at,
    updated_at,
    ingested_at
from deduped 
where rn = 1
