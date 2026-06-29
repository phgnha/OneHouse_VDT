-- Cấu hình dbt cho model này: 
-- Vật lý hóa (materialized) dưới dạng 'view' (không lưu data ra bảng vật lý mới, truy vấn trực tiếp)
-- Lưu trong schema 'staging'
{{
  config(
    materialized='view',
    schema='staging'
  )
}}

-- Khối CTE (Common Table Expression) đọc dữ liệu gốc
with source_plans as (
    select *
    -- Trỏ đến bảng nguồn 'billing_plans' nằm trong nguồn hệ thống 'bronze_billing' (catalog bronze)
    from {{ source('bronze_billing', 'billing_plans') }}
    -- Chỉ lấy các gói cước đang còn hiệu lực, lọc bỏ các dòng bị xóa logic (is_deleted = true từ Debezium CDC)
    where is_deleted = false
),

-- Khối CTE dùng để chuẩn hóa kiểu dữ liệu (Data Type Casting) và định dạng lại nội dung
normalized as (
    select
        cast(plan_id as varchar)    as plan_id,        -- Mã gói cước chuyển về chuỗi
        cast(plan_name as varchar)  as plan_name,      -- Tên gói cước chuyển về chuỗi
        lower(cast(plan_type as varchar)) as plan_type,-- Loại gói cước, chuyển về chữ thường để đồng nhất
        cast(monthly_fee as double) as monthly_fee,    -- Phí duy trì hàng tháng chuyển về dạng số thập phân kép
        cast(data_quota_gb as double) as data_quota_gb,-- Dung lượng data theo GB dạng số kép
        cast(voice_minutes as integer) as voice_minutes,-- Phút gọi nội mạng/ngoại mạng dạng số nguyên
        cast(created_at as timestamp(6)) as created_at, -- Chuẩn hóa kiểu timestamp có phần ngàn mili-giây
        cast(updated_at as timestamp(6)) as updated_at, -- Chuẩn hóa kiểu timestamp
        cast(ingested_at as timestamp(6)) as ingested_at-- Thời điểm đưa vào Data Lake
    from source_plans
    -- Quy tắc kiểm tra tính hợp lệ cơ bản: Mã gói không được trống, và phí hàng tháng không được âm
    where plan_id is not null
      and monthly_fee >= 0
)

-- Trả về dữ liệu cuối cùng đã được chuẩn hóa
select * from normalized
