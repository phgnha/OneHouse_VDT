{{
  -- Cấu hình tạo bảng vật lý hoàn chỉnh ở lớp Gold
  -- Bảng này gom chung dữ liệu từ nhiều domain nên thích hợp làm nguồn cho Dashboard
  config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dashboard', 'billing']
  )
}}

/*
  Customer 360 — Bảng tổng hợp đa chiều về khách hàng (Cross-domain JOIN) giữa:
    1. Thông tin thuê bao (Từ CSDL thanh toán Billing - CDC)
    2. Thông tin gói cước (Từ CSDL thanh toán Billing - CDC)
    3. Chất lượng mạng lưới tại trạm gốc của khách hàng (Từ dữ liệu viễn thông Telemetry - bảng heatmap)
    4. Dữ liệu meta trạm BTS (seed data)

  Mục đích: Phát hiện các khách hàng đang trả tiền nhiều (VIP) nhưng lại đang sử dụng dịch vụ ở các trạm BTS kém chất lượng, từ đó ưu tiên chăm sóc.
*/

-- Lấy danh sách những người dùng còn đang kích hoạt
with active_subscribers as (
    select *
    from {{ ref('stg_subscribers') }}
    where status = 'active'
),

-- Lấy danh mục các gói cước đang có
plans as (
    select *
    from {{ ref('stg_billing_plans') }}
),

-- Lấy thông tin hiệu năng hiện tại của trạm thu phát sóng (từ bảng Heatmap đã tính trước)
cell_quality as (
    select
        cell_id,
        province,
        district,
        latitude,
        longitude,
        severity,
        drop_call_rate_pct,
        qoe_issue_rate_pct,
        cell_load_score,
        avg_latency_ms,
        data_traffic_gb,
        total_calls,
        total_data_sessions
    from {{ ref('gold_cell_heatmap') }}
),

-- Kết hợp tất cả lại để tạo ra góc nhìn 360 độ về một khách hàng
customer_360 as (
    select
        -- Thông tin cơ bản người dùng
        sub.subscriber_id,
        sub.full_name,
        sub.status,
        sub.activated_at,

        -- Thông tin cước phí và gói dịch vụ
        plans.plan_id,
        plans.plan_name,
        plans.plan_type,
        plans.monthly_fee,
        plans.data_quota_gb,
        plans.voice_minutes,

        -- Vị trí hoạt động thường xuyên (home cell)
        sub.home_cell_id,
        cq.province,
        cq.district,
        cq.latitude,
        cq.longitude,

        -- Chất lượng mạng viễn thông tại trạm của người dùng
        cq.severity           as home_cell_severity,
        cq.drop_call_rate_pct as home_cell_drop_rate,
        cq.qoe_issue_rate_pct as home_cell_qoe_issue_rate,
        cq.cell_load_score    as home_cell_load_score,
        cq.avg_latency_ms     as home_cell_avg_latency,
        cq.data_traffic_gb    as home_cell_traffic_gb,

        -- Thuộc tính dẫn xuất: Điểm tác động khách hàng (Customer Impact Score)
        -- Công thức dựa trên sự kết hợp giữa: Doanh thu tạo ra, Mức độ nghẽn trạm và Cấp độ nguy hiểm
        round(
            (coalesce(plans.monthly_fee, 0) / 100000.0)
            * (coalesce(cq.cell_load_score, 0) / 100.0)
            * case
                when cq.severity = 'CRITICAL' then 3.0
                when cq.severity = 'WARNING'  then 1.5
                else 0.5
              end
            * 100,
        2) as customer_impact_score,

        -- Phân loại mức độ rủi ro mất khách hàng (Churn risk tier)
        case
            when cq.severity = 'CRITICAL' and plans.monthly_fee >= 300000 then 'HIGH_RISK'
            when cq.severity = 'CRITICAL' or  plans.monthly_fee >= 500000 then 'MEDIUM_RISK'
            when cq.severity = 'WARNING'  and plans.monthly_fee >= 200000 then 'MEDIUM_RISK'
            else 'LOW_RISK'
        end as churn_risk_tier,

        cast(localtimestamp(6) as timestamp(6)) as updated_at

    from active_subscribers as sub
    -- Nối với bảng gói cước
    inner join plans
        on sub.plan_id = plans.plan_id
    -- Nối với bảng đo chất lượng trạm BTS (Dùng LEFT JOIN phòng khi BTS mới lên sóng chưa có KPI)
    left join cell_quality as cq
        on sub.home_cell_id = cq.cell_id
)

select * from customer_360
