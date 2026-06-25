{{
  config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dashboard', 'billing']
  )
}}

/*
  Plan Revenue Impact Analysis (Phân tích ảnh hưởng chất lượng mạng đến doanh thu gói cước)
  Thống kê tổng hợp bảng Customer 360 theo từng nhóm Gói Cước để tính toán:
    - Tổng số lượng thuê bao và doanh thu hàng tháng trên mỗi gói.
    - Có bao nhiêu thuê bao đang bị ảnh hưởng bởi các trạm lỗi nặng (CRITICAL/WARNING).
    - Ước tính mức doanh thu rủi ro (có nguy cơ mất nếu khách hàng rời mạng do trải nghiệm kém).
*/

-- Lấy nguồn dữ liệu từ góc nhìn Customer 360 (Bảng gold)
with customer_data as (
    select *
    from {{ ref('gold_customer_360') }}
),

-- Nhóm dữ liệu theo từng ID gói cước
plan_aggregates as (
    select
        plan_id,
        plan_name,
        plan_type,
        monthly_fee,
        data_quota_gb,
        voice_minutes,

        -- Đếm số lượng thuê bao
        count(*) as subscriber_count,
        count_if(home_cell_severity = 'CRITICAL') as critical_cell_subscribers, -- Thuê bao ở trạm nguy hiểm
        count_if(home_cell_severity = 'WARNING')  as warning_cell_subscribers,  -- Thuê bao ở trạm bị cảnh báo
        count_if(home_cell_severity = 'NORMAL' or home_cell_severity is null) as healthy_cell_subscribers, -- Thuê bao an toàn

        -- Tính tổng doanh thu
        round(cast(count(*) as double) * monthly_fee, 2) as total_monthly_revenue,
        -- Tính doanh thu nằm trong vùng có nguy cơ rời mạng cao
        round(cast(count_if(home_cell_severity = 'CRITICAL') as double) * monthly_fee, 2) as critical_revenue_at_risk,
        round(cast(count_if(home_cell_severity in ('CRITICAL', 'WARNING')) as double) * monthly_fee, 2) as total_revenue_at_risk,

        -- Đếm số lượng theo mức độ rủi ro rời bỏ nhà mạng (Churn Risk)
        count_if(churn_risk_tier = 'HIGH_RISK')   as high_risk_subscribers,
        count_if(churn_risk_tier = 'MEDIUM_RISK') as medium_risk_subscribers,

        -- Trung bình chất lượng mạng của những khách hàng đang dùng gói cước này
        round(avg(home_cell_avg_latency), 2) as avg_subscriber_latency_ms,
        round(avg(home_cell_drop_rate), 3)   as avg_subscriber_drop_rate,
        round(avg(customer_impact_score), 2) as avg_impact_score,

        cast(localtimestamp(6) as timestamp(6)) as updated_at

    from customer_data
    -- Nhóm (GROUP BY) theo đặc tả cấu hình của gói cước
    group by plan_id, plan_name, plan_type, monthly_fee, data_quota_gb, voice_minutes
)

-- Trả về bảng phân tích và thêm vào một số chỉ số tính toán cấp độ plan (gói cước)
select
    *,
    -- Tính phần trăm tỷ lệ doanh thu đang đối diện với rủi ro so với tổng doanh thu
    round(
        coalesce(total_revenue_at_risk, 0) /
        nullif(total_monthly_revenue, 0) * 100,
    2) as revenue_at_risk_pct,

    -- Phân loại trạng thái sức khỏe tài chính cho gói cước
    case
        -- Nếu hơn 20% doanh thu nằm ở khu vực trạm lỗi nguy cấp -> Đánh giá gói cước này có rủi ro NGUY CẤP
        when coalesce(critical_revenue_at_risk, 0) / nullif(total_monthly_revenue, 0) > 0.20
            then 'CRITICAL'
        -- Nếu hơn 15% tổng doanh thu rủi ro (Critical+Warning) -> Đánh giá CẢNH BÁO
        when coalesce(total_revenue_at_risk, 0) / nullif(total_monthly_revenue, 0) > 0.15
            then 'WARNING'
        -- Ngoài ra là khỏe mạnh
        else 'HEALTHY'
    end as plan_health_status

from plan_aggregates
