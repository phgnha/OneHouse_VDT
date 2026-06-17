{{ config(materialized='view', schema='gold', tags=['gold', 'dashboard']) }}

with ranked as (
    select
        kpis.*,
        row_number() over (
            partition by cell_id
            order by window_start desc
        ) as recency_rank
    from {{ ref('gold_network_kpis') }} as kpis
),

latest as (
    select *
    from ranked
    where recency_rank = 1
)

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
    case
        when coalesce(drop_call_rate_pct, 0.0) >= 5.0
          or coalesce(qoe_issue_rate_pct, 0.0) >= 18.0
          or coalesce(cell_load_score, 0.0) >= 85.0
            then 'CRITICAL'
        when coalesce(drop_call_rate_pct, 0.0) >= 2.0
          or coalesce(qoe_issue_rate_pct, 0.0) >= 10.0
          or coalesce(cell_load_score, 0.0) >= 65.0
            then 'WARNING'
        else 'NORMAL'
    end as severity,
    updated_at
from latest
