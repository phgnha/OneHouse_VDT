{{
  config(
    materialized='incremental',
    unique_key=['window_start', 'cell_id', 'network_type'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

with base_events as (
    select *
    from {{ ref('silver_cell_events') }}
    {% if is_incremental() %}
      where event_ts >= cast(localtimestamp(6) as timestamp(6)) - interval '{{ var("gold_delta_minutes", 30) }}' minute
    {% endif %}
),

bucketed as (
    select
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

select
    window_start,
    window_start + interval '15' minute as window_end,
    cell_id,
    province,
    district,
    region,
    latitude,
    longitude,
    vendor,
    band,
    network_type,
    count(*) as total_events,
    count_if(event_type = 'VOICE') as total_calls,
    count_if(event_type = 'DATA') as total_data_sessions,
    count_if(is_call_failure) as dropped_calls,
    count_if(has_qoe_issue) as qoe_issue_events,
    round(coalesce(sum(total_mb), 0.0) / 1024.0, 4) as data_traffic_gb,
    round(avg(latency_ms), 2) as avg_latency_ms,
    round(avg(jitter_ms), 2) as avg_jitter_ms,
    round(avg(packet_loss_pct), 3) as avg_packet_loss_pct,
    round(avg(rsrp), 2) as avg_rsrp,
    round(avg(sinr), 2) as avg_sinr,
    round(
        100.0 * count_if(is_call_failure) / nullif(count_if(event_type = 'VOICE'), 0),
        3
    ) as drop_call_rate_pct,
    round(
        100.0 * count_if(has_qoe_issue) / nullif(count(*), 0),
        3
    ) as qoe_issue_rate_pct,
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
