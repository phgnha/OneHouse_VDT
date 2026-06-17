with source_events as (
    select *
    from {{ source('bronze', 'telecom_events') }}
),

normalized as (
    select
        cast(event_id as varchar) as event_id,
        cast(event_ts as timestamp(6)) as event_ts,
        cast(subscriber_id as varchar) as subscriber_id,
        upper(cast(cell_id as varchar)) as cell_id,
        upper(cast(event_type as varchar)) as event_type,
        upper(cast(network_type as varchar)) as network_type,
        upper(cast(band as varchar)) as band,
        lower(cast(device_type as varchar)) as device_type,
        upper(cast(call_status as varchar)) as call_status,
        cast(call_duration_sec as integer) as call_duration_sec,
        lower(cast(drop_reason as varchar)) as drop_reason,
        coalesce(cast(download_mb as double), 0.0) as download_mb,
        coalesce(cast(upload_mb as double), 0.0) as upload_mb,
        cast(latency_ms as double) as latency_ms,
        cast(jitter_ms as double) as jitter_ms,
        cast(packet_loss_pct as double) as packet_loss_pct,
        cast(rsrp as double) as rsrp,
        cast(sinr as double) as sinr,
        cast(source as varchar) as source,
        cast(kafka_key as varchar) as kafka_key,
        cast(kafka_timestamp as timestamp(6)) as kafka_timestamp,
        cast(raw_payload as varchar) as raw_payload,
        cast(ingested_at as timestamp(6)) as ingested_at,
        cast(event_date as date) as event_date,
        cast(event_hour as integer) as event_hour
    from source_events
    where event_id is not null
      and event_ts is not null
)

select
    *,
    download_mb + upload_mb as total_mb,
    event_type = 'VOICE' and call_status in ('DROPPED', 'FAILED') as is_call_failure,
    (
        coalesce(latency_ms, 0.0) > 120
        or coalesce(packet_loss_pct, 0.0) > 2.0
        or coalesce(rsrp, 0.0) < -110
        or coalesce(sinr, 99.0) < 5
    ) as has_qoe_issue
from normalized
