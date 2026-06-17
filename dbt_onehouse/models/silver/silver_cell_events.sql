with ranked_events as (
    select
        events.*,
        row_number() over (
            partition by event_id
            order by ingested_at desc
        ) as row_num
    from {{ ref('stg_telecom_events') }} as events
    where cell_id is not null
      and event_type in ('VOICE', 'DATA')
      and network_type in ('4G', '5G')
      and event_ts <= cast(localtimestamp(6) as timestamp(6)) + interval '5' minute
),

deduplicated as (
    select *
    from ranked_events
    where row_num = 1
),

enriched as (
    select
        events.event_id,
        events.event_ts,
        events.subscriber_id,
        events.cell_id,
        bts.province,
        bts.district,
        bts.region,
        bts.latitude,
        bts.longitude,
        bts.vendor,
        coalesce(events.band, upper(bts.band)) as band,
        bts.site_capacity_gbps,
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
    inner join {{ ref('bts_metadata') }} as bts
        on events.cell_id = upper(bts.cell_id)
)

select *
from enriched
