# OneHouse Data Contract

## Kafka event fields

| Field | Type | Notes |
| --- | --- | --- |
| event_id | string | Unique source event ID. |
| event_ts | timestamp string | UTC event time. |
| subscriber_id | string | Simulated mobile subscriber. |
| cell_id | string | BTS cell key. |
| event_type | string | `VOICE` or `DATA`. |
| network_type | string | `4G` or `5G`. |
| band | string | LTE/NR band. |
| device_type | string | Simulated device category. |
| call_status | string | `COMPLETED`, `DROPPED`, `FAILED`, or null for data sessions. |
| call_duration_sec | integer | Voice only. |
| drop_reason | string | Voice failure reason. |
| download_mb | double | Data volume. |
| upload_mb | double | Data volume. |
| latency_ms | double | Experience metric. |
| jitter_ms | double | Experience metric. |
| packet_loss_pct | double | Experience metric. |
| rsrp | double | Radio signal strength. |
| sinr | double | Signal quality. |

## KPI definitions

`drop_call_rate_pct = dropped_calls / total_calls * 100`

`data_traffic_gb = sum(download_mb + upload_mb) / 1024`

`qoe_issue_rate_pct = qoe_issue_events / total_events * 100`

`cell_load_score` is a bounded operational score combining traffic pressure,
call failures, and QoE issue count. It is intended for ranking hotspots, not as a
network-standard KPI.
