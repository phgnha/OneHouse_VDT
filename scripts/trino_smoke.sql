show catalogs;
show schemas from iceberg;
show tables from iceberg.gold;

select
    cell_id,
    province,
    district,
    severity,
    drop_call_rate_pct,
    qoe_issue_rate_pct,
    cell_load_score,
    data_traffic_gb
from iceberg.gold.gold_cell_heatmap
order by cell_load_score desc
limit 20;
