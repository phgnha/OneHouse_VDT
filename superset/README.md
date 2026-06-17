This folder contains Superset bootstrap assets.

The container startup script:

1. Migrates the Superset metadata database.
2. Creates the admin user from environment variables.
3. Registers the Trino Iceberg database URI.
4. Seeds a dashboard named `OneHouse Network Experience`.

If the internal Superset chart API changes, the service still starts. Create the
same dashboard manually from these datasets:

- `gold.gold_cell_heatmap` for the geographical heatmap.
- `gold.gold_network_kpis` for 15-minute KPI trends.
