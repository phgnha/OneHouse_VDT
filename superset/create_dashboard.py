import json
import os


def main() -> None:
    from superset.app import create_app
    from superset.extensions import db

    trino_uri = os.getenv("SUPERSET_TRINO_URI", "trino://admin@trino:8080/iceberg/gold")

    app = create_app()
    with app.app_context():
        from superset.connectors.sqla.models import SqlaTable, TableColumn
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        database = (
            db.session.query(Database)
            .filter(Database.database_name == "Trino Iceberg")
            .one_or_none()
        )
        if database is None:
            database = Database(database_name="Trino Iceberg", sqlalchemy_uri=trino_uri)
            db.session.add(database)
        else:
            database.sqlalchemy_uri = trino_uri
        db.session.commit()

        heatmap_dataset = ensure_dataset(
            database=database,
            table_name="gold_cell_heatmap",
            schema="gold",
            columns=[
                "cell_id",
                "province",
                "district",
                "region",
                "latitude",
                "longitude",
                "severity",
                "drop_call_rate_pct",
                "qoe_issue_rate_pct",
                "cell_load_score",
                "data_traffic_gb",
                "window_start",
                "window_end",
            ],
        )
        kpi_dataset = ensure_dataset(
            database=database,
            table_name="gold_network_kpis",
            schema="gold",
            columns=[
                "window_start",
                "cell_id",
                "province",
                "network_type",
                "drop_call_rate_pct",
                "qoe_issue_rate_pct",
                "cell_load_score",
                "data_traffic_gb",
            ],
        )

        heatmap_chart = ensure_chart(
            name="OneHouse BTS Heatmap",
            dataset=heatmap_dataset,
            viz_type="deck_scatter",
            params={
                "adhoc_filters": [],
                "all_columns": [],
                "color_picker": {"r": 255, "g": 87, "b": 34, "a": 0.85},
                "datasource": f"{heatmap_dataset.id}__table",
                "dimension": "severity",
                "js_columns": ["cell_id", "province", "district", "severity"],
                "latlong": ["latitude", "longitude"],
                "mapbox_style": "mapbox://styles/mapbox/light-v9",
                "metric": {
                    "aggregate": "AVG",
                    "column": {"column_name": "cell_load_score"},
                    "expressionType": "SIMPLE",
                    "label": "Avg Cell Load Score"
                },
                "point_radius_fixed": {"type": "fix", "value": 1200},
                "row_limit": 1000,
                "time_range": "No filter",
                "viewport": {
                    "latitude": 15.9,
                    "longitude": 106.1,
                    "zoom": 4.9,
                    "bearing": 0,
                    "pitch": 0,
                },
            },
        )

        trend_chart = ensure_chart(
            name="Drop Call Rate by Cell",
            dataset=kpi_dataset,
            viz_type="echarts_timeseries_line",
            params={
                "adhoc_filters": [],
                "datasource": f"{kpi_dataset.id}__table",
                "groupby": ["cell_id"],
                "metrics": [
                    {
                        "aggregate": "AVG",
                        "column": {"column_name": "drop_call_rate_pct"},
                        "expressionType": "SIMPLE",
                        "label": "Avg Drop Call Rate"
                    }
                ],
                "row_limit": 10000,
                "time_grain_sqla": "PT15M",
                "time_range": "No filter",
                "x_axis": "window_start",
            },
        )

        dashboard = (
            db.session.query(Dashboard)
            .filter(Dashboard.dashboard_title == "OneHouse Network Experience")
            .one_or_none()
        )
        if dashboard is None:
            dashboard = Dashboard(dashboard_title="OneHouse Network Experience")
            db.session.add(dashboard)
            db.session.flush()

        dashboard.slices = [heatmap_chart, trend_chart]
        dashboard.position_json = json.dumps(
            {
                "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
                "GRID_ID": {
                    "type": "GRID",
                    "id": "GRID_ID",
                    "children": ["ROW_1", "ROW_2"],
                    "parents": ["ROOT_ID"],
                },
                "ROW_1": {
                    "type": "ROW",
                    "id": "ROW_1",
                    "children": ["CHART_HEATMAP"],
                    "parents": ["ROOT_ID", "GRID_ID"],
                },
                "ROW_2": {
                    "type": "ROW",
                    "id": "ROW_2",
                    "children": ["CHART_TREND"],
                    "parents": ["ROOT_ID", "GRID_ID"],
                },
                "CHART_HEATMAP": {
                    "type": "CHART",
                    "id": "CHART_HEATMAP",
                    "children": [],
                    "parents": ["ROOT_ID", "GRID_ID", "ROW_1"],
                    "meta": {"chartId": heatmap_chart.id, "height": 64, "width": 12},
                },
                "CHART_TREND": {
                    "type": "CHART",
                    "id": "CHART_TREND",
                    "children": [],
                    "parents": ["ROOT_ID", "GRID_ID", "ROW_2"],
                    "meta": {"chartId": trend_chart.id, "height": 44, "width": 12},
                },
            }
        )
        db.session.commit()
        print("Superset OneHouse dashboard is ready")


def ensure_dataset(database, table_name, schema, columns):
    from superset.extensions import db
    from superset.connectors.sqla.models import SqlaTable, TableColumn

    dataset = (
        db.session.query(SqlaTable)
        .filter(
            SqlaTable.database_id == database.id,
            SqlaTable.table_name == table_name,
            SqlaTable.schema == schema,
        )
        .one_or_none()
    )
    if dataset is None:
        dataset = SqlaTable(table_name=table_name, schema=schema, database=database)
        db.session.add(dataset)
        db.session.flush()

    existing = {column.column_name for column in dataset.columns}
    for column_name in columns:
        if column_name not in existing:
            db.session.add(TableColumn(column_name=column_name, type="VARCHAR", table=dataset))
    db.session.commit()
    return dataset


def ensure_chart(name, dataset, viz_type, params):
    from superset.extensions import db
    from superset.models.slice import Slice

    chart = db.session.query(Slice).filter(Slice.slice_name == name).one_or_none()
    if chart is None:
        chart = Slice(slice_name=name)
        db.session.add(chart)

    chart.viz_type = viz_type
    chart.datasource_id = dataset.id
    chart.datasource_type = "table"
    chart.params = json.dumps(params)
    db.session.commit()
    return chart


if __name__ == "__main__":
    main()
