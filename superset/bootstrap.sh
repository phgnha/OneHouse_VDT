#!/usr/bin/env bash
set -euo pipefail

export SUPERSET_CONFIG_PATH=/app/onehouse/superset/superset_config.py

superset db upgrade
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname OneHouse \
  --lastname Admin \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true
superset init

superset set-database-uri \
  -d "Trino Iceberg" \
  -u "${SUPERSET_TRINO_URI:-trino://admin@trino:8080/iceberg/gold}" || true

python /app/onehouse/superset/create_dashboard.py || true

exec superset run -h 0.0.0.0 -p 8088 --with-threads
