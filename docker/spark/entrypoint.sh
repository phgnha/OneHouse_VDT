#!/bin/bash
set -e

# Catalog name configurable via env (default: viettel)
CATALOG_NAME="${ICEBERG_CATALOG:-viettel}"

case "$SPARK_MODE" in
  master)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host 0.0.0.0 \
      --port 7077 \
      --webui-port 8080
    ;;
  worker)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --webui-port 8081 \
      "$SPARK_MASTER_URL"
    ;;
  submit)
    exec "$SPARK_HOME/bin/spark-submit" \
      --master "$SPARK_MASTER_URL" \
      --deploy-mode client \
      --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
      --conf "spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog" \
      --conf "spark.sql.catalog.${CATALOG_NAME}.catalog-impl=org.apache.iceberg.rest.RESTCatalog" \
      --conf "spark.sql.catalog.${CATALOG_NAME}.uri=${ICEBERG_REST_URI:-http://iceberg-rest:8181}" \
      --conf "spark.sql.catalog.${CATALOG_NAME}.warehouse=${ICEBERG_WAREHOUSE:-s3://warehouse/}" \
      --conf "spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
      --conf "spark.sql.catalog.${CATALOG_NAME}.s3.endpoint=${S3_ENDPOINT:-http://minio:9000}" \
      --conf "spark.sql.catalog.${CATALOG_NAME}.s3.path-style-access=true" \
      --conf "spark.sql.catalog.${CATALOG_NAME}.s3.access-key-id=${AWS_ACCESS_KEY_ID}" \
      --conf "spark.sql.catalog.${CATALOG_NAME}.s3.secret-access-key=${AWS_SECRET_ACCESS_KEY}" \
      --conf "spark.sql.defaultCatalog=${CATALOG_NAME}" \
      --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT:-http://minio:9000}" \
      --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" \
      --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
      --conf "spark.hadoop.fs.s3a.path.style.access=true" \
      --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
      --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
      --conf "spark.sql.shuffle.partitions=4" \
      ${SPARK_JOB_SCRIPT:-/opt/onehouse/spark/jobs/stream_to_iceberg.py}
    ;;
  *)
    exec "$@"
    ;;
esac
