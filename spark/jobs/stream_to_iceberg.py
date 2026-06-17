import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, hour, to_date, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


CATALOG = env("ICEBERG_CATALOG", "iceberg")
TOPIC = env("ONEHOUSE_KAFKA_TOPIC", "telecom.raw_logs")
BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
CHECKPOINT_LOCATION = env(
    "SPARK_CHECKPOINT_LOCATION",
    "/tmp/onehouse/checkpoints/bronze_telecom_events",
)
BRONZE_TABLE = "bronze.telecom_events"


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("subscriber_id", StringType()),
        StructField("cell_id", StringType()),
        StructField("event_type", StringType()),
        StructField("network_type", StringType()),
        StructField("band", StringType()),
        StructField("device_type", StringType()),
        StructField("call_status", StringType()),
        StructField("call_duration_sec", IntegerType()),
        StructField("drop_reason", StringType()),
        StructField("download_mb", DoubleType()),
        StructField("upload_mb", DoubleType()),
        StructField("latency_ms", DoubleType()),
        StructField("jitter_ms", DoubleType()),
        StructField("packet_loss_pct", DoubleType()),
        StructField("rsrp", DoubleType()),
        StructField("sinr", DoubleType()),
        StructField("source", StringType()),
    ]
)


def create_bronze_table(spark: SparkSession) -> None:
    spark.sql(f"USE {CATALOG}")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            event_id STRING,
            event_ts TIMESTAMP,
            subscriber_id STRING,
            cell_id STRING,
            event_type STRING,
            network_type STRING,
            band STRING,
            device_type STRING,
            call_status STRING,
            call_duration_sec INT,
            drop_reason STRING,
            download_mb DOUBLE,
            upload_mb DOUBLE,
            latency_ms DOUBLE,
            jitter_ms DOUBLE,
            packet_loss_pct DOUBLE,
            rsrp DOUBLE,
            sinr DOUBLE,
            source STRING,
            kafka_key STRING,
            kafka_timestamp TIMESTAMP,
            raw_payload STRING,
            ingested_at TIMESTAMP,
            event_date DATE,
            event_hour INT
        )
        USING iceberg
        PARTITIONED BY (days(event_ts), network_type)
        TBLPROPERTIES (
            'format-version'='2',
            'write.target-file-size-bytes'='134217728'
        )
        """
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("onehouse-bronze-stream")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    create_bronze_table(spark)

    kafka_rows = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = kafka_rows.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_payload"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"),
    ).select(
        "event.*",
        "kafka_key",
        "kafka_timestamp",
        "raw_payload",
    )

    bronze = (
        parsed.withColumn("event_ts", to_timestamp("event_ts"))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("event_date", to_date("event_ts"))
        .withColumn("event_hour", hour("event_ts"))
        .where(col("event_id").isNotNull())
        .withWatermark("event_ts", "10 minutes")
        .dropDuplicates(["event_id"])
        .select(
            "event_id",
            "event_ts",
            "subscriber_id",
            "cell_id",
            "event_type",
            "network_type",
            "band",
            "device_type",
            "call_status",
            "call_duration_sec",
            "drop_reason",
            "download_mb",
            "upload_mb",
            "latency_ms",
            "jitter_ms",
            "packet_loss_pct",
            "rsrp",
            "sinr",
            "source",
            "kafka_key",
            "kafka_timestamp",
            "raw_payload",
            "ingested_at",
            "event_date",
            "event_hour",
        )
    )

    query = (
        bronze.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="20 seconds")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .toTable(BRONZE_TABLE)
    )

    print(
        f"Streaming Kafka topic {TOPIC} from {BOOTSTRAP_SERVERS} into {BRONZE_TABLE}",
        flush=True,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
