"""CDC Billing Stream — Spark Structured Streaming with MERGE INTO (Upsert).

Reads CDC events from Debezium via Redpanda topics:
  - billing.public.billing_plans
  - billing.public.subscribers

Uses foreachBatch + MERGE INTO (Iceberg Format V2) for Upsert semantics,
unlike the telemetry stream which uses simple Append.
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp, get_json_object
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def env(name: str, default: str) -> str:
    """Hàm tiện ích để lấy giá trị biến môi trường, trả về giá trị mặc định nếu biến không tồn tại."""
    return os.getenv(name, default)


# Cấu hình danh mục (Catalog) của Iceberg, thông tin Kafka Broker và thư mục lưu Checkpoint của Spark
CATALOG = env("ICEBERG_CATALOG", "viettel")
BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
CHECKPOINT_BASE = env(
    "SPARK_CHECKPOINT_LOCATION",
    "s3a://warehouse/checkpoints",
)

# Tên các topic Redpanda chứa dữ liệu CDC (đã được làm phẳng nhờ Debezium ExtractNewRecordState)
PLANS_TOPIC = "billing.public.billing_plans"
SUBSCRIBERS_TOPIC = "billing.public.subscribers"

# Tên các bảng đích Iceberg ở lớp Bronze
PLANS_TABLE = "bronze.billing_plans"
SUBSCRIBERS_TABLE = "bronze.subscribers"

# Schema (cấu trúc dữ liệu) cấu hình cho bảng billing_plans khớp với cấu trúc trong DB PostgreSQL
PLAN_SCHEMA = StructType([
    StructField("plan_id", StringType()),
    StructField("plan_name", StringType()),
    StructField("plan_type", StringType()),
    StructField("monthly_fee", DoubleType()),
    StructField("data_quota_gb", DoubleType()),
    StructField("voice_minutes", IntegerType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
    StructField("__deleted", StringType()),  # Cờ cho biết bản ghi đã bị xóa (Debezium rewrite mode)
])

# Schema cấu hình cho bảng subscribers khớp với cấu trúc dữ liệu người dùng
SUBSCRIBER_SCHEMA = StructType([
    StructField("subscriber_id", StringType()),
    StructField("full_name", StringType()),
    StructField("plan_id", StringType()),
    StructField("home_cell_id", StringType()),
    StructField("status", StringType()),
    StructField("activated_at", StringType()),
    StructField("updated_at", StringType()),
    StructField("__deleted", StringType()),  # Cờ trạng thái xóa
])


def create_bronze_tables(spark: SparkSession) -> None:
    """Tạo mới (hoặc bỏ qua nếu đã tồn tại) các bảng bronze hỗ trợ tính năng Upsert.
    
    Yêu cầu Iceberg Format V2 và thiết lập merge-on-read cho các tác vụ cập nhật/xóa.
    """
    spark.sql(f"USE {CATALOG}")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")

    # Bảng gói cước: Không chia partition vì số lượng thường nhỏ và ít thay đổi
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {PLANS_TABLE} (
            plan_id       STRING,
            plan_name     STRING,
            plan_type     STRING,
            monthly_fee   DOUBLE,
            data_quota_gb DOUBLE,
            voice_minutes INT,
            created_at    TIMESTAMP,
            updated_at    TIMESTAMP,
            is_deleted    BOOLEAN,
            ingested_at   TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version'='2',
            'write.merge.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.delete.mode'='merge-on-read'
        )
    """)

    # Bảng thuê bao: Phân mảnh dữ liệu (partitioning) theo trạng thái (status)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SUBSCRIBERS_TABLE} (
            subscriber_id STRING,
            full_name     STRING,
            plan_id       STRING,
            home_cell_id  STRING,
            status        STRING,
            activated_at  TIMESTAMP,
            updated_at    TIMESTAMP,
            is_deleted    BOOLEAN,
            ingested_at   TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (status)
        TBLPROPERTIES (
            'format-version'='2',
            'write.merge.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.delete.mode'='merge-on-read'
        )
    """)


def merge_billing_plans(batch_df: DataFrame, batch_id: int) -> None:
    """Cập nhật (Upsert) dữ liệu cho bảng billing_plans trong từng mini-batch bằng cú pháp MERGE INTO.
    
    Xử lý insert dòng mới, update dòng cũ, hoặc delete nếu có cờ `__deleted`.
    """
    if batch_df.isEmpty():
        return

    # Tạo temporary view để có thể thực thi truy vấn SQL từ dataframe
    view_name = f"plans_batch_{batch_id}"
    batch_df.createOrReplaceTempView(view_name)

    # MERGE INTO so khớp bằng `plan_id`
    batch_df.sparkSession.sql(f"""
        MERGE INTO {PLANS_TABLE} AS target
        USING {view_name} AS source
        ON target.plan_id = source.plan_id
        WHEN MATCHED AND source.is_deleted = true THEN DELETE
        WHEN MATCHED THEN UPDATE SET
            plan_name     = source.plan_name,
            plan_type     = source.plan_type,
            monthly_fee   = source.monthly_fee,
            data_quota_gb = source.data_quota_gb,
            voice_minutes = source.voice_minutes,
            created_at    = source.created_at,
            updated_at    = source.updated_at,
            is_deleted    = source.is_deleted,
            ingested_at   = source.ingested_at
        WHEN NOT MATCHED THEN INSERT *
    """)


def merge_subscribers(batch_df: DataFrame, batch_id: int) -> None:
    """Cập nhật (Upsert) dữ liệu cho bảng subscribers trong từng mini-batch bằng cú pháp MERGE INTO."""
    if batch_df.isEmpty():
        return

    # Tạo temporary view cho dataframe hiện hành
    view_name = f"subs_batch_{batch_id}"
    batch_df.createOrReplaceTempView(view_name)

    # MERGE INTO so khớp bằng khóa `subscriber_id`
    batch_df.sparkSession.sql(f"""
        MERGE INTO {SUBSCRIBERS_TABLE} AS target
        USING {view_name} AS source
        ON target.subscriber_id = source.subscriber_id
        WHEN MATCHED AND source.is_deleted = true THEN DELETE
        WHEN MATCHED THEN UPDATE SET
            full_name    = source.full_name,
            plan_id      = source.plan_id,
            home_cell_id = source.home_cell_id,
            status       = source.status,
            activated_at = source.activated_at,
            updated_at   = source.updated_at,
            is_deleted   = source.is_deleted,
            ingested_at  = source.ingested_at
        WHEN NOT MATCHED THEN INSERT *
    """)


def build_cdc_stream(
    spark: SparkSession,
    topic: str,
    schema: StructType,
    table: str,
    merge_fn,
    checkpoint_suffix: str,
):
    """Khởi tạo một luồng Streaming Query đọc CDC events từ Kafka và MERGE vào Iceberg Table.
    
    Args:
        spark: Phiên bản (session) Spark.
        topic: Tên Kafka topic chứa CDC data.
        schema: Cấu trúc dữ liệu mong muốn giải mã từ chuỗi JSON.
        table: Tên bảng Iceberg đích đến.
        merge_fn: Hàm thực hiện merge_into (upsert/delete).
        checkpoint_suffix: Tên folder lưu trữ thông tin checkpoint cho tính năng fault-tolerance.
    """
    # 1. Đọc stream raw từ broker Redpanda/Kafka
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest") # Đọc từ record đầu tiên
        .option("failOnDataLoss", "false")     # Tránh sập luồng khi lỡ mất checkpoint hoặc event
        .load()
    )

    # 2. Xử lý logic giải mã dữ liệu chuỗi json ra thành các cột struct và format kiểu thời gian
    parsed = (
        raw.select(
            from_json(get_json_object(col("value").cast("string"), "$.payload"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
    )

    if "created_at" in [f.name for f in schema.fields]:
        parsed = parsed.withColumn("created_at", to_timestamp("created_at"))

    parsed = (
        parsed.withColumn("updated_at", to_timestamp("updated_at"))
        .withColumn("is_deleted", col("__deleted").eqNullSafe("true")) # Chuyển đổi cờ xóa
        .withColumn("ingested_at", current_timestamp()) # Thời điểm dữ liệu cập bến Iceberg
        .drop("__deleted")
    )

    # Chuyển đổi riêng lẻ field `activated_at` nếu nó tồn tại trong schema (chỉ có ở bảng subscribers)
    if "activated_at" in [f.name for f in schema.fields]:
        parsed = parsed.withColumn("activated_at", to_timestamp("activated_at"))

    checkpoint_location = f"{CHECKPOINT_BASE}/{checkpoint_suffix}"

    # 3. Kích hoạt luồng Stream bằng cách chạy `foreachBatch` đi kèm hàm Upsert MERGE INTO
    query = (
        parsed.writeStream
        .foreachBatch(merge_fn)
        .outputMode("update")
        .trigger(processingTime="30 seconds") # Định kỳ gom batch 30s một lần
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

    return query


def main() -> None:
    """Hàm chạy chính Spark."""
    # Khởi tạo Spark Session với tính năng kiểm tra schema được tắt để tùy biến tự decode JSON
    spark = (
        SparkSession.builder.appName("onehouse-cdc-billing-stream")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.cores.max", "1")
        .getOrCreate()
    )
    # Ẩn bớt các log thừa
    spark.sparkContext.setLogLevel("WARN")

    # Bước 1: Tạo trước các bảng Bronze ở Iceberg catalog (nếu chưa có)
    create_bronze_tables(spark)

    print(f"Starting CDC streams from Redpanda ({BOOTSTRAP_SERVERS})", flush=True)
    print(f"  → {PLANS_TOPIC} → {PLANS_TABLE}", flush=True)
    print(f"  → {SUBSCRIBERS_TOPIC} → {SUBSCRIBERS_TABLE}", flush=True)

    # Bước 2: Kích hoạt CDC streaming process ngầm cho 2 bảng
    plans_query = build_cdc_stream(
        spark, PLANS_TOPIC, PLAN_SCHEMA, PLANS_TABLE,
        merge_billing_plans, "cdc_billing_plans",
    )

    subs_query = build_cdc_stream(
        spark, SUBSCRIBERS_TOPIC, SUBSCRIBER_SCHEMA, SUBSCRIBERS_TABLE,
        merge_subscribers, "cdc_subscribers",
    )

    # Chờ đợi luồng (đảm bảo chương trình không bị tắt tự động)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
