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
    """Hàm lấy giá trị từ biến môi trường (environment variables), dùng `default` nếu không có."""
    return os.getenv(name, default)


# Định nghĩa các cấu hình mặc định (có thể ghi đè qua environment variables)
CATALOG = env("ICEBERG_CATALOG", "viettel")                                      # Tên Iceberg Catalog
TOPIC = env("ONEHOUSE_KAFKA_TOPIC", "telecom.raw_logs")                          # Tên Kafka Topic chứa log giả lập
BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")              # Địa chỉ Kafka/Redpanda broker
CHECKPOINT_LOCATION = env(
    "SPARK_CHECKPOINT_LOCATION",
    "s3a://warehouse/checkpoints/bronze_telecom_events",                         # Nơi lưu trữ Spark Streaming checkpoint
)
BRONZE_TABLE = "bronze.telecom_events"                                           # Tên bảng Data Lakehouse đích


# Schema quy định trước cho dòng dữ liệu log (để giải mã JSON thành các cột cụ thể)
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
    """Tạo bảng bronze.telecom_events trên Iceberg nếu nó chưa tồn tại.
    
    Bảng được tối ưu bằng cách chia phân mảnh (Partitioning) theo ngày và loại mạng lưới.
    """
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
        -- Phân vùng dữ liệu (partition): nhóm theo từng ngày phát sinh sự kiện và loại mạng (4G/5G)
        PARTITIONED BY (days(event_ts), network_type)
        TBLPROPERTIES (
            'format-version'='2',
            'write.target-file-size-bytes'='134217728' -- Định cỡ file mặc định ~128MB để tránh vấn đề "small files"
        )
        """
    )


def main() -> None:
    """Hàm chạy luồng xử lý chính: đọc stream từ Kafka, chuẩn hóa và lưu xuống Iceberg."""
    # Khởi tạo Spark Session
    spark = (
        SparkSession.builder.appName("onehouse-bronze-stream")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )
    # Tắt hiển thị các log INFO không cần thiết
    spark.sparkContext.setLogLevel("WARN")
    
    # Đảm bảo bảng đích đã sẵn sàng
    create_bronze_table(spark)

    # 1. Mở kết nối đọc Data Stream từ Redpanda broker
    kafka_rows = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")  # Chỉ lấy các event mới nhất từ thời điểm chạy
        .option("failOnDataLoss", "false")    # Chống dừng ứng dụng nếu lỡ mất vài block Kafka
        .load()
    )

    # 2. Xử lý thô dữ liệu nhận được (giải mã chuỗi byte sang string, giải nén cấu trúc JSON)
    parsed = kafka_rows.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_payload"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"),
    ).select(
        "event.*", # Bung toàn bộ các thuộc tính từ trong object event ra làm cột độc lập
        "kafka_key",
        "kafka_timestamp",
        "raw_payload",
    )

    # 3. Tinh chỉnh và làm sạch (chuẩn hóa kiểu thời gian, lọc dữ liệu rác, loại bỏ trùng lặp record)
    bronze = (
        parsed.withColumn("event_ts", to_timestamp("event_ts"))
        .withColumn("ingested_at", current_timestamp()) # Thời gian chính thức cập bến Data Lake
        .withColumn("event_date", to_date("event_ts"))  # Tính toán field chuyên phân mảnh
        .withColumn("event_hour", hour("event_ts"))     # Trích xuất riêng thông tin giờ (để dùng sau)
        .where(col("event_id").isNotNull())             # Lọc bỏ các gói rác thiếu định danh
        .withWatermark("event_ts", "10 minutes")        # Chấp nhận độ trễ (late-arriving data) đến 10 phút
        .dropDuplicates(["event_id"])                   # Deduplication (Bỏ bản ghi trùng trên Kafka theo ID)
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

    # 4. Ghi liên tục dữ liệu chuẩn bị vào Iceberg theo kiểu "chỉ nối thêm" (append mode)
    query = (
        bronze.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="20 seconds") # Thực hiện batch mỗi 20s
        .option("checkpointLocation", CHECKPOINT_LOCATION) # Lưu trạng thái (offset)
        .toTable(BRONZE_TABLE)
    )

    print(
        f"Streaming Kafka topic {TOPIC} from {BOOTSTRAP_SERVERS} into {BRONZE_TABLE}",
        flush=True,
    )
    
    # Chặn luồng chính để duy trì trạng thái stream
    query.awaitTermination()


if __name__ == "__main__":
    main()
