# Khai báo tham số đầu vào cho script PowerShell
param(
    [Parameter(Position = 0)]
    # Giới hạn các hành động (Action) hợp lệ mà người dùng có thể nhập vào
    [ValidateSet("start", "cdc", "dbt", "orchestration", "visualization", "smoke", "logs", "stop")]
    [string]$Action = "start" # Mặc định nếu không truyền tham số sẽ là 'start'
)

# Cấu hình để script dừng lại (Stop) ngay lập tức nếu gặp bất kỳ lỗi nào thay vì chạy tiếp
$ErrorActionPreference = "Stop"

# Hàm tiện ích: Kiểm tra xem file biến môi trường (.env) đã có chưa
function Ensure-EnvFile {
    # Nếu file .env chưa tồn tại và có file .env.example mẫu, thì copy ra một bản
    if (-not (Test-Path ".env") -and (Test-Path ".env.example")) {
        Copy-Item ".env.example" ".env"
    }
}

# Cấu trúc rẽ nhánh dựa vào lệnh Action người dùng nhập
switch ($Action) {
    "start" {
        # Khởi động hạ tầng cốt lõi (Data Lake, Kafka, Spark Streaming)
        Ensure-EnvFile
        # Dùng docker compose để build và chạy ngầm (-d) các dịch vụ cơ sở
        docker compose up -d --build redpanda redpanda-console minio minio-init iceberg-rest trino simulator spark-master spark-worker spark-streaming
    }
    "cdc" {
        # Khởi động các luồng dữ liệu thay đổi (Change Data Capture)
        Ensure-EnvFile
        # Chạy CSDL PostgreSQL (Billing), giả lập thay đổi và Debezium, cùng với Spark Job CDC
        docker compose up -d --build postgres-billing billing-generator debezium debezium-init spark-cdc
    }
    "dbt" {
        # Chạy thủ công luồng pipeline dbt (Thường dùng khi test hoặc không muốn dùng Airflow)
        # --profile tools: Bật profile các tool xử lý data 1 lần
        docker compose --profile tools run --rm dbt seed --full-refresh # Nạp lại dữ liệu master data (seed)
        docker compose --profile tools run --rm dbt run                 # Chạy ELT
        docker compose --profile tools run --rm dbt test                # Chạy kiểm thử chất lượng
        docker compose --profile tools run --rm dbt docs generate       # Xuất tài liệu data lineage
    }
    "orchestration" {
        # Khởi động cụm điều phối công việc Airflow
        # --profile orchestration: Khởi động kèm các dịch vụ của Airflow
        docker compose --profile orchestration up -d --build airflow-init airflow-webserver airflow-scheduler
    }
    "visualization" {
        # Khởi động dịch vụ BI/Dashboard (Apache Superset)
        docker compose --profile visualization up -d --build superset
    }
    "smoke" {
        # Chạy test kết nối nhanh đến Trino (Smoke test)
        # Đọc trực tiếp câu lệnh từ file sql và truyền vào command line của container trino
        Get-Content -Raw "scripts/trino_smoke.sql" | docker exec -i onehouse-trino trino --catalog viettel --schema gold
    }
    "logs" {
        # Theo dõi (tail/follow -f) log trực tiếp từ các container sinh log chính
        docker compose logs -f simulator spark-master spark-worker spark-streaming spark-cdc trino
    }
    "stop" {
        # Dừng và gỡ bỏ toàn bộ các container của tất cả các profile
        docker compose --profile tools --profile orchestration --profile visualization down
    }
}
