# Khai báo tham số đầu vào cho script PowerShell
param(
    [Parameter(Position = 0)]
    # Giới hạn các hành động (Action) hợp lệ mà người dùng có thể nhập vào
    [ValidateSet("start", "cdc", "dbt", "orchestration", "visualization", "smoke", "cdc-test", "logs", "stop")]
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
        docker compose up -d --build kafka kafka-ui minio minio-init iceberg-rest trino simulator spark-master spark-worker spark-streaming
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
    "cdc-test" {
        # Test CDC cu the bang cach update 1 subscriber trong PostgreSQL roi kiem tra
        Write-Host "1. Kiem tra gold_customer_360 hien tai cho SUB_00001:"
        docker exec -i onehouse-trino trino --catalog viettel --schema gold --execute "SELECT subscriber_id, full_name, churn_risk_tier FROM gold_customer_360 WHERE subscriber_id = 'SUB_00001'"

        Write-Host "`n2. Update PostgreSQL (Cap nhat plan cho SUB_00001 sang PLAN_PREMIUM):"
        docker exec -i onehouse-postgres-billing psql -U viettel -d viettel_billing -c "UPDATE subscribers SET plan_id = 'PLAN_PREMIUM' WHERE subscriber_id = 'SUB_00001';"

        Write-Host "`n3. Cho 15s de Spark CDC process ban ghi va chay lai dbt de cap nhat gold_customer_360:"
        Start-Sleep -Seconds 15
        docker compose --profile tools run --rm dbt run --select stg_subscribers gold_customer_360

        Write-Host "`n4. Kiem tra lai gold_customer_360 (churn_risk_tier hoac thuoc tinh khac se thay doi):"
        docker exec -i onehouse-trino trino --catalog viettel --schema gold --execute "SELECT subscriber_id, full_name, plan_id, churn_risk_tier FROM gold_customer_360 WHERE subscriber_id = 'SUB_00001'"
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
