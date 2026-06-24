from __future__ import annotations

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

# Cấu hình các đường dẫn làm việc (working directory) cho môi trường dbt chạy trong Airflow
DBT_PROJECT_DIR = "/opt/airflow/dbt_onehouse" # Thư mục chứa project dbt Onehouse
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles" # Thư mục chứa file profiles.yml để kết nối DB
DBT_LOG_DIR = "/opt/airflow/logs/dbt" # Nơi lưu log chạy dbt
DBT_TARGET_DIR = "/opt/airflow/dbt_target" # Nơi chứa các file build/compiled của dbt
DBT_BIN = "/home/airflow/.local/bin/dbt" # Đường dẫn thực thi lệnh dbt cli


def trino_is_ready() -> bool:
    """Hàm kiểm tra (Sensor) xem Trino (query engine) đã khởi động và sẵn sàng nhận kết nối hay chưa."""
    import trino

    # Lấy thông số kết nối từ biến môi trường
    host = os.getenv("TRINO_HOST", "trino")
    port = int(os.getenv("TRINO_PORT", "8080"))
    user = os.getenv("TRINO_USER", "admin")

    try:
        # Cố gắng tạo kết nối dbapi tới Trino Catalog "viettel"
        conn = trino.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog="viettel",
            schema="gold",
            http_scheme="http",
        )
        cursor = conn.cursor()
        # Chạy một lệnh SELECT 1 đơn giản để ping Database
        cursor.execute("SELECT 1")
        cursor.fetchone()
        return True # Trả về True báo hiệu Database đã sẵn sàng
    except Exception as exc:  # Nếu không kết nối được, Airflow sẽ tự động log lại và thử lại theo cấu hình Sensor
        print(f"Trino is not ready yet: {exc}")
        return False # Trả về False để Sensor tiếp tục đợi và poke lại sau


# Môi trường chạy các BashOperator của dbt
DEFAULT_ENV = {
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "DBT_TARGET_PATH": DBT_TARGET_DIR,
    "TRINO_HOST": os.getenv("TRINO_HOST", "trino"),
    "TRINO_PORT": os.getenv("TRINO_PORT", "8080"),
    "TRINO_USER": os.getenv("TRINO_USER", "admin"),
}


def dbt_cmd(command: str) -> str:
    """Hàm sinh tự động câu lệnh bash command để gọi dbt CLI với các tham số đường dẫn chuẩn xác.
    
    Tạo sẵn thư mục log, target và di chuyển vào thư mục dự án dbt trước khi chạy lệnh.
    """
    return (
        f"mkdir -p {DBT_LOG_DIR} {DBT_TARGET_DIR} && "
        f"cd {DBT_PROJECT_DIR} && "
        f"{DBT_BIN} --log-path {DBT_LOG_DIR} "
        f"{command} --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

# Khởi tạo Airflow DAG (Directed Acyclic Graph)
with DAG(
    dag_id="onehouse_dbt_medallion_pipeline", # ID định danh của DAG
    description="Run OneHouse dbt ELT and data quality gates on Trino/Iceberg.", # Mô tả DAG
    start_date=datetime(2026, 1, 1), # Ngày bắt đầu hiệu lực
    schedule="*/15 * * * *", # Lịch trình chạy: mỗi 15 phút một lần
    catchup=False, # Không chạy bù các khoảng thời gian bị lỡ trong quá khứ
    max_active_runs=1, # Đảm bảo chỉ có tối đa 1 phiên bản của DAG chạy tại một thời điểm
    default_args={
        "owner": "onehouse",
        "retries": 2, # Cho phép chạy lại 2 lần nếu thất bại
        "retry_delay": timedelta(minutes=2), # Nghỉ 2 phút trước mỗi lần thử lại
    },
    tags=["onehouse", "dbt", "trino", "iceberg"], # Gắn thẻ (tag) để dễ phân loại trong Airflow UI
) as dag:
    
    # Tác vụ 1: Sensor chờ đợi cho đến khi Trino sẵn sàng (Poke 20s/lần, timeout sau 5 phút)
    wait_for_trino = PythonSensor(
        task_id="wait_for_trino",
        python_callable=trino_is_ready,
        poke_interval=20,
        timeout=300,
        mode="poke",
    )

    # Tác vụ 2: Chạy dbt debug để kiểm tra trước các kết nối và profile
    debug_profile = BashOperator(
        task_id="dbt_debug",
        bash_command=dbt_cmd("debug"),
        env=DEFAULT_ENV,
    )

    # Tác vụ 3: Tải dữ liệu tĩnh/mẫu (seed data) như bts_metadata từ file CSV lên Database
    seed_reference_data = BashOperator(
        task_id="dbt_seed_bts_metadata",
        bash_command=dbt_cmd("seed --select bts_metadata --full-refresh"),
        env=DEFAULT_ENV,
    )

    # Tác vụ 4: Thực thi biến đổi dữ liệu (ELT) ở 2 lớp Staging và Silver
    run_staging_and_silver = BashOperator(
        task_id="dbt_run_staging_silver",
        bash_command=dbt_cmd("run --select staging silver"),
        env=DEFAULT_ENV,
    )

    # Tác vụ 5: Thực thi biến đổi dữ liệu gia tăng (incremental update) cho lớp Gold
    run_gold_incremental = BashOperator(
        task_id="dbt_run_gold_incremental",
        bash_command=dbt_cmd("run --select gold"),
        env=DEFAULT_ENV,
    )

    # Tác vụ 6: Chạy Data Quality Gate (kiểm thử chất lượng dữ liệu) trên các bảng lớp Gold
    quality_gate_gold = BashOperator(
        task_id="dbt_test_gold_quality_gate",
        bash_command=dbt_cmd("test --select tag:gold"),
        env=DEFAULT_ENV,
    )

    # Tác vụ 7: Xuất tài liệu (documentation) mô tả luồng dữ liệu dbt sau khi hoàn thành
    generate_lineage_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=dbt_cmd("docs generate"),
        env=DEFAULT_ENV,
    )

    # --- Thiết lập luồng trình tự chạy DAG bằng các toán tử phụ thuộc bitshift >> ---
    (
        wait_for_trino
        >> debug_profile
        >> seed_reference_data
        >> run_staging_and_silver
        >> run_gold_incremental
        >> quality_gate_gold
        >> generate_lineage_docs
    )
