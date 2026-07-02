"""Billing data generator — simulates OLTP changes for CDC pipeline.

Connects to PostgreSQL billing database and continuously performs
INSERT/UPDATE operations on subscribers and billing_plans tables.
Debezium captures these changes via WAL and publishes to Kafka.

Usage:
  Set GENERATOR_MODE=billing in Docker Compose environment.
"""

import os
import random
import signal
import string
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

# Biến cờ (flag) dùng để dừng vòng lặp chính một cách an toàn khi nhận được tín hiệu dừng
STOP = False

# Danh sách các mã trạm thu phát sóng (BTS) tương ứng với dữ liệu mẫu trong file bts_metadata.csv
CELL_IDS = [
    "HN_CAU_GIAY_001",
    "HN_HOAN_KIEM_002",
    "HCM_Q1_001",
    "HCM_THU_DUC_002",
    "DN_HAI_CHAU_001",
    "HP_LE_CHAN_001",
    "CT_NINH_KIEU_001",
    "QNH_HA_LONG_001",
]

# Danh sách các mã gói cước viễn thông giả lập
PLAN_IDS = [
    "ST90", "ST120", "V200", "V300", "ECO30",
    "MAX90", "MAX200", "BIZ300", "BIZ500", "VIP",
]

# Danh sách các đầu số điện thoại di động của mạng Viettel
VIETTEL_PREFIXES = ["086", "096", "097", "098", "032", "033", "034", "035"]

# Danh sách họ của người Việt dùng để tạo tên ngẫu nhiên
FIRST_NAMES = [
    "Nguyen", "Tran", "Le", "Pham", "Hoang", "Phan", "Vu", "Vo",
    "Dang", "Bui", "Do", "Ho", "Ngo", "Duong", "Ly",
]

# Danh sách tên của người Việt dùng để tạo tên ngẫu nhiên
LAST_NAMES = [
    "Anh", "Binh", "Cuong", "Dung", "Ha", "Hieu", "Hung", "Huong",
    "Khanh", "Linh", "Mai", "Minh", "Nam", "Phuong", "Quang",
    "Son", "Thanh", "Thao", "Tuan", "Van",
]


def _handle_signal(_: int, __: Any) -> None:
    """Xử lý tín hiệu ngắt (SIGINT/SIGTERM) để tắt chương trình một cách mượt mà bằng cách gán cờ STOP thành True."""
    global STOP
    STOP = True


def random_msisdn() -> str:
    """Tạo số điện thoại di động ngẫu nhiên thuộc mạng Viettel (bao gồm đầu số và 7 chữ số ngẫu nhiên)."""
    prefix = random.choice(VIETTEL_PREFIXES)
    suffix = "".join(random.choices(string.digits, k=7))
    return f"{prefix}{suffix}"


def random_name() -> str:
    """Tạo tên người dùng ngẫu nhiên bằng cách ghép ngẫu nhiên một họ và một tên."""
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def utc_now() -> str:
    """Lấy thời gian hiện tại theo múi giờ UTC và định dạng nó dưới dạng chuỗi chuẩn 'YYYY-MM-DD HH:MM:SS'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def connect_with_retry(max_retries: int = 20) -> psycopg2.extensions.connection:
    """Thiết lập kết nối với cơ sở dữ liệu PostgreSQL. Nếu không thành công, sẽ thử lại cho đến max_retries lần."""
    # Lấy thông tin cấu hình kết nối từ biến môi trường hoặc dùng giá trị mặc định
    host = os.getenv("POSTGRES_HOST", "postgres-billing")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "viettel")
    password = os.getenv("POSTGRES_PASSWORD", "viettel")
    dbname = os.getenv("POSTGRES_DB", "viettel_billing")

    # Thử kết nối nhiều lần theo số lượng retry quy định
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=host, port=port, user=user, password=password, dbname=dbname
            )
            # Kích hoạt tính năng autocommit để tự động ghi các thay đổi (không cần commit thủ công)
            conn.autocommit = True
            print(f"Connected to PostgreSQL at {host}:{port}/{dbname}", flush=True)
            return conn
        except Exception as exc:
            print(f"Connection attempt {attempt}/{max_retries} failed: {exc}", flush=True)
            # Nghỉ 3 giây trước khi thử kết nối lại
            time.sleep(3)

    # Nếu sau tất cả các lần thử nghiệm đều thất bại thì văng lỗi (RuntimeError)
    raise RuntimeError(f"Failed to connect after {max_retries} attempts")


def seed_subscribers(conn: psycopg2.extensions.connection, count: int = 200) -> None:
    """Insert initial batch of subscribers if table is empty.
    
    Thêm dữ liệu thuê bao ban đầu vào bảng subscribers nếu số lượng hiện có ít hơn biến count.
    """
    cur = conn.cursor()
    # Kiểm tra số lượng thuê bao hiện đang có trong bảng
    cur.execute("SELECT count(*) FROM subscribers")
    existing = cur.fetchone()[0]
    if existing >= count:
        print(f"Subscribers already seeded ({existing} rows)", flush=True)
        return

    # Khởi tạo mảng các row để chuẩn bị thực hiện batch insert
    rows = []
    for _ in range(count - existing):
        rows.append((
            random_msisdn(),           # ID người dùng là số điện thoại
            random_name(),             # Tên
            random.choice(PLAN_IDS),   # Gói cước đang sử dụng ngẫu nhiên
            random.choice(CELL_IDS),   # Khu vực hoạt động ngẫu nhiên
            "active",                  # Trạng thái ban đầu
            utc_now(),                 # Thời gian kích hoạt
            utc_now(),                 # Thời gian cập nhật gần nhất
        ))

    # Thực thi insert các dữ liệu thuê bao, bỏ qua các trường hợp bị trùng khóa (subscriber_id)
    execute_values(
        cur,
        """INSERT INTO subscribers
           (subscriber_id, full_name, plan_id, home_cell_id, status, activated_at, updated_at)
           VALUES %s
           ON CONFLICT (subscriber_id) DO NOTHING""",
        rows,
    )
    print(f"Seeded {len(rows)} subscribers", flush=True)


def update_subscribers(conn: psycopg2.extensions.connection, batch: int) -> int:
    """Randomly update subscriber records — plan changes, status changes, cell moves.
    
    Cập nhật dữ liệu người dùng ngẫu nhiên, mô phỏng người dùng đổi gói cước, thay đổi trạng thái thuê bao, 
    di chuyển sang trạm BTS khác, hoặc thay đổi tên.
    """
    cur = conn.cursor()
    updated = 0
    # Chạy vòng lặp tạo ra các event cập nhật
    for _ in range(batch):
        # Lấy ngẫu nhiên hành động cập nhật kèm theo tỷ lệ/trọng số mong muốn
        action = random.choices(
            ["change_plan", "change_status", "move_cell", "update_name"],
            weights=[0.4, 0.2, 0.3, 0.1],
            k=1,
        )[0]

        # Tìm một người dùng đang có trạng thái 'active' (đang hoạt động) bất kỳ
        cur.execute(
            "SELECT subscriber_id FROM subscribers WHERE status = 'active' ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            continue
        sub_id = row[0]

        # Thực thi câu lệnh SQL UPDATE tùy theo hành động ngẫu nhiên vừa bốc được
        if action == "change_plan":
            new_plan = random.choice(PLAN_IDS)
            cur.execute(
                "UPDATE subscribers SET plan_id = %s, updated_at = %s WHERE subscriber_id = %s",
                (new_plan, utc_now(), sub_id),
            )
        elif action == "change_status":
            new_status = random.choices(
                ["active", "suspended", "terminated"], weights=[0.7, 0.2, 0.1], k=1
            )[0]
            cur.execute(
                "UPDATE subscribers SET status = %s, updated_at = %s WHERE subscriber_id = %s",
                (new_status, utc_now(), sub_id),
            )
        elif action == "move_cell":
            new_cell = random.choice(CELL_IDS)
            cur.execute(
                "UPDATE subscribers SET home_cell_id = %s, updated_at = %s WHERE subscriber_id = %s",
                (new_cell, utc_now(), sub_id),
            )
        else:
            cur.execute(
                "UPDATE subscribers SET full_name = %s, updated_at = %s WHERE subscriber_id = %s",
                (random_name(), utc_now(), sub_id),
            )
        updated += 1
    return updated


def insert_new_subscribers(conn: psycopg2.extensions.connection, batch: int) -> int:
    """Insert new subscribers.
    
    Sinh và thêm mới người dùng giả lập với các thông số ngẫu nhiên.
    """
    cur = conn.cursor()
    rows = []
    # Sinh dữ liệu row để đưa vào bảng
    for _ in range(batch):
        rows.append((
            random_msisdn(),           # Thuê bao (số Viettel ngẫu nhiên)
            random_name(),             # Tên ngẫu nhiên
            random.choice(PLAN_IDS),   # Chọn gói cước ngẫu nhiên
            random.choice(CELL_IDS),   # Gán ngẫu nhiên cho một BTS cụ thể
            "active",                  # Tự động active ban đầu
            utc_now(),                 # Thời gian active
            utc_now(),                 # Thời gian cập nhật lần đầu tiên
        ))
    # Batch Insert dữ liệu mới tạo (bỏ qua nếu trùng subscriber_id)
    execute_values(
        cur,
        """INSERT INTO subscribers
           (subscriber_id, full_name, plan_id, home_cell_id, status, activated_at, updated_at)
           VALUES %s
           ON CONFLICT (subscriber_id) DO NOTHING""",
        rows,
    )
    return len(rows)


def update_billing_plans(conn: psycopg2.extensions.connection) -> int:
    """Occasionally update a billing plan (price change, quota change).
    
    Cập nhật các thông tin của gói cước, như thay đổi phí cố định hàng tháng hoặc thay đổi giới hạn dung lượng dữ liệu.
    """
    cur = conn.cursor()
    plan_id = random.choice(PLAN_IDS)
    # Lựa chọn ngẫu nhiên giữa việc cập nhật học phí (fee) hoặc hạn ngạch dung lượng (quota)
    action = random.choice(["fee", "quota"])

    if action == "fee":
        # Sinh một khoản thay đổi phí (delta) rồi thực hiện UPDATE, đảm bảo phí không bị âm dùng hàm GREATEST
        delta = random.choice([-10000, -5000, 5000, 10000, 20000])
        cur.execute(
            "UPDATE billing_plans SET monthly_fee = GREATEST(0, monthly_fee + %s), updated_at = %s WHERE plan_id = %s",
            (delta, utc_now(), plan_id),
        )
    else:
        # Sinh một khoản thay đổi dung lượng GB (delta) rồi thực hiện UPDATE, đảm bảo giá trị không rơi vào số âm
        delta = random.choice([-1.0, -0.5, 0.5, 1.0, 2.0, 5.0])
        cur.execute(
            "UPDATE billing_plans SET data_quota_gb = GREATEST(0, COALESCE(data_quota_gb, 0) + %s), updated_at = %s WHERE plan_id = %s",
            (delta, utc_now(), plan_id),
        )
    return 1


def main() -> None:
    """Hàm chạy chính để liên tục sinh luồng dữ liệu giả lập (OLTP event)."""
    # Gắn handler cho các tín hiệu gián đoạn để có thể ngắt kết nối an toàn (graceful shutdown)
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Đọc cấu hình từ environment variables
    batch_size = int(os.getenv("BATCH_SIZE", "10"))
    interval = int(os.getenv("INTERVAL_SEC", "5"))

    # Kết nối cơ sở dữ liệu và seed dữ liệu ban đầu
    conn = connect_with_retry()
    seed_subscribers(conn)

    print(
        f"Billing generator running: batch={batch_size}, interval={interval}s",
        flush=True,
    )

    cycle = 0
    # Vòng lặp chính liên tục sinh dữ liệu chừng nào cờ STOP chưa bị bật thành True
    while not STOP:
        try:
            # Quy mô của mỗi loại event: 70% updates, 20% inserts, 10% plan changes
            update_count = int(batch_size * 0.7)
            insert_count = int(batch_size * 0.2)
            plan_updates = max(1, int(batch_size * 0.1))

            # Thực thi các hàm sinh và thay đổi dữ liệu
            updated = update_subscribers(conn, update_count)
            inserted = insert_new_subscribers(conn, insert_count)
            plans = sum(update_billing_plans(conn) for _ in range(plan_updates))

            cycle += 1
            # Cứ mỗi 10 vòng lặp thì log ra kết quả một lần để tiện theo dõi log hệ thống
            if cycle % 10 == 0:
                print(
                    f"[cycle {cycle}] updated={updated} inserted={inserted} plans={plans}",
                    flush=True,
                )

        except psycopg2.OperationalError as exc:
            # Xử lý trường hợp bị rớt kết nối Database giữa chừng: Log ra và tìm cách kết nối lại
            print(f"Connection lost: {exc}. Reconnecting...", flush=True)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect_with_retry()

        # Nghỉ trong giây lát theo `interval` trước khi vào chu kỳ update kế tiếp
        time.sleep(interval)

    # Khối lệnh cleanup sau khi thoát vòng lặp: đóng connection lại gọn gàng
    try:
        conn.close()
    except Exception:
        pass
    print("Billing generator stopped cleanly", flush=True)


if __name__ == "__main__":
    main()
