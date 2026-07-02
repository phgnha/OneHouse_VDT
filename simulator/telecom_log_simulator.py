import json
import os
import random
import signal
import string
import time
from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4

from kafka import KafkaProducer

# Danh sách thông tin chi tiết về các trạm BTS dùng để giả lập vị trí và thông số kỹ thuật
BTS_CELLS = [
    {
        "cell_id": "HN_CAU_GIAY_001",
        "province": "Ha Noi",
        "district": "Cau Giay",
        "latitude": 21.0362,
        "longitude": 105.7829,
        "band": "n78",
        "risk": 0.08, # Xác suất (8%) trạm này bị lỗi hoặc chất lượng kém
    },
    {
        "cell_id": "HN_HOAN_KIEM_002",
        "province": "Ha Noi",
        "district": "Hoan Kiem",
        "latitude": 21.0287,
        "longitude": 105.8524,
        "band": "B3",
        "risk": 0.05,
    },
    {
        "cell_id": "HCM_Q1_001",
        "province": "Ho Chi Minh",
        "district": "Quan 1",
        "latitude": 10.7769,
        "longitude": 106.7009,
        "band": "n78",
        "risk": 0.09,
    },
    {
        "cell_id": "HCM_THU_DUC_002",
        "province": "Ho Chi Minh",
        "district": "Thu Duc",
        "latitude": 10.8494,
        "longitude": 106.7537,
        "band": "B7",
        "risk": 0.06,
    },
    {
        "cell_id": "DN_HAI_CHAU_001",
        "province": "Da Nang",
        "district": "Hai Chau",
        "latitude": 16.0544,
        "longitude": 108.2022,
        "band": "B3",
        "risk": 0.04,
    },
    {
        "cell_id": "HP_LE_CHAN_001",
        "province": "Hai Phong",
        "district": "Le Chan",
        "latitude": 20.8449,
        "longitude": 106.6881,
        "band": "B8",
        "risk": 0.03,
    },
    {
        "cell_id": "CT_NINH_KIEU_001",
        "province": "Can Tho",
        "district": "Ninh Kieu",
        "latitude": 10.0452,
        "longitude": 105.7469,
        "band": "B3",
        "risk": 0.05,
    },
    {
        "cell_id": "QNH_HA_LONG_001",
        "province": "Quang Ninh",
        "district": "Ha Long",
        "latitude": 20.9599,
        "longitude": 107.0425,
        "band": "B7",
        "risk": 0.07,
    },
]

# Các loại mạng có thể giả lập (4G, 5G)
NETWORK_TYPES = ["4G", "5G"]
# Danh sách thiết bị phần cứng của người dùng cuối
DEVICE_TYPES = ["android", "ios", "iot_router", "mifi"]
# Danh sách các lý do cuộc gọi bị rớt dùng trong các log giả lập lỗi thoại
CALL_DROP_REASONS = ["radio_link_failure", "handover_failure", "congestion", "core_timeout"]

# Biến trạng thái toàn cục để quản lý việc tắt chương trình
STOP = False


def _handle_signal(_: int, __: Any) -> None:
    """Xử lý tín hiệu dừng chương trình (như Ctrl+C, SIGTERM từ Docker)."""
    global STOP
    STOP = True


def utc_now() -> str:
    """Lấy thời gian hiện tại chuẩn UTC theo định dạng chuỗi ISO8601 dùng cho log (có mili-giây)."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def subscriber_id() -> str:
    """Sinh ngẫu nhiên số điện thoại chuẩn của Việt Nam, ví dụ: 849xxxxxxxx."""
    suffix = "".join(random.choices(string.digits, k=8))
    return f"849{suffix}"


def clamp(value: float, lower: float, upper: float) -> float:
    """Hàm chặn giới hạn giá trị. Nếu value vượt ra ngoài khoảng [lower, upper] thì trả về cận tương ứng."""
    return max(lower, min(upper, value))


def build_event(bad_record_ratio: float) -> Dict[str, Any]:
    """Sinh ra ngẫu nhiên một event log (lịch sử viễn thông) theo tỷ lệ phần trăm được cấu hình sẵn."""
    
    # Lựa chọn ngẫu nhiên trạm thu phát sóng. Các trạm có rủi ro lỗi cao (>=0.07) được ưu tiên chọn nhiều hơn (trọng số 1.6)
    cell = random.choices(
        BTS_CELLS,
        weights=[1.6 if item["risk"] >= 0.07 else 1.0 for item in BTS_CELLS],
        k=1,
    )[0]
    
    # Quyết định xem có xảy ra sự cố không dựa trên tỷ lệ "risk" (rủi ro) của trạm
    issue = random.random() < cell["risk"]
    
    # Phân loại event là DATA (dùng mạng) hay VOICE (gọi điện thoại)
    event_type = random.choices(["DATA", "VOICE"], weights=[0.68, 0.32], k=1)[0]
    # Phân loại mạng (4G hoặc 5G)
    network_type = random.choices(NETWORK_TYPES, weights=[0.62, 0.38], k=1)[0]

    # Tính toán ngẫu nhiên các chỉ số chất lượng mạng cơ bản sử dụng hàm phân phối chuẩn
    # Nếu xảy ra issue, các chỉ số bị làm cho tệ đi (latency và loss tăng, tín hiệu rsrp và sinr giảm)
    latency = random.gauss(48, 16) + (random.uniform(40, 180) if issue else 0)
    packet_loss = random.expovariate(2.2) + (random.uniform(1.5, 7.5) if issue else 0)
    rsrp = random.gauss(-92, 8) - (random.uniform(8, 20) if issue else 0)
    sinr = random.gauss(17, 5) - (random.uniform(5, 14) if issue else 0)

    call_status = None
    call_duration_sec = None
    drop_reason = None
    download_mb = 0.0
    upload_mb = 0.0

    if event_type == "VOICE":
        # Event về cuộc gọi (VOICE)
        failed = issue or random.random() < 0.015
        # Trạng thái cuộc gọi (rớt mạng, thất bại, hoặc thành công)
        call_status = random.choice(["DROPPED", "FAILED"]) if failed else "COMPLETED"
        # Thời lượng cuộc gọi tính bằng giây
        call_duration_sec = int(clamp(random.gauss(126, 80), 1, 900))
        # Lý do rớt cuộc gọi (chỉ có khi failed = True)
        drop_reason = random.choice(CALL_DROP_REASONS) if failed else None
    else:
        # Event về sử dụng Data (DATA)
        download_mb = round(clamp(random.lognormvariate(2.0, 0.9), 0.05, 900.0), 3)
        upload_mb = round(clamp(random.lognormvariate(0.8, 0.7), 0.01, 160.0), 3)

    # Đóng gói tất cả thuộc tính thành một Event Log hoàn chỉnh chuẩn JSON
    event: Dict[str, Any] = {
        "event_id": str(uuid4()),
        "event_ts": utc_now(),
        "subscriber_id": subscriber_id(),
        "cell_id": cell["cell_id"],
        "event_type": event_type,
        "network_type": network_type,
        "band": cell["band"],
        "device_type": random.choice(DEVICE_TYPES),
        "call_status": call_status,
        "call_duration_sec": call_duration_sec,
        "drop_reason": drop_reason,
        "download_mb": download_mb,
        "upload_mb": upload_mb,
        "latency_ms": round(clamp(latency, 5.0, 650.0), 2),
        "jitter_ms": round(clamp(random.gauss(8, 4) + (random.uniform(15, 70) if issue else 0), 0.1, 180), 2),
        "packet_loss_pct": round(clamp(packet_loss, 0.0, 30.0), 3),
        "rsrp": round(clamp(rsrp, -140.0, -60.0), 2),
        "sinr": round(clamp(sinr, -10.0, 35.0), 2),
        "source": "onehouse-simulator",
    }

    # Cố tình làm hỏng dữ liệu (ví dụ để test data pipeline error handling) dựa trên `bad_record_ratio`
    if random.random() < bad_record_ratio:
        field_to_corrupt = random.choice(["cell_id", "event_ts", "event_type"])
        event[field_to_corrupt] = None

    return event


def main() -> None:
    """Hàm chạy chính để khởi tạo nhà sản xuất log Kafka và đẩy dữ liệu liên tục."""
    # Bắt tín hiệu để có thể tắt simulator an toàn
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Đọc tham số cấu hình từ environment variables
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("ONEHOUSE_KAFKA_TOPIC", "telecom.raw_logs")
    events_per_second = int(os.getenv("SIMULATOR_EVENTS_PER_SECOND", "20"))
    duplicate_ratio = float(os.getenv("SIMULATOR_DUPLICATE_RATIO", "0.01"))
    bad_record_ratio = float(os.getenv("SIMULATOR_BAD_RECORD_RATIO", "0.005"))

    # Thiết lập Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        client_id="onehouse-telecom-simulator",
        key_serializer=lambda value: value.encode("utf-8"), # Key sẽ được mã hóa về định dạng byte (utf-8)
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"), # Log Value (JSON payload) cũng mã hóa byte
        linger_ms=50,   # Group nhiều event vào chung một batch trước khi gửi đi (trong vòng 50ms) để tối ưu
        retries=10,     # Số lần thử lại nếu gửi thất bại
    )

    print(
        f"Producing {events_per_second} events/sec to {topic} via {bootstrap_servers}",
        flush=True,
    )

    last_event: Dict[str, Any] | None = None
    
    # Vòng lặp bắn event liên tục cho đến khi nhận tín hiệu STOP
    while not STOP:
        loop_started = time.time()
        
        # Bắn ra lượng event mong muốn trong mỗi chu kỳ (vd: mỗi 1 giây)
        for _ in range(events_per_second):
            # Quyết định nên sinh sự kiện mới (build_event) hay giả lập duplicate lại sự kiện vừa đẩy lên trước đó
            event = last_event if last_event and random.random() < duplicate_ratio else build_event(bad_record_ratio)
            # Gửi dữ liệu về topic Kafka, dùng cell_id làm khóa chính (partition key)
            producer.send(topic, key=str(event.get("cell_id") or "unknown"), value=event)
            last_event = event

        # Xả buffer gửi đi để hoàn thành batch hiện tại
        producer.flush(timeout=2)
        elapsed = time.time() - loop_started
        
        # Đảm bảo duy trì tốc độ đều đặn (nghỉ số giây dư ra nếu chưa hết 1 giây cho batch này)
        if elapsed < 1:
            time.sleep(1 - elapsed)

    # Đẩy toàn bộ dữ liệu cuối cùng đi trước khi thoát
    producer.flush(timeout=10)
    print("Simulator stopped cleanly", flush=True)


if __name__ == "__main__":
    main()
