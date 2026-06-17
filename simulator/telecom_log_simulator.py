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


BTS_CELLS = [
    {
        "cell_id": "HN_CAU_GIAY_001",
        "province": "Ha Noi",
        "district": "Cau Giay",
        "latitude": 21.0362,
        "longitude": 105.7829,
        "band": "n78",
        "risk": 0.08,
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

NETWORK_TYPES = ["4G", "5G"]
DEVICE_TYPES = ["android", "ios", "iot_router", "mifi"]
CALL_DROP_REASONS = ["radio_link_failure", "handover_failure", "congestion", "core_timeout"]

STOP = False


def _handle_signal(_: int, __: Any) -> None:
    global STOP
    STOP = True


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def subscriber_id() -> str:
    suffix = "".join(random.choices(string.digits, k=8))
    return f"849{suffix}"


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def build_event(bad_record_ratio: float) -> Dict[str, Any]:
    cell = random.choices(
        BTS_CELLS,
        weights=[1.6 if item["risk"] >= 0.07 else 1.0 for item in BTS_CELLS],
        k=1,
    )[0]
    issue = random.random() < cell["risk"]
    event_type = random.choices(["DATA", "VOICE"], weights=[0.68, 0.32], k=1)[0]
    network_type = random.choices(NETWORK_TYPES, weights=[0.62, 0.38], k=1)[0]

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
        failed = issue or random.random() < 0.015
        call_status = random.choice(["DROPPED", "FAILED"]) if failed else "COMPLETED"
        call_duration_sec = int(clamp(random.gauss(126, 80), 1, 900))
        drop_reason = random.choice(CALL_DROP_REASONS) if failed else None
    else:
        download_mb = round(clamp(random.lognormvariate(2.0, 0.9), 0.05, 900.0), 3)
        upload_mb = round(clamp(random.lognormvariate(0.8, 0.7), 0.01, 160.0), 3)

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

    if random.random() < bad_record_ratio:
        field_to_corrupt = random.choice(["cell_id", "event_ts", "event_type"])
        event[field_to_corrupt] = None

    return event


def main() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    topic = os.getenv("ONEHOUSE_KAFKA_TOPIC", "telecom.raw_logs")
    events_per_second = int(os.getenv("SIMULATOR_EVENTS_PER_SECOND", "20"))
    duplicate_ratio = float(os.getenv("SIMULATOR_DUPLICATE_RATIO", "0.01"))
    bad_record_ratio = float(os.getenv("SIMULATOR_BAD_RECORD_RATIO", "0.005"))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        client_id="onehouse-telecom-simulator",
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        linger_ms=50,
        retries=10,
    )

    print(
        f"Producing {events_per_second} events/sec to {topic} via {bootstrap_servers}",
        flush=True,
    )

    last_event: Dict[str, Any] | None = None
    while not STOP:
        loop_started = time.time()
        for _ in range(events_per_second):
            event = last_event if last_event and random.random() < duplicate_ratio else build_event(bad_record_ratio)
            producer.send(topic, key=str(event.get("cell_id") or "unknown"), value=event)
            last_event = event

        producer.flush(timeout=2)
        elapsed = time.time() - loop_started
        if elapsed < 1:
            time.sleep(1 - elapsed)

    producer.flush(timeout=10)
    print("Simulator stopped cleanly", flush=True)


if __name__ == "__main__":
    main()
