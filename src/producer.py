import argparse
import json
import random
import time
from datetime import datetime
from pathlib import Path
import pyarrow.parquet as pq
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"
RAW_EVENTS_TOPIC = "telecom.events.raw"
ALERTS_TOPIC = "telecom.alerts.raw"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
PARQUET_PATH = DATA_DIR / "raw_telecom_events.parquet"
OFFSET_FILE = DATA_DIR / ".producer_offset"


def load_cursor(reset: bool = False) -> int:
    if reset or not OFFSET_FILE.exists():
        return 0
    return int(OFFSET_FILE.read_text().strip() or "0")


def save_cursor(offset: int) -> None:
    OFFSET_FILE.write_text(str(offset))


def ensure_topics_exist(bootstrap_servers: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics

    topics_to_create = []
    for topic in (RAW_EVENTS_TOPIC, ALERTS_TOPIC):
        if topic not in existing:
            topics_to_create.append(NewTopic(topic, num_partitions=3, replication_factor=1))

    if topics_to_create:
        futures = admin.create_topics(topics_to_create)
        for topic, future in futures.items():
            try:
                future.result()
                print(f"[INFO] Created Kafka topic: {topic} (3 partitions)")
            except Exception as exc:
                print(f"[WARN] Topic {topic} creation note: {exc}")


def serialize_row(row: dict) -> dict:
    serialized = dict(row)
    if isinstance(serialized.get("timestamp"), datetime):
        serialized["timestamp"] = serialized["timestamp"].isoformat()
    return serialized


def run_producer(limit: int, batch_sleep_ms: int, reset_offset: bool = False, seed: int = 42) -> None:
    random.seed(seed)
    ensure_topics_exist(BOOTSTRAP_SERVERS)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "linger.ms": 10,
            "batch.num.messages": 1000,
        }
    )

    full_table = pq.read_table(PARQUET_PATH)
    total_rows = full_table.num_rows
    start_offset = load_cursor(reset=reset_offset)

    if start_offset >= total_rows:
        print(f"[INFO] Cursor ({start_offset:,}) reached end of dataset ({total_rows:,}). Wrapping to 0.")
        start_offset = 0

    end_offset = min(start_offset + limit, total_rows)
    slice_len = end_offset - start_offset

    print(
        f"[INFO] Streaming rows [{start_offset:,} -> {end_offset:,}] "
        f"({slice_len:,} rows) from {PARQUET_PATH.name}..."
    )
    rows = full_table.slice(start_offset, slice_len).to_pylist()

    stats = {
        "clean_sent": 0,
        "noise_heartbeats": 0,
        "duplicates_injected": 0,
        "dirty_metrics_injected": 0,
        "corrupt_json_injected": 0,
    }
    recent_buffer: list[dict] = []

    for idx, raw_row in enumerate(rows, start=1):
        event = serialize_row(raw_row)
        key_bytes = (event.get("tower_id") or "UNKNOWN").encode("utf-8")

        # 1. Inject ~2% dirty values (unnormalized strings or out-of-range numbers)
        if random.random() < 0.02:
            choice = random.choice(["case", "negative_users", "high_packet_loss"])
            if choice == "case":
                event["severity"] = f"  {event['severity'].upper()} "
            elif choice == "negative_users":
                event["affected_users"] = -random.randint(1, 50)
            else:
                event["packet_loss_percent"] = 125.5
            stats["dirty_metrics_injected"] += 1
        else:
            stats["clean_sent"] += 1

        producer.produce(
            RAW_EVENTS_TOPIC,
            key=key_bytes,
            value=json.dumps(event).encode("utf-8"),
        )

        # Keep a rolling buffer of recent events to sample duplicates from
        recent_buffer.append(event)
        if len(recent_buffer) > 200:
            recent_buffer.pop(0)

        # 2. Inject ~5% duplicate deliveries (same event_id & timestamp)
        if random.random() < 0.05 and recent_buffer:
            dup_event = random.choice(recent_buffer)
            producer.produce(
                RAW_EVENTS_TOPIC,
                key=dup_event["tower_id"].encode("utf-8"),
                value=json.dumps(dup_event).encode("utf-8"),
            )
            stats["duplicates_injected"] += 1

        # 3. Inject ~10% routine heartbeat noise (filtered out by ksqlDB)
        if random.random() < 0.10:
            heartbeat = {
                "event_id": f"HB-{start_offset + idx:08d}",
                "timestamp": event["timestamp"],
                "tower_id": event["tower_id"],
                "city": event["city"],
                "event_type": "heartbeat",
                "severity": "info",
                "affected_users": 0,
                "packet_loss_percent": 0.0,
                "network_type": event["network_type"],
                "operator": event["operator"],
            }
            producer.produce(
                RAW_EVENTS_TOPIC,
                key=key_bytes,
                value=json.dumps(heartbeat).encode("utf-8"),
            )
            stats["noise_heartbeats"] += 1

        # 4. Inject ~0.5% corrupt JSON directly into telecom.alerts.raw to test Bronze/Silver resilience
        if random.random() < 0.005:
            corrupt_payload = f'{{"event_id": "CORRUPT-{start_offset + idx}", "tower_id": "{event["tower_id"]}", BROKEN_JSON'
            producer.produce(
                ALERTS_TOPIC,
                key=key_bytes,
                value=corrupt_payload.encode("utf-8"),
            )
            stats["corrupt_json_injected"] += 1

        if idx % 2000 == 0:
            producer.poll(0)
            if batch_sleep_ms > 0:
                time.sleep(batch_sleep_ms / 1000.0)
            print(f"  -> Processed {idx:,}/{slice_len:,} rows in this batch...")

    producer.flush()
    save_cursor(end_offset)

    print("\n=== Producer Run Summary ===")
    for k, v in stats.items():
        print(f"  {k}: {v:,}")
    print(f"  next_cursor_offset: {end_offset:,} (saved to {OFFSET_FILE.name})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chaos Kafka Producer for Telecom Alerts")
    parser.add_argument("--limit", type=int, default=10000, help="Number of base Parquet rows to stream")
    parser.add_argument("--sleep-ms", type=int, default=50, help="Sleep ms every 2,000 rows")
    parser.add_argument("--reset-offset", action="store_true", help="Reset cursor back to row 0")
    args = parser.parse_args()

    run_producer(limit=args.limit, batch_sleep_ms=args.sleep_ms, reset_offset=args.reset_offset)
