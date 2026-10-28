import argparse
import time
from pathlib import Path
import pyarrow.parquet as pq
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from chaos import ChaosInjector

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


def run_producer(limit: int, batch_sleep_ms: int, reset_offset: bool = False, seed: int = 42) -> None:
    ensure_topics_exist(BOOTSTRAP_SERVERS)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "linger.ms": 10,
            "batch.num.messages": 1000,
        }
    )
    chaos = ChaosInjector(
        raw_events_topic=RAW_EVENTS_TOPIC,
        alerts_topic=ALERTS_TOPIC,
        seed=seed,
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

    for idx, raw_row in enumerate(rows, start=1):
        for msg in chaos.process_row(raw_row, absolute_row_num=start_offset + idx):
            producer.produce(msg.topic, key=msg.key, value=msg.value)

        if idx % 2000 == 0:
            producer.poll(0)
            if batch_sleep_ms > 0:
                time.sleep(batch_sleep_ms / 1000.0)
            print(f"  -> Processed {idx:,}/{slice_len:,} rows in this batch...")

    producer.flush()
    save_cursor(end_offset)
    chaos.stats.print_summary(next_cursor=end_offset, offset_filename=OFFSET_FILE.name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chaos Kafka Producer for Telecom Alerts")
    parser.add_argument("--limit", type=int, default=10000, help="Number of base Parquet rows to stream")
    parser.add_argument("--sleep-ms", type=int, default=50, help="Sleep ms every 2,000 rows")
    parser.add_argument("--reset-offset", action="store_true", help="Reset cursor back to row 0")
    args = parser.parse_args()

    run_producer(limit=args.limit, batch_sleep_ms=args.sleep_ms, reset_offset=args.reset_offset)
