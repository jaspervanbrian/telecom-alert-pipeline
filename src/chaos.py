from dataclasses import dataclass
from datetime import datetime
import json
import random


@dataclass(frozen=True)
class OutboundMessage:
    """Represents a single Kafka message ready to be published."""

    topic: str
    key: bytes
    value: bytes


@dataclass
class ProducerStats:
    """Tracks counts of clean vs. synthetic chaos messages emitted."""

    clean_sent: int = 0
    noise_heartbeats: int = 0
    duplicates_injected: int = 0
    dirty_metrics_injected: int = 0
    corrupt_json_injected: int = 0

    def print_summary(self, next_cursor: int, offset_filename: str) -> None:
        print("\n=== Producer Run Summary ===")
        print(f"  clean_sent: {self.clean_sent:,}")
        print(f"  noise_heartbeats: {self.noise_heartbeats:,}")
        print(f"  duplicates_injected: {self.duplicates_injected:,}")
        print(f"  dirty_metrics_injected: {self.dirty_metrics_injected:,}")
        print(f"  corrupt_json_injected: {self.corrupt_json_injected:,}")
        print(f"  next_cursor_offset: {next_cursor:,} (saved to {offset_filename})")


class ChaosInjector:
    """
    Pure domain service that transforms a clean dataset row into one or more
    outbound Kafka payloads, injecting controlled rates of real-world anomalies.
    """

    def __init__(
        self,
        raw_events_topic: str,
        alerts_topic: str,
        seed: int = 42,
        buffer_size: int = 200,
    ) -> None:
        self.raw_events_topic = raw_events_topic
        self.alerts_topic = alerts_topic
        self.rng = random.Random(seed)
        self.buffer_size = buffer_size
        self.recent_buffer: list[dict] = []
        self.stats = ProducerStats()

    @staticmethod
    def _serialize_row(row: dict) -> dict:
        serialized = dict(row)
        if isinstance(serialized.get("timestamp"), datetime):
            serialized["timestamp"] = serialized["timestamp"].isoformat()
        return serialized

    def _mutate_dirty_value(self, event: dict) -> None:
        choice = self.rng.choice(["case", "negative_users", "high_packet_loss"])
        if choice == "case":
            event["severity"] = f"  {event['severity'].upper()} "
        elif choice == "negative_users":
            event["affected_users"] = -self.rng.randint(1, 50)
        else:
            event["packet_loss_percent"] = 125.5

    def _build_heartbeat(self, event: dict, absolute_row_num: int) -> dict:
        return {
            "event_id": f"HB-{absolute_row_num:08d}",
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

    def process_row(self, raw_row: dict, absolute_row_num: int) -> list[OutboundMessage]:
        messages: list[OutboundMessage] = []
        event = self._serialize_row(raw_row)
        key_bytes = (event.get("tower_id") or "UNKNOWN").encode("utf-8")

        # 1. Primary event (~2% chance of dirty unnormalized values)
        if self.rng.random() < 0.02:
            self._mutate_dirty_value(event)
            self.stats.dirty_metrics_injected += 1
        else:
            self.stats.clean_sent += 1

        messages.append(
            OutboundMessage(
                topic=self.raw_events_topic,
                key=key_bytes,
                value=json.dumps(event).encode("utf-8"),
            )
        )

        # Update rolling buffer for duplicate sampling
        self.recent_buffer.append(event)
        if len(self.recent_buffer) > self.buffer_size:
            self.recent_buffer.pop(0)

        # 2. ~5% duplicate delivery (simulates at-least-once retry)
        if self.rng.random() < 0.05 and self.recent_buffer:
            dup_event = self.rng.choice(self.recent_buffer)
            messages.append(
                OutboundMessage(
                    topic=self.raw_events_topic,
                    key=dup_event["tower_id"].encode("utf-8"),
                    value=json.dumps(dup_event).encode("utf-8"),
                )
            )
            self.stats.duplicates_injected += 1

        # 3. ~10% routine heartbeat noise (filtered out upstream by ksqlDB)
        if self.rng.random() < 0.10:
            heartbeat = self._build_heartbeat(event, absolute_row_num)
            messages.append(
                OutboundMessage(
                    topic=self.raw_events_topic,
                    key=key_bytes,
                    value=json.dumps(heartbeat).encode("utf-8"),
                )
            )
            self.stats.noise_heartbeats += 1

        # 4. ~0.5% corrupt JSON sent to alerts topic (tests Bronze/Silver fault tolerance)
        if self.rng.random() < 0.005:
            corrupt_payload = (
                f'{{"event_id": "CORRUPT-{absolute_row_num}", '
                f'"tower_id": "{event["tower_id"]}", BROKEN_JSON'
            )
            messages.append(
                OutboundMessage(
                    topic=self.alerts_topic,
                    key=key_bytes,
                    value=corrupt_payload.encode("utf-8"),
                )
            )
            self.stats.corrupt_json_injected += 1

        return messages
