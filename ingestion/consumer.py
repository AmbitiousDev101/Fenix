"""
Fenix — Kafka/Redpanda Consumer
Consumes messages from Redpanda topics and lands them as raw JSON files
in the Bronze layer, organized by date partitions.
"""

import json
import sys
import time
import signal
import logging
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

from config.settings import (
    REDPANDA_BROKER, TOPIC_MATCHES, TOPIC_STANDINGS,
    CONSUMER_GROUP, CONSUMER_BATCH_SECONDS, BRONZE_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class BronzeWriter:
    """Consumes from Redpanda and writes batched JSON files to the Bronze layer."""

    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": REDPANDA_BROKER,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.running = True

        # Graceful shutdown on signals (SIGINT handled via KeyboardInterrupt on Windows)
        try:
            signal.signal(signal.SIGTERM, self._shutdown)
        except (OSError, ValueError):
            pass  # Signal not available on this platform/thread

    def _shutdown(self, signum, frame):
        logger.info("  Shutdown signal received, finishing current batch...")
        self.running = False

    def _write_batch(self, topic: str, messages: list[dict]):
        """Write a batch of messages as a JSON file to the Bronze layer."""
        if not messages:
            return

        # Determine subdirectory from topic name
        if "matches" in topic:
            sub_dir = "matches"
        elif "standings" in topic:
            sub_dir = "standings"
        else:
            sub_dir = "other"

        now = datetime.now(timezone.utc)
        date_partition = f"date={now.strftime('%Y-%m-%d')}"
        batch_filename = f"batch_{now.strftime('%Y%m%d_%H%M%S')}.json"

        output_dir = BRONZE_PATH / sub_dir / date_partition
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / batch_filename

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(messages, f, indent=2, default=str)

        logger.info(f"  → Wrote {len(messages)} messages to {output_path}")

    def consume(self, timeout_seconds: int = CONSUMER_BATCH_SECONDS):
        """
        Subscribe to topics and consume messages in batches.
        Writes accumulated messages when the timeout elapses.
        """
        self.consumer.subscribe([TOPIC_MATCHES, TOPIC_STANDINGS])
        logger.info(f"  Subscribed to [{TOPIC_MATCHES}, {TOPIC_STANDINGS}]")
        logger.info(f"  Consuming for {timeout_seconds}s...")

        batches: dict[str, list[dict]] = {
            TOPIC_MATCHES: [],
            TOPIC_STANDINGS: [],
        }
        start_time = time.monotonic()
        total_consumed = 0

        try:
            while self.running:
                elapsed = time.monotonic() - start_time
                if elapsed >= timeout_seconds:
                    break

                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"  ✗ Consumer error: {msg.error()}")
                    continue

                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    topic = msg.topic()
                    if topic in batches:
                        batches[topic].append(value)
                        total_consumed += 1
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning(f"  ⚠ Skipping malformed message: {e}")

        except KeyboardInterrupt:
            logger.info("  Interrupted by user")
        finally:
            # Write all remaining batches
            for topic, messages in batches.items():
                self._write_batch(topic, messages)

            # Commit offsets and close
            try:
                self.consumer.commit()
            except Exception:
                pass
            self.consumer.close()
            logger.info(f"  Consumer closed. Total messages consumed: {total_consumed}")


def main(timeout: int = CONSUMER_BATCH_SECONDS):
    """Create a BronzeWriter and consume messages."""
    logger.info("=" * 60)
    logger.info("FENIX CONSUMER — Starting Bronze writer")
    logger.info("=" * 60)

    writer = BronzeWriter()
    writer.consume(timeout_seconds=timeout)
    logger.info("FENIX CONSUMER — Bronze write complete ✓")


if __name__ == "__main__":
    t = int(sys.argv[1]) if len(sys.argv) > 1 else CONSUMER_BATCH_SECONDS
    main(timeout=t)
