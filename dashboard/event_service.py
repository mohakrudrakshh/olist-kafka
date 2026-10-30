
import os
import json
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition
from dotenv import load_dotenv

load_dotenv()


def get_recent_events(topic="olist.raw.orders", limit=25):
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "olist-dashboard-event-reader",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "auto.offset.reset": "earliest"
    })

    events = []

    try:
        metadata = consumer.list_topics(topic=topic, timeout=5)

        if topic not in metadata.topics:
            raise ValueError("Topic does not exist")

        topic_metadata = metadata.topics[topic]

        if topic_metadata.error is not None:
            raise RuntimeError(str(topic_metadata.error))

        assignments = []

        for partition_id in topic_metadata.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition_id),
                timeout=5
            )

            # Read only a small tail of each partition.
            start = max(low, high - limit)

            assignments.append(
                TopicPartition(topic, partition_id, start)
            )

        consumer.assign(assignments)

        # Poll for a bounded amount of time.
        import time
        deadline = time.monotonic() + 4

        while time.monotonic() < deadline:
            message = consumer.poll(0.2)

            if message is None:
                continue

            if message.error():
                # Partition EOF is expected.
                from confluent_kafka import KafkaError
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))

            try:
                payload = json.loads(
                    message.value().decode("utf-8")
                )
            except (ValueError, UnicodeDecodeError):
                payload = {}

            timestamp_type, timestamp_ms = message.timestamp()

            timestamp = (
                datetime.fromtimestamp(
                    timestamp_ms / 1000,
                    timezone.utc
                ).isoformat()
                if timestamp_ms and timestamp_ms > 0
                else None
            )

            events.append({
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "timestamp": timestamp,
                "operation": payload.get("operation", "UNKNOWN"),
                "source": payload.get("source", "unknown"),
                "table": payload.get("table", "unknown"),
                "key": (
                    message.key().decode("utf-8")
                    if message.key() else None
                )
            })

        events.sort(
            key=lambda e: (
                e["timestamp"] or "",
                e["partition"],
                e["offset"]
            ),
            reverse=True
        )

        return events[:limit]

    finally:
        consumer.close()
