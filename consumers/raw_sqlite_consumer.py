
import os
import json
import sqlite3
from pathlib import Path

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv()

DB_PATH = Path("data/olist_kafka.db")

consumer = Consumer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "group.id": "olist-raw-sqlite-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

conn = sqlite3.connect(DB_PATH, timeout=30)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA busy_timeout=30000")

conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_orders (
        record_key TEXT PRIMARY KEY,
        record_json TEXT NOT NULL,
        source_format TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()

consumer.subscribe(["olist.raw.orders"])

print("Raw SQLite consumer started", flush=True)

processed = 0

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        event = json.loads(msg.value().decode("utf-8"))
        key = msg.key().decode("utf-8")
        operation = event.get("operation", "UPSERT").upper()

        if operation == "DELETE":
            conn.execute(
                "DELETE FROM raw_orders WHERE record_key = ?",
                (key,)
            )
        elif operation in ("INSERT", "UPDATE", "UPSERT"):
            conn.execute("""
                INSERT INTO raw_orders
                    (record_key, record_json, source_format)
                VALUES (?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    record_json = excluded.record_json,
                    source_format = excluded.source_format,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                key,
                json.dumps(event["data"]),
                event.get("source", "unknown")
            ))
        else:
            raise ValueError(f"Unsupported operation: {operation}")

        conn.commit()
        consumer.commit(message=msg, asynchronous=False)

        processed += 1
        print(
            f"Processed {processed}: {operation} "
            f"from {event.get('source')}",
            flush=True
        )

except KeyboardInterrupt:
    print("\nStopping raw consumer...")

finally:
    consumer.close()
    conn.close()
