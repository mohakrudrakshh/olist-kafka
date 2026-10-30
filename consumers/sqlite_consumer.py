
import os
import json
import sqlite3
import sys
from pathlib import Path


from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv()

TOPICS = [
    "olist.orders",
    "olist.order_items",
    "olist.customers",
    "olist.products",
    "olist.payments",
    "olist.reviews",
    "olist.sellers",
    "olist.geolocation",
    "olist.category_translation"
]

# Store each record as JSON, with a stable composite key.
# This preserves every column without guessing SQLite types.
DB_PATH = Path("data/olist_kafka.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def open_database():
    conn = sqlite3.connect(
        DB_PATH,
        timeout=30
    )
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    for topic in TOPICS:
        table = topic.replace("olist.", "")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table}" (
                record_key TEXT PRIMARY KEY,
                record_json TEXT NOT NULL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

    conn.commit()
    return conn


def save_message(conn, msg):
    event = json.loads(msg.value().decode("utf-8"))
    table = msg.topic().replace("olist.", "")
    key = msg.key().decode("utf-8")

    operation = event.get("operation", "INSERT").upper()

    if operation == "DELETE":
        conn.execute(
            f'DELETE FROM "{table}" WHERE record_key = ?',
            (key,)
        )
    elif operation in ("INSERT", "UPDATE", "UPSERT"):
        conn.execute(
            f"""
            INSERT INTO "{table}"
                (record_key, record_json)
            VALUES (?, ?)
            ON CONFLICT(record_key)
            DO UPDATE SET
                record_json = excluded.record_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, json.dumps(event["data"]))
        )
    else:
        raise ValueError(f"Unsupported operation: {operation}")

    conn.commit()


def main():
    consumer_name = sys.argv[1] if len(sys.argv) > 1 else "consumer-1"

    consumer = Consumer({
        "bootstrap.servers": os.environ[
            "KAFKA_BOOTSTRAP_SERVERS"
        ],
        "group.id": "olist-sqlite-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    conn = open_database()
    consumer.subscribe(TOPICS)

    print(f"{consumer_name} started")
    print(f"Subscribed to {len(TOPICS)} topics")
    print(f"SQLite database: {DB_PATH.resolve()}", flush=True)

    processed = 0

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                save_message(conn, msg)
                consumer.commit(message=msg, asynchronous=False)
                processed += 1

                if processed % 50 == 0:
                    print(
                        f"{consumer_name}: processed {processed} messages",
                        flush=True
                    )

            except Exception as exc:
                print(f"Processing failed: {exc}", flush=True)
                raise

    except KeyboardInterrupt:
        print(f"\n{consumer_name} stopping...")

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
