
import os
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PARQUET_PATH = PROJECT_ROOT / "data/parquet/olist_orders.parquet"

def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Unsupported value: {type(value)}")

def main(limit):
    producer = Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "enable.idempotence": True,
        "acks": "all"
    })

    delivered = 0
    failed = 0
    submitted = 0

    def callback(err, msg):
        nonlocal delivered, failed
        if err:
            failed += 1
            print("Delivery error:", err)
        else:
            delivered += 1

    parquet_file = pq.ParquetFile(PARQUET_PATH)

    for batch in parquet_file.iter_batches(batch_size=100):
        for record in batch.to_pylist():
            if submitted >= limit:
                break

            record = {k.upper(): v for k, v in record.items()}
            key = json.dumps([record["ORDER_ID"]])

            event = {
                "operation": "UPSERT",
                "source": "parquet",
                "table": "orders",
                "data": record
            }

            producer.produce(
                "olist.raw.orders",
                key=key.encode("utf-8"),
                value=json.dumps(
                    event, default=json_default
                ).encode("utf-8"),
                callback=callback
            )

            producer.poll(0)
            submitted += 1

        if submitted >= limit:
            break

    remaining = producer.flush(timeout=60)

    print("Submitted:", submitted)
    print("Delivered:", delivered)
    print("Failed:", failed)
    print("Still queued:", remaining)

    if failed or remaining or delivered != submitted:
        raise RuntimeError("Kafka delivery incomplete")

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    if limit <= 0:
        raise ValueError("Limit must be positive")
    main(limit)
