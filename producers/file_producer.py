
import os
import sys
import csv
import json
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path(__file__).resolve().parents[2] / "raw"

FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}

KEYS = {
    "orders": ["ORDER_ID"],
    "order_items": ["ORDER_ID", "ORDER_ITEM_ID"],
    "customers": ["CUSTOMER_ID"],
    "products": ["PRODUCT_ID"],
    "payments": ["ORDER_ID", "PAYMENT_SEQUENTIAL"],
    "reviews": ["REVIEW_ID", "ORDER_ID"],
    "sellers": ["SELLER_ID"],
    "geolocation": [
        "GEOLOCATION_ZIP_CODE_PREFIX",
        "GEOLOCATION_LAT",
        "GEOLOCATION_LNG",
        "GEOLOCATION_CITY",
        "GEOLOCATION_STATE"
    ],
    "category_translation": ["PRODUCT_CATEGORY_NAME"]
}


def main(table, limit):
    if table not in FILES:
        raise ValueError(f"Unknown table: {table}")

    path = RAW_DIR / FILES[table]

    if not path.exists():
        raise FileNotFoundError(path)

    producer = Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "enable.idempotence": True,
        "acks": "all"
    })

    topic = f"olist.{table}"
    delivered = 0
    failed = 0

    def callback(err, msg):
        nonlocal delivered, failed
        if err:
            failed += 1
            print(f"Delivery error: {err}")
        else:
            delivered += 1

    submitted = 0

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for raw_record in reader:
            if submitted >= limit:
                break

            record = {
                column.upper(): value
                for column, value in raw_record.items()
            }

            key = json.dumps([
                record[column] for column in KEYS[table]
            ])

            event = {
                "operation": "UPSERT",
                "source": "csv",
                "table": table,
                "data": record
            }

            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=callback
            )

            producer.poll(0)
            submitted += 1

    remaining = producer.flush(timeout=60)

    print(f"\nSource: {path.name}")
    print(f"Topic: {topic}")
    print(f"Submitted: {submitted}")
    print(f"Delivered: {delivered}")
    print(f"Failed: {failed}")
    print(f"Still queued: {remaining}")

    if failed or remaining or delivered != submitted:
        raise RuntimeError("Kafka delivery incomplete")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python producers/file_producer.py "
            "<table> <limit>"
        )
        sys.exit(1)

    table_name = sys.argv[1].lower()
    row_limit = int(sys.argv[2])

    if row_limit <= 0:
        raise ValueError("Limit must be positive")

    main(table_name, row_limit)
