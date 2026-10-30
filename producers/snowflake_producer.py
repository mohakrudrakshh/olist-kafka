
import os
import sys
import json
from datetime import date, datetime
from decimal import Decimal

import snowflake.connector
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

# Start with ORDERS. We'll expand to all 9 tables.
TABLE_KEYS = {
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


def serialize_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Unsupported type: {type(value)}")


def main(table_name, limit):
    if table_name not in TABLE_KEYS:
        raise ValueError(f"Unsupported table: {table_name}")

    producer = Producer({
        "bootstrap.servers": os.environ[
            "KAFKA_BOOTSTRAP_SERVERS"
        ],
        "enable.idempotence": True,
        "acks": "all"
    })

    connection = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="ETL"
    )

    topic = f"olist.{table_name}"
    delivered = 0
    failed = 0

    def delivery_report(err, msg):
        nonlocal delivered, failed
        if err is not None:
            failed += 1
            print(f"Delivery failed: {err}")
        else:
            delivered += 1

    try:
        cursor = connection.cursor()
        try:
            query = f"SELECT * FROM OLIST_ECOMMERCE.ETL.{table_name.upper()}"

            if limit > 0:
                query += f" LIMIT {limit}"

            cursor.execute(query)
            

            columns = [
                column[0] for column in cursor.description
            ]

            submitted = 0

            for row in cursor:
                record = dict(zip(columns, row))

                key_values = [
                    record[column]
                    for column in TABLE_KEYS[table_name]
                ]

                # Consistent serialization of composite keys.
                key = json.dumps(
                    key_values,
                    default=serialize_value
                )

                event = {
                    "operation": "INSERT",
                    "source": "snowflake",
                    "table": table_name,
                    "data": record
                }

                payload = json.dumps(
                    event,
                    default=serialize_value
                )

                while True:
                    try:
                        producer.produce(
                            topic=topic,
                            key=key.encode("utf-8"),
                            value=payload.encode("utf-8"),
                            callback=delivery_report
                        )
                        break
                    except BufferError:
                        producer.poll(1)

                producer.poll(0)
                submitted += 1

                if submitted % 5000 == 0:
                    print(
                        f"{table_name}: submitted {submitted:,}, "
                        f"delivered {delivered:,}, failed {failed:,}",
                        flush=True
                    )

            remaining = producer.flush(timeout=300)

            print(f"\nTopic: {topic}")
            print(f"Submitted: {submitted}")
            print(f"Delivered: {delivered}")
            print(f"Failed: {failed}")
            print(f"Still queued: {remaining}")

            if failed or remaining or delivered != submitted:
                raise RuntimeError(
                    "Some Kafka messages were not delivered"
                )

        finally:
            cursor.close()

    finally:
        connection.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python producers/snowflake_producer.py "
            "<table> <limit>"
        )
        sys.exit(1)

    table = sys.argv[1].lower()
    limit = int(sys.argv[2])

    if limit < 0:
        raise ValueError("Limit must be zero or positive")

    main(table, limit)
