
import os
import json
import sys

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

producer = Producer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "enable.idempotence": True,
    "acks": "all"
})

TEST_ORDER_ID = "KAFKA-CRUD-TEST-001"
TOPIC = "olist.orders"


def send_event(operation, data=None):
    event = {
        "operation": operation,
        "source": "crud_demo",
        "table": "orders",
        "data": data
    }

    # Match the key format used by our Snowflake producer.
    key = json.dumps([TEST_ORDER_ID])

    errors = []

    def delivery_report(err, msg):
        if err:
            errors.append(str(err))
        else:
            print(
                f"{operation} delivered to "
                f"{msg.topic()} partition {msg.partition()}"
            )

    producer.produce(
        TOPIC,
        key=key.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report
    )

    remaining = producer.flush(timeout=30)

    if errors or remaining:
        raise RuntimeError(
            f"Kafka delivery failed: {errors}; queued={remaining}"
        )


def main():
    if len(sys.argv) != 2:
        print("Usage: python producers/crud_producer.py CREATE|UPDATE|DELETE")
        sys.exit(1)

    operation = sys.argv[1].upper()

    if operation == "CREATE":
        send_event("INSERT", {
            "ORDER_ID": TEST_ORDER_ID,
            "CUSTOMER_ID": "KAFKA-TEST-CUSTOMER",
            "ORDER_STATUS": "created"
        })

    elif operation == "UPDATE":
        send_event("UPDATE", {
            "ORDER_ID": TEST_ORDER_ID,
            "CUSTOMER_ID": "KAFKA-TEST-CUSTOMER",
            "ORDER_STATUS": "delivered"
        })

    elif operation == "DELETE":
        send_event("DELETE")

    else:
        raise ValueError(f"Unsupported operation: {operation}")


if __name__ == "__main__":
    main()
