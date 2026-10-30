
import json
import os
import sys
import uuid

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

TOPIC = "olist.raw.orders"

# Same key and event ID for every retransmission.
event_id = str(uuid.uuid4())
order_id = f"KAFKA-RETRY-{event_id[:8]}"

event = {
    "operation": "UPSERT",
    "source": "retransmission_demo",
    "table": "orders",
    "event_id": event_id,
    "data": {
        "ORDER_ID": order_id,
        "CUSTOMER_ID": "RETRY-DEMO-CUSTOMER",
        "ORDER_STATUS": "created"
    }
}

try:
    attempts = int(sys.argv[1]) if len(sys.argv) > 1 else 3
except ValueError:
    raise SystemExit("Attempts must be an integer")

if not 2 <= attempts <= 10:
    raise SystemExit("Attempts must be between 2 and 10")

producer = Producer({
    "bootstrap.servers": os.environ[
        "KAFKA_BOOTSTRAP_SERVERS"
    ],
    "enable.idempotence": True,
    "acks": "all"
})

deliveries = []

def delivery_callback(error, message):
    if error:
        deliveries.append({
            "success": False,
            "error": str(error)
        })
    else:
        deliveries.append({
            "success": True,
            "partition": message.partition(),
            "offset": message.offset()
        })

print("Event ID:", event_id)
print("Order ID:", order_id)
print("Application transmissions:", attempts)

for attempt in range(1, attempts + 1):
    producer.produce(
        TOPIC,
        key=json.dumps([order_id]).encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        on_delivery=delivery_callback
    )

    producer.poll(0)

    print(f"Transmission {attempt} submitted")

remaining = producer.flush(timeout=20)

print("\nDelivery results:")

for delivery in deliveries:
    print(delivery)

print("Delivered:", sum(d["success"] for d in deliveries))
print("Failed:", sum(not d["success"] for d in deliveries))
print("Still queued:", remaining)
