
import os
import sys
import io
import csv
import json
import tarfile
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

TAR_PATH = Path("data/archives/olist_orders.tar")
TOPIC = "olist.raw.orders"


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
            print("Delivery failed:", err)
        else:
            delivered += 1

    with tarfile.open(TAR_PATH, "r") as archive:
        member = archive.getmember("olist_orders_dataset.csv")

        with archive.extractfile(member) as binary_file:
            with io.TextIOWrapper(
                binary_file,
                encoding="utf-8-sig",
                newline=""
            ) as text_file:

                reader = csv.DictReader(text_file)

                for row in reader:
                    if submitted >= limit:
                        break

                    record = {
                        key.upper(): value
                        for key, value in row.items()
                    }

                    key = json.dumps([record["ORDER_ID"]])

                    event = {
                        "operation": "UPSERT",
                        "source": "tar_csv",
                        "table": "orders",
                        "data": record
                    }

                    producer.produce(
                        TOPIC,
                        key=key.encode("utf-8"),
                        value=json.dumps(event).encode("utf-8"),
                        callback=callback
                    )

                    producer.poll(0)
                    submitted += 1

    remaining = producer.flush(timeout=60)

    print("Archive:", TAR_PATH)
    print("Topic:", TOPIC)
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
