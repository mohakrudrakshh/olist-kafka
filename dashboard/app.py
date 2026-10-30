
from pathlib import Path
import sqlite3

from event_service import get_recent_events
from flask import Flask, jsonify, render_template
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv
import os
import subprocess
import sys
from flask import request
import json
from datetime import datetime, timezone
from confluent_kafka import Producer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

app = Flask(__name__)

DB_PATH = PROJECT_ROOT / "data" / "olist_kafka.db"

@app.route("/")
def home():
    return render_template("index.html")


@app.route("/api/cluster")
def cluster():
    try:
        admin = AdminClient({
            "bootstrap.servers": os.environ[
                "KAFKA_BOOTSTRAP_SERVERS"
            ]
        })

        metadata = admin.list_topics(timeout=5)

        topics = {
            name: len(topic.partitions)
            for name, topic in metadata.topics.items()
            if name.startswith("olist.")
        }

        return jsonify({
            "connected": True,
            "brokers": len(metadata.brokers),
            "topics": len(topics),
            "partitions": sum(topics.values()),
            "topic_details": topics
        })

    except Exception as exc:
        return jsonify({
            "connected": False,
            "error": str(exc)
        }), 503


@app.route("/api/database")
def database():
    try:
        with sqlite3.connect(DB_PATH, timeout=5) as conn:
            counts = {}

            for table in ("orders", "raw_orders"):
                exists = conn.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type='table' AND name=?
                    """,
                    (table,)
                ).fetchone()

                if exists:
                    counts[table] = conn.execute(
                        f'SELECT COUNT(*) FROM "{table}"'
                    ).fetchone()[0]

        return jsonify(counts)

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500




@app.route("/api/produce", methods=["POST"])
def produce():
    payload = request.get_json(silent=True) or {}

    source = payload.get("source")
    limit = payload.get("limit", 10)

    scripts = {
        "csv": PROJECT_ROOT / "producers" / "file_producer.py",
        "parquet": PROJECT_ROOT / "producers" / "parquet_producer.py",
        "tar": PROJECT_ROOT / "producers" / "tar_producer.py",
    }

    if source not in scripts:
        return jsonify({"error": "Invalid producer"}), 400

    if type(limit) is not int or not 1 <= limit <= 1000:
        return jsonify({
            "error": "Limit must be between 1 and 1000"
        }), 400

    command = [sys.executable, str(scripts[source])]

    if source == "csv":
        command.extend(["orders", str(limit)])
    else:
        command.append(str(limit))

    try:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=120
        )

        return jsonify({
            "success": result.returncode == 0,
            "output": result.stdout,
            "error": result.stderr,
        }), 200 if result.returncode == 0 else 500

    except subprocess.TimeoutExpired:
        return jsonify({
            "error": "Producer timed out"
        }), 504


@app.route("/api/consumers")
def consumers():
    groups = [
        "olist-sqlite-consumer-group",
        "olist-raw-sqlite-group"
    ]

    results = []

    for group in groups:
        command = [
            "docker", "exec", "kafka1",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "kafka1:9092",
            "--describe",
            "--group", group
        ]

        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=15
            )

            if result.returncode != 0:
                results.append({
                    "group": group,
                    "error": result.stderr.strip()
                })
                continue

            partitions = []
            total_lag = 0

            for line in result.stdout.splitlines():
                parts = line.split()

                if not parts or parts[0] != group:
                    continue

                if len(parts) < 6:
                    continue

                try:
                    lag = int(parts[5])
                except ValueError:
                    lag = None

                partitions.append({
                    "topic": parts[1],
                    "partition": parts[2],
                    "offset": parts[3],
                    "end_offset": parts[4],
                    "lag": lag
                })

                if lag is not None:
                    total_lag += lag

            results.append({
                "group": group,
                "partitions": len(partitions),
                "total_lag": total_lag,
                "details": partitions
            })

        except Exception as exc:
            results.append({
                "group": group,
                "error": str(exc)
            })

    return jsonify(results)




@app.route("/api/consumer-assignments")
def consumer_assignments():
    group = request.args.get(
        "group", "olist-sqlite-consumer-group"
    )

    allowed_groups = {
        "olist-sqlite-consumer-group",
        "olist-raw-sqlite-group"
    }

    if group not in allowed_groups:
        return jsonify({"error": "Invalid group"}), 400

    command = [
        "docker", "exec", "kafka1",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "kafka1:9092",
        "--describe",
        "--group", group,
        "--members",
        "--verbose"
    ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=15
        )

        if result.returncode != 0:
            return jsonify({
                "error": result.stderr.strip()
            }), 500

        members = []

        for line in result.stdout.splitlines():
            parts = line.split()

            if not parts or parts[0] != group:
                continue

            if len(parts) < 6:
                continue

            members.append({
                "consumer_id": parts[1],
                "host": parts[2],
                "client_id": parts[3],
                "partitions": parts[4],
                "assignments": " ".join(parts[5:])
            })

        return jsonify({
            "group": group,
            "members": members
        })

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500



DEMO_ORDER_ID = "KAFKA-FLASK-DEMO-001"


@app.route("/api/crud", methods=["POST"])
def crud():
    payload = request.get_json(silent=True) or {}
    action = payload.get("action", "").upper()

    if action not in {"CREATE", "UPDATE", "DELETE"}:
        return jsonify({"error": "Invalid CRUD action"}), 400

    producer = Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "enable.idempotence": True,
        "acks": "all"
    })

    key = json.dumps([DEMO_ORDER_ID])

    if action == "DELETE":
        operation = "DELETE"
        data = None
    else:
        operation = "INSERT" if action == "CREATE" else "UPDATE"
        data = {
            "ORDER_ID": DEMO_ORDER_ID,
            "CUSTOMER_ID": "FLASK-DEMO-CUSTOMER",
            "ORDER_STATUS": (
                "created" if action == "CREATE" else "delivered"
            ),
            "ORDER_PURCHASE_TIMESTAMP": datetime.now(
                timezone.utc
            ).isoformat()
        }

    event = {
        "operation": operation,
        "source": "flask",
        "table": "orders",
        "data": data
    }

    deliveries = []

    def callback(err, msg):
        deliveries.append({
            "error": str(err) if err else None,
            "partition": msg.partition() if not err else None,
            "offset": msg.offset() if not err else None
        })

    producer.produce(
        "olist.orders",
        key=key.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=callback
    )

    remaining = producer.flush(timeout=15)

    if remaining or not deliveries or deliveries[0]["error"]:
        return jsonify({
            "error": "Kafka delivery failed",
            "details": deliveries
        }), 503

    return jsonify({
        "success": True,
        "action": action,
        "order_id": DEMO_ORDER_ID,
        "delivery": deliveries[0]
    })


@app.route("/api/crud/status")
def crud_status():
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT record_json
            FROM orders
            WHERE record_key = ?
            """,
            (json.dumps([DEMO_ORDER_ID]),)
        ).fetchone()

    return jsonify({
        "exists": row is not None,
        "record": json.loads(row[0]) if row else None
    })


@app.route("/api/topics")
def topic_explorer():
    try:
        admin = AdminClient({
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        })

        metadata = admin.list_topics(timeout=10)

        topics = []

        for name, topic in sorted(metadata.topics.items()):
            if not name.startswith("olist."):
                continue

            partitions = []

            for partition_id, partition in sorted(
                topic.partitions.items()
            ):
                partitions.append({
                    "id": partition_id,
                    "leader": partition.leader,
                    "replicas": partition.replicas,
                    "isr": partition.isrs,
                    "healthy": (
                        partition.leader >= 0
                        and len(partition.isrs) == len(partition.replicas)
                    )
                })

            topics.append({
                "name": name,
                "partition_count": len(partitions),
                "replication_factor": (
                    len(partitions[0]["replicas"])
                    if partitions else 0
                ),
                "partitions": partitions
            })

        return jsonify({
            "topics": topics,
            "total_topics": len(topics)
        })

    except Exception as exc:
        return jsonify({"error": str(exc)}), 503



def get_broker_status():
    result = subprocess.run(
        ["docker", "inspect", "--format",
         "{{.State.Running}}", "kafka2"],
        capture_output=True,
        text=True,
        timeout=10
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())

    return result.stdout.strip().lower() == "true"


@app.route("/api/broker/status")
def broker_status():
    try:
        return jsonify({
            "broker": "kafka2",
            "running": get_broker_status()
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@app.route("/api/broker/control", methods=["POST"])
def broker_control():
    payload = request.get_json(silent=True) or {}
    action = payload.get("action")

    if action not in ("stop", "start"):
        return jsonify({"error": "Invalid action"}), 400

    try:
        currently_running = get_broker_status()

        if action == "stop":
            if not currently_running:
                return jsonify({
                    "error": "Broker 2 is already stopped"
                }), 409

            # Do not intentionally remove another broker
            # if one of the surviving brokers is unavailable.
            for broker in ("kafka1", "kafka3"):
                check = subprocess.run(
                    ["docker", "inspect", "--format",
                     "{{.State.Running}}", broker],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if (
                    check.returncode != 0
                    or check.stdout.strip().lower() != "true"
                ):
                    return jsonify({
                        "error": f"{broker} is not running. "
                                 "Stopping kafka2 is blocked."
                    }), 409

        result = subprocess.run(
            ["docker", action, "kafka2"],
            capture_output=True,
            text=True,
            timeout=45
        )

        if result.returncode != 0:
            return jsonify({
                "error": result.stderr.strip()
            }), 500

        return jsonify({
            "success": True,
            "action": action,
            "broker": "kafka2",
            "running": get_broker_status()
        })

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/events")
def recent_events():
    topic = request.args.get(
        "topic", "olist.raw.orders"
    )

    try:
        limit = int(request.args.get("limit", 25))
    except ValueError:
        return jsonify({"error": "Invalid limit"}), 400

    if not 1 <= limit <= 100:
        return jsonify({
            "error": "Limit must be between 1 and 100"
        }), 400

    # Only permit topics belonging to this project.
    if not topic.startswith("olist."):
        return jsonify({"error": "Invalid topic"}), 400

    try:
        events = get_recent_events(topic, limit)

        return jsonify({
            "topic": topic,
            "count": len(events),
            "events": events
        })

    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@app.route("/api/retransmission", methods=["POST"])
def run_retransmission():
    payload = request.get_json(silent=True) or {}
    attempts = payload.get("attempts", 3)

    if type(attempts) is not int or not 2 <= attempts <= 10:
        return jsonify({
            "error": "Attempts must be between 2 and 10"
        }), 400

    script = PROJECT_ROOT / "producers" / "retransmission_demo.py"

    try:
        result = subprocess.run(
            [sys.executable, str(script), str(attempts)],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=45
        )

        if result.returncode != 0:
            return jsonify({
                "success": False,
                "error": result.stderr or result.stdout
            }), 500

        import ast
        import re

        output = result.stdout

        order_match = re.search(
            r"Order ID:\s*(\S+)", output
        )

        event_match = re.search(
            r"Event ID:\s*(\S+)", output
        )

        if not order_match or not event_match:
            return jsonify({
                "error": "Unable to parse producer output",
                "output": output
            }), 500

        order_id = order_match.group(1)
        event_id = event_match.group(1)

        deliveries = []

        for line in output.splitlines():
            line = line.strip()

            if line.startswith("{'success':"):
                deliveries.append(ast.literal_eval(line))

        successful = [
            item for item in deliveries
            if item.get("success")
        ]

        return jsonify({
            "success": len(successful) == attempts,
            "event_id": event_id,
            "order_id": order_id,
            "attempts": attempts,
            "delivered": len(successful),
            "deliveries": deliveries,
            "output": output
        })

    except subprocess.TimeoutExpired:
        return jsonify({
            "error": "Retransmission test timed out"
        }), 504
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/retransmission/status")
def retransmission_status():
    order_id = request.args.get("order_id", "")

    if not order_id.startswith("KAFKA-RETRY-"):
        return jsonify({"error": "Invalid order ID"}), 400

    key = json.dumps([order_id])

    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        row = conn.execute(
            """
            SELECT record_json
            FROM raw_orders
            WHERE record_key = ?
            """,
            (key,)
        ).fetchone()

    return jsonify({
        "order_id": order_id,
        "unique_records": 1 if row else 0,
        "record": json.loads(row[0]) if row else None
    })



@app.route("/api/brokers/health")
def brokers_health():
    try:
        from broker_health import snapshot
        return jsonify(snapshot())
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


from pathlib import Path
from datetime import datetime, timezone
import threading

SNOWFLAKE_RUN_FILE = PROJECT_ROOT / "data" / "snowflake_runs.json"
SNOWFLAKE_RUN_LOCK = threading.Lock()


def read_snowflake_runs():
    if not SNOWFLAKE_RUN_FILE.exists():
        return []

    with SNOWFLAKE_RUN_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_snowflake_run(table, submitted, delivered, failed):
    with SNOWFLAKE_RUN_LOCK:
        runs = read_snowflake_runs()

        runs.append({
            "table": table,
            "topic": f"olist.{table}",
            "submitted": submitted,
            "delivered": delivered,
            "failed": failed,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

        SNOWFLAKE_RUN_FILE.parent.mkdir(
            parents=True, exist_ok=True
        )

        with SNOWFLAKE_RUN_FILE.open("w", encoding="utf-8") as file:
            json.dump(runs, file, indent=2)







@app.route("/api/snowflake/publish", methods=["POST"])
def snowflake_publish():
    payload = request.get_json(silent=True) or {}

    table = payload.get("table", "")
    limit = payload.get("limit", 100)

    allowed_tables = {
        "orders", "order_items", "customers",
        "products", "payments", "reviews",
        "sellers", "geolocation", "category_translation"
    }

    if table not in allowed_tables:
        return jsonify({"error": "Invalid Snowflake table"}), 400

    if type(limit) is not int or not 1 <= limit <= 1000:
        return jsonify({"error": "Limit must be between 1 and 1000"}), 400

    script = PROJECT_ROOT / "producers" / "snowflake_producer.py"

    if not script.is_file():
        return jsonify({
            "error": "Snowflake producer script not found"
        }), 500

    try:
        result = subprocess.run(
            [sys.executable, str(script), table, str(limit)],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=120
        )


        if result.returncode == 0:
            import re

            submitted_match = re.search(
                r"Submitted:\s*(\d+)", result.stdout
            )

            delivered_match = re.search(
                r"Delivered:\s*(\d+)", result.stdout
            )

            failed_match = re.search(
                r"Failed:\s*(\d+)", result.stdout
            )

            if submitted_match and delivered_match and failed_match:
                save_snowflake_run(
                    table=table,
                    submitted=int(submitted_match.group(1)),
                    delivered=int(delivered_match.group(1)),
                    failed=int(failed_match.group(1))
                )

        return jsonify({
            "success": result.returncode == 0,
            "table": table,
            "topic": f"olist.{table}",
            "output": result.stdout,
            "error": result.stderr if result.returncode else None
        }), 200 if result.returncode == 0 else 500

    except subprocess.TimeoutExpired:
        return jsonify({
            "error": "Snowflake producer timed out"
        }), 504





    

@app.route("/api/snowflake/status", methods=["GET"])
def snowflake_status():
    import snowflake.connector
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")

    connection = None

    try:
        connection = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema="ETL",
            login_timeout=10,
            network_timeout=15
        )

        cursor = connection.cursor()

        try:
            cursor.execute("""
                SELECT TABLE_NAME
                FROM OLIST_ECOMMERCE.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'ETL'
                  AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)

            allowed = {
                "ORDERS", "ORDER_ITEMS", "CUSTOMERS",
                "PRODUCTS", "PAYMENTS", "REVIEWS",
                "SELLERS", "GEOLOCATION", "CATEGORY_TRANSLATION"
            }

            tables = [
                row[0]
                for row in cursor.fetchall()
                if row[0] in allowed
            ]

            return jsonify({
                "connected": True,
                "database": "OLIST_ECOMMERCE",
                "schema": "ETL",
                "table_count": len(tables),
                "tables": tables
            })

        finally:
            cursor.close()

    except Exception:
        app.logger.exception("Snowflake status check failed")
        return jsonify({
            "connected": False,
            "error": "Snowflake connection or metadata query failed"
        }), 503

    finally:
        if connection is not None:
            connection.close()

@app.route("/api/snowflake/runs", methods=["GET"])
def snowflake_runs():
    runs = read_snowflake_runs()

    return jsonify({
        "total_published": sum(
            run["delivered"] for run in runs
        ),
        "last_run": runs[-1]["timestamp"] if runs else None,
        "recent_runs": runs[-10:][::-1]
    })


if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=5001,
        debug=True
    )
