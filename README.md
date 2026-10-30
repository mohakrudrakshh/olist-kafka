# Olist Kafka Operations Center

**Multi-source, event-driven data ingestion and Kafka operations demonstration using Apache Kafka, Python, Snowflake, CSV, Parquet, TAR archives, SQLite, Docker, and Flask.**

## Overview

This project explores how different data sources can publish events into a shared distributed streaming platform. Python producers ingest Olist e-commerce records from **Snowflake ETL tables, CSV files, Parquet files, and TAR archives**. Apache Kafka retains and distributes events through partitioned topics; Python consumers persist records into SQLite. A Flask-based Operations Center provides visibility into cluster health, topics, consumer lag, throughput, and publishing activity.

The project also includes producers for **CRUD operations** and **retransmission/duplicate-handling demonstrations**. These are testing paths, not additional external data sources.

> **Verification scope:** The full Snowflake `ORDERS` ingestion has been verified end to end at the unique-record-count level (99,443 records; zero final consumer lag). Other ingestion formats are implemented in the repository, but equivalent full-load verification is not claimed here.

## Architecture

```text
   +--------------------+  +------------------+
   | Snowflake ETL      |  | CSV datasets     |
   +--------------------+  +------------------+
   +--------------------+  +------------------+
   | Parquet files      |  | TAR archives     |
   +--------------------+  +------------------+
                \             /
                 \           /
             Source-specific Python producers
           (Snowflake / CSV / Parquet / TAR)
                           |
                           v
                Apache Kafka (KRaft)
           +-----------+-----------+-----------+
           |  kafka1   |  kafka2   |  kafka3   |
           +-----------+-----------+-----------+
              Topics / partitions / replicas
                           |
                           v
                  Python consumers
             (transformed / raw streams)
                           |
                           v
                     SQLite DB
                 data/olist_kafka.db

      Flask Operations Center (monitoring and controls)
         brokers | topics | partitions | consumer lag
          throughput | events | Snowflake publishing
```

Different producers may publish to different topics and use different message structures; the diagram represents the overall platform, **not a claim that every producer follows the exact same downstream path**.

## Technology stack

| Layer | Technology | Purpose |
|---|---|---|
| Structured warehouse source | Snowflake | Reads transformed Olist data from `OLIST_ECOMMERCE.ETL` |
| File sources | CSV, Apache Parquet, TAR | Demonstrates ingestion of flat files, columnar files, and archived datasets |
| Producer applications | Python | Source-specific extraction, event creation, and publishing |
| Streaming infrastructure | Apache Kafka 3.9.1 | Partitioned event storage, replication, consumer groups |
| Cluster runtime | Docker, Kafka KRaft | Runs three local brokers without ZooKeeper |
| Consumers | Python, `confluent-kafka` | Processes events and persists state |
| Local sink | SQLite | Stores consumed records for verification and demonstration |
| Operations UI | Flask, HTML, CSS, JavaScript | Cluster and pipeline observability |

## Source-specific producers

| File | Input | Role |
|---|---|---|
| `producers/snowflake_producer.py` | Snowflake ETL tables | Queries transformed tables and publishes keyed JSON events |
| `producers/file_producer.py` | CSV | Publishes records from local CSV files |
| `producers/parquet_producer.py` | Parquet | Publishes records read from a columnar file |
| `producers/tar_producer.py` | TAR archive | Publishes records read from archived input |
| `producers/crud_producer.py` | Generated CRUD events | Demonstrates create, update, and delete behavior |
| `producers/retransmission_demo.py` | Test events | Exercises replay/retransmission scenarios |
| `producers/convert_to_parquet.py` | File conversion | Prepares Parquet input; it is not itself a Kafka producer |

Sample input assets are located under `raw/`, `data/parquet/`, and `data/archives/`. Source-specific command-line arguments, event shapes, and topic destinations should be checked in each script before running; they are not assumed to be identical.

### Why multiple source formats?

- **CSV:** Common exports from legacy systems, business partners, and operational tools.
- **Parquet:** Columnar format often used in analytical data lakes and batch pipelines.
- **TAR:** Packaged historical datasets or file-delivery bundles.
- **Snowflake:** Transformed warehouse data used as an upstream source for downstream event consumers.

Kafka receives serialized events from the producers regardless of the original storage format. **This is a batch/source-file publishing demonstration with continuously polling consumers, not automatic Snowflake CDC.**

## Kafka cluster

- **Brokers:** `kafka1`, `kafka2`, `kafka3`
- **Host bootstrap addresses:** `localhost:19092`, `localhost:29092`, `localhost:39092`
- **Docker-network bootstrap address:** `kafka1:9092`
- **Metadata mode:** KRaft
- **Transformed-table topics:** 9 topics, each with 3 partitions and replication factor 3
- **Raw Orders topic:** `olist.raw_orders`, 6 partitions
- **Total observed topology:** 10 topics / 33 partitions
- **Transformed-data consumer group:** `olist-sqlite-consumer-group`
- **Consumers in the verified Orders test:** 3

Partitioning enables parallel processing. Replication distributes partition copies among brokers for fault tolerance; it does **not** mean that three consumers each process every event.

## Snowflake transformed tables

The Snowflake producer reads from `OLIST_ECOMMERCE.ETL`.

| Snowflake table | Kafka topic | Source rows at verification |
|---|---|---:|
| `ORDERS` | `olist.orders` | 99,443 |
| `ORDER_ITEMS` | `olist.order_items` | 112,651 |
| `CUSTOMERS` | `olist.customers` | 99,449 |
| `PRODUCTS` | `olist.products` | 32,952 |
| `PAYMENTS` | `olist.payments` | 103,887 |
| `REVIEWS` | `olist.reviews` | 99,225 |
| `SELLERS` | `olist.sellers` | 3,096 |
| `GEOLOCATION` | `olist.geolocation` | 738,328 |
| `CATEGORY_TRANSLATION` | `olist.category_translation` | 72 |

Counts are a snapshot of source rows. For composite-key tables, the number of unique keys can differ from the number of source rows. **Only Orders has completed full-load reconciliation.**

## Snowflake producer behavior

File: `producers/snowflake_producer.py`

1. Loads Kafka and Snowflake settings from `.env`.
2. Connects to Snowflake and selects rows from the requested table.
3. Converts timestamps, dates, and decimals into JSON-compatible values.
4. Builds a Kafka key using table-specific business key columns.
5. Publishes an event to `olist.<table_name>`.
6. Processes delivery callbacks, handles producer queue backpressure, and flushes outstanding messages.

Example **illustrative shortened** event:

```json
{
  "operation": "INSERT",
  "source": "snowflake",
  "table": "orders",
  "data": {
    "ORDER_ID": "example-order-id",
    "ORDER_STATUS": "delivered"
  }
}
```

The Kafka key is separate from the JSON value. Keyed events generally retain ordering relative to other events with the same key within a partition, assuming stable partitioning.

Relevant producer settings:

```python
{
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "enable.idempotence": True,
    "acks": "all"
}
```

Producer idempotence protects against certain retry-induced duplicates; it does **not** provide end-to-end exactly-once processing or prevent duplicates when a complete source table is deliberately republished.

### Publish Snowflake data

```powershell
# Small demonstration batch
python producers/snowflake_producer.py orders 10

# Entire Orders table (already completed and verified)
python producers/snowflake_producer.py orders 0

# Example full Sellers table (run only when ready)
python producers/snowflake_producer.py sellers 0
```

In this script, `0` means **no SQL LIMIT**. Repeating a full load republishes the rows.

## Consumer design and delivery semantics

Main transformed-table consumer: `consumers/sqlite_consumer.py`.

For each Kafka message, it:

1. Polls its assigned Kafka partitions.
2. Decodes the event and its key.
3. Applies `INSERT`/`UPDATE`/`UPSERT` as a SQLite upsert, or `DELETE` as a delete.
4. Commits the SQLite transaction.
5. Synchronously commits the Kafka offset **after** the database write succeeds.

The consumer uses `enable.auto.commit=False`. This supports **at-least-once processing**: if the process crashes after SQLite commits but before the Kafka offset commits, the message can be replayed. Primary-key UPSERTs make repeated writes safe for the current-state representation.

The consumer currently commits **each message individually** in both SQLite and Kafka. This is easy to demonstrate but limits high-volume throughput. A separate `consumers/raw_sqlite_consumer.py` supports the raw-orders path.

## Flask Operations Center

Local URL: **http://127.0.0.1:5001**

The dashboard includes views or controls for:

- Broker and cluster health
- Topics, partitions, leaders, and replica health
- Consumer groups, offsets, and lag
- Message throughput and operational activity
- Event exploration and publishing
- Snowflake connection and small-batch publishing
- Fault-tolerance and CRUD/retransmission demonstrations

The dashboard's Snowflake publish workflow is designed for **1–1,000 records** per request. Full-table loads use the Python CLI. CLI runs may **not** appear in the dashboard's dashboard-triggered publishing history.

## Verified end-to-end result: Snowflake Orders

The Orders table was fully published from Snowflake and consumed into SQLite.

| Check | Observed result |
|---|---:|
| Snowflake `COUNT(DISTINCT ORDER_ID)` | **99,443** |
| Producer submitted | **99,443** |
| Producer delivered | **99,443** |
| Producer delivery failures | **0** |
| Messages left queued after flush | **0** |
| SQLite `orders` rows | **99,443** |
| Final Kafka consumer lag for `olist.orders` | **0** |

During the load, observed lag fell from **67,238 → 31,411 → 0**. This illustrates **producer–consumer decoupling**: the producer completed its publishing work while Kafka retained a backlog that consumers processed later.

Snowflake and SQLite matched at the **distinct-key count** level. A field-by-field source-to-sink reconciliation has **not** been performed. Full-table validation for the other eight Snowflake tables, and equivalent full-load validation for each file-format ingestion path, remain future work.

## Useful PowerShell checks

```powershell
# Running containers
docker ps

# Consumer group offsets and lag
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --describe --group olist-sqlite-consumer-group

# Orders partitions only
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --describe --group olist-sqlite-consumer-group | Select-String "olist.orders"

# SQLite Orders count
@'
import sqlite3
conn = sqlite3.connect("data/olist_kafka.db")
print("SQLite Orders:", conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0])
conn.close()
'@ | python
```

Consumer lag = **log-end offset − committed consumer offset** for a partition. A zero value means the consumer group was caught up at the time of measurement.

## Repository layout

```text
olist-kafka-platform/
├── README.md
├── .gitignore
├── docker-compose.yml
├── config/
│   └── test_snowflake.py
├── producers/
│   ├── snowflake_producer.py
│   ├── file_producer.py
│   ├── parquet_producer.py
│   ├── tar_producer.py
│   ├── convert_to_parquet.py
│   ├── crud_producer.py
│   └── retransmission_demo.py
├── consumers/
│   ├── sqlite_consumer.py
│   └── raw_sqlite_consumer.py
├── dashboard/
│   ├── app.py
│   ├── broker_health.py
│   ├── event_service.py
│   ├── templates/
│   └── static/
├── raw/                 # CSV inputs
└── data/
    ├── archives/        # TAR input
    ├── parquet/         # Parquet input
    └── olist_kafka.db    # Local database; gitignored
```

Other local backup files may be present in the repository.

## Presentation walkthrough (5 minutes)

1. **Problem and architecture:** Explain why Kafka decouples producers from downstream consumers and how multiple formats enter the platform.
2. **Sources:** Point out the separate CSV, Parquet, TAR, and Snowflake producer scripts.
3. **Cluster:** Show three Kafka brokers, topics, partitions, leaders, and replicas.
4. **Consumers and dashboard:** Show the three-member consumer group, operational telemetry, and consumer lag.
5. **Evidence:** Show the completed **99,443-record Orders** verification and final **zero lag**.

## Limitations and future improvements

- Complete full-load validation for remaining Snowflake tables and each file-format producer.
- Compare source and destination **record values**, not only counts.
- Add continuous change capture or incremental source synchronization rather than manual table snapshots.
- Batch SQLite transactions and Kafka offset commits to improve throughput.
- Add dead-letter topics, structured error logging, and alerting.
- Add automated integration and broker failover tests.
- Consider a production-oriented database and stronger deployment security.

## Configuration and security

Store credentials in environment variables or a local `.env` file. **Never commit `.env`, passwords, API keys, or other secrets.** A sanitized `.env.example` may document required variable names. SQLite runtime files and virtual environments should remain outside version control.

This is a local learning and demonstration environment, not a production-hardened deployment.
