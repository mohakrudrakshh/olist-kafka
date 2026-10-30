# Olist Kafka Operations Center

**An event-driven data engineering and Kafka observability demonstration using Snowflake, Apache Kafka, Python, SQLite, Docker, and Flask.**

## Project overview

This project demonstrates how a distributed event-streaming platform can move transformed e-commerce data from Snowflake to downstream storage while exposing operational behavior through a web dashboard.

A Python producer extracts records from the `OLIST_ECOMMERCE.ETL` schema in Snowflake and publishes JSON events to Apache Kafka. Three Kafka brokers provide replicated partition storage. Three Python consumers in one consumer group process the transformed-table topics and persist record state in SQLite. A Flask Operations Center provides visibility into Kafka health and activity.

The project is designed as a hands-on demonstration of **topics, partitions, replication, consumer groups, offsets, lag, idempotent writes, and producer–consumer decoupling**.

## Architecture

```text
                  Snowflake
           OLIST_ECOMMERCE.ETL
                    |
                    | SQL extraction
                    v
           Python Snowflake Producer
           - JSON event serialization
           - record-based message keys
           - idempotent Kafka producer
                    |
                    v
        Apache Kafka (KRaft, Docker)
        +---------+---------+---------+
        | kafka1  | kafka2  | kafka3  |
        +---------+---------+---------+
        | Topics / partitions / replicas |
        +-------------------------------+
                    |
          Consumer group (3 members)
          /         |          \
     Consumer 1  Consumer 2  Consumer 3
          \         |          /
                    v
             SQLite database
             data/olist_kafka.db

       Flask Kafka Operations Center
       - broker / topic visibility
       - partition leadership and replication
       - consumer groups and lag
       - throughput and operational views
       - Snowflake publishing controls
```

A separate raw-orders consumer and topic are also part of the platform.

## Technology stack

| Component | Technology | Responsibility |
|---|---|---|
| Source data | Snowflake | Transformed Olist e-commerce tables |
| Event streaming | Apache Kafka 3.9.1 | Durable, partitioned event log |
| Cluster orchestration | Docker | Runs the Kafka brokers locally |
| Metadata management | Kafka KRaft | Kafka metadata without ZooKeeper |
| Producer | Python, `confluent-kafka`, Snowflake Connector | Extracts and publishes JSON events |
| Consumers | Python, `confluent-kafka` | Reads events and updates SQLite |
| Downstream database | SQLite | Local materialized record state |
| Monitoring interface | Flask | Kafka Operations Center dashboard |

## Kafka cluster configuration

- **Brokers:** `kafka1`, `kafka2`, `kafka3`
- **Host bootstrap addresses:** `localhost:19092`, `localhost:29092`, `localhost:39092`
- **Container bootstrap address:** `kafka1:9092`
- **Kafka mode:** KRaft
- **Transformed-data topics:** 9, each with 3 partitions and replication factor 3
- **Raw orders topic:** 1, with 6 partitions
- **Total topics / partitions:** 10 topics / 33 partitions
- **Transformed-data consumer group:** `olist-sqlite-consumer-group`
- **Active transformed-data consumers in the verified test:** 3

With three consumers and three partitions per transformed topic, each consumer can own one partition from each topic, distributing processing across the group.

## Data sources and topics

The producer reads from the Snowflake `OLIST_ECOMMERCE.ETL` schema.

| Snowflake table | Kafka topic | Source row count at verification |
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

**Note:** Source row counts are a snapshot. They are not necessarily equal to distinct record-key counts, especially for tables with composite keys. Full-load verification is complete for **Orders only**.

## Event format

Each transformed record is published as a JSON event with a stable Kafka key derived from the table's identifying column(s).

Example (illustrative, shortened):

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

- The **topic** identifies the dataset.
- The **key** determines partition assignment and supports consistent record identity.
- The **value** contains the operation, source, table, and record data.
- Events with the same key are routed to the same partition under a stable partitioning configuration.

## Producer design

File: `producers/snowflake_producer.py`

1. Loads credentials and Kafka settings from environment variables.
2. Connects to Snowflake and selects the requested ETL table.
3. Serializes Snowflake values (including dates, timestamps, and decimals) into JSON-compatible values.
4. Builds a key from the table-specific key columns.
5. Publishes each event to the matching Kafka topic.
6. Processes delivery callbacks and flushes pending messages before reporting results.

Producer configuration includes:

```python
{
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "enable.idempotence": True,
    "acks": "all"
}
```

Producer idempotence protects against certain retry-related duplicates **within producer sessions**; it does not make the entire Snowflake-to-SQLite workflow exactly-once.

### Publish records

Activate the project's Python virtual environment, ensure Kafka is running, and use:

```powershell
# Publish up to 10 Orders records
python producers/snowflake_producer.py orders 10

# Publish the complete Sellers table
python producers/snowflake_producer.py sellers 0
```

In the current producer implementation, **`0` means no SQL LIMIT (full table)**. Avoid repeating full-table publishes unintentionally.

## Consumer design

File: `consumers/sqlite_consumer.py`

The transformed-data consumers:

1. Poll Kafka for a message.
2. Decode its key and JSON event.
3. Apply an insert/update (UPSERT) or delete in the corresponding SQLite table.
4. Commit the SQLite transaction.
5. Synchronously commit the processed Kafka offset.

The consumer uses **manual offset commits** (`enable.auto.commit=False`). Database commits happen before Kafka offset commits.

### Delivery semantics

The pipeline provides **at-least-once processing** rather than end-to-end exactly-once delivery. If a consumer crashes after the SQLite commit but before its Kafka offset commit, Kafka can replay the message. A primary-key-based SQLite UPSERT makes replay safe for the stored current-record state.

The current implementation commits SQLite and Kafka offsets **per message**. This is simple to reason about and verify, but it can limit throughput under larger workloads.

## Flask Operations Center

Local dashboard: **http://127.0.0.1:5001**

The dashboard is intended to demonstrate and inspect:

- Kafka broker and cluster health
- Topic and partition information
- Partition leaders and replica health
- Consumer groups, committed offsets, and lag
- Message throughput and operational activity
- Snowflake source integration and small-batch publishing
- Fault-tolerance concepts and operational testing

**Important:** The dashboard's Snowflake publishing workflow is limited to small demo batches (1–1,000 records). Full-table loads are executed through the Python CLI. CLI publishing may not appear in the dashboard's dashboard-triggered run history.

## Verified end-to-end test: Orders

The complete `ORDERS` table was published from Snowflake and consumed into SQLite.

| Verification | Result |
|---|---:|
| Snowflake `COUNT(DISTINCT ORDER_ID)` | **99,443** |
| Kafka messages submitted | **99,443** |
| Kafka messages delivered | **99,443** |
| Kafka delivery failures | **0** |
| Messages remaining in producer queue | **0** |
| SQLite `orders` row count | **99,443** |
| Final consumer lag across Orders partitions | **0** |

### Observed lag recovery

During the full load, the producer published faster than the consumers could persist events. The measured Orders consumer lag fell from **67,238** to **31,411**, and ultimately to **0**.

This demonstrates **asynchronous decoupling**: the producer can finish publishing while Kafka retains messages for downstream consumers to process at their own pace.

The Snowflake and SQLite **distinct-key counts matched**. A complete field-by-field reconciliation has not yet been performed.

## Operational verification commands

### Inspect consumer lag

```powershell
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --describe --group olist-sqlite-consumer-group
```

To focus on Orders:

```powershell
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --describe --group olist-sqlite-consumer-group | Select-String "olist.orders"
```

**Lag** is the difference between the log-end offset and the consumer group's committed offset. Zero lag means the group's committed offsets have caught up with the topic's log ends at the time of inspection.

### Check SQLite Orders count

```powershell
@'
import sqlite3
conn = sqlite3.connect("data/olist_kafka.db")
print("SQLite Orders:", conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0])
conn.close()
'@ | python
```

### Check running Docker containers

```powershell
docker ps
```

## Fault tolerance and design choices

**Why Kafka?** Kafka separates ingestion from downstream processing, retains events independently of consumer speed, and allows consumer groups to scale across partitions.

**Why three brokers?** Three brokers enable replication and allow the project to demonstrate partition leadership and broker-failure behavior. Successful failover depends on replica synchronization and cluster configuration.

**Why three consumers?** Consumers in one group share assigned partitions. Three consumers can process the three partitions of `olist.orders` concurrently.

**Why SQLite?** It provides a lightweight local destination for validating record persistence and demonstrating event processing without deploying another database service.

**Why commit SQLite before Kafka offsets?** It avoids marking a message processed before its database write succeeds. Replayed messages can be handled by record-key UPSERTs.

## Current scope and future improvements

### Verified

- Three-broker Kafka cluster and three active transformed-data consumers
- Nine transformed-data topics with partitioned, replicated storage
- Successful full-table producer run for Orders
- Zero consumer lag after the Orders full load
- Exact match between Snowflake and SQLite unique Orders counts
- Successful small-table producer test for Category Translation

### Still to validate or extend

- Full-table ingestion and distinct-key reconciliation for the remaining eight ETL tables
- Field-by-field reconciliation and data quality checks
- Structured error handling and dead-letter topics
- Throughput benchmarks and alert thresholds
- Production-grade downstream database and transaction tuning
- Repeatable automated integration and failover tests

## Suggested five-minute demo

1. **Architecture:** Explain Snowflake → producer → Kafka → consumer group → SQLite.
2. **Kafka cluster:** Show three brokers, topic partitions, leaders, and replicas.
3. **Consumers:** Show three group members and their partition assignments.
4. **Live monitoring:** Show the dashboard's lag, throughput, and operational views.
5. **Evidence:** Present the 99,443-record Orders reconciliation and zero final lag.

> **Key takeaway:** The project demonstrates not just moving data, but observing how a distributed event pipeline behaves when producers and consumers operate at different speeds.

## Configuration and security

Credentials are loaded from environment variables (for example, Snowflake connection settings and `KAFKA_BOOTSTRAP_SERVERS`). Do **not** commit `.env` files, passwords, or other secrets to source control. This is a local demonstration architecture; production deployments require additional authentication, authorization, encryption, monitoring, and operational controls.
