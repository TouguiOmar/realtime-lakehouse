# Real-Time Lakehouse — CDC + Kafka + Apache Iceberg

A production-grade local data engineering project demonstrating a full real-time lakehouse pipeline using Change Data Capture (CDC), Apache Kafka, Apache Spark Structured Streaming, and Apache Iceberg with a medallion architecture.

---

## Architecture

```
Postgres (WAL)
     │
     ▼
Debezium CDC ──► Kafka Topics ──► Spark Structured Streaming
                                          │
                              ┌───────────┼───────────┐
                              ▼           ▼           ▼
                           Bronze      Silver       Gold
                          (raw CDC)  (upserts)  (aggregates)
                              └───────────┴───────────┘
                                      Iceberg on MinIO
                                          │
                              ┌───────────┼───────────┐
                              ▼           ▼           ▼
                            Trino        dbt       Airflow
                          (ad-hoc)  (transform)  (orchestrate)
```

### Data Flow

1. **Postgres** emits row-level changes via the Write-Ahead Log (WAL)
2. **Debezium** captures `INSERT / UPDATE / DELETE` events and publishes them to Kafka topics
3. **Spark Structured Streaming** consumes Kafka topics and writes the raw CDC envelope to the **Bronze** Iceberg layer
4. **dbt Core** transforms Bronze into a deduplicated, upserted **Silver** layer using Iceberg `MERGE INTO`
5. **dbt Core** aggregates Silver into business-ready **Gold** tables
6. **Trino** serves ad-hoc SQL queries directly on Iceberg
7. **Airflow** orchestrates dbt runs and data quality checks on a schedule
8. **Great Expectations** validates data quality at each layer

---

## Stack

| Layer | Technology |
|---|---|
| Source DB | PostgreSQL 15 |
| CDC | Debezium 2.4 |
| Message bus | Apache Kafka (Confluent 7.5) |
| Schema management | Confluent Schema Registry |
| Stream processing | Apache Spark 3.5 Structured Streaming |
| Table format | Apache Iceberg |
| Object storage | MinIO (S3-compatible) |
| Transformation | dbt Core |
| Query engine | Trino |
| Orchestration | Apache Airflow 2.8 |
| Data quality | Great Expectations |

---

## Prerequisites

- Docker Desktop (Engine 24+, Compose v2)
- 8 GB RAM allocated to Docker
- Ports free: `5432, 7077, 8080, 8081, 8082, 8083, 8085, 9000, 9001, 29092`

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/realtime-lakehouse.git
cd realtime-lakehouse

# 2. Start the full stack
docker compose up -d

# 3. Wait for all services to be healthy (~2 min)
docker compose ps

# 4. Register the Debezium connector
cmd /c "docker exec lakehouse-connect curl -X POST http://localhost:8083/connectors -H ""Content-Type: application/json"" -d @/debezium/register-connector.json"

# 5. Seed some data
docker exec lakehouse-postgres psql -U postgres -d ecommerce \
  -c "INSERT INTO orders (customer_id, status, total_usd) VALUES (1, 'pending', 99.99), (2, 'completed', 149.50);"

# 6. Verify CDC events are flowing
docker exec lakehouse-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.public.orders \
  --from-beginning --max-messages 5

# 7. Start the Bronze Spark writer
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark-apps/bronze_writer.py
```

---

## Current Progress

| Phase | Status | Description |
|---|---|---|
| 1 · Docker setup | ✅ Done | Full 9-service stack running |
| 2 · Postgres + CDC | ✅ Done | Schema, replication slot, publication |
| 3 · Kafka + Debezium | ✅ Done | CDC events flowing, decimal fix applied |
| 4 · Spark → Bronze | 🔧 In Progress | Writer works, verifying data in Iceberg |
| 5 · dbt Silver/Gold | ⏳ Pending | MERGE upserts + aggregates |
| 6 · Orchestrate + quality | ⏳ Pending | Airflow DAGs + Great Expectations |

---

## Service UIs

| Service | URL | Credentials |
|---|---|---|
| Spark Master | http://localhost:8080 | — |
| Spark Worker | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | `minioadmin / minioadmin` |
| Kafka Connect REST | http://localhost:8083 | — |
| Schema Registry | http://localhost:8081 | — |
| Airflow | http://localhost:8085 | `admin / admin` |

---

## Project Structure

```
realtime-lakehouse/
├── docker-compose.yml          # Full local stack definition
├── postgres/
│   ├── init.sql                # Source schema + Debezium user setup
│   └── grants.sql              # Debezium permissions + publication
├── debezium/
│   └── register-connector.json # Debezium Postgres connector config
├── spark/
│   ├── bronze_writer.py        # Spark Structured Streaming → Iceberg Bronze
│   └── ivy2/                   # Cached Spark/Ivy jars (persists across restarts)
├── dbt/
│   └── models/
│       ├── silver/             # Dedup + upsert models (coming)
│       └── gold/               # Business aggregate models (coming)
├── airflow/
│   └── dags/                   # Pipeline orchestration DAGs (coming)
└── great_expectations/         # Data quality checkpoints (coming)
```

---

## Medallion Layers

### Bronze
Raw CDC events stored as-is. Every `INSERT`, `UPDATE`, and `DELETE` is preserved with the full `before`/`after` payload and operation type (`op`). Enables full audit trail and replay.

### Silver
Deduplicated, upserted current state of each entity. Handles all four CDC op types (`r`, `c`, `u`, `d`). Uses Iceberg `MERGE INTO` for exactly-once upsert semantics. Soft-deletes rows where `op = 'd'` using an `is_deleted` flag.

### Gold
Business-ready aggregates. Daily revenue, order counts by status, customer lifetime value. Materialized as Iceberg tables for fast query performance via Trino.

---

## CDC Event Structure

Every Kafka message from Debezium follows this envelope:

```json
{
  "op": "c",
  "before": null,
  "after": {
    "id": 1,
    "customer_id": 4,
    "status": "pending",
    "total_usd": "75.00",
    "created_at": "2026-04-21T14:49:11.903714Z",
    "updated_at": "2026-04-21T14:49:11.903714Z"
  },
  "source": {
    "connector": "postgresql",
    "db": "ecommerce",
    "table": "orders",
    "lsn": 29048488,
    "ts_ms": 1776782951904
  },
  "ts_ms": 1776782952099
}
```

Op types: `r` = snapshot, `c` = insert, `u` = update, `d` = delete

---

## Known Issues & Next Steps

### 🔧 In Progress
- Spark worker consumes all cores when `bronze_orders_writer` is running, blocking ad-hoc queries. Fix: configure `spark.cores.max` to limit the streaming job to fewer cores, leaving headroom for other applications.

### ⏳ Up Next
- [ ] Fix Spark resource allocation — limit bronze writer to 2 cores
- [ ] Verify Bronze data landed in Iceberg via spark-sql
- [ ] dbt Silver model — deduplication + MERGE upserts
- [ ] dbt Gold model — daily revenue and order aggregates
- [ ] Airflow DAG — orchestrate dbt runs every 15 minutes
- [ ] Great Expectations — data quality checkpoints on Silver
- [ ] Iceberg compaction + snapshot expiry maintenance tasks
- [ ] Trino query layer with sample analytical queries

---

## Key Engineering Decisions

**Why Iceberg over Delta Lake?**
Iceberg's open spec and catalog-agnostic design make it easier to run locally without a managed metastore. It also has first-class support for `MERGE INTO` which is essential for CDC upsert patterns.

**Why JSON converter instead of Avro?**
The Debezium Docker image does not bundle the Confluent Avro serializer. For a local dev stack, JSON converters work identically and avoid the dependency. In production, switch to Avro + Schema Registry for schema enforcement and smaller message sizes.

**Why `REPLICA IDENTITY FULL` on Postgres tables?**
By default, Postgres only includes the primary key in the WAL `before` image on updates. `REPLICA IDENTITY FULL` captures the entire old row, which is required for the Silver MERGE to correctly handle updates and compute change deltas.

**Why `decimal.handling.mode=string` in Debezium?**
By default Debezium encodes `NUMERIC` columns as base64 binary (`"Jw8="`). Setting `string` mode emits human-readable decimals (`"99.99"`) which are easier to parse in Spark and dbt without extra decoding logic.

---

## License

MIT