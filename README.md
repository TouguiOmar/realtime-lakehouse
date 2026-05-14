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
                              ┌───────────┴───────────┐
                              ▼                       ▼
                           Airflow               Spark SQL
                        (orchestrate)           (ad-hoc queries)
```

### Data Flow

1. **Postgres** emits row-level changes via the Write-Ahead Log (WAL)
2. **Debezium** captures `INSERT / UPDATE / DELETE` events and publishes them to Kafka topics
3. **Spark Structured Streaming** (`bronze_writer.py`) consumes Kafka topics and writes the raw CDC envelope to the **Bronze** Iceberg layer
4. **Spark** (`silver_writer.py`) reads Bronze, deduplicates events using `ROW_NUMBER()` on `ts_ms`, and merges into **Silver** using Iceberg `MERGE INTO`
5. **Spark** (`gold_writer.py`) aggregates Silver into 3 business-ready **Gold** tables
6. **Airflow** orchestrates Silver and Gold runs every 15 minutes

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
| Orchestration | Apache Airflow 2.8 |

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

# 2. Set up environment variables
cp .env.example .env   # fill in your values

# 3a. Windows — start everything in one command
.\start.ps1            # opens Bronze writer in Terminal 2 automatically

# 3b. Linux / Mac
make demo

# 4. Run Silver + Gold transforms
.\run_silver.ps1       # Windows
.\run_gold.ps1

# or
make silver            # Linux / Mac
make gold
```

---

## All Commands

### Windows (PowerShell)

| Script | Description |
|---|---|
| `.\start.ps1` | Start full stack + register Debezium + seed data + launch Bronze writer |
| `.\run_silver.ps1` | Run Silver MERGE writer |
| `.\run_gold.ps1` | Run Gold aggregations |

### Linux / Mac (Make)

```bash
make help         # show all available commands
make up           # start the stack
make down         # stop and wipe volumes
make restart      # full restart
make register     # register Debezium connector
make seed         # insert sample data
make bronze       # start Bronze streaming writer (blocking)
make silver       # run Silver MERGE
make gold         # run Gold aggregations
make pipeline     # run Silver then Gold in sequence
make demo         # full end-to-end demo
make status       # show service health
make verify-cdc   # consume 5 Kafka CDC events
make connector-status  # check Debezium connector health
```

---

## Current Progress

| Phase | Status | Description |
|---|---|---|
| 1 · Docker setup | ✅ Done | Full 9-service stack running |
| 2 · Postgres + CDC | ✅ Done | Schema, replication slot, publication |
| 3 · Kafka + Debezium | ✅ Done | CDC events flowing, decimal fix applied |
| 4 · Spark → Bronze | ✅ Done | Streaming CDC events landing in Iceberg |
| 5 · Silver + Gold | ✅ Done | MERGE upserts + 3 Gold aggregates |
| 6 · Orchestration | ✅ Done | Airflow DAG + automation scripts |

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
├── .env.example            # environment variable template (copy to .env)
├── Makefile                # Linux/Mac one-command interface
├── start.ps1               # Windows full startup script
├── run_silver.ps1          # Windows Silver runner
├── run_gold.ps1            # Windows Gold runner
├── docker-compose.yml      # full local stack definition
├── postgres/
│   ├── init.sql            # source schema + Debezium user setup
│   └── grants.sql          # Debezium permissions + publication
├── debezium/
│   └── register-connector.json
├── spark/
│   ├── bronze_writer.py    # Streaming CDC → Iceberg Bronze
│   ├── silver_writer.py    # Bronze → Silver MERGE upserts
│   ├── gold_writer.py      # Silver → Gold aggregations
│   └── ivy2/               # cached Spark/Ivy jars (persists across restarts)
└── airflow/
    └── dags/
        └── lakehouse_dag.py  # orchestrates Silver + Gold every 15 min
```

---

## Medallion Layers

### Bronze
Raw CDC events stored as-is. Every `INSERT`, `UPDATE`, and `DELETE` is preserved with the full `before`/`after` payload and operation type (`op`). Enables full audit trail and replay.

### Silver
Deduplicated, upserted current state of each entity. Uses `ROW_NUMBER()` over `ts_ms` to keep the latest event per order, then merges into Silver using Iceberg `MERGE INTO`. Soft-deletes rows where `op = 'd'` using an `is_deleted` flag.

### Gold
Three business-ready aggregate tables:

| Table | Description |
|---|---|
| `daily_revenue` | Revenue + order count per day |
| `order_summary` | Order count + revenue per status |
| `customer_stats` | Lifetime value + avg order per customer |

All Gold tables exclude soft-deleted rows automatically.

---

## CDC Event Structure

Every Kafka message from Debezium follows this envelope:

```json
{
  "op": "u",
  "before": {"id":1, "status":"pending",   "total_usd":"99.99"},
  "after":  {"id":1, "status":"completed", "total_usd":"99.99"},
  "source": {"connector":"postgresql", "db":"ecommerce", "table":"orders", "lsn":29048488},
  "ts_ms":  1776782952099
}
```

Op types: `r` = snapshot, `c` = insert, `u` = update, `d` = delete

---

## Pipeline Results

### Silver table after UPDATE + DELETE
```
+---+-----------+---------+---------+----------+
|id |customer_id|status   |total_usd|is_deleted|
+---+-----------+---------+---------+----------+
|1  |1          |completed|99.99    |false     |  ← updated
|2  |2          |completed|149.50   |true      |  ← soft deleted
|3  |3          |pending  |49.00    |false     |  ← unchanged
+---+-----------+---------+---------+----------+
```

### Gold — daily_revenue
```
+----------+-----------+-----------+-------------+
|day       |order_count|revenue_usd|avg_order_usd|
+----------+-----------+-----------+-------------+
|2026-05-08|2          |148.99     |74.495       |
+----------+-----------+-----------+-------------+
```

### Gold — order_summary
```
+---------+-----------+-------------+
|status   |order_count|total_revenue|
+---------+-----------+-------------+
|completed|1          |99.99        |
|pending  |1          |49.0         |
+---------+-----------+-------------+
```

### Gold — customer_stats
```
+-----------+-----------+--------------+-------------+
|customer_id|order_count|lifetime_value|avg_order_usd|
+-----------+-----------+--------------+-------------+
|1          |1          |99.99         |99.99        |
|3          |1          |49.0          |49.0         |
+-----------+-----------+--------------+-------------+
```

---

## Key Engineering Decisions

**Why PySpark scripts instead of dbt?**
The Spark Thrift Server required by `dbt-spark` is not included in the `apache/spark` Docker image. Using PySpark scripts directly gives the same transformation power with full control over the MERGE logic, and is more transparent for understanding CDC semantics at the engine level.

**Why Iceberg over Delta Lake?**
Iceberg's open spec and catalog-agnostic design makes it easier to run locally without a managed metastore. It also has first-class support for `MERGE INTO` which is essential for CDC upsert patterns.

**Why JSON converter instead of Avro?**
The Debezium Docker image does not bundle the Confluent Avro serializer. For a local dev stack, JSON converters work identically. In production, switch to Avro + Schema Registry for schema enforcement and smaller message sizes.

**Why `REPLICA IDENTITY FULL` on Postgres tables?**
By default, Postgres only includes the primary key in the WAL `before` image on updates. `REPLICA IDENTITY FULL` captures the entire old row, required for Silver MERGE to correctly handle updates and deletes.

**Why `decimal.handling.mode=string` in Debezium?**
By default Debezium encodes `NUMERIC` columns as base64 binary. Setting `string` mode emits human-readable decimals which are easier to parse in Spark without extra decoding logic.

**Why soft deletes in Silver?**
Hard-deleting rows in Silver would lose the information that a record was deleted. Soft deletes with `is_deleted=true` allow Gold aggregations to exclude deleted records while preserving the audit trail.

**Why MERGE in Silver but overwrite in Gold?**
Silver is a live table read by multiple consumers — overwriting it would cause downtime. MERGE updates only changed rows atomically. Gold is always fully recomputed from Silver so overwrite is safe, simpler, and faster.

---

## What's Next

- [ ] Great Expectations — data quality checkpoints on Silver
- [ ] Iceberg compaction + snapshot expiry maintenance tasks
- [ ] Add `order_items` table to the pipeline
- [ ] Trino query layer for ad-hoc SQL on Gold tables
- [ ] OpenLineage for data lineage tracking
- [ ] Grafana dashboard for pipeline observability
