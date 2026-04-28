"""
bronze_writer.py
────────────────
Spark Structured Streaming job that consumes CDC events from Kafka
and writes the raw envelope into the Bronze Iceberg table on MinIO.

Handles all 4 Debezium op types:
  r = snapshot read
  c = insert
  u = update
  d = delete
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

# ── Iceberg + MinIO + Kafka package coordinates ───────────────────
PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = "kafka:9092"
KAFKA_TOPIC      = "cdc.public.orders"
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
WAREHOUSE        = "s3a://lakehouse/warehouse"
CHECKPOINT_PATH  = "s3a://lakehouse-checkpoints/bronze_orders"
CATALOG          = "lakehouse"
BRONZE_TABLE     = f"{CATALOG}.bronze.orders"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze_orders_writer")
        .config("spark.jars.packages", PACKAGES)
        # ── Iceberg catalog ──────────────────────────────────────
        .config(f"spark.sql.catalog.{CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        # ── MinIO / S3A ──────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # ── Extensions ───────────────────────────────────────────
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


# ── Debezium JSON envelope schema ─────────────────────────────────
# We capture the raw after/before as strings and parse in Silver.
# Only extract top-level fields needed for Bronze.
ENVELOPE_SCHEMA = StructType([
    StructField("op",     StringType(), True),   # c/u/d/r
    StructField("ts_ms",  LongType(),   True),   # event timestamp (ms)
    StructField("before", StringType(), True),   # old row JSON
    StructField("after",  StringType(), True),   # new row JSON
])


def create_bronze_table(spark: SparkSession) -> None:
    """Create the Bronze Iceberg table if it doesn't exist."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            op           STRING   COMMENT 'CDC operation: c=insert u=update d=delete r=snapshot',
            ts_ms        BIGINT   COMMENT 'Debezium event timestamp in milliseconds',
            before       STRING   COMMENT 'JSON string of old row state (null for inserts)',
            after        STRING   COMMENT 'JSON string of new row state (null for deletes)',
            ingested_at  TIMESTAMP COMMENT 'Wall-clock time this record landed in Bronze'
        )
        USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    print(f"✔ Bronze table ready: {BRONZE_TABLE}")


def process_batch(batch_df, batch_id: int) -> None:
    """
    foreachBatch handler.
    Filters out tombstone messages (null value) then appends to Bronze.
    """
    count = batch_df.count()
    if count == 0:
        return

    print(f"Batch {batch_id}: writing {count} CDC events to Bronze")

    (
        batch_df
        .write
        .format("iceberg")
        .mode("append")
        .save(BRONZE_TABLE)
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Create table on startup
    create_bronze_table(spark)

    # ── Read from Kafka ───────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse Debezium envelope ───────────────────────────────────
    parsed = (
        raw
        # Kafka value is bytes → cast to string first
        .withColumn("json_str", col("value").cast(StringType()))
        # Drop tombstone messages (null value = Debezium delete marker)
        .filter(col("json_str").isNotNull())
        # Parse the envelope
        .withColumn("envelope", from_json(col("json_str"), ENVELOPE_SCHEMA))
        # Flatten into Bronze columns
        .select(
            col("envelope.op").alias("op"),
            col("envelope.ts_ms").alias("ts_ms"),
            col("envelope.before").cast(StringType()).alias("before"),
            col("envelope.after").cast(StringType()).alias("after"),
            current_timestamp().alias("ingested_at"),
        )
        # Keep all op types: r (snapshot), c (insert), u (update), d (delete)
        .filter(col("op").isin("r", "c", "u", "d"))
    )

    # ── Write to Iceberg Bronze via foreachBatch ──────────────────
    query = (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", BRONZE_TABLE)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")   # micro-batch every 10s
        .start()
    )

    print(f"✔ Streaming query started — writing to {BRONZE_TABLE}")
    print("   Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()