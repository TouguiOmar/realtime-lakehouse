"""
silver_writer.py
────────────────
Reads from Bronze Iceberg table, deduplicates CDC events,
and merges into the Silver Iceberg table.

Handles all 4 Debezium op types:
  r = snapshot read  → upsert
  c = insert         → upsert
  u = update         → upsert
  d = delete         → soft delete (is_deleted = True)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, row_number, current_timestamp, lit, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DecimalType, TimestampType
)
from pyspark.sql.window import Window

# ── Config ────────────────────────────────────────────────────────
PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
WAREHOUSE        = "s3a://lakehouse/warehouse"
CATALOG          = "lakehouse"
BRONZE_TABLE     = f"{CATALOG}.bronze.orders"
SILVER_TABLE     = f"{CATALOG}.silver.orders"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_orders_writer")
        .config("spark.jars.packages", PACKAGES)
        .config(f"spark.sql.catalog.{CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .master("local[2]")
        .getOrCreate()
    )


# ── Schema of the after/before JSON payload ───────────────────────
ORDER_SCHEMA = StructType([
    StructField("id",          LongType(),    True),
    StructField("customer_id", LongType(),    True),
    StructField("status",      StringType(),  True),
    StructField("total_usd",   StringType(),  True),  # string due to decimal.handling.mode=string
    StructField("created_at",  StringType(),  True),
    StructField("updated_at",  StringType(),  True),
])


def create_silver_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            id          BIGINT       COMMENT 'Order primary key',
            customer_id BIGINT       COMMENT 'Customer foreign key',
            status      STRING       COMMENT 'Order status',
            total_usd   STRING       COMMENT 'Order total in USD',
            created_at  STRING       COMMENT 'Original creation timestamp',
            updated_at  STRING       COMMENT 'Last update timestamp',
            is_deleted  BOOLEAN      COMMENT 'Soft delete flag',
            dbt_updated_at TIMESTAMP COMMENT 'When this record was last written to Silver'
        )
        USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.upsert.enabled' = 'true'
        )
    """)
    print(f"✔ Silver table ready: {SILVER_TABLE}")


def run_merge(spark: SparkSession) -> None:
    """
    Read all Bronze events, deduplicate by keeping the latest
    event per order id (by ts_ms), then MERGE into Silver.
    """

    # ── Read Bronze ───────────────────────────────────────────────
    bronze = spark.read.format("iceberg").load(BRONZE_TABLE)

    # ── Parse after/before JSON ───────────────────────────────────
    parsed = (
        bronze
        .withColumn("after_parsed",  from_json(col("after"),  ORDER_SCHEMA))
        .withColumn("before_parsed", from_json(col("before"), ORDER_SCHEMA))
    )

    # ── Get the row id regardless of op type ─────────────────────
    with_id = parsed.withColumn(
        "order_id",
        when(col("op") == "d", col("before_parsed.id"))
        .otherwise(col("after_parsed.id"))
    )

    # ── Deduplicate: keep latest event per order_id ───────────────
    window = Window.partitionBy("order_id").orderBy(col("ts_ms").desc())
    deduped = (
        with_id
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # ── Build the staging dataframe ───────────────────────────────
    staged = deduped.select(
        col("order_id").alias("id"),
        when(col("op") == "d", col("before_parsed.customer_id"))
            .otherwise(col("after_parsed.customer_id")).alias("customer_id"),
        when(col("op") == "d", col("before_parsed.status"))
            .otherwise(col("after_parsed.status")).alias("status"),
        when(col("op") == "d", col("before_parsed.total_usd"))
            .otherwise(col("after_parsed.total_usd")).alias("total_usd"),
        when(col("op") == "d", col("before_parsed.created_at"))
            .otherwise(col("after_parsed.created_at")).alias("created_at"),
        when(col("op") == "d", col("before_parsed.updated_at"))
            .otherwise(col("after_parsed.updated_at")).alias("updated_at"),
        when(col("op") == "d", lit(True)).otherwise(lit(False)).alias("is_deleted"),
        current_timestamp().alias("dbt_updated_at"),
    )

    # ── Register as temp view for MERGE SQL ──────────────────────
    staged.createOrReplaceTempView("staged_orders")

    count = staged.count()
    print(f"   Staging {count} deduplicated orders for MERGE")

    # ── MERGE INTO Silver ─────────────────────────────────────────
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING staged_orders AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            target.customer_id    = source.customer_id,
            target.status         = source.status,
            target.total_usd      = source.total_usd,
            target.created_at     = source.created_at,
            target.updated_at     = source.updated_at,
            target.is_deleted     = source.is_deleted,
            target.dbt_updated_at = source.dbt_updated_at
        WHEN NOT MATCHED THEN INSERT (
            id, customer_id, status, total_usd,
            created_at, updated_at, is_deleted, dbt_updated_at
        ) VALUES (
            source.id, source.customer_id, source.status, source.total_usd,
            source.created_at, source.updated_at, source.is_deleted, source.dbt_updated_at
        )
    """)

    print("✔ MERGE complete")

    # ── Show result ───────────────────────────────────────────────
    print("\n── Silver table (current state) ──────────────────────")
    spark.read.format("iceberg").load(SILVER_TABLE) \
        .select("id", "customer_id", "status", "total_usd", "is_deleted", "dbt_updated_at") \
        .orderBy("id") \
        .show(truncate=False)


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    create_silver_table(spark)
    run_merge(spark)

    spark.stop()
    print("✔ Silver writer done.")


if __name__ == "__main__":
    main()