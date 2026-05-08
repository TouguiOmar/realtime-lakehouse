"""
gold_writer.py
──────────────
Reads from Silver Iceberg table and writes business-ready
aggregates to the Gold layer.

Gold tables:
  1. daily_revenue   — revenue and order count per day
  2. order_summary   — current count per status
  3. customer_stats  — lifetime value per customer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, to_date, current_timestamp
)

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
SILVER_TABLE     = f"{CATALOG}.silver.orders"
GOLD_DAILY       = f"{CATALOG}.gold.daily_revenue"
GOLD_SUMMARY     = f"{CATALOG}.gold.order_summary"
GOLD_CUSTOMERS   = f"{CATALOG}.gold.customer_stats"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold_orders_writer")
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


def create_gold_tables(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DAILY} (
            day           STRING,
            order_count   BIGINT,
            revenue_usd   DOUBLE,
            avg_order_usd DOUBLE,
            computed_at   TIMESTAMP
        ) USING iceberg
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_SUMMARY} (
            status        STRING,
            order_count   BIGINT,
            total_revenue DOUBLE,
            computed_at   TIMESTAMP
        ) USING iceberg
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_CUSTOMERS} (
            customer_id   BIGINT,
            order_count   BIGINT,
            lifetime_value DOUBLE,
            avg_order_usd DOUBLE,
            computed_at   TIMESTAMP
        ) USING iceberg
    """)

    print("✔ Gold tables ready")


def write_gold(spark: SparkSession) -> None:
    # ── Read Silver (exclude soft-deleted rows) ───────────────────
    silver = (
        spark.read.format("iceberg").load(SILVER_TABLE)
        .filter(col("is_deleted") == False)
        .withColumn("total_usd", col("total_usd").cast("double"))
        .withColumn("day", to_date(col("updated_at")))
    )

    now = current_timestamp()

    # ── 1. Daily revenue ──────────────────────────────────────────
    daily = (
        silver.groupBy("day")
        .agg(
            count("*").alias("order_count"),
            sum("total_usd").alias("revenue_usd"),
            avg("total_usd").alias("avg_order_usd"),
        )
        .withColumn("computed_at", now)
        .orderBy("day")
    )

    daily.write.format("iceberg").mode("overwrite").save(GOLD_DAILY)
    print("✔ daily_revenue written")
    daily.show(truncate=False)

    # ── 2. Order summary by status ────────────────────────────────
    summary = (
        silver.groupBy("status")
        .agg(
            count("*").alias("order_count"),
            sum("total_usd").alias("total_revenue"),
        )
        .withColumn("computed_at", now)
        .orderBy("status")
    )

    summary.write.format("iceberg").mode("overwrite").save(GOLD_SUMMARY)
    print("✔ order_summary written")
    summary.show(truncate=False)

    # ── 3. Customer lifetime value ────────────────────────────────
    customers = (
        silver.groupBy("customer_id")
        .agg(
            count("*").alias("order_count"),
            sum("total_usd").alias("lifetime_value"),
            avg("total_usd").alias("avg_order_usd"),
        )
        .withColumn("computed_at", now)
        .orderBy("customer_id")
    )

    customers.write.format("iceberg").mode("overwrite").save(GOLD_CUSTOMERS)
    print("✔ customer_stats written")
    customers.show(truncate=False)


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    create_gold_tables(spark)
    write_gold(spark)

    spark.stop()
    print("✔ Gold writer done.")


if __name__ == "__main__":
    main()