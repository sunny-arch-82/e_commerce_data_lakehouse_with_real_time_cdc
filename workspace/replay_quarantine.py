"""Replay quarantined CDC events back to Kafka with an audit trail.

By default this command is a dry run. Pass --execute to actually republish records.
The original Kafka topic, key and raw Avro value are preserved.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
QUARANTINE_PATH = os.getenv("DELTA_QUARANTINE_PATH", "s3a://lakehouse/quarantine") + "/cdc_bad_records"
REPLAY_AUDIT_PATH = os.getenv("DELTA_MONITORING_PATH", "s3a://lakehouse/monitoring") + "/replay_audit"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EcommerceCDCQuarantineReplay")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def ensure_audit_table(spark: SparkSession) -> None:
    schema = T.StructType([
        T.StructField("source_event_id", T.StringType(), False),
        T.StructField("table_name", T.StringType(), False),
        T.StructField("topic", T.StringType(), False),
        T.StructField("original_partition", T.IntegerType(), False),
        T.StructField("original_offset", T.LongType(), False),
        T.StructField("quarantine_reason", T.StringType(), False),
        T.StructField("replayed_at", T.TimestampType(), False),
    ])
    try:
        spark.read.format("delta").load(REPLAY_AUDIT_PATH)
    except Exception:
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(REPLAY_AUDIT_PATH)


def main() -> None:
    parser = argparse.ArgumentParser(description="Safely replay quarantined CDC events")
    parser.add_argument("--table", help="Only replay one source table, e.g. orders")
    parser.add_argument("--reason", help="Only replay one quarantine reason")
    parser.add_argument("--limit", type=int, default=100, help="Maximum records to replay")
    parser.add_argument("--execute", action="store_true", help="Actually write records back to Kafka")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_audit_table(spark)

    quarantine = spark.read.format("delta").load(QUARANTINE_PATH)
    audit = spark.read.format("delta").load(REPLAY_AUDIT_PATH).select("source_event_id")
    pending = quarantine.join(audit, "source_event_id", "left_anti")

    if args.table:
        pending = pending.filter(F.col("table_name") == args.table)
    if args.reason:
        pending = pending.filter(F.col("quarantine_reason") == args.reason)

    pending = pending.orderBy("quarantined_at").limit(args.limit)
    count = pending.count()
    print(f"Replay candidates: {count}")
    if count == 0:
        return

    pending.select(
        "source_event_id", "table_name", "topic", "partition", "offset", "quarantine_reason", "schema_id", "quarantined_at"
    ).show(min(count, 50), truncate=False)

    if not args.execute:
        print("Dry run only. Re-run with --execute after the schema/data issue is fixed.")
        return

    kafka_rows = pending.select(
        F.col("topic"),
        F.unbase64("key_base64").alias("key"),
        F.unbase64("value_base64").alias("value"),
    )
    kafka_rows.write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).save()

    replayed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    audit_rows = pending.select(
        "source_event_id",
        "table_name",
        "topic",
        F.col("partition").alias("original_partition"),
        F.col("offset").alias("original_offset"),
        "quarantine_reason",
    ).withColumn("replayed_at", F.lit(replayed_at))
    audit_rows.write.format("delta").mode("append").save(REPLAY_AUDIT_PATH)
    print(f"Replayed {count} records and appended replay audit rows.")


if __name__ == "__main__":
    main()
