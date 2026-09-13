"""Real-time CDC stream processor with schema contracts, quarantine, and lag metrics.

Pipeline: PostgreSQL -> Debezium -> Kafka -> Spark -> Delta Lake.

Key reliability features added for this portfolio version:
1. Schema-drift detection using the Confluent Schema Registry schema id embedded in
   every Kafka Avro message plus a field-level contract check against local .avsc files.
2. Bad-record quarantine in Delta instead of silently dropping malformed CDC events.
3. Per-microbatch CDC lag metrics and replay metadata for operational monitoring.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import time
from datetime import datetime, timezone
from typing import Any

import requests
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro


# -----------------------------------------------------------------------------
# Runtime configuration
# -----------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
SCHEMA_DIR = os.getenv("SCHEMA_DIR", "/schemas")
TOPIC_PREFIX = os.getenv("CDC_TOPIC_PREFIX", "ecommerce")

DELTA_BASE = os.getenv("DELTA_STAGING_PATH", "s3a://lakehouse/staging")
QUARANTINE_BASE = os.getenv("DELTA_QUARANTINE_PATH", "s3a://lakehouse/quarantine")
MONITORING_BASE = os.getenv("DELTA_MONITORING_PATH", "s3a://lakehouse/monitoring")
CHECKPOINT_BASE = os.getenv("DELTA_CHECKPOINT_PATH", "s3a://lakehouse/checkpoints")

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "ecommerce")
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")

CDC_TOPICS = {
    f"{TOPIC_PREFIX}.public.users": "users",
    f"{TOPIC_PREFIX}.public.orders": "orders",
    f"{TOPIC_PREFIX}.public.order_items": "order_items",
    f"{TOPIC_PREFIX}.public.events": "events",
}

AVRO2SPARK = {
    "string": T.StringType(),
    "int": T.IntegerType(),
    "long": T.LongType(),
    "double": T.DoubleType(),
    "float": T.FloatType(),
    "boolean": T.BooleanType(),
}

VALID_OPS = ("r", "c", "u", "d")


def wait_for_tcp(host: str, port: int, label: str, timeout: int = 120) -> None:
    print(f"Waiting for {label} ({host}:{port})...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            socket.create_connection((host, port), timeout=2).close()
            print(" OK")
            return
        except OSError:
            print(".", end="", flush=True)
            time.sleep(3)
    raise TimeoutError(f"{label} not ready after {timeout}s")


def build_spark() -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
        time.sleep(2)

    spark = (
        SparkSession.builder
        .appName("EcommerceCDCStreaming")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    return spark


def load_local_schema(table: str) -> dict[str, Any]:
    with open(f"{SCHEMA_DIR}/{table}.avsc", "r", encoding="utf-8") as f:
        return json.load(f)


def avro_type_name(avro_type: Any) -> str:
    """Return the downstream contract type for an Avro scalar/union/logical type.

    Debezium timestamps are Avro long values with logical/connect metadata, while the
    existing lakehouse contract intentionally casts them to strings before Delta/dbt.
    Normalize those temporal encodings to ``string`` so a normal Debezium timestamp
    does not look like drift; a genuine business-type change still does.
    """
    if isinstance(avro_type, list):
        non_null = [x for x in avro_type if x != "null"]
        return avro_type_name(non_null[0]) if non_null else "null"
    if isinstance(avro_type, dict):
        logical = str(avro_type.get("logicalType", ""))
        connect_name = str(avro_type.get("connect.name", avro_type.get("connect.name", "")))
        temporal_marker = f"{logical} {connect_name}".lower()
        if any(token in temporal_marker for token in ("timestamp", "date", "time")):
            return "string"
        return avro_type_name(avro_type.get("type", "string"))
    return str(avro_type)


def schema_to_struct(avro_schema: dict[str, Any]) -> T.StructType:
    fields = []
    for field in avro_schema.get("fields", []):
        raw_type = field["type"]
        nullable = isinstance(raw_type, list) and "null" in raw_type
        type_name = avro_type_name(raw_type)
        spark_type = AVRO2SPARK.get(type_name, T.StringType())
        fields.append(T.StructField(field["name"], spark_type, nullable))
    return T.StructType(fields)


def compare_schema_contract(expected: dict[str, Any], actual: dict[str, Any]) -> dict[str, Any]:
    """Compare business fields, ignoring Debezium metadata fields prefixed with '__'."""
    exp = {f["name"]: avro_type_name(f["type"]) for f in expected.get("fields", [])}
    act = {
        f["name"]: avro_type_name(f["type"])
        for f in actual.get("fields", [])
        if not f["name"].startswith("__")
    }
    added = sorted(set(act) - set(exp))
    removed = sorted(set(exp) - set(act))
    changed = sorted(
        {name: {"expected": exp[name], "actual": act[name]} for name in set(exp) & set(act) if exp[name] != act[name]}.items()
    )
    changed_dict = {k: v for k, v in changed}
    return {
        "is_drift": bool(added or removed or changed_dict),
        "added_fields": added,
        "removed_fields": removed,
        "changed_types": changed_dict,
    }


def fetch_latest_subject_schema(topic: str) -> tuple[int, str, dict[str, Any]]:
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{topic}-value/versions/latest"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    payload = response.json()
    schema_str = payload["schema"]
    schema_obj = json.loads(schema_str)
    return int(payload["id"]), schema_str, schema_obj


def fetch_schema_by_id(schema_id: int) -> dict[str, Any]:
    response = requests.get(f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}", timeout=15)
    response.raise_for_status()
    return json.loads(response.json()["schema"])


def schema_id_column(value_col: str = "value") -> F.Column:
    # Confluent wire format = 1-byte magic byte + 4-byte big-endian schema id + Avro payload.
    return F.conv(F.substring(F.hex(F.col(value_col)), 3, 8), 16, 10).cast("long")


def trino_connection():
    import trino as trino_client

    return trino_client.dbapi.connect(host=TRINO_HOST, port=8080, user="admin", catalog="delta", schema="staging")


def trino_execute(conn, sql: str) -> bool:
    import trino as trino_client

    try:
        cur = conn.cursor()
        cur.execute(sql)
        try:
            cur.fetchall()
        except trino_client.exceptions.TrinoDataError:
            pass
        return True
    except Exception as exc:
        print(f"  WARN Trino: {exc}")
        return False


def register_delta_table(conn, schema_name: str, table_name: str, location: str) -> bool:
    """Register an existing Delta location in Trino/Hive Metastore if needed."""
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*) FROM information_schema.tables "
        f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
    )
    if cur.fetchone()[0]:
        return True
    sql = (
        "CALL delta.system.register_table("
        f"schema_name => '{schema_name}', "
        f"table_name => '{table_name}', "
        f"table_location => '{location}'"
        ")"
    )
    return trino_execute(conn, sql)


def bootstrap_static_tables(spark: SparkSession, conn) -> None:
    trino_execute(conn, "CREATE SCHEMA IF NOT EXISTS staging")
    for src in ("products", "dist_centers"):
        name = f"ref_{src}"
        path = f"{DELTA_BASE}/{name}"
        df = (
            spark.read.format("jdbc")
            .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")
            .option("dbtable", f"public.{src}")
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load()
            .withColumn("operation", F.lit("r"))
            .withColumn("event_ts_ms", F.expr("cast(unix_timestamp() * 1000 as long)"))
        )
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        ok = register_delta_table(conn, "staging", name, path)
        print(f"  [{'OK' if ok else 'WARN'}] staging.{name}: {df.count()} rows")


def ensure_monitoring_tables(spark: SparkSession, conn) -> None:
    trino_execute(conn, "CREATE SCHEMA IF NOT EXISTS monitoring WITH (location = 's3a://lakehouse/monitoring/')")

    metric_schema = T.StructType([
        T.StructField("table_name", T.StringType(), False),
        T.StructField("topic", T.StringType(), False),
        T.StructField("batch_id", T.LongType(), False),
        T.StructField("processed_records", T.LongType(), False),
        T.StructField("quarantined_records", T.LongType(), False),
        T.StructField("max_cdc_lag_ms", T.LongType(), True),
        T.StructField("avg_cdc_lag_ms", T.DoubleType(), True),
        T.StructField("max_kafka_offset", T.LongType(), True),
        T.StructField("recorded_at", T.TimestampType(), False),
    ])
    drift_schema = T.StructType([
        T.StructField("table_name", T.StringType(), False),
        T.StructField("topic", T.StringType(), False),
        T.StructField("expected_schema_id", T.LongType(), True),
        T.StructField("observed_schema_id", T.LongType(), True),
        T.StructField("drift_details_json", T.StringType(), False),
        T.StructField("detected_at", T.TimestampType(), False),
    ])

    for name, schema in (("cdc_batch_metrics", metric_schema), ("schema_drift_events", drift_schema)):
        path = f"{MONITORING_BASE}/{name}"
        try:
            spark.read.format("delta").load(path)
        except Exception:
            spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)
        register_delta_table(conn, "monitoring", name, path)


def register_quarantine_table(spark: SparkSession, conn) -> None:
    trino_execute(conn, "CREATE SCHEMA IF NOT EXISTS quarantine WITH (location = 's3a://lakehouse/quarantine/')")
    schema = T.StructType([
        T.StructField("source_event_id", T.StringType(), False),
        T.StructField("table_name", T.StringType(), False),
        T.StructField("topic", T.StringType(), False),
        T.StructField("partition", T.IntegerType(), False),
        T.StructField("offset", T.LongType(), False),
        T.StructField("kafka_ts", T.TimestampType(), True),
        T.StructField("schema_id", T.LongType(), True),
        T.StructField("quarantine_reason", T.StringType(), False),
        T.StructField("key_base64", T.StringType(), True),
        T.StructField("value_base64", T.StringType(), True),
        T.StructField("quarantined_at", T.TimestampType(), False),
    ])
    path = f"{QUARANTINE_BASE}/cdc_bad_records"
    try:
        spark.read.format("delta").load(path)
    except Exception:
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)
    register_delta_table(conn, "quarantine", "cdc_bad_records", path)


def record_schema_drift(
    spark: SparkSession,
    table: str,
    topic: str,
    expected_schema_id: int | None,
    observed_schema_id: int | None,
    details: dict[str, Any],
) -> None:
    row = [(
        table,
        topic,
        expected_schema_id,
        observed_schema_id,
        json.dumps(details, sort_keys=True),
        datetime.now(timezone.utc).replace(tzinfo=None),
    )]
    schema = "table_name string, topic string, expected_schema_id long, observed_schema_id long, drift_details_json string, detected_at timestamp"
    spark.createDataFrame(row, schema=schema).write.format("delta").mode("append").save(
        f"{MONITORING_BASE}/schema_drift_events"
    )


def write_quarantine(df: DataFrame, table: str) -> int:
    qdf = (
        df.filter(F.col("_quarantine_reason").isNotNull())
        .select(
            F.concat_ws(":", F.col("topic"), F.col("partition"), F.col("offset")).alias("source_event_id"),
            F.lit(table).alias("table_name"),
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_ts"),
            "schema_id",
            F.col("_quarantine_reason").alias("quarantine_reason"),
            F.base64("key").alias("key_base64"),
            F.base64("value").alias("value_base64"),
            F.current_timestamp().alias("quarantined_at"),
        )
    )
    count = qdf.count()
    if count:
        qdf.write.format("delta").mode("append").save(f"{QUARANTINE_BASE}/cdc_bad_records")
    return count


def write_lag_metrics(df: DataFrame, table: str, topic: str, batch_id: int, quarantined_count: int) -> None:
    valid = df.filter(F.col("_quarantine_reason").isNull())
    processed = valid.count()
    agg = valid.agg(
        F.max("cdc_lag_ms").alias("max_lag"),
        F.avg("cdc_lag_ms").alias("avg_lag"),
        F.max("offset").alias("max_offset"),
    ).collect()[0]
    row = [(
        table,
        topic,
        int(batch_id),
        int(processed),
        int(quarantined_count),
        int(agg["max_lag"]) if agg["max_lag"] is not None else None,
        float(agg["avg_lag"]) if agg["avg_lag"] is not None else None,
        int(agg["max_offset"]) if agg["max_offset"] is not None else None,
        datetime.now(timezone.utc).replace(tzinfo=None),
    )]
    schema = (
        "table_name string, topic string, batch_id long, processed_records long, "
        "quarantined_records long, max_cdc_lag_ms long, avg_cdc_lag_ms double, "
        "max_kafka_offset long, recorded_at timestamp"
    )
    spark_session = df.sparkSession
    spark_session.createDataFrame(row, schema=schema).write.format("delta").mode("append").save(
        f"{MONITORING_BASE}/cdc_batch_metrics"
    )


def detect_unapproved_schema_ids(
    spark: SparkSession,
    batch_df: DataFrame,
    table: str,
    topic: str,
    expected_schema_id: int | None,
    local_schema: dict[str, Any],
) -> None:
    observed = [r[0] for r in batch_df.select("schema_id").where("schema_id IS NOT NULL").distinct().collect()]
    for schema_id in observed:
        if expected_schema_id is not None and schema_id == expected_schema_id:
            continue
        try:
            actual = fetch_schema_by_id(int(schema_id))
            details = compare_schema_contract(local_schema, actual)
        except Exception as exc:
            details = {"is_drift": True, "schema_lookup_error": str(exc)}
        record_schema_drift(spark, table, topic, expected_schema_id, int(schema_id), details)


def upsert_valid_rows(batch_df: DataFrame, table: str) -> None:
    valid = batch_df.filter(F.col("_quarantine_reason").isNull())
    payload_cols = [c for c in valid.columns if c.startswith("data__")]
    selected = [F.col(c).alias(c.removeprefix("data__")) for c in payload_cols]
    selected += [
        F.col("op"),
        F.col("ts_ms").cast("string"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("partition").alias("partition"),
        F.col("offset").alias("offset"),
        F.col("cdc_lag_ms"),
    ]
    valid = valid.select(*selected)

    ups = valid.filter("op != 'd'")
    dels = valid.filter("op = 'd'")

    if not ups.isEmpty():
        view_name = f"cdc_updates_{table}"
        ups.createOrReplaceGlobalTempView(view_name)
        spark = batch_df.sparkSession
        spark.sql(f"""
            MERGE INTO delta.`{DELTA_BASE}/{table}` AS t
            USING (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY CAST(ts_ms AS BIGINT) DESC, offset DESC) AS rn
                FROM global_temp.{view_name}
            ) AS s
            ON s.id = t.id AND s.rn = 1
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    if not dels.isEmpty():
        ids = [r.id for r in dels.select("id").distinct().collect()]
        if ids:
            batch_df.sparkSession.sql(
                f"DELETE FROM delta.`{DELTA_BASE}/{table}` WHERE id IN ({','.join(repr(i) for i in ids)})"
            )


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    table: str,
    topic: str,
    expected_schema_id: int | None,
    local_schema: dict[str, Any],
) -> None:
    batch_df.persist()
    try:
        if batch_df.isEmpty():
            return
        detect_unapproved_schema_ids(batch_df.sparkSession, batch_df, table, topic, expected_schema_id, local_schema)
        quarantined = write_quarantine(batch_df, table)
        upsert_valid_rows(batch_df, table)
        write_lag_metrics(batch_df, table, topic, batch_id, quarantined)
        print(f"[{table}] batch={batch_id} quarantine={quarantined}")
    finally:
        batch_df.unpersist()


def prepare_delta_table(spark: SparkSession, table: str, table_schema: T.StructType) -> None:
    extra = [
        T.StructField("op", T.StringType()),
        T.StructField("ts_ms", T.StringType()),
        T.StructField("kafka_ts", T.TimestampType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("cdc_lag_ms", T.LongType()),
    ]
    schema = T.StructType(list(table_schema.fields) + extra)
    path = f"{DELTA_BASE}/{table}"
    try:
        spark.read.format("delta").load(path)
    except Exception:
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


def build_stream(
    spark: SparkSession,
    topic: str,
    table: str,
    local_schema: dict[str, Any],
    table_schema: T.StructType,
):
    approved_schema_id, approved_schema_str, registry_schema = fetch_latest_subject_schema(topic)
    contract = compare_schema_contract(local_schema, registry_schema)

    if contract["is_drift"]:
        print(f"[SCHEMA DRIFT] {topic}: {json.dumps(contract, sort_keys=True)}")
        record_schema_drift(spark, table, topic, None, approved_schema_id, contract)
        expected_schema_id = None
    else:
        expected_schema_id = approved_schema_id
        print(f"[SCHEMA OK] {topic}: approved schema id={approved_schema_id}")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "10000")
        .load()
        .withColumn("schema_id", schema_id_column())
    )

    # Decode only records whose schema id is approved. Unapproved versions remain raw
    # and are routed to quarantine before they can break the stream.
    approved_condition = F.lit(False) if expected_schema_id is None else F.col("schema_id") == F.lit(expected_schema_id)
    decoded = raw.withColumn(
        "payload",
        F.when(
            approved_condition,
            from_avro(F.expr("substring(value, 6)"), approved_schema_str, {"mode": "PERMISSIVE"}),
        ),
    )

    business_cols = [F.col(f"payload.{field.name}").cast(field.dataType).alias(f"data__{field.name}") for field in table_schema.fields]
    parsed = decoded.select(
        "key",
        "value",
        "topic",
        "partition",
        "offset",
        "timestamp",
        "schema_id",
        "payload",
        *business_cols,
        F.col("payload.__op").alias("op"),
        F.col("payload.__ts_ms").cast("long").alias("ts_ms"),
    )

    primary_key = F.col("data__id")
    parsed = (
        parsed.withColumn(
            "_quarantine_reason",
            F.when(~approved_condition, F.lit("SCHEMA_DRIFT_UNAPPROVED"))
            .when(F.col("payload").isNull(), F.lit("AVRO_DECODE_ERROR"))
            .when(primary_key.isNull(), F.lit("MISSING_PRIMARY_KEY"))
            .when(~F.col("op").isin(*VALID_OPS), F.lit("INVALID_CDC_OPERATION"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "cdc_lag_ms",
            F.when(F.col("ts_ms").isNotNull(), (F.unix_millis(F.current_timestamp()) - F.col("ts_ms")).cast("long")),
        )
    )

    return (
        parsed.writeStream
        .foreachBatch(lambda df, bid: process_batch(df, bid, table, topic, expected_schema_id, local_schema))
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{table}")
        .queryName(f"cdc_{table}")
        .start()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the e-commerce CDC stream processor")
    parser.add_argument("--skip-wait", action="store_true", help="Skip TCP dependency checks")
    args = parser.parse_args()

    if not args.skip_wait:
        for host, port, label in (
            ("postgres", 5432, "PostgreSQL"),
            ("minio", 9000, "MinIO"),
            ("hive-metastore", 9083, "Hive Metastore"),
            ("spark-master", 7077, "Spark Master"),
            ("kafka", 9092, "Kafka Broker"),
            ("schema-registry", 8081, "Schema Registry"),
        ):
            wait_for_tcp(host, port, label)

    spark = build_spark()
    print("Spark session ready.")
    conn = trino_connection()

    local_schemas = {table: load_local_schema(table) for table in CDC_TOPICS.values()}
    table_schemas = {table: schema_to_struct(schema) for table, schema in local_schemas.items()}

    print("Bootstrapping reference tables...")
    bootstrap_static_tables(spark, conn)
    ensure_monitoring_tables(spark, conn)
    register_quarantine_table(spark, conn)

    for table, schema in table_schemas.items():
        prepare_delta_table(spark, table, schema)

    trino_execute(conn, "CREATE SCHEMA IF NOT EXISTS staging")
    for table in table_schemas:
        register_delta_table(conn, "staging", table, f"{DELTA_BASE}/{table}")

    queries = []
    for topic, table in CDC_TOPICS.items():
        query = build_stream(spark, topic, table, local_schemas[table], table_schemas[table])
        queries.append(query)
        print(f"Stream started: {topic} -> {table}")

    print(f"All {len(queries)} CDC streams are running with schema quarantine and lag monitoring.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
