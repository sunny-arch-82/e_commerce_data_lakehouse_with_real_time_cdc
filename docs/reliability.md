# CDC Reliability Layer

## Goal

Keep the CDC stream available when producer schemas or individual records are bad, while making failures measurable and replayable.

## Schema contract

Local Avro files under `infra/schema-registry/schemas/` define the expected business fields. At stream startup, the Spark processor fetches the latest subject schema from Schema Registry and compares business field names/types. Debezium metadata fields prefixed with `__` are ignored for the field-level comparison.

For every Kafka event, Spark extracts the Confluent schema ID from the first five bytes of the wire payload. Only the approved ID is decoded. A new/unapproved ID is routed to quarantine and recorded in `monitoring.schema_drift_events`.

This intentionally favors safety over automatic schema evolution. A schema change becomes active only after the local contract and downstream transformations are reviewed and updated.

## Quarantine design

`quarantine.cdc_bad_records` stores the original Kafka key/value in Base64 along with topic, partition, offset, schema ID, failure reason and timestamp. Keeping the raw bytes makes the record replayable without reconstructing the Debezium event.

A stable `source_event_id` is built from `topic:partition:offset` and is used by replay auditing.

## CDC lag

For valid CDC records:

```text
cdc_lag_ms = Spark processing time - Debezium source ts_ms
```

`monitoring.cdc_batch_metrics` stores one row per source table and micro-batch. This separates event-time freshness from Kafka offset progress.

## Replay safety

`workspace/replay_quarantine.py`:

1. reads quarantine rows;
2. anti-joins against `monitoring/replay_audit`;
3. filters by table/reason when requested;
4. defaults to a dry run;
5. republishes the exact original Kafka key/value only with `--execute`;
6. appends successfully replayed `source_event_id` values to the replay audit table.

Schema-drift records should not be replayed until the schema contract has been intentionally updated; otherwise they will be quarantined again.
