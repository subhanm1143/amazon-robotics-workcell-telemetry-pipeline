"""
Reads raw workcell events off the `workcell-events` Kinesis stream, validates
each one, and routes it to exactly one of two places: valid events land as
typed JSON under staging/valid/ for the windowing job in the next step, and
anything that fails validation goes to the `workcell-events-dlq` Kinesis
stream instead of being dropped. Nothing is ever silently discarded.
"""
from __future__ import annotations

import os

from pyflink.table import DataTypes, EnvironmentSettings, Row, TableEnvironment
from pyflink.table.udf import udf

from schemas.workcell_event_schema import validate

KINESIS_ENDPOINT = os.environ.get("KINESIS_ENDPOINT", "http://localhost:4566")
REGION = "us-east-1"
CONNECTOR_JAR = os.path.abspath(
    "flink-jobs/lib/flink-sql-connector-aws-kinesis-streams-4.3.0-1.19.jar"
)

VALID_ROW_TYPE = DataTypes.ROW([
    DataTypes.FIELD("workcell_id", DataTypes.STRING()),
    DataTypes.FIELD("workcell_type", DataTypes.STRING()),
    DataTypes.FIELD("event_type", DataTypes.STRING()),
    DataTypes.FIELD("status", DataTypes.STRING()),
    DataTypes.FIELD("occurred_at_millis", DataTypes.BIGINT()),
    DataTypes.FIELD("fault_code", DataTypes.STRING()),
    DataTypes.FIELD("throughput_per_min", DataTypes.INT()),
])


@udf(result_type=DataTypes.BOOLEAN())
def is_valid(raw: str) -> bool:
    return validate(raw).valid


@udf(result_type=DataTypes.STRING())
def error_reason(raw: str) -> str:
    return validate(raw).error or ""


@udf(result_type=VALID_ROW_TYPE)
def parse_valid(raw: str) -> Row:
    # Only called after is_valid(raw) filtered the row, so .parsed is never None
    # here — but attributes is optional, so its keys are read defensively.
    parsed = validate(raw).parsed
    attrs = parsed.get("attributes") or {}
    return Row(
        parsed["workcellId"],
        parsed["workcellType"],
        parsed["eventType"],
        parsed["status"],
        parsed["occurredAtMillis"],
        attrs.get("faultCode"),
        attrs.get("throughputPerMin"),
    )


def main() -> None:
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("pipeline.jars", f"file://{CONNECTOR_JAR}")
    t_env.create_temporary_function("is_valid", is_valid)
    t_env.create_temporary_function("error_reason", error_reason)
    t_env.create_temporary_function("parse_valid", parse_valid)

    t_env.execute_sql(f"""
        CREATE TABLE raw_events (
            event_raw STRING
        ) WITH (
            'connector' = 'kinesis',
            'stream' = 'workcell-events',
            'aws.region' = '{REGION}',
            'aws.endpoint' = '{KINESIS_ENDPOINT}',
            'scan.stream.initpos' = 'LATEST',
            'format' = 'raw'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE valid_events (
            workcell_id STRING,
            workcell_type STRING,
            event_type STRING,
            status STRING,
            occurred_at TIMESTAMP_LTZ(3),
            fault_code STRING,
            throughput_per_min INT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'staging/valid',
            'format' = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE dead_letter_events (
            event_raw STRING,
            error_reason STRING
        ) WITH (
            'connector' = 'kinesis',
            'stream' = 'workcell-events-dlq',
            'aws.region' = '{REGION}',
            'aws.endpoint' = '{KINESIS_ENDPOINT}',
            'sink.partitioner' = 'random',
            'format' = 'json'
        )
    """)

    # Two statements against the same source table, executed as one Flink job
    # via a StatementSet — the router does not read the stream twice.
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql("""
        INSERT INTO valid_events
        SELECT
            parsed.workcell_id,
            parsed.workcell_type,
            parsed.event_type,
            parsed.status,
            TO_TIMESTAMP_LTZ(parsed.occurred_at_millis, 3),
            parsed.fault_code,
            parsed.throughput_per_min
        FROM (SELECT parse_valid(event_raw) AS parsed FROM raw_events WHERE is_valid(event_raw))
    """)
    stmt_set.add_insert_sql("""
        INSERT INTO dead_letter_events
        SELECT event_raw, error_reason(event_raw)
        FROM raw_events WHERE NOT is_valid(event_raw)
    """)
    stmt_set.execute().wait()


if __name__ == "__main__":
    main()
