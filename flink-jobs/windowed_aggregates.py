"""
Reads the validated events staged by the previous job and folds them into
1-minute tumbling windows per workcell type: how many events, how many were
faults, the fault rate, and the average throughput reported by the
non-faulted ones. This is the shape a BI dashboard actually wants — nobody
graphs individual events.
"""
from __future__ import annotations

from pyflink.table import EnvironmentSettings, TableEnvironment

WINDOW = "1"  # minutes; kept as a constant so a demo run can be re-pointed at
              # a shorter window without hunting through the SQL below


def main() -> None:
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    t_env.execute_sql("""
        CREATE TABLE valid_events (
            workcell_id STRING,
            workcell_type STRING,
            event_type STRING,
            status STRING,
            occurred_at TIMESTAMP_LTZ(3),
            fault_code STRING,
            throughput_per_min INT,
            WATERMARK FOR occurred_at AS occurred_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'staging/valid',
            'format' = 'json'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE workcell_aggregates (
            workcell_type STRING,
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            event_count BIGINT,
            fault_count BIGINT,
            fault_rate DOUBLE,
            avg_throughput DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'staging/aggregates',
            'format' = 'json'
        )
    """)

    # A tumbling window on event time, not processing time: a window closes
    # only once the watermark passes its end, so a burst of late-arriving
    # events from a slow shard still lands in the window it actually belongs
    # to, not the window Flink happened to be processing when it showed up.
    t_env.execute_sql(f"""
        INSERT INTO workcell_aggregates
        SELECT
            workcell_type,
            window_start,
            window_end,
            COUNT(*) AS event_count,
            SUM(CASE WHEN event_type = 'fault' THEN 1 ELSE 0 END) AS fault_count,
            CAST(SUM(CASE WHEN event_type = 'fault' THEN 1 ELSE 0 END) AS DOUBLE)
                / CAST(COUNT(*) AS DOUBLE) AS fault_rate,
            AVG(throughput_per_min) AS avg_throughput
        FROM TABLE(
            TUMBLE(TABLE valid_events, DESCRIPTOR(occurred_at), INTERVAL '{WINDOW}' MINUTE)
        )
        GROUP BY workcell_type, window_start, window_end
    """).wait()


if __name__ == "__main__":
    main()
