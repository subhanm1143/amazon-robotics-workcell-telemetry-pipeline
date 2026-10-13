"""
Loads whatever the Flink jobs staged locally into the two Iceberg tables:
every JSON line under staging/valid/ into raw_events, every line under
staging/aggregates/ into window_aggregates. A real Cosmos would skip this
disk hop and have Flink write Iceberg directly through its Iceberg sink;
staging through JSON files here keeps the Flink jobs (steps 3-4) and the
Iceberg commit (this step) independently runnable, which is worth the extra
hop in a project meant to be run one stage at a time.
"""
from __future__ import annotations

import glob
import json
from datetime import datetime, timezone

import pyarrow as pa
from pyiceberg.catalog import load_catalog

from lake.create_tables import AGG_SCHEMA, AGG_TABLE, CATALOG_PROPS, NAMESPACE, RAW_SCHEMA, RAW_TABLE


def _read_ndjson(pattern: str) -> list[dict]:
    rows = []
    for path in glob.glob(pattern, recursive=True):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    return rows


def _parse_ts(value: str) -> datetime:
    # Flink's json format writes TIMESTAMP_LTZ as an ISO-8601 string; PyIceberg
    # wants a real datetime with a timezone, not the string Flink handed us.
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def load_raw() -> int:
    rows = _read_ndjson("staging/valid/**/*.json")
    if not rows:
        return 0
    table = pa.table({
        "workcell_id": [r["workcell_id"] for r in rows],
        "workcell_type": [r["workcell_type"] for r in rows],
        "event_type": [r["event_type"] for r in rows],
        "status": [r["status"] for r in rows],
        "occurred_at": [_parse_ts(r["occurred_at"]) for r in rows],
        "fault_code": [r.get("fault_code") for r in rows],
        "throughput_per_min": [r.get("throughput_per_min") for r in rows],
    }, schema=RAW_SCHEMA.as_arrow())
    cat = load_catalog("rest", **CATALOG_PROPS)
    cat.load_table(f"{NAMESPACE}.{RAW_TABLE}").append(table)
    return len(rows)


def load_aggregates() -> int:
    rows = _read_ndjson("staging/aggregates/**/*.json")
    if not rows:
        return 0
    table = pa.table({
        "workcell_type": [r["workcell_type"] for r in rows],
        "window_start": [_parse_ts(r["window_start"]) for r in rows],
        "window_end": [_parse_ts(r["window_end"]) for r in rows],
        "event_count": [r["event_count"] for r in rows],
        "fault_count": [r["fault_count"] for r in rows],
        "fault_rate": [r["fault_rate"] for r in rows],
        "avg_throughput": [r.get("avg_throughput") for r in rows],
    }, schema=AGG_SCHEMA.as_arrow())
    cat = load_catalog("rest", **CATALOG_PROPS)
    cat.load_table(f"{NAMESPACE}.{AGG_TABLE}").append(table)
    return len(rows)


def main() -> None:
    n_raw = load_raw()
    n_agg = load_aggregates()
    print(f"loaded {n_raw} raw events, {n_agg} window aggregates")


if __name__ == "__main__":
    main()
