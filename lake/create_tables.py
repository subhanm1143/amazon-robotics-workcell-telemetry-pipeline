"""
Registers the Cosmos namespace and two Iceberg tables in the REST catalog:
raw_events (every valid event, unaggregated) and window_aggregates (the
1-minute rollups from the previous step). Both partition by workcell_type
first and event date second, because "which workcell type faults most" is
the question this whole project exists to answer fast.
"""
from __future__ import annotations

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import DoubleType, LongType, NestedField, StringType, TimestamptzType

NAMESPACE = "cosmos"
RAW_TABLE = "raw_events"
AGG_TABLE = "window_aggregates"

CATALOG_PROPS = {
    "uri": "http://localhost:8181",
    "warehouse": "s3://cosmos-lake/warehouse",
    "s3.endpoint": "http://localhost:4566",
    "s3.access-key-id": "test",
    "s3.secret-access-key": "test",
    "s3.path-style-access": "true",
}

RAW_SCHEMA = Schema(
    NestedField(1, "workcell_id", StringType(), required=True),
    NestedField(2, "workcell_type", StringType(), required=True),
    NestedField(3, "event_type", StringType(), required=True),
    NestedField(4, "status", StringType(), required=True),
    NestedField(5, "occurred_at", TimestamptzType(), required=True),
    NestedField(6, "fault_code", StringType(), required=False),
    NestedField(7, "throughput_per_min", LongType(), required=False),
)

AGG_SCHEMA = Schema(
    NestedField(1, "workcell_type", StringType(), required=True),
    NestedField(2, "window_start", TimestamptzType(), required=True),
    NestedField(3, "window_end", TimestamptzType(), required=True),
    NestedField(4, "event_count", LongType(), required=True),
    NestedField(5, "fault_count", LongType(), required=True),
    NestedField(6, "fault_rate", DoubleType(), required=True),
    NestedField(7, "avg_throughput", DoubleType(), required=False),
)


def _spec(time_field_id: int, type_field_id: int) -> PartitionSpec:
    # workcell_type first: "which type faults most" never has to scan a day
    # it doesn't need. day(occurred_at) second, so a day's data is still one
    # cheap prune away once you do filter by time.
    return PartitionSpec(
        PartitionField(source_id=type_field_id, field_id=1000, transform=IdentityTransform(), name="workcell_type"),
        PartitionField(source_id=time_field_id, field_id=1001, transform=DayTransform(), name="occurred_at_day"),
    )


def main() -> None:
    cat = load_catalog("rest", **CATALOG_PROPS)
    try:
        cat.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    try:
        cat.create_table(f"{NAMESPACE}.{RAW_TABLE}", schema=RAW_SCHEMA, partition_spec=_spec(5, 2))
        print(f"created {NAMESPACE}.{RAW_TABLE}")
    except TableAlreadyExistsError:
        print(f"{NAMESPACE}.{RAW_TABLE} already exists")

    try:
        cat.create_table(f"{NAMESPACE}.{AGG_TABLE}", schema=AGG_SCHEMA, partition_spec=_spec(2, 1))
        print(f"created {NAMESPACE}.{AGG_TABLE}")
    except TableAlreadyExistsError:
        print(f"{NAMESPACE}.{AGG_TABLE} already exists")


if __name__ == "__main__":
    main()
