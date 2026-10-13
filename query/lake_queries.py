"""
The Athena stand-in: DuckDB reads the same Iceberg tables straight out of the
REST catalog and answers the questions Cosmos' BI team actually asks, without
copying anything into a warehouse first — the same substitution
de-lakehouse-iceberg-partitioning already made for its own query layer.
"""
from __future__ import annotations

import duckdb

CATALOG_URI = "http://localhost:8181"
WAREHOUSE = "cosmos"


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        CREATE SECRET (
            TYPE s3,
            KEY_ID 'test',
            SECRET 'test',
            ENDPOINT 'localhost:4566',
            URL_STYLE 'path',
            USE_SSL false
        )
    """)
    con.execute(f"""
        ATTACH '{WAREHOUSE}' AS cosmos_lake (
            TYPE iceberg,
            ENDPOINT '{CATALOG_URI}'
        )
    """)
    return con


def which_type_faults_most(con: duckdb.DuckDBPyConnection) -> list:
    # Grouping the AGGREGATE table, not raw_events: the fault rate per type
    # was already computed once per window when it was cheap to compute, so
    # answering this at query time costs a sum over a few thousand rows
    # instead of a scan over every raw event ever landed.
    return con.execute("""
        SELECT
            workcell_type,
            SUM(fault_count) AS total_faults,
            SUM(event_count) AS total_events,
            SUM(fault_count) * 1.0 / SUM(event_count) AS fault_rate
        FROM cosmos_lake.cosmos.window_aggregates
        GROUP BY workcell_type
        ORDER BY fault_rate DESC
    """).fetchall()


def busiest_windows(con: duckdb.DuckDBPyConnection, limit: int = 5) -> list:
    return con.execute(f"""
        SELECT workcell_type, window_start, event_count
        FROM cosmos_lake.cosmos.window_aggregates
        ORDER BY event_count DESC
        LIMIT {limit}
    """).fetchall()


def main() -> None:
    con = _connect()
    print("Which workcell type faults most?")
    for row in which_type_faults_most(con):
        print(f"  {row[0]:<12} faults={row[1]:>4} events={row[2]:>5} rate={row[3]:.3f}")

    print("\nBusiest 1-minute windows:")
    for row in busiest_windows(con):
        print(f"  {row[0]:<12} {row[1]} events={row[2]}")


if __name__ == "__main__":
    main()
