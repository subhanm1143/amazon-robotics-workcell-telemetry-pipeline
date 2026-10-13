import express from "express";
import duckdb from "duckdb";

const app = express();
const PORT = process.env.PORT ?? 4000;

const db = new duckdb.Database(":memory:");

function query<T = unknown>(sql: string): Promise<T[]> {
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => (err ? reject(err) : resolve(rows as T[])));
  });
}

async function attachLake(): Promise<void> {
  await query("INSTALL iceberg; LOAD iceberg;");
  await query("INSTALL httpfs; LOAD httpfs;");
  await query(`
    CREATE SECRET (
      TYPE s3, KEY_ID 'test', SECRET 'test',
      ENDPOINT 'localhost:4566', URL_STYLE 'path', USE_SSL false
    )
  `);
  await query(`
    ATTACH 'cosmos' AS cosmos_lake (TYPE iceberg, ENDPOINT 'http://localhost:8181')
  `);
}

// Same query as query/lake_queries.py's which_type_faults_most — the
// dashboard and the Python analyst read the same Iceberg tables directly,
// so neither one can drift from what the other is looking at.
app.get("/api/fault-rates", async (_req, res) => {
  const rows = await query(`
    SELECT workcell_type,
           SUM(fault_count) AS total_faults,
           SUM(event_count) AS total_events,
           SUM(fault_count) * 1.0 / SUM(event_count) AS fault_rate
    FROM cosmos_lake.cosmos.window_aggregates
    GROUP BY workcell_type
    ORDER BY fault_rate DESC
  `);
  res.json(rows);
});

attachLake()
  .then(() => app.listen(PORT, () => console.log(`dashboard on :${PORT}`)))
  .catch((err) => {
    console.error("failed to attach lake:", err);
    process.exit(1);
  });
