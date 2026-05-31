# Screenshots guide

Drop your screenshots in this folder using the exact file names below. The
project README already links to them, so once the files are here they show up
automatically.

Tip for clean shots: use your browser at ~110% zoom, capture just the relevant
panel (not the whole desktop), and crop out anything with your account number if
you prefer to keep it private (it is not sensitive, but it looks tidier).

| File name | Where to capture it | What it should show |
|---|---|---|
| `01-glue-job.png` | AWS Console → **Glue** → ETL jobs → `ecomlake-dev-bronze-to-silver` → **Runs** tab | A run with status **Succeeded** in green |
| `02-s3-layers.png` | AWS Console → **S3** → your `ecomlake-dev-lake-...` bucket | The folders `bronze/`, `ecomlake_silver.db/`, `reference/`, `scripts/` |
| `03-athena-results.png` | AWS Console → **Athena** → Query editor (workgroup `ecomlake-dev`) → run the query below | The results table with real revenue rows |
| `04-iceberg-snapshots.png` | Same Athena editor → run the snapshots query below | The Iceberg snapshot history (proves time travel) |
| `05-dbt-tests.png` | Your terminal | The `PASS=5 WARN=0 ERROR=0` line from `dbt build` |
| `06-dashboard.png` | The Streamlit dashboard (optional, if you build it) | The charts of revenue and conversion |

## Queries to run for the screenshots

For `03-athena-results.png` (set the workgroup to `ecomlake-dev` first):

```sql
SELECT category,
       sum(revenue)      AS total_revenue,
       sum(sessions)     AS sessions,
       avg(conversion_rate) AS avg_conversion
FROM ecomlake_gold.daily_revenue
GROUP BY category
ORDER BY total_revenue DESC;
```

For `04-iceberg-snapshots.png` (the detail that impresses, shows table versioning):

```sql
SELECT committed_at, snapshot_id, operation
FROM "ecomlake_silver"."sessions$snapshots"
ORDER BY committed_at;
```
