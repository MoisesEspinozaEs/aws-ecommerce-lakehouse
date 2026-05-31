"""
Glue (Spark) job: promote bronze raw clicks to silver sessions.

This is the cheap, deployable demo path (no Flink). It does in batch what the
Flink job does in the stream:
  - read raw click events that Firehose landed in bronze
  - sessionize per user with a 30 minute inactivity gap, using window functions
  - enrich with the product catalog
  - aggregate one row per session
  - enforce a schema contract, quarantine what fails instead of dropping it
  - MERGE into the silver Iceberg table so re-runs correct rows instead of duplicating

Run it with:
    aws glue start-job-run --job-name ecomlake-dev-bronze-to-silver

Configuration comes from job arguments, not from the code, so the same script
runs against any account, bucket or environment:
    --lake_bucket    S3 bucket backing the lakehouse        (required)
    --silver_db      Glue database for the silver layer      (default: ecomlake_silver)
    --session_gap_minutes  inactivity gap that ends a session (default: 30)
Terraform passes --lake_bucket automatically; the others fall back to sensible
defaults. Table names (sessions, quarantine_sessions) are part of the data model,
so they stay fixed on purpose.
"""

import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Glue's getResolvedOptions treats every name you pass as required, so we can't
# just list optional args. Instead we ask only for the ones actually present in
# sys.argv, then fall back to defaults for the rest. This keeps --lake_bucket
# required while making --silver_db and --session_gap_minutes truly optional.
_REQUIRED = ["lake_bucket"]
_OPTIONAL = {"silver_db": "ecomlake_silver", "session_gap_minutes": "30"}

_present_optional = [name for name in _OPTIONAL if f"--{name}" in sys.argv]
args = getResolvedOptions(sys.argv, _REQUIRED + _present_optional)

LAKE = args["lake_bucket"]
SILVER_DB = args.get("silver_db", _OPTIONAL["silver_db"])
SESSION_GAP_SECONDS = int(args.get("session_gap_minutes", _OPTIONAL["session_gap_minutes"])) * 60

SESSIONS_TABLE = f"glue_catalog.{SILVER_DB}.sessions"
QUARANTINE_TABLE = f"glue_catalog.{SILVER_DB}.quarantine_sessions"

sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session

# Iceberg catalog wiring (Glue as the metastore, S3 as the warehouse)
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{LAKE}/")
spark.conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog",
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO",
)

# 1. Read the raw clicks Firehose landed in bronze
raw = (
    spark.read.json(f"s3://{LAKE}/bronze/raw_clicks/")
    .withColumn("event_time", F.to_timestamp("event_time"))
    .filter(F.col("user_id").isNotNull() & F.col("event_time").isNotNull())
)

# 2. Sessionize with window functions: a gap over 30 minutes starts a new session
by_user_time = Window.partitionBy("user_id").orderBy("event_time")
sessionized = (
    raw
    .withColumn("prev_time", F.lag("event_time").over(by_user_time))
    .withColumn(
        "gap_seconds",
        F.col("event_time").cast("long") - F.col("prev_time").cast("long"),
    )
    .withColumn(
        "is_new_session",
        ((F.col("gap_seconds").isNull()) | (F.col("gap_seconds") > SESSION_GAP_SECONDS)).cast("int"),
    )
    .withColumn("session_num", F.sum("is_new_session").over(by_user_time))
    .withColumn("session_id", F.concat_ws("-", F.col("user_id"), F.col("session_num")))
)

# 3. Enrich with the product catalog
products = spark.read.parquet(f"s3://{LAKE}/reference/products/")
enriched = sessionized.join(products, on="product_id", how="left")

# 4. Aggregate one row per session
sessions = (
    enriched
    .groupBy("session_id", "user_id")
    .agg(
        F.coalesce(F.first("category", ignorenulls=True), F.lit("unknown")).alias("category"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0)).alias("revenue"),
        F.min("event_time").alias("session_start"),
        F.max("event_time").alias("session_end"),
    )
    .withColumn("revenue", F.col("revenue").cast("decimal(12,2)"))
)

# 5. Enforce the contract. Anything that fails goes to quarantine, not the floor.
contract = (
    F.col("user_id").isNotNull()
    & F.col("session_id").isNotNull()
    & (F.col("event_count") > 0)
    & (F.col("revenue") >= 0)
)
valid = sessions.filter(contract)
quarantine = sessions.filter(~contract).withColumn("quarantined_at", F.current_timestamp())

# 6. Make sure the target Iceberg tables exist (first run creates them)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SESSIONS_TABLE} (
        session_id    string,
        user_id       string,
        category      string,
        event_count   bigint,
        revenue       decimal(12, 2),
        session_start timestamp,
        session_end   timestamp
    ) USING iceberg
    """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
        session_id    string,
        user_id       string,
        category      string,
        event_count   bigint,
        revenue       decimal(12, 2),
        session_start timestamp,
        session_end   timestamp,
        quarantined_at timestamp
    ) USING iceberg
    """
)

# 7. MERGE valid rows so a re-run corrects existing sessions instead of duplicating
valid.createOrReplaceTempView("staged_sessions")
spark.sql(
    f"""
    MERGE INTO {SESSIONS_TABLE} AS t
    USING staged_sessions AS s
    ON t.session_id = s.session_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

quarantine.write.format("iceberg").mode("append").save(QUARANTINE_TABLE)

print(f"silver merge complete: {valid.count()} valid, {quarantine.count()} quarantined")
