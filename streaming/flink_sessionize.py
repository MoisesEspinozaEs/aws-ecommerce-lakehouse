"""
Managed Service for Apache Flink job (PyFlink).

Reads raw click events from Kinesis, groups them into user sessions with a
30 minute inactivity gap, enriches each event with product metadata, and writes
the result to the bronze Iceberg table through Firehose.

The point of doing this in Flink rather than batch: sessionization is stateful.
We need to hold events per user until they go quiet, and we need watermarks so a
late event lands in the right session instead of starting a new one.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

SESSION_GAP = "30' MINUTE"


def build_pipeline(t_env: StreamTableEnvironment) -> None:
    # Source: raw clicks from Kinesis
    t_env.execute_sql(
        """
        CREATE TABLE clicks_raw (
            user_id     STRING,
            event_type  STRING,
            product_id  STRING,
            url         STRING,
            event_time  TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kinesis',
            'stream' = 'ecomlake-dev-clickstream',
            'aws.region' = 'us-east-1',
            'scan.stream.initpos' = 'LATEST',
            'format' = 'json'
        )
        """
    )

    # Small enrichment table (product catalog), kept in the Glue catalog
    t_env.execute_sql(
        """
        CREATE TABLE products (
            product_id STRING,
            category   STRING,
            price      DECIMAL(10, 2)
        ) WITH (
            'connector' = 'filesystem',
            'path' = 's3://ecomlake-dev-lake/reference/products/',
            'format' = 'parquet'
        )
        """
    )

    # Sink: bronze Iceberg table
    t_env.execute_sql(
        """
        CREATE TABLE bronze_sessions (
            session_id    STRING,
            user_id       STRING,
            category      STRING,
            event_count   BIGINT,
            revenue       DECIMAL(12, 2),
            session_start TIMESTAMP(3),
            session_end   TIMESTAMP(3)
        ) WITH (
            'connector' = 'iceberg',
            'catalog-name' = 'glue_catalog',
            'warehouse' = 's3://ecomlake-dev-lake/bronze/',
            'database-name' = 'ecomlake_bronze'
        )
        """
    )

    # Sessionize, enrich, aggregate. SESSION window closes after the inactivity gap.
    t_env.execute_sql(
        f"""
        INSERT INTO bronze_sessions
        SELECT
            CONCAT(c.user_id, '-', CAST(SESSION_START(c.event_time, INTERVAL {SESSION_GAP}) AS STRING)) AS session_id,
            c.user_id,
            COALESCE(p.category, 'unknown') AS category,
            COUNT(*) AS event_count,
            SUM(CASE WHEN c.event_type = 'purchase' THEN p.price ELSE 0 END) AS revenue,
            SESSION_START(c.event_time, INTERVAL {SESSION_GAP}) AS session_start,
            SESSION_END(c.event_time, INTERVAL {SESSION_GAP}) AS session_end
        FROM clicks_raw AS c
        LEFT JOIN products AS p ON c.product_id = p.product_id
        GROUP BY
            c.user_id,
            COALESCE(p.category, 'unknown'),
            SESSION(c.event_time, INTERVAL {SESSION_GAP})
        """
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    build_pipeline(t_env)


if __name__ == "__main__":
    main()
