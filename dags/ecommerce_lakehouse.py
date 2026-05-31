"""
Airflow DAG (runs on MWAA): the batch side of the lakehouse.

Order: refresh silver with Glue, build gold with dbt, run the quality gate.
The DAG fails closed. If dbt tests fail, gold is not published and the last good
version stays live. Every task is idempotent, so a retry never double counts.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.bash import BashOperator

# Environment-specific values come from Airflow Variables (set per environment in
# the Airflow UI or via env vars), never hardcoded. Defaults keep the DAG runnable
# out of the box for the demo.
AWS_REGION = Variable.get("aws_region", default_var=os.getenv("AWS_REGION", "us-east-1"))
GLUE_JOB = Variable.get("glue_job_name", default_var="ecomlake-dev-bronze-to-silver")
DBT_DIR = Variable.get("dbt_dir", default_var="/usr/local/airflow/dbt")

default_args = {
    "owner": "espinozamoises",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="ecommerce_lakehouse",
    description="Bronze to silver to gold, gated on data quality",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ecommerce", "lakehouse", "iceberg"],
) as dag:

    bronze_to_silver = GlueJobOperator(
        task_id="bronze_to_silver",
        job_name=GLUE_JOB,
        region_name=AWS_REGION,
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=f"cd {DBT_DIR} && dbt run --select gold",
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        # dbt test exits non-zero on failure, which fails the task and stops the DAG
        bash_command=f"cd {DBT_DIR} && dbt test --select gold source:silver",
    )

    bronze_to_silver >> build_gold >> quality_gate
