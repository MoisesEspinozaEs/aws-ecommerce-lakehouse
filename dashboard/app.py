"""
E-Commerce Lakehouse — analytics dashboard.

Reads the gold marts straight from the lakehouse through Amazon Athena, so what
you see here is the real output of the pipeline, not a static export. Built with
Streamlit so it runs locally with zero hosting cost.

Run:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import os

import boto3
import pandas as pd
import plotly.express as px
import streamlit as st

REGION = os.environ.get("AWS_REGION", "us-east-1")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "ecomlake-dev")
GOLD_SCHEMA = os.environ.get("GOLD_SCHEMA", "ecomlake_gold")

st.set_page_config(
    page_title="E-Commerce Lakehouse | Analytics",
    page_icon="📊",
    layout="wide",
)

ACCENT = "#FF6B35"
PALETTE = ["#FF6B35", "#004E89", "#1B998B", "#E84855", "#C5C3C6"]


@st.cache_data(ttl=300)
def run_athena(sql: str) -> pd.DataFrame:
    """Run a query on Athena and return the result as a DataFrame."""
    athena = boto3.client("athena", region_name=REGION)
    qid = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query {state}: {reason}")

    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    header = [c["VarCharValue"] for c in rows[0]["Data"]]
    data = [[c.get("VarCharValue") for c in r["Data"]] for r in rows[1:]]
    return pd.DataFrame(data, columns=header)


st.title("Real-Time E-Commerce Lakehouse")
st.caption(
    "Live analytics served from Apache Iceberg gold tables via Amazon Athena. "
    "Data flows clickstream → S3 → Glue (Spark) → dbt → Athena."
)

try:
    df = run_athena(
        f"""
        SELECT activity_date, category, sessions, users, revenue,
               converting_sessions, conversion_rate
        FROM {GOLD_SCHEMA}.daily_revenue
        """
    )
except Exception as exc:  # noqa: BLE001 - surface any AWS/config error in the UI
    st.error(f"Could not read from Athena: {exc}")
    st.stop()

for col in ["sessions", "users", "revenue", "converting_sessions", "conversion_rate"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")
df["activity_date"] = pd.to_datetime(df["activity_date"], errors="coerce")

# KPI row
total_revenue = df["revenue"].sum()
total_sessions = int(df["sessions"].sum())
total_users = int(df["users"].sum())
avg_conv = df["conversion_rate"].mean()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total revenue", f"${total_revenue:,.0f}")
k2.metric("Sessions", f"{total_sessions:,}")
k3.metric("Unique users", f"{total_users:,}")
k4.metric("Avg. conversion", f"{avg_conv:.1%}")

st.divider()

left, right = st.columns(2)
with left:
    st.subheader("Revenue by category")
    by_cat = (
        df.groupby("category", as_index=False)["revenue"].sum()
        .sort_values("revenue", ascending=False)
    )
    fig = px.bar(by_cat, x="category", y="revenue", color="category",
                 color_discrete_sequence=PALETTE)
    fig.update_layout(showlegend=False, height=380)
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Conversion rate by category")
    by_conv = (
        df.groupby("category", as_index=False)["conversion_rate"].mean()
        .sort_values("conversion_rate", ascending=False)
    )
    fig = px.bar(by_conv, x="category", y="conversion_rate", color="category",
                 color_discrete_sequence=PALETTE)
    fig.update_layout(showlegend=False, height=380, yaxis_tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Revenue over time")
by_day = df.groupby("activity_date", as_index=False)["revenue"].sum()
fig = px.area(by_day, x="activity_date", y="revenue", color_discrete_sequence=[ACCENT])
fig.update_layout(height=320)
st.plotly_chart(fig, use_container_width=True)

with st.expander("See the underlying gold table"):
    st.dataframe(df.sort_values("revenue", ascending=False), use_container_width=True)

st.caption("Built by Moises Espinoza Estrada · Senior Data Engineer")
