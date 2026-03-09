"""
Retail Data Aggregation DAG (Silver → Gold)
============================================
Reads clean Parquet files from S3 Silver, computes business KPIs
(daily revenue, top products, revenue by store, payment mix),
and writes aggregated Parquet files to the Gold layer.

Schedule: Daily at 8:00 AM  (1 hour after transformation_dag finishes)
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ============================================================
# Bucket config — reads from docker-compose env (set from .env)
# ============================================================
SILVER_BUCKET = os.environ.get("S3_BRONZE_BUCKET", "retail-logistics-data-lake-jerico")
GOLD_BUCKET = os.environ.get("S3_BRONZE_BUCKET", "retail-logistics-data-lake-jerico")

# ============================================================
# Default Arguments
# ============================================================
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ============================================================
# Aggregation Functions
# ============================================================

def aggregate_daily_revenue(**kwargs):
    """Compute total revenue, transaction count, and avg order value per day."""
    import boto3
    import pandas as pd
    import os

    ds_nodash = kwargs["ds_nodash"]
    silver_key = f"silver/transactions/retail_transactions_{ds_nodash}.parquet"
    gold_key = f"gold/kpis/daily_revenue_{ds_nodash}.parquet"
    local_silver = f"/tmp/silver_txn_{ds_nodash}.parquet"
    local_gold = f"/tmp/daily_revenue_{ds_nodash}.parquet"

    s3 = boto3.client("s3")
    s3.download_file(SILVER_BUCKET, silver_key, local_silver)

    df = pd.read_parquet(local_silver)
    df["date"] = df["transaction_date"].dt.date

    agg = df.groupby("date").agg(
        total_revenue=("total_amount", "sum"),
        transaction_count=("transaction_id", "count"),
        avg_order_value=("total_amount", "mean"),
    ).reset_index()

    agg["avg_order_value"] = agg["avg_order_value"].round(2)
    agg["total_revenue"] = agg["total_revenue"].round(2)

    agg.to_parquet(local_gold, index=False)
    s3.upload_file(local_gold, GOLD_BUCKET, gold_key)
    print(f"✅ Daily revenue KPIs → s3://{GOLD_BUCKET}/{gold_key} ({len(agg)} days)")

    os.remove(local_silver)
    os.remove(local_gold)


def aggregate_top_products(**kwargs):
    """Rank top 20 products by revenue and units sold."""
    import boto3
    import pandas as pd
    import os

    ds_nodash = kwargs["ds_nodash"]
    silver_key = f"silver/transactions/retail_transactions_{ds_nodash}.parquet"
    gold_key = f"gold/kpis/top_products_{ds_nodash}.parquet"
    local_silver = f"/tmp/silver_txn_prod_{ds_nodash}.parquet"
    local_gold = f"/tmp/top_products_{ds_nodash}.parquet"

    s3 = boto3.client("s3")
    s3.download_file(SILVER_BUCKET, silver_key, local_silver)

    df = pd.read_parquet(local_silver)

    agg = df.groupby(["product_category", "product_name"]).agg(
        total_revenue=("total_amount", "sum"),
        units_sold=("quantity", "sum"),
        transaction_count=("transaction_id", "count"),
    ).reset_index().sort_values("total_revenue", ascending=False).head(20)

    agg.to_parquet(local_gold, index=False)
    s3.upload_file(local_gold, GOLD_BUCKET, gold_key)
    print(f"✅ Top products KPIs → s3://{GOLD_BUCKET}/{gold_key}")

    os.remove(local_silver)
    os.remove(local_gold)


def aggregate_revenue_by_store(**kwargs):
    """Compute revenue and transaction volume per store location."""
    import boto3
    import pandas as pd
    import os

    ds_nodash = kwargs["ds_nodash"]
    silver_key = f"silver/transactions/retail_transactions_{ds_nodash}.parquet"
    gold_key = f"gold/kpis/store_revenue_{ds_nodash}.parquet"
    local_silver = f"/tmp/silver_txn_store_{ds_nodash}.parquet"
    local_gold = f"/tmp/store_revenue_{ds_nodash}.parquet"

    s3 = boto3.client("s3")
    s3.download_file(SILVER_BUCKET, silver_key, local_silver)

    df = pd.read_parquet(local_silver)

    agg = df.groupby("store_location").agg(
        total_revenue=("total_amount", "sum"),
        transaction_count=("transaction_id", "count"),
        avg_order_value=("total_amount", "mean"),
        unique_customers=("customer_id", "nunique"),
    ).reset_index().sort_values("total_revenue", ascending=False)

    agg["avg_order_value"] = agg["avg_order_value"].round(2)
    agg["total_revenue"] = agg["total_revenue"].round(2)

    agg.to_parquet(local_gold, index=False)
    s3.upload_file(local_gold, GOLD_BUCKET, gold_key)
    print(f"✅ Store revenue KPIs → s3://{GOLD_BUCKET}/{gold_key} ({len(agg)} stores)")

    os.remove(local_silver)
    os.remove(local_gold)


def verify_gold_fn(**kwargs):
    """Verify Gold Parquet KPI files exist in S3 (boto3 — aws CLI not in container)."""
    import boto3
    ds_nodash = kwargs["ds_nodash"]
    s3 = boto3.client("s3")
    keys = [
        f"gold/kpis/daily_revenue_{ds_nodash}.parquet",
        f"gold/kpis/top_products_{ds_nodash}.parquet",
        f"gold/kpis/store_revenue_{ds_nodash}.parquet",
    ]
    for key in keys:
        resp = s3.head_object(Bucket=GOLD_BUCKET, Key=key)
        size_kb = resp["ContentLength"] / 1024
        print(f"  ✅ {key}  ({size_kb:.1f} KB)")
    print("✅ Gold layer verified!")


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="retail_data_aggregation",
    default_args=default_args,
    description="Aggregate Silver Parquet to Gold KPIs (revenue, top products, store performance)",
    schedule_interval="0 8 * * *",  # 8:00 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "aggregation", "gold", "kpis"],
) as dag:

    daily_revenue_task = PythonOperator(
        task_id="aggregate_daily_revenue",
        python_callable=aggregate_daily_revenue,
    )

    top_products_task = PythonOperator(
        task_id="aggregate_top_products",
        python_callable=aggregate_top_products,
    )

    store_revenue_task = PythonOperator(
        task_id="aggregate_revenue_by_store",
        python_callable=aggregate_revenue_by_store,
    )

    verify_gold = PythonOperator(
        task_id="verify_gold_uploads",
        python_callable=verify_gold_fn,
    )

    # All three aggregations run in parallel, then verify
    [daily_revenue_task, top_products_task, store_revenue_task] >> verify_gold
