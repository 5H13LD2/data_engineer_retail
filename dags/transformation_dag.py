"""
Retail Data Transformation DAG (Bronze → Silver)
=================================================
Pulls raw retail_transactions.csv and weather_data.json from S3 Bronze,
applies cleaning / deduplication / type enforcement, and writes Parquet
files to the Silver layer.

Schedule: Daily at 7:00 AM  (1 hour after ingestion_dag finishes)
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ============================================================
# Bucket config — reads from docker-compose env (set from .env)
# ============================================================
BRONZE_BUCKET = os.environ.get("S3_BRONZE_BUCKET", "retail-logistics-data-lake-jerico")
SILVER_BUCKET = os.environ.get("S3_BRONZE_BUCKET", "retail-logistics-data-lake-jerico")  # same bucket, silver/ prefix

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
# Helper functions (run inside the worker container)
# ============================================================

def transform_transactions(**kwargs):
    """
    1. Download raw CSV from S3 Bronze.
    2. Deduplicate by transaction_id.
    3. Cast data types (unit_price → float, quantity → int, etc.).
    4. Drop rows with null critical fields.
    5. Upload cleaned Parquet to S3 Silver.
    """
    import boto3
    import pandas as pd
    import os

    ds_nodash = kwargs["ds_nodash"]
    raw_key = f"raw/transactions/retail_transactions_{ds_nodash}.csv"
    silver_key = f"silver/transactions/retail_transactions_{ds_nodash}.parquet"
    local_raw = f"/tmp/retail_transactions_{ds_nodash}.csv"
    local_silver = f"/tmp/retail_transactions_{ds_nodash}.parquet"

    s3 = boto3.client("s3")
    print(f"⬇️  Downloading s3://{BRONZE_BUCKET}/{raw_key} ...")
    s3.download_file(BRONZE_BUCKET, raw_key, local_raw)

    df = pd.read_csv(local_raw)
    print(f"  Raw rows: {len(df):,}")

    # -- Type enforcement --
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")

    # -- Drop nulls in critical columns --
    df.dropna(subset=["transaction_id", "transaction_date", "customer_id", "total_amount"], inplace=True)

    # -- Deduplication --
    before = len(df)
    df.drop_duplicates(subset=["transaction_id"], inplace=True)
    print(f"  Removed {before - len(df):,} duplicate rows.")

    # -- Save as Parquet --
    df.to_parquet(local_silver, index=False)
    print(f"  Clean rows: {len(df):,}")

    s3.upload_file(local_silver, SILVER_BUCKET, silver_key)
    print(f"✅ Silver transactions uploaded → s3://{SILVER_BUCKET}/{silver_key}")

    # Cleanup
    os.remove(local_raw)
    os.remove(local_silver)


def transform_weather(**kwargs):
    """
    1. Download raw weather JSON from S3 Bronze.
    2. Flatten + validate fields.
    3. Upload cleaned Parquet to S3 Silver.
    """
    import boto3
    import pandas as pd
    import os
    import json

    ds_nodash = kwargs["ds_nodash"]
    raw_key = f"raw/weather/weather_data_{ds_nodash}.json"
    silver_key = f"silver/weather/weather_data_{ds_nodash}.parquet"
    local_raw = f"/tmp/weather_data_{ds_nodash}.json"
    local_silver = f"/tmp/weather_data_{ds_nodash}.parquet"

    s3 = boto3.client("s3")
    print(f"⬇️  Downloading s3://{BRONZE_BUCKET}/{raw_key} ...")
    s3.download_file(BRONZE_BUCKET, raw_key, local_raw)

    with open(local_raw) as f:
        records = json.load(f)

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["temp"] = pd.to_numeric(df["temp"], errors="coerce")
    df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")
    df.dropna(subset=["city", "temp", "timestamp"], inplace=True)

    df.to_parquet(local_silver, index=False)
    s3.upload_file(local_silver, SILVER_BUCKET, silver_key)
    print(f"✅ Silver weather uploaded → s3://{SILVER_BUCKET}/{silver_key}")

    os.remove(local_raw)
    os.remove(local_silver)


def verify_silver_fn(**kwargs):
    """Verify Silver Parquet files exist in S3 using boto3 (aws CLI not in container)."""
    import boto3
    ds_nodash = kwargs["ds_nodash"]
    s3 = boto3.client("s3")
    keys = [
        f"silver/transactions/retail_transactions_{ds_nodash}.parquet",
        f"silver/weather/weather_data_{ds_nodash}.parquet",
    ]
    for key in keys:
        resp = s3.head_object(Bucket=SILVER_BUCKET, Key=key)
        size_kb = resp["ContentLength"] / 1024
        print(f"  ✅ {key}  ({size_kb:.1f} KB)")
    print("✅ Silver layer verified!")


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="retail_data_transformation",
    default_args=default_args,
    description="Transform Bronze CSV/JSON to Silver Parquet (clean, deduplicate, type-cast)",
    schedule_interval="0 7 * * *",  # 7:00 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "transformation", "silver"],
) as dag:

    transform_txn_task = PythonOperator(
        task_id="transform_transactions",
        python_callable=transform_transactions,
    )

    transform_weather_task = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    verify_silver = PythonOperator(
        task_id="verify_silver_uploads",
        python_callable=verify_silver_fn,
    )

    # Both transformations run in parallel, then verify
    [transform_txn_task, transform_weather_task] >> verify_silver
