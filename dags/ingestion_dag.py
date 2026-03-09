"""
Retail Data Ingestion DAG
=========================
Ang DAG na ito ang nag-o-orchestrate ng data ingestion pipeline:
1. Generate 100k retail transactions (data_gen.py)
2. Fetch weather data for all store locations (weather_api_fetcher.py)
3. Upload both datasets to Amazon S3 (Bronze layer) via boto3

Schedule: Daily at 6:00 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

# ============================================================
# Config — reads from docker-compose env vars (set from .env)
# ============================================================
BRONZE_BUCKET = os.environ.get("S3_BRONZE_BUCKET", "retail-logistics-data-lake-jerico")

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
# Upload Functions (boto3 — aws CLI not installed in container)
# ============================================================

def upload_transactions_fn(**kwargs):
    """Upload retail_transactions.csv to S3 Bronze layer using boto3."""
    import boto3

    ds_nodash = kwargs["ds_nodash"]
    local_file = "/opt/airflow/data_gen/output/retail_transactions.csv"
    s3_key = f"raw/transactions/retail_transactions_{ds_nodash}.csv"

    if not os.path.exists(local_file):
        raise FileNotFoundError(
            f"❌ {local_file} not found. Did generate_retail_transactions succeed?"
        )

    s3 = boto3.client("s3")
    print(f"⬆️  Uploading {local_file} → s3://{BRONZE_BUCKET}/{s3_key}")
    s3.upload_file(local_file, BRONZE_BUCKET, s3_key)
    print(f"✅ Transactions uploaded → s3://{BRONZE_BUCKET}/{s3_key}")


def upload_weather_fn(**kwargs):
    """Upload weather_data.json to S3 Bronze layer using boto3."""
    import boto3

    ds_nodash = kwargs["ds_nodash"]
    local_file = "/opt/airflow/data_gen/output/weather_data.json"
    s3_key = f"raw/weather/weather_data_{ds_nodash}.json"

    if not os.path.exists(local_file):
        raise FileNotFoundError(
            f"❌ {local_file} not found. Did fetch_weather_data succeed?"
        )

    s3 = boto3.client("s3")
    print(f"⬆️  Uploading {local_file} → s3://{BRONZE_BUCKET}/{s3_key}")
    s3.upload_file(local_file, BRONZE_BUCKET, s3_key)
    print(f"✅ Weather data uploaded → s3://{BRONZE_BUCKET}/{s3_key}")


def verify_uploads_fn(**kwargs):
    """List uploaded files in S3 to confirm both uploads succeeded."""
    import boto3

    ds_nodash = kwargs["ds_nodash"]
    s3 = boto3.client("s3")

    prefixes = [
        f"raw/transactions/retail_transactions_{ds_nodash}.csv",
        f"raw/weather/weather_data_{ds_nodash}.json",
    ]

    print(f"🔍 Verifying uploads in s3://{BRONZE_BUCKET}/...")
    for key in prefixes:
        try:
            resp = s3.head_object(Bucket=BRONZE_BUCKET, Key=key)
            size_kb = resp["ContentLength"] / 1024
            print(f"  ✅ {key}  ({size_kb:.1f} KB)")
        except Exception as e:
            raise FileNotFoundError(f"❌ Missing in S3: s3://{BRONZE_BUCKET}/{key} — {e}")

    print("✅ All uploads verified!")


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="retail_data_ingestion",
    default_args=default_args,
    description="Ingestion pipeline: Generate retail data + fetch weather → upload to S3",
    schedule_interval="0 6 * * *",  # Araw-araw, 6:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "ingestion", "bronze"],
) as dag:

    # ----------------------------------------------------------
    # Task 1: Generate Retail Transaction Data (100k rows)
    # ----------------------------------------------------------
    generate_transactions = BashOperator(
        task_id="generate_retail_transactions",
        bash_command=(
            "cd /opt/airflow/data_gen && "
            "NUM_ROWS=100000 DATA_OUTPUT_DIR=/opt/airflow/data_gen/output python data_gen.py"
        ),
    )

    # ----------------------------------------------------------
    # Task 2: Fetch Weather Data from OpenWeatherMap API
    # ----------------------------------------------------------
    fetch_weather = BashOperator(
        task_id="fetch_weather_data",
        bash_command=(
            "cd /opt/airflow/data_gen && "
            "python weather_api_fetcher.py"
        ),
    )

    # ----------------------------------------------------------
    # Task 3: Upload Retail Data to S3 (Bronze Layer)
    # ----------------------------------------------------------
    upload_transactions_to_s3 = PythonOperator(
        task_id="upload_transactions_to_s3",
        python_callable=upload_transactions_fn,
    )

    # ----------------------------------------------------------
    # Task 4: Upload Weather Data to S3 (Bronze Layer)
    # ----------------------------------------------------------
    upload_weather_to_s3 = PythonOperator(
        task_id="upload_weather_to_s3",
        python_callable=upload_weather_fn,
    )

    # ----------------------------------------------------------
    # Task 5: Verify Uploads via boto3 head_object
    # ----------------------------------------------------------
    verify_s3_uploads = PythonOperator(
        task_id="verify_s3_uploads",
        python_callable=verify_uploads_fn,
    )

    # ----------------------------------------------------------
    # Task Dependencies (DAG Flow)
    # ----------------------------------------------------------
    # generate_transactions ──┐
    #                         ├──> upload_transactions ──┐
    # fetch_weather ──────────┤                          ├──> verify
    #                         └──> upload_weather ───────┘

    generate_transactions >> upload_transactions_to_s3
    fetch_weather >> upload_weather_to_s3
    [upload_transactions_to_s3, upload_weather_to_s3] >> verify_s3_uploads
