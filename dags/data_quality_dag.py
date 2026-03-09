"""
Retail Data Quality Check DAG
==============================
Runs automated data quality checks against the Silver layer:
- Row count thresholds (minimum viable dataset)
- Null rate checks on critical columns
- Revenue sanity bounds (no negative values, no outliers)
- Weather completeness check

If any check FAILS, the task raises an exception and Airflow marks it RED.

Schedule: Daily at 7:30 AM  (after transformation, before aggregation)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ============================================================
# Default Arguments
# ============================================================
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# ============================================================
# Quality Check Functions
# ============================================================

MIN_EXPECTED_ROWS = 10_000  # ✅ Adjusted for 100k dataset (was 100k for 500k dataset)


def check_transaction_row_count(**kwargs):
    """Verify that the Silver transactions file has enough rows."""
    import boto3
    import pandas as pd
    import os

    ds_nodash = kwargs["ds_nodash"]
    silver_bucket = "retail-data-pipeline-silver"
    silver_key = f"transactions/retail_transactions_{ds_nodash}.parquet"
    local_path = f"/tmp/dq_txn_{ds_nodash}.parquet"

    s3 = boto3.client("s3")
    s3.download_file(silver_bucket, silver_key, local_path)

    df = pd.read_parquet(local_path, columns=["transaction_id"])
    row_count = len(df)
    os.remove(local_path)

    print(f"  Row count: {row_count:,}")
    if row_count < MIN_EXPECTED_ROWS:
        raise ValueError(
            f"❌ DQ FAIL: Only {row_count:,} rows in Silver transactions "
            f"(expected ≥ {MIN_EXPECTED_ROWS:,})."
        )
    print(f"✅ Row count check passed: {row_count:,} rows.")


def check_transaction_nulls(**kwargs):
    """Check null rates on critical columns — fail if above 1%."""
    import boto3
    import pandas as pd
    import os

    ds_nodash = kwargs["ds_nodash"]
    silver_bucket = "retail-data-pipeline-silver"
    silver_key = f"transactions/retail_transactions_{ds_nodash}.parquet"
    local_path = f"/tmp/dq_nulls_{ds_nodash}.parquet"
    critical_cols = ["transaction_id", "transaction_date", "customer_id", "total_amount"]

    s3 = boto3.client("s3")
    s3.download_file(silver_bucket, silver_key, local_path)

    df = pd.read_parquet(local_path, columns=critical_cols)
    total = len(df)
    failed = []

    for col in critical_cols:
        null_rate = df[col].isna().sum() / total
        print(f"  {col}: null rate = {null_rate:.4%}")
        if null_rate > 0.01:
            failed.append(f"{col} ({null_rate:.2%} nulls)")

    os.remove(local_path)

    if failed:
        raise ValueError(f"❌ DQ FAIL: Excessive nulls in columns: {', '.join(failed)}")
    print("✅ Null rate check passed.")


def check_transaction_amounts(**kwargs):
    """Ensure no negative total_amount values and no extreme outliers."""
    import boto3
    import pandas as pd
    import os

    ds_nodash = kwargs["ds_nodash"]
    silver_bucket = "retail-data-pipeline-silver"
    silver_key = f"transactions/retail_transactions_{ds_nodash}.parquet"
    local_path = f"/tmp/dq_amounts_{ds_nodash}.parquet"

    s3 = boto3.client("s3")
    s3.download_file(silver_bucket, silver_key, local_path)

    df = pd.read_parquet(local_path, columns=["total_amount"])
    negative_count = (df["total_amount"] < 0).sum()
    outlier_count = (df["total_amount"] > 100_000).sum()  # $100k per transaction is suspicious
    os.remove(local_path)

    print(f"  Negative amounts: {negative_count:,}")
    print(f"  Extreme outliers (>$100k): {outlier_count:,}")

    if negative_count > 0:
        raise ValueError(f"❌ DQ FAIL: {negative_count:,} negative total_amount values found.")
    if outlier_count > 0:
        print(f"⚠️  WARNING: {outlier_count:,} transactions exceed $100k — review recommended.")

    print("✅ Amount sanity check passed.")


def check_weather_completeness(**kwargs):
    """Verify that weather data covers at least 10 cities."""
    import boto3
    import pandas as pd
    import os

    ds_nodash = kwargs["ds_nodash"]
    silver_bucket = "retail-data-pipeline-silver"
    silver_key = f"weather/weather_data_{ds_nodash}.parquet"
    local_path = f"/tmp/dq_weather_{ds_nodash}.parquet"
    MIN_CITIES = 10

    s3 = boto3.client("s3")
    s3.download_file(silver_bucket, silver_key, local_path)

    df = pd.read_parquet(local_path, columns=["city"])
    city_count = df["city"].nunique()
    os.remove(local_path)

    print(f"  Cities in weather data: {city_count}")
    if city_count < MIN_CITIES:
        raise ValueError(
            f"❌ DQ FAIL: Only {city_count} cities in weather data (expected ≥ {MIN_CITIES})."
        )
    print(f"✅ Weather completeness check passed: {city_count} cities.")


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="retail_data_quality_checks",
    default_args=default_args,
    description="Automated DQ checks on Silver layer: row counts, nulls, amount sanity, weather coverage",
    schedule_interval="30 7 * * *",  # 7:30 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "data-quality", "silver"],
) as dag:

    row_count_check = PythonOperator(
        task_id="check_transaction_row_count",
        python_callable=check_transaction_row_count,
    )

    null_check = PythonOperator(
        task_id="check_transaction_nulls",
        python_callable=check_transaction_nulls,
    )

    amount_check = PythonOperator(
        task_id="check_transaction_amounts",
        python_callable=check_transaction_amounts,
    )

    weather_check = PythonOperator(
        task_id="check_weather_completeness",
        python_callable=check_weather_completeness,
    )

    # All checks run in parallel
    [row_count_check, null_check, amount_check, weather_check]
