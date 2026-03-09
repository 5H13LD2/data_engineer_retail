"""
Retail Pipeline Health Monitor DAG
====================================
A weekly maintenance DAG that:
1. Checks S3 layer sizes (Bronze / Silver / Gold) and alerts if storage is anomalous.
2. Scans for and reports stale files (older than 7 days with no updates).
3. Validates that each pipeline layer ran successfully by cross-checking
   object counts between Bronze → Silver → Gold.
4. Sends a consolidated Slack-style summary report (printed to logs).

Schedule: Every Sunday at 10:00 AM (weekly audit)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ============================================================
# Default Arguments
# ============================================================
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BRONZE_BUCKET = "retail-data-pipeline-bronze"
SILVER_BUCKET = "retail-data-pipeline-silver"
GOLD_BUCKET = "retail-data-pipeline-gold"


# ============================================================
# Monitor Functions
# ============================================================

def audit_s3_layer_sizes(**kwargs):
    """
    Sum object sizes for each S3 layer and print a storage report.
    Raises if any layer is completely empty.
    """
    import boto3

    s3 = boto3.client("s3")
    layers = {
        "Bronze": BRONZE_BUCKET,
        "Silver": SILVER_BUCKET,
        "Gold": GOLD_BUCKET,
    }
    report = []

    for layer_name, bucket in layers.items():
        paginator = s3.get_paginator("list_objects_v2")
        total_size_bytes = 0
        total_objects = 0

        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                total_size_bytes += obj["Size"]
                total_objects += 1

        size_mb = total_size_bytes / (1024 * 1024)
        report.append(
            f"  {layer_name}: {total_objects} objects | {size_mb:.2f} MB"
        )

        if total_objects == 0:
            raise ValueError(f"❌ MONITOR FAIL: {layer_name} bucket ({bucket}) is empty!")

    print("📦 S3 Layer Storage Audit:")
    for line in report:
        print(line)
    print("✅ All layers have data.")


def audit_stale_files(**kwargs):
    """
    Find S3 objects in Bronze that haven't been updated in > 7 days.
    Prints a warning list (does not fail the task).
    """
    import boto3
    from datetime import timezone

    s3 = boto3.client("s3")
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    stale = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BRONZE_BUCKET):
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff:
                stale.append(f"  {obj['Key']} (last modified: {obj['LastModified'].date()})")

    if stale:
        print(f"⚠️  {len(stale)} stale files found in Bronze (>7 days old):")
        for line in stale[:20]:  # Cap output at 20 lines
            print(line)
        if len(stale) > 20:
            print(f"  ... and {len(stale) - 20} more.")
    else:
        print("✅ No stale files in Bronze bucket.")


def cross_layer_object_count_check(**kwargs):
    """
    Verify that Silver has at least as many object keys as Bronze,
    and Gold has at least 3 KPI files (one per aggregation type).
    """
    import boto3

    s3 = boto3.client("s3")

    def count_objects(bucket, prefix=""):
        paginator = s3.get_paginator("list_objects_v2")
        count = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            count += len(page.get("Contents", []))
        return count

    bronze_txn = count_objects(BRONZE_BUCKET, "transactions/")
    silver_txn = count_objects(SILVER_BUCKET, "transactions/")
    gold_kpis = count_objects(GOLD_BUCKET, "kpis/")

    print(f"  Bronze transactions/ objects : {bronze_txn}")
    print(f"  Silver transactions/ objects : {silver_txn}")
    print(f"  Gold kpis/           objects : {gold_kpis}")

    issues = []
    if silver_txn < bronze_txn:
        issues.append(
            f"Silver ({silver_txn}) has fewer transaction objects than Bronze ({bronze_txn}). "
            "Some files may not have been transformed."
        )
    if gold_kpis < 3:
        issues.append(
            f"Gold has only {gold_kpis} KPI files (expected ≥ 3). "
            "Aggregation may be incomplete."
        )

    if issues:
        print("⚠️  Layer count discrepancies detected:")
        for issue in issues:
            print(f"   - {issue}")
    else:
        print("✅ Cross-layer object counts look healthy.")


def generate_pipeline_health_report(**kwargs):
    """Print a combined health summary for the weekly report."""
    execution_date = kwargs["execution_date"]
    print("=" * 60)
    print(f"  🏥  RETAIL PIPELINE WEEKLY HEALTH REPORT")
    print(f"  📅  Report Date : {execution_date.strftime('%Y-%m-%d')}")
    print("=" * 60)
    print("  Pipeline Layer Schedule:")
    print("    06:00 AM → Ingestion DAG (Bronze)")
    print("    07:00 AM → Transformation DAG (Silver)")
    print("    07:30 AM → Data Quality DAG (DQ Checks)")
    print("    08:00 AM → Aggregation DAG (Gold)")
    print("    09:00 AM → Snowflake Load DAG")
    print("=" * 60)
    print("✅ Weekly health check complete.")


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="retail_pipeline_health_monitor",
    default_args=default_args,
    description="Weekly audit: S3 layer sizes, stale file detection, cross-layer object count validation",
    schedule_interval="0 10 * * 0",  # Every Sunday at 10:00 AM
    start_date=datetime(2024, 1, 7),
    catchup=False,
    tags=["retail", "monitoring", "health", "audit"],
) as dag:

    layer_size_audit = PythonOperator(
        task_id="audit_s3_layer_sizes",
        python_callable=audit_s3_layer_sizes,
    )

    stale_files_audit = PythonOperator(
        task_id="audit_stale_files",
        python_callable=audit_stale_files,
    )

    cross_layer_check = PythonOperator(
        task_id="cross_layer_object_count_check",
        python_callable=cross_layer_object_count_check,
    )

    health_report = PythonOperator(
        task_id="generate_pipeline_health_report",
        python_callable=generate_pipeline_health_report,
    )

    # Audits run in parallel → then print the final report
    [layer_size_audit, stale_files_audit, cross_layer_check] >> health_report
