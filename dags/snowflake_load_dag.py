"""
Snowflake Load DAG (Gold → Snowflake)
======================================
Loads Gold-layer Parquet KPI files from S3 into Snowflake tables
using the COPY INTO command via the Snowflake Airflow provider.

Tables loaded:
  - RETAIL.GOLD.DAILY_REVENUE
  - RETAIL.GOLD.TOP_PRODUCTS
  - RETAIL.GOLD.STORE_REVENUE

Schedule: Daily at 9:00 AM  (1 hour after aggregation_dag finishes)
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ============================================================
# Snowflake connection config (set via Airflow Connections UI)
# Connection ID: snowflake_retail_conn
# ============================================================
import os

SNOWFLAKE_CONN_ID = "snowflake_retail_conn"
SNOWFLAKE_DATABASE = "RETAIL"
SNOWFLAKE_SCHEMA = "GOLD"
GOLD_BUCKET = os.environ.get("S3_BRONZE_BUCKET", "retail-logistics-data-lake-jerico")  # same bucket, gold/ prefix

# ============================================================
# Load Functions
# ============================================================

def load_daily_revenue_to_snowflake(**kwargs):
    """COPY daily_revenue Parquet from S3 Gold into Snowflake."""
    import snowflake.connector
    import os

    ds_nodash = kwargs["ds_nodash"]
    s3_path = f"s3://{GOLD_BUCKET}/gold/kpis/daily_revenue_{ds_nodash}.parquet"
    table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.DAILY_REVENUE"

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()
    try:
        # Truncate partition for today before loading (idempotent)
        cur.execute(f"DELETE FROM {table} WHERE DATE = '{kwargs['ds']}'")

        copy_sql = f"""
            COPY INTO {table}
            FROM '{s3_path}'
            CREDENTIALS = (AWS_ROLE = '{{{{ var.value.snowflake_s3_role }}}}')
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = ABORT_STATEMENT;
        """
        cur.execute(copy_sql)
        print(f"✅ Loaded daily_revenue into {table}")
    finally:
        cur.close()
        conn.close()


def load_top_products_to_snowflake(**kwargs):
    """COPY top_products Parquet from S3 Gold into Snowflake."""
    import snowflake.connector
    import os

    ds_nodash = kwargs["ds_nodash"]
    s3_path = f"s3://{GOLD_BUCKET}/gold/kpis/top_products_{ds_nodash}.parquet"
    table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.TOP_PRODUCTS"

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM {table} WHERE LOAD_DATE = '{kwargs['ds']}'")
        copy_sql = f"""
            COPY INTO {table}
            FROM '{s3_path}'
            CREDENTIALS = (AWS_ROLE = '{{{{ var.value.snowflake_s3_role }}}}')
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = ABORT_STATEMENT;
        """
        cur.execute(copy_sql)
        print(f"✅ Loaded top_products into {table}")
    finally:
        cur.close()
        conn.close()


def load_store_revenue_to_snowflake(**kwargs):
    """COPY store_revenue Parquet from S3 Gold into Snowflake."""
    import snowflake.connector
    import os

    ds_nodash = kwargs["ds_nodash"]
    s3_path = f"s3://{GOLD_BUCKET}/gold/kpis/store_revenue_{ds_nodash}.parquet"
    table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STORE_REVENUE"

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM {table} WHERE LOAD_DATE = '{kwargs['ds']}'")
        copy_sql = f"""
            COPY INTO {table}
            FROM '{s3_path}'
            CREDENTIALS = (AWS_ROLE = '{{{{ var.value.snowflake_s3_role }}}}')
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = ABORT_STATEMENT;
        """
        cur.execute(copy_sql)
        print(f"✅ Loaded store_revenue into {table}")
    finally:
        cur.close()
        conn.close()


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="retail_snowflake_load",
    default_args=default_args,
    description="Load Gold Parquet KPIs from S3 into Snowflake RETAIL.GOLD tables",
    schedule_interval="0 9 * * *",  # 9:00 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "snowflake", "gold", "load"],
) as dag:

    load_revenue = PythonOperator(
        task_id="load_daily_revenue",
        python_callable=load_daily_revenue_to_snowflake,
    )

    load_products = PythonOperator(
        task_id="load_top_products",
        python_callable=load_top_products_to_snowflake,
    )

    load_stores = PythonOperator(
        task_id="load_store_revenue",
        python_callable=load_store_revenue_to_snowflake,
    )

    confirm_load = BashOperator(
        task_id="confirm_snowflake_load",
        bash_command=(
            'echo "✅ All Gold KPIs successfully loaded into Snowflake for {{ ds }}"'
        ),
    )

    # All three loads run in parallel, then confirm
    [load_revenue, load_products, load_stores] >> confirm_load
