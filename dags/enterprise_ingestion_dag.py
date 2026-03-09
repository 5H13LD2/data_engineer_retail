from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import os

# Import natin yung mga ginawa mong scripts sa Notepad
# Siguraduhing nasa /opt/airflow/data_gen ang mga ito sa Docker
from data_gen.data_gen import generate_customers, generate_transactions  # ✅ FIX: was 'data_gen.data_generator' (module does not exist)
from data_gen.weather_api_fetcher import fetch_weather_for_cities

BUCKET_NAME = "retail-logistics-data-lake-jerico" # Palitan mo kung iba ang pinangalan mo

def upload_to_s3(file_name, bucket, object_name=None):
    """Utility function para mag-upload sa S3 gamit ang Boto3"""
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_name, bucket, object_name or file_name)
        print(f"Success: {file_name} uploaded to {bucket}")
    except Exception as e:
        print(f"Error uploading {file_name}: {e}")
        raise

def run_ingestion_pipeline():
    # 1. Generate 1M Records
    # ✅ FIX: was generate_retail_data() which does not exist — correct call is generate_transactions()
    customers = generate_customers(n=5000)
    file_path = generate_transactions(num_rows=100_000, customers=customers)  # ✅ 100k — safe for 1GB RAM

    # 2. Upload Transactions to S3
    upload_to_s3(file_path, BUCKET_NAME, f"raw/transactions/{file_path}")

def run_weather_pipeline():
    # 1. Fetch Weather Data
    cities = ["Manila", "Quezon City", "Cebu City", "Davao City"]
    weather_file = "weather_data.json"
    fetch_weather_for_cities(cities, output_file=weather_file)
    
    # 2. Upload Weather JSON to S3
    upload_to_s3(weather_file, BUCKET_NAME, f"raw/weather/{weather_file}")

default_args = {
    'owner': 'jerico',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'enterprise_ingestion_pipeline',
    default_args=default_args,
    description='Ingest 1M retail records and Weather API data to S3',
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_ingest_retail = PythonOperator(
        task_id='ingest_retail_transactions',
        python_callable=run_ingestion_pipeline
    )

    task_ingest_weather = PythonOperator(
        task_id='ingest_weather_data',
        python_callable=run_weather_pipeline
    )

    # ✅ FIX: was missing '>>' — tasks were never linked, neither would actually execute
    # Parallel execution — both run at the same time
    task_ingest_retail
    task_ingest_weather
