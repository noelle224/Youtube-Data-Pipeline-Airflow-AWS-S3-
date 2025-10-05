from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(__file__))

# Ensure Airflow can find your ETL code
#sys.path.append("/home/ubuntu/airflow/youtube_dag")  # change if your ETL is elsewhere

# Import your ETL function
from youtube_etl import fetch_youtube_videos

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='youtube_etl_dag',
    default_args=default_args,
    description='Daily ETL for YouTube channel videos using PySpark',
    schedule='@daily',  # adjust as needed
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['youtube', 'pyspark', 'etl']
) as dag:

    # Task: Run the ETL function
    run_youtube_etl = PythonOperator(
        task_id='fetch_youtube_videos',
        python_callable=fetch_youtube_videos
    )

    # If you have more tasks, define dependencies here
    # e.g., task1 >> task2

