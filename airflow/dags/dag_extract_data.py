from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import etl_tasks

default_args = {
    'owner': 'bot_user',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 6, 6, 0, 0),  # set today's day
    'email': ['not@used.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'extract_data',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    description='Extract data from hh.ru API and store it to .JSON files in /temp_storage',
    schedule_interval=timedelta(days=1),
    is_paused_upon_creation=False
) as dag:

    getting_data = PythonOperator(
        task_id='getting_data',
        python_callable=etl_tasks.extract_job_from_hh,
        dag=dag
    )

    getting_data
