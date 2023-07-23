from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import etl_tasks

default_args = {
    'owner': 'bot_user',
    'depends_on_past': False,
    'email': ['not@used.com'],
    'start_date': datetime(2023, 7, 23, 6, 30, 5),  # set today's day
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'transform_data',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    description='Combine information from .JSON files to one .csv file, and remove .JSON files',
    schedule_interval=timedelta(days=1),
    is_paused_upon_creation=False
) as dag:

    extract_sensor = FileSensor(
        task_id='extract_comlete',
        filepath='/temp_storage/EXTRACT_SUCCESS.TXT'
    )

    assemble_data = PythonOperator(
        task_id='transform_data',
        python_callable=etl_tasks.combine_to_csv
    )

    remove_json = BashOperator(
        task_id='clean_tmp_stg',
        bash_command='rm /temp_storage/*.json'
    )

    remove_extr_suc = BashOperator(
        task_id='remove_extr_suc',
        bash_command='rm /temp_storage/EXTRACT_SUCCESS.TXT'
    )

    transform_success = BashOperator(
        task_id='_transform_success',
        bash_command='touch /temp_storage/TRANSFORM_SUCCESS.TXT'
    )

    extract_sensor >> assemble_data >> [
        remove_json, remove_extr_suc, transform_success]
