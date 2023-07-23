from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import etl_tasks

default_args = {
    'owner': 'bot_user',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 23, 6, 30, 10),  # set today's day
    'email': ['not@used.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'load_stg',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    description='Load recived data to stg',
    schedule_interval=timedelta(days=1),
    is_paused_upon_creation=False
) as dag:

    transform_sensor = FileSensor(
        task_id='transform_comlete',
        filepath='/temp_storage/TRANSFORM_SUCCESS.TXT'
    )

    load_to_stg = PythonOperator(
        task_id='load_to_stg',
        python_callable=etl_tasks.job_stg_filling
    )

    rename_res = BashOperator(
        task_id='rename_res',
        bash_command=f'mv /temp_storage/csv/result.csv /temp_storage/csv/result_{date.today()}.csv'
    )

    remove_trans_suc = BashOperator(
        task_id='remove_trans_suc',
        bash_command='rm /temp_storage/TRANSFORM_SUCCESS.TXT'
    )

    load_success = BashOperator(
        task_id='load_success',
        bash_command='touch /temp_storage/LOAD_SUCCESS.TXT'
    )

    transform_sensor >> load_to_stg >> [
        rename_res, remove_trans_suc, load_success]
