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
    'start_date': datetime(2023, 7, 23, 6, 30, 15),  # set today's day
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'update_stg_n_mart',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    schedule_interval=timedelta(days=1),
    description='load new jobs to dw table and update datamart',
    is_paused_upon_creation=False
) as dag:

    load_sensor = FileSensor(
        task_id='load_sensor',
        filepath='/temp_storage/LOAD_SUCCESS.TXT'
    )

    update_dw_table = PythonOperator(
        task_id='Update_dw_table',
        python_callable=etl_tasks.add_to_working_table
    )

    load_mart = PythonOperator(
        task_id='Update_mart',
        python_callable=etl_tasks.create_send_data_mart
    )

    clear_success = BashOperator(
        task_id='remove_load_suc',
        bash_command='rm /temp_storage/LOAD_SUCCESS.TXT'
    )

    load_sensor >> update_dw_table >> load_mart >> clear_success
