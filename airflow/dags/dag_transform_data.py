from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import etl_tasks

default_args = {
    'owner': 'bot_user',
    'depends_on_past': False,
    'email': ['not@used.com'],
    'start_date': datetime(2023, 6, 6, 6, 0, 0),  # set today's day
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

    extract_sensor = ExternalTaskSensor(
        task_id='extract_sensor',
        external_dag_id='extract_data',
        external_task_id='getting_data',
        timeout=600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode='poke'
    )

    assemble_data = PythonOperator(
        task_id='transform_data',
        python_callable=etl_tasks.combine_to_csv,
        dag=dag
    )

    remove_json = BashOperator(
        task_id='clean_tmp_stg',
        bash_command='rm /temp_storage/*.json',
        dag=dag
    )

    extract_sensor >> assemble_data >> remove_json
