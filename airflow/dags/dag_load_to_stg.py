from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import etl_tasks

default_args = {
    'owner': 'bot_user',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 30, 6, 0, 0),  # set today's day
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

    transform_sensor = ExternalTaskSensor(
        task_id='transform_sensor',
        external_dag_id='transform_data',
        external_task_id='clean_tmp_stg',
        timeout=600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_delta=timedelta(minutes=5),
        mode='poke'
    )

    load_to_stg = PythonOperator(
        task_id='load_to_stg',
        python_callable=etl_tasks.job_stg_filling,
        dag=dag
    )

    rename_res = BashOperator(
        task_id='rename_res',
        bash_command=f'mv /temp_storage/csv/result.csv /temp_storage/csv/result_{date.today()}.csv',
        dag=dag
    )

    transform_sensor >> load_to_stg >> rename_res
