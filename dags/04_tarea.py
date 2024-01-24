from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime , timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Ivan Valdes',
    'depends_on_past': False,
    'email_on_failure': 'ivmigliore@gmail.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def start_process():
    print(" INICIO EL PROCESO!")

def load_bi():
    print(" Load Bi!")

def load_raw_2():
    print(" Hola Raw 2!")

def load_raw_1():
    print(" Hola Raw 1!")

def load_master_1():
    print(" Hola Master 1!")

def load_master_2():
    print(" Hola Master 2!")

with DAG(
    dag_id="Tarea_sesion_5", schedule="@once",
    start_date=days_ago(1), 
    description='Tarea sesion 5'
) as dag:
    step_start = PythonOperator(
        task_id='step_start',
        python_callable=start_process,
        dag=dag
    )
    step_load_raw_1 = PythonOperator(
        task_id='step_load_raw_1',
        python_callable=load_raw_1,
        dag=dag
    )
    step_load_raw_2 = PythonOperator(
        task_id='step_load_raw_2',
        python_callable=load_raw_2,
        dag=dag
    )
    step_master_1 = PythonOperator(
        task_id='step_master_1',
        python_callable=load_master_1,
        dag=dag
    )
    step_master_2 = PythonOperator(
        task_id='step_master_2',
        python_callable=load_master_2,
        dag=dag
    )
    step_bi = PythonOperator(
        task_id='step_bi',
        python_callable=load_bi,
        dag=dag
    )
    step_start>>step_load_raw_1
    step_start>>step_load_raw_2
    step_load_raw_1>>step_master_1
    step_load_raw_2>>step_master_2
    step_master_1>>step_bi
    step_master_2>>step_bi
