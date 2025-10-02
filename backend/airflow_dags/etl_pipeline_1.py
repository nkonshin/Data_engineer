from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def extract():
    print("Извлекаем данные")

def transform():
    print("Трансформируем данные")

def load():
    print("Загружаем данные в clickhouse")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_pipeline_1',
    default_args=default_args,
    description='ETL pipeline to clickhouse',
    schedule_interval='0 * * * *',
    start_date=days_ago(1),
    tags=['etl'],
) as dag:
    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    t3 = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    t1 >> t2 >> t3
