import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)

from etl import extract, transform, load, join_data

dag = DAG(
    "etl_dag",
    description="Extract, Transform and Load automation routines.",
    start_date=datetime(2024, 8, 1),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
)

extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract,
    # op_kwargs={'city': 'Belem'},
    dag=dag,
)

join_data_task = PythonOperator(task_id="join_data", python_callable=join_data, dag=dag)

transform_task = PythonOperator(
    task_id="transform_data", python_callable=transform, dag=dag
)

load_task = PythonOperator(task_id="load_data", python_callable=load, dag=dag)

extract_task >> join_data_task >> transform_task >> load_task
