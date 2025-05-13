from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define a Python function for the PythonOperator
def print_message():
    print("Hello from PythonOperator!")

with DAG(
    dag_id='my_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # BashOperator task
    bash_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello, Airflow!"'
    )

    # PythonOperator task
    python_task = PythonOperator(
        task_id='print_message',
        python_callable=print_message
    )

    # Set task dependencies
    bash_task >> python_task