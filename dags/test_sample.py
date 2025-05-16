from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Define a function for the BashOperator command
def get_bash_command():
    return 'echo "Hello, Airflow!"'


# Define a function for the PythonOperator
def print_message():
    print("Hello from PythonOperator!")


with DAG(
        dag_id='my_dag',
        start_date=datetime(2025, 1, 1),
        schedule_interval='*/10 * * * * *',  # Run every 10 seconds
        catchup=False,
) as dag:
    # BashOperator task using function
    bash_task = BashOperator(
        task_id='say_hello_bash',
        bash_command=get_bash_command()
    )

    # PythonOperator task
    python_task = PythonOperator(
        task_id='print_message_python',
        python_callable=print_message
    )

    # Set task dependencies
    bash_task >> python_task
