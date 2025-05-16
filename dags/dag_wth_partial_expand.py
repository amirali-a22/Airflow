from airflow.decorators import task
from airflow import DAG
from datetime import datetime

with DAG(dag_id="example_partial", start_date=datetime(2022, 1, 1)) as dag:
    @task
    def add(x: int, y: int):
        return x + y

    # y=10 is constant for all mapped tasks
    added_values = add.partial(y=10).expand(x=[1, 2, 3])