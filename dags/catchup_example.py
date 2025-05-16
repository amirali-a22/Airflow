from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id="simple_catchup_mapping",
    start_date=datetime(2025, 5, 14),  # Start 2 days ago
    schedule_interval="@daily",  # Run daily
    catchup=True,  # Process missed runs
) as dag:

    # Task to generate a list of numbers
    @task
    def get_numbers():
        return [1, 2, 3]  # Simple list to map over

    # Task to process each number
    @task
    def process_number(num, date):
        print(f"Processing {num} for {date}")
        return num * 2

    # Get numbers and map process_number task
    numbers = get_numbers()
    processed = process_number.partial(date="{{ ds }}").expand(num=numbers)