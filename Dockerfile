FROM apache/airflow:latest-python3.12

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

USER airflow
