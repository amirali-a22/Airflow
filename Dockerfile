FROM apache/airflow:3.0.2

USER airflow

COPY requirements.txt .

RUN pip install -r requirements.txt

USER airflow
