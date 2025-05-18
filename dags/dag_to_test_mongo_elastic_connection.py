from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
from elasticsearch import Elasticsearch


def test_mongo_connection():
    mongo_uri = "mongodb://admin:admin@mongo:27017/"
    client = MongoClient(mongo_uri)
    dbs = client.list_database_names()
    print("MongoDB databases:", dbs)
    client.close()


def test_elasticsearch_connection():
    es = Elasticsearch(hosts=["http://elasticsearch:9200"])
    print("Pinging Elasticsearch...")
    print(es.ping())


with DAG(
        dag_id='test_mongo_elastic_connection',
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["test", "mongo", "elastic"]
) as dag:
    mongo_test_task = PythonOperator(
        task_id='test_mongo_connection',
        python_callable=test_mongo_connection
    )

    elastic_test_task = PythonOperator(
        task_id='test_elasticsearch_connection',
        python_callable=test_elasticsearch_connection
    )

    mongo_test_task >> elastic_test_task
