from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from elasticsearch import Elasticsearch

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 0
}

with DAG(
    dag_id="check_elasticsearch",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["test", "elasticsearch"]
) as dag:

    @task
    def ping_elasticsearch():
        import os
        es_url = os.getenv("ELASTIC_URL", "http://elasticsearch:9200")
        es = Elasticsearch(hosts=[es_url])

        if es.ping():
            print(f"✅ Successfully connected to Elasticsearch at {es_url}")
        else:
            raise Exception(f"❌ Could not connect to Elasticsearch at {es_url}")

        health = es.cluster.health()
        print("🔍 Cluster health info:")
        print(health)

    ping_elasticsearch()
