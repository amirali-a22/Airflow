from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
from elasticsearch import Elasticsearch


def read_and_save_fullname():
    import elasticsearch
    print(f"Elasticsearch client version: {elasticsearch.__version__}")

    mongo_uri = "mongodb://admin:admin@mongo:27017/"
    client = MongoClient(mongo_uri)
    db = client['airflow']
    collection = db['airflow_collection']
    es = Elasticsearch(hosts=["http://elasticsearch:9200"])

    docs_count = collection.count_documents({})
    print(f"Found {docs_count} documents in MongoDB")

    for doc in collection.find():
        # Handling first_name and last_name in different structures
        first_name = doc.get('first_name', '')  # top-level field
        last_name = doc.get('last_name', '')  # top-level field

        # If the profile_info field exists, try to get first_name and last_name from it
        profile_info = doc.get('profile_info', {})
        if profile_info:
            first_name = first_name or profile_info.get('first_name', '')
            last_name = last_name or profile_info.get('last_name', '')

        full_name = f"{first_name} {last_name}".strip()

        es_doc = {
            "full_name": full_name,
            "first_name": first_name,
            "last_name": last_name,
        }

        print(es_doc)
        es.index(index="airflow_users", id=str(doc.get('_id')), document=es_doc)
        print(f"Indexed document ID: {str(doc.get('_id'))} with full_name: {full_name}")

    client.close()


with DAG(
        dag_id='mongo_to_elastic_fullname',
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,  # manual trigger
        catchup=False,
        tags=["mongo", "elastic", "example"]
) as dag:
    task = PythonOperator(
        task_id='read_and_save_fullname',
        python_callable=read_and_save_fullname
    )
