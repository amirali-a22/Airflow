from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient


def insert_10k_docs():
    mongo_uri = "mongodb://admin:admin@mongo:27017/"
    client = MongoClient(mongo_uri)
    db = client['airflow']
    collection = db['airflow_collection']

    bulk_docs = []
    for i in range(1, 10001):
        doc = {
            "first_name": f"first_{i}",
            "last_name": f"last_{i}"
        }
        bulk_docs.append(doc)

    # Insert all documents in bulk
    collection.insert_many(bulk_docs)
    client.close()
    print("Inserted 10,000 documents into MongoDB.")


with DAG(
        dag_id='insert_10k_to_mongo',
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["mongo", "insert"]
) as dag:
    insert_task = PythonOperator(
        task_id='insert_10k_docs',
        python_callable=insert_10k_docs
    )
