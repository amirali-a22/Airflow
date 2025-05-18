from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


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

    # Collecting data in bulk to optimize indexing
    actions = []
    for doc in collection.find():
        first_name = doc.get('first_name', '')
        last_name = doc.get('last_name', '')
        full_name = f"{first_name} {last_name}".strip()

        es_doc = {
            "_op_type": "index",  # Ensures the operation is an index (upsert)
            "_index": "optimized_dag",  # The index name is defined as 'optimized_dag'
            "_id": str(doc.get('_id')),  # Use MongoDB '_id' as the doc id in Elasticsearch
            "_source": {
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
            }
        }
        actions.append(es_doc)

        # Print to see what is being inserted
        print(es_doc)

    # Insert in bulk to Elasticsearch
    if actions:
        success, failed = bulk(es, actions)
        print(f"Successfully indexed {success} documents.")
        if failed:
            print(f"Failed to index {failed} documents.")
    else:
        print("No documents to index.")

    client.close()


with DAG(
        dag_id='mongo_to_elastic_fullname_optimized',
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,  # manual trigger
        catchup=False,
        tags=["mongo", "elastic", "optimized"]
) as dag:
    # Task to read from MongoDB and insert into Elasticsearch under the 'optimized_dag' index
    insert_json_task = PythonOperator(
        task_id='read_and_save_fullname',
        python_callable=read_and_save_fullname
    )
