from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from bson import ObjectId
import json

# Helper function to serialize MongoDB documents (convert ObjectId to string)
def serialize_doc(doc):
    if isinstance(doc, dict):
        return {k: serialize_doc(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [serialize_doc(i) for i in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    else:
        return doc

# Task 1: Retrieve documents from MongoDB and serialize them
def retrieve_documents_from_mongo(**context):
    mongo_uri = "mongodb://admin:admin@mongo:27017/"
    client = MongoClient(mongo_uri)
    db = client['airflow']
    collection = db['airflow_collection']

    docs = list(collection.find())
    client.close()

    serialized_docs = [serialize_doc(doc) for doc in docs]

    # Push serialized docs as JSON string via XCom
    context['ti'].xcom_push(key='mongo_docs', value=json.dumps(serialized_docs))


# Task 2: Process serialized JSON docs and prepare Elasticsearch bulk actions
def process_documents_for_elastic(**context):
    docs_json = context['ti'].xcom_pull(task_ids='retrieve_documents_from_mongo', key='mongo_docs')
    docs = json.loads(docs_json)

    actions = []
    for doc in docs:
        first_name = doc.get('first_name', '')
        last_name = doc.get('last_name', '')
        full_name = f"{first_name} {last_name}".strip()

        es_doc = {
            "_op_type": "index",
            "_index": "optimized_dag",
            "_id": str(doc.get('_id')),
            "_source": {
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
            }
        }
        actions.append(es_doc)

    # Push actions to XCom for next task
    context['ti'].xcom_push(key='es_actions', value=actions)


# Task 3: Bulk insert documents into Elasticsearch
def bulk_insert_to_elastic(**context):
    es = Elasticsearch(hosts=["http://elasticsearch:9200"])

    actions = context['ti'].xcom_pull(task_ids='process_documents_for_elastic', key='es_actions')
    if actions:
        success, failed = bulk(es, actions)
        print(f"Successfully indexed {success} documents.")
        if failed:
            print(f"Failed to index {failed} documents.")
    else:
        print("No documents to index.")

    es.close()


with DAG(
    dag_id='mongo_to_elastic_fullname_optimized_3_tasks',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=["mongo", "elastic", "optimized"]
) as dag:

    retrieve_docs_task = PythonOperator(
        task_id='retrieve_documents_from_mongo',
        python_callable=retrieve_documents_from_mongo,
        provide_context=True
    )

    process_docs_task = PythonOperator(
        task_id='process_documents_for_elastic',
        python_callable=process_documents_for_elastic,
        provide_context=True
    )

    bulk_insert_task = PythonOperator(
        task_id='bulk_insert_to_elastic',
        python_callable=bulk_insert_to_elastic,
        provide_context=True
    )

    retrieve_docs_task >> process_docs_task >> bulk_insert_task
