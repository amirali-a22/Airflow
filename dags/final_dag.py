import csv
import os
import requests
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from bson import ObjectId
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pymongo import MongoClient
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

BATCH_SIZE = 100  # Process 1000 documents per batch


def serialize_doc(doc):
    if isinstance(doc, dict):
        return {k: serialize_doc(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [serialize_doc(i) for i in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    else:
        return doc


@task
def maybe_drop_index(**kwargs):
    drop_index_flag = kwargs["params"].get("drop_index", False)
    if not drop_index_flag:
        print("🟡 drop_index is False or not set. Skipping index deletion.")
        return

    ELASTIC_URL = os.getenv("ELASTIC_URL")
    index_name = "v2_optimized_dag"

    if not ELASTIC_URL:
        raise ValueError("ELASTIC_URL environment variable not set")

    es = Elasticsearch(ELASTIC_URL)
    try:
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            print(f"🧹 Deleted Elasticsearch index: {index_name}")
        else:
            print(f"ℹ️ Index {index_name} does not exist. Nothing to delete.")
    except Exception as e:
        print(f"❌ Failed to delete index {index_name}: {e}")
    finally:
        es.close()


@task
def print_env():
    import os
    print("🔍 ENV DEBUG START")
    for k in ["MONGO_URL", "MONGO_DB", "MONGO_COLLECTION", "ELASTIC_URL"]:
        print(f"{k} = {os.getenv(k)}")
    print("🔍 ENV DEBUG END")


def notify(context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_id = context['task_instance'].task_id
    try_number = context['task_instance'].try_number

    alert_message = f"""
    🚨 *DAG Task Failure Alert* 🚨

    *DAG ID:* `{dag_id}`
    *Run ID:* `{run_id}`
    *Task ID:* `{task_id}`
    *Try:* {try_number}
    *Timestamp:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
    """

    try:
        slack_token = Variable.get("slack_bot_token")
        client = WebClient(token=slack_token)
        response = client.chat_postMessage(
            channel="C0807NDTTFD",
            text=alert_message
        )
        print(f"Slack message sent successfully: {response['ts']}")
    except SlackApiError as e:
        print(f"Slack API error: {e.response['error']}")
    except Exception as e:
        print(f"Slack notify error: {e}")


@task
def retrieve_batch(batch_info):
    offset, size = batch_info
    mongo_uri = os.getenv("MONGO_URL")
    db_name = os.getenv("MONGO_DB")
    collection_name = os.getenv("MONGO_COLLECTION")
    if not all([mongo_uri, db_name, collection_name]):
        raise ValueError("Missing MongoDB environment variables")

    client = MongoClient(mongo_uri)
    collection = client[db_name][collection_name]

    docs = list(collection.find().skip(offset).limit(size))
    client.close()
    return [serialize_doc(doc) for doc in docs]


# Load job title to category mapping from CSV
csv_path = Path(__file__).parent / "categories.csv"
job_title_to_category = {}
with open(csv_path, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    subcategory_columns = [col for col in reader.fieldnames if col.startswith('Subcategory')]
    for row in reader:
        category = row.get('Category', '').strip()
        for col in subcategory_columns:
            subcategory = (row.get(col, '') or '').strip()
            if subcategory:
                job_title_to_category[subcategory] = category


@task
def process_batch(batch):
    actions = []
    for doc in batch:
        first_name = doc.get('profile_info', {}).get('first_name', '')
        last_name = doc.get('profile_info', {}).get('last_name', '')
        full_name = f"{first_name} {last_name}".strip()
        last_job_title = doc.get('experience_info', {}).get('last_job_title', '')
        category = job_title_to_category.get(last_job_title, 'Others')

        es_doc = {
            "_op_type": "index",
            "_index": "v2_optimized_dag",
            "_id": str(doc.pop('_id')),
            "_source": {
                **doc,
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "last_job_title": last_job_title,
                "category": category,
            }
        }
        actions.append(es_doc)
    return actions


@task
def save_batch(actions):
    ELASTIC_URL = os.getenv("ELASTIC_URL")
    if not ELASTIC_URL:
        raise ValueError("ELASTIC_URL environment variable not set")
    es = Elasticsearch(hosts=[ELASTIC_URL])
    if actions:
        success, failed = bulk(es, actions)
        print(f"Indexed: {success} success, {failed} failed")
        es.close()
        return {"success": success, "failed": failed}
    else:
        es.close()
        print("Empty batch skipped")
        return {"success": 0, "failed": 0}


@task
def calculate_batches(**kwargs):
    mongo_uri = os.getenv("MONGO_URL")
    db_name = os.getenv("MONGO_DB")
    collection_name = os.getenv("MONGO_COLLECTION")
    if not all([mongo_uri, db_name, collection_name]):
        raise ValueError("Missing MongoDB environment variables")

    client = MongoClient(mongo_uri)
    collection = client[db_name][collection_name]
    total_docs_param = kwargs['params'].get('total_docs')
    if total_docs_param is not None:
        total_docs = int(total_docs_param)
        print(f"Using total_docs from params: {total_docs}")
    else:
        total_docs = collection.count_documents({})
        print(f"No param provided. Using total_docs from MongoDB: {total_docs}")

    batch_offsets = [(i, min(BATCH_SIZE, total_docs - i)) for i in range(0, total_docs, BATCH_SIZE)]

    client.close()
    return batch_offsets


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'on_failure_callback': notify,
}

with DAG(
        dag_id='final',
        default_args=default_args,
        start_date=datetime(2025, 1, 1),
        schedule=None,  # Manual trigger
        catchup=False,
        tags={"mongo", "elastic", "batch", "dynamic"},
        params={
            "total_docs": None,
            "drop_index": False

        }
) as dag:
    env = print_env()
    drop = maybe_drop_index()
    batch_offsets = calculate_batches()
    batches = retrieve_batch.expand(batch_info=batch_offsets)
    processed = process_batch.expand(batch=batches)
    save_batch.expand(actions=processed)

    env >> drop >> batches
