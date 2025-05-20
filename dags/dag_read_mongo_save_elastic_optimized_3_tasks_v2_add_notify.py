from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import TaskInstance
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.db import create_session
from airflow.utils.trigger_rule import TriggerRule
from bson import ObjectId
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pymongo import MongoClient
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Batch size configuration
BATCH_SIZE = 1000  # Process 1000 documents per batch


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


# Notification function for Slack
def notify(**context):
    # Get DAG details from context
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']

    # Query failed tasks from the Airflow metadata database
    failed_tasks = []
    with create_session() as session:
        failed_tis = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.execution_date == execution_date,
            TaskInstance.state == 'failed'
        ).all()
        # Extract base task names, removing instance suffixes (e.g., retrieve_batch.1000 -> retrieve_batch)
        failed_tasks = list(set(ti.task_id.split('.')[0] for ti in failed_tis))

    # Construct the alert message
    alert_message = f"""
    🚨 *DAG Task Failure Alert* 🚨

    #Priority: High ⚠️
    *Service:* `{dag_id}`
    *Alert:* One or more tasks in the DAG have failed.

    🔍 *Details:*
    - DAG ID: `{dag_id}`
    - Execution Date: `{execution_date.isoformat()}`
    - Failed Tasks: {', '.join(failed_tasks) if failed_tasks else 'Unknown (check Airflow logs)'}

    💡 *Suggested Action:*
    Investigate the Airflow logs for the failed task(s). Check MongoDB/Elasticsearch connectivity and task-specific errors.

    📅 *Timestamp:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}

    #Alert #IncidentResponse #DataPipeline 🔧
    """

    # Retrieve Slack bot token from Airflow Variables
    try:
        slack_token = Variable.get("slack_bot_token")
    except KeyError:
        raise Exception("Slack bot token not found in Airflow Variables. Please set 'slack_bot_token'.")

    # Initialize Slack client
    client = WebClient(token=slack_token)

    try:
        # Send message to Slack channel
        response = client.chat_postMessage(
            channel="C0807NDTTFD",
            text=alert_message
        )
        print(f"Slack message sent successfully: {response['ts']}")
    except SlackApiError as e:
        print(f"Error sending Slack message: {e.response['error']}")
        raise  # Re-raise to mark the task as failed in Airflow


# Task 1: Read one batch from MongoDB
@task
def retrieve_batch(batch_info):
    offset, size = batch_info
    mongo_uri = "mongodb://admin:admin@mongo:27017/"
    client = MongoClient(mongo_uri)
    db = client['airflow']
    collection = db['airflow_collection']

    # Fetch one batch using skip and limit
    docs = list(collection.find().skip(offset).limit(size))
    serialized_docs = [serialize_doc(doc) for doc in docs]

    client.close()
    print(f"Retrieved batch at offset {offset} with {len(serialized_docs)} documents")
    return serialized_docs


# Task 2: Process one batch for Elasticsearch
@task
def process_batch(batch):
    actions = []
    for doc in batch:
        first_name = doc.get('first_name', '')
        last_name = doc.get('last_name', '')
        full_name = f"{first_name} {last_name}".strip()

        es_doc = {
            "_op_type": "index",
            "_index": "v2_optimized_dag",
            "_id": str(doc.get('_id')),
            "_source": {
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
            }
        }
        actions.append(es_doc)
    print(f"Processed batch with {len(actions)} actions")
    return actions


# Task 3: Save one batch to Elasticsearch
@task
def save_batch(actions):
    es = Elasticsearch(hosts=["http://elasticsearch:9200"])
    if actions:
        success, failed = bulk(es, actions)
        print(f"Batch indexed: {success} documents successful, {failed} failed")
        es.close()
        return {"success": success, "failed": failed}
    else:
        print("Empty batch, skipping")
        es.close()
        return {"success": 0, "failed": 0}


# Task 0: Calculate batch offsets
@task
def calculate_batches():
    z = 2 / 0
    print(z)
    mongo_uri = "mongodb://admin:admin@mongo:27017/"
    client = MongoClient(mongo_uri)
    db = client['airflow']
    collection = db['airflow_collection']

    total_docs = collection.count_documents({})
    batch_offsets = [(i, min(BATCH_SIZE, total_docs - i)) for i in range(0, total_docs, BATCH_SIZE)]

    client.close()
    print(f"Total documents: {total_docs}, Number of batches: {len(batch_offsets)}")
    return batch_offsets


with DAG(
        dag_id='mongo_to_elastic_fullname_optimized_3_tasks_v2_notify',
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,  # Manual trigger
        catchup=False,
        tags=["mongo", "elastic", "batch", "dynamic"]
) as dag:
    # Task 0: Calculate batch offsets
    batch_offsets = calculate_batches()

    # Task 1: Retrieve one batch at a time
    batches = retrieve_batch.expand(batch_info=batch_offsets)

    # Task 2: Process each batch
    processed_batches = process_batch.expand(batch=batches)

    # Task 3: Save each processed batch to Elasticsearch
    saved_batches = save_batch.expand(actions=processed_batches)

    # Task 4: Notify on failure
    notify_task = PythonOperator(
        task_id="notify_failure",
        python_callable=notify,
        trigger_rule=TriggerRule.ONE_FAILED,  # Run if *any* upstream task failed
        provide_context=True,  # Pass Airflow context to the notify function
        dag=dag,
    )

    # Set task dependencies
    [batch_offsets, batches, processed_batches, saved_batches] >> notify_task
