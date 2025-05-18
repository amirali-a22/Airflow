from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from bson import ObjectId
import json
import os
from datetime import datetime


def insert_json_to_mongo():
    # MongoDB connection URI
    mongo_uri = "mongodb://admin:admin@mongo:27017/"
    client = MongoClient(mongo_uri)
    db = client['airflow']  # Replace with your DB name
    collection = db['airflow_collection']

    # Adjust the path to your JSON file (Make sure the path is correct inside the Airflow container)
    json_file_path = '/opt/airflow/dags/person_temp.json'

    # Check if the JSON file exists
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"JSON file not found at {json_file_path}")

    # Open and load the JSON file
    with open(json_file_path, 'r') as f:
        data = json.load(f)

    # Function to convert $oid fields to ObjectId
    def convert_oid(doc):
        if isinstance(doc, dict):
            for k, v in doc.items():
                if k == "_id" and isinstance(v, dict) and "$oid" in v:
                    doc[k] = ObjectId(v["$oid"])
                else:
                    convert_oid(v)  # Recursively convert nested dicts
        elif isinstance(doc, list):
            for item in doc:
                convert_oid(item)
        return doc

    # Convert any $oid fields to actual ObjectId in data
    data = convert_oid(data)

    # Insert data into MongoDB
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

    client.close()
    print(f"Inserted data from {json_file_path} into MongoDB collection 'airflow_collection'.")


with DAG(
        dag_id='insert_json_to_mongo',
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,  # Run on demand
        catchup=False,
        tags=["mongo", "insert", "json"]
) as dag:
    # Task to insert JSON data into MongoDB
    insert_json_task = PythonOperator(
        task_id='insert_json_to_mongo_task',
        python_callable=insert_json_to_mongo
    )
