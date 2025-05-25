import json

from bson import json_util  # For handling MongoDB-specific types like ObjectId
from pymongo import MongoClient


def retrieve_batch(batch_info=None):
    offset, size = (0, 10) if batch_info is None else batch_info
    mongo_uri = "mongodb://admin:admin@localhost:27017/"
    client = MongoClient(mongo_uri)
    db = client['linkedin']
    collection = db['linkedin_person']

    # Fetch one batch using skip and limit
    docs = list(collection.find().skip(offset).limit(size))

    client.close()

    # Convert documents to JSON format
    json_docs = json.dumps(docs, default=json_util.default)

    print(f"Retrieved batch at offset {offset} with {len(docs)} documents")
    with open('mongo_person.json', 'w') as f:
        f.write(json.dumps(docs, default=json_util.default, indent=4))


retrieve_batch()
