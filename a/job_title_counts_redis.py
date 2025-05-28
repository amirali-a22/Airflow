import redis
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()

# Connect to MongoDB
print("Connecting to MongoDB at mongodb://admin:admin@localhost:27017/...")
client = MongoClient("mongodb://admin:admin@localhost:27017/")
db = client["linkedin"]
collection = db["linkedin_person"]

# Connect to Redis
print("Connecting to Redis at redis://localhost:6379/0...")
redis_client = redis.Redis.from_url("redis://default:redis_password@193.36.85.229:6379/0", decode_responses=True)

# Redis hash name for storing job titles and their counts
hash_name = "linkedin_job_titles"

# Use MongoDB aggregation to unwind and group job titles
print("Running MongoDB aggregation to extract job titles...")
pipeline = [
    {"$unwind": "$experiences"},  # Flatten the experiences array
    {"$match": {"experiences.job_title": {"$exists": True, "$ne": ""}}},  # Filter valid job titles
    {"$group": {
        "_id": "$experiences.job_title",  # Group by job title
        "count": {"$sum": 1}  # Count occurrences
    }}
]

# Initialize counters
total_job_titles = 0
total_records = collection.count_documents({})  # Total documents in collection

# Process aggregation results and update Redis in batches
print("Processing aggregation results and updating Redis...")
batch_size = 1000  # Adjust batch size based on your dataset
job_title_batch = {}

for doc in collection.aggregate(pipeline):
    job_title = doc["_id"].strip()
    count = doc["count"]
    job_title_batch[job_title] = count
    total_job_titles += count

    # Update Redis when batch size is reached
    if len(job_title_batch) >= batch_size:
        with redis_client.pipeline() as pipe:
            for title, count in job_title_batch.items():
                pipe.hincrby(hash_name, title, count)
            pipe.execute()
        print(f"Updated Redis with batch of {len(job_title_batch)} job titles")
        job_title_batch.clear()

# Update Redis with any remaining job titles
if job_title_batch:
    with redis_client.pipeline() as pipe:
        for title, count in job_title_batch.items():
            pipe.hincrby(hash_name, title, count)
        pipe.execute()
    print(f"Updated Redis with final batch of {len(job_title_batch)} job titles")

# Retrieve job titles and counts from Redis
job_title_counts = redis_client.hgetall(hash_name)
# Convert counts to integers
job_title_counts = {title: int(count) for title, count in job_title_counts.items()}

# Calculate metrics
unique_job_titles = len(job_title_counts)
duplicate_count = total_job_titles - unique_job_titles

# Print results
print("\nResults:")
print(f"Total records processed: {total_records}")
print(f"Total job titles found: {total_job_titles}")
print(f"Unique job titles: {unique_job_titles}")
print(f"Duplicate job titles: {duplicate_count}")
print(f"Job titles with redundancy: {job_title_counts}")

# Clean up Redis (optional, comment out to persist data)
print("Cleaning up Redis hash...")
redis_client.delete(hash_name)

# Close connections
client.close()
redis_client.close()
print("MongoDB and Redis connections closed.")
