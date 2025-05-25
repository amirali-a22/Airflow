import json
import re
from collections import defaultdict

from bson import json_util
from pymongo import MongoClient


def extract_keywords(job_title):
    """Extract significant keywords from a job title."""
    if not job_title:
        return []
    # Convert to lowercase and remove special characters
    job_title = job_title.lower()
    job_title = re.sub(r'[^\w\s]', '', job_title)

    # Simple stop words list
    stop_words = {'and', 'or', 'the', 'of', 'in', 'at', 'for', 'to'}

    # Split into words and filter out stop words
    words = job_title.split()
    keywords = [word for word in words if word not in stop_words and len(word) > 2]

    return keywords


def categorize_job_title(job_title, category_map):
    """Assign a job title to a dynamic category based on keywords."""
    keywords = extract_keywords(job_title)
    if not keywords:
        return "Other"

    # Find the most significant keyword (e.g., longest or first relevant word)
    for keyword in keywords:
        if keyword in category_map:
            return category_map[keyword]

    # If no matching category, create a new one based on the first keyword
    primary_keyword = keywords[0]
    category_map[primary_keyword] = primary_keyword.capitalize()
    return category_map[primary_keyword]


def categorize_jobs(data, category_map):
    """Categorize all job titles in the experiences array of a LinkedIn profile."""
    categorized_jobs = []

    if "experiences" in data:
        for experience in data["experiences"]:
            job_title = experience.get("job_title", "")
            if job_title:  # Only process if job_title exists
                category = categorize_job_title(job_title, category_map)
                categorized_jobs.append({
                    "job_title": job_title,
                    "category": category
                })

    return categorized_jobs


def retrieve_and_categorize_batch(batch_info=None):
    """Retrieve a batch from MongoDB and dynamically categorize job titles."""
    offset, size = (0, 10) if batch_info is None else batch_info
    mongo_uri = "mongodb://admin:admin@localhost:27017/"
    client = MongoClient(mongo_uri)
    db = client['linkedin']
    collection = db['linkedin_person']

    # Fetch batch
    docs = list(collection.find().skip(offset).limit(size))
    client.close()

    # Dynamic category map to track keyword-to-category mappings
    category_map = {}

    # Categorize job titles across all documents
    all_categorized_jobs = []
    for doc in docs:
        categorized_jobs = categorize_jobs(doc, category_map)
        all_categorized_jobs.extend(categorized_jobs)

    # Print consolidated output
    json_output = json.dumps(all_categorized_jobs, default=json_util.default, indent=2)
    print(f"Categorized {len(all_categorized_jobs)} job titles across {len(docs)} profiles:")
    print(json_output)

    # Summarize categories
    category_counts = defaultdict(int)
    for job in all_categorized_jobs:
        category_counts[job["category"]] += 1

    summary = [{"category": category, "count": count} for category, count in sorted(category_counts.items())]
    json_summary = json.dumps(summary, indent=2)
    print("\nCategory Summary:")
    print(json_summary)

    return all_categorized_jobs


def retrieve_and_categorize_batch(batch_info=None):
    """Retrieve a batch from MongoDB and dynamically categorize job titles."""
    offset, size = (0, 10) if batch_info is None else batch_info
    mongo_uri = "mongodb://admin:admin@localhost:27017/"
    client = MongoClient(mongo_uri)
    db = client['linkedin']
    collection = db['linkedin_person']

    # Fetch batch
    docs = list(collection.find().skip(offset).limit(size))

    # Dynamic category map to track keyword-to-category mappings
    category_map = {}

    # Categorize job titles across all documents
    all_categorized_jobs = []
    for doc in docs:
        categorized_jobs = categorize_jobs(doc, category_map)
        all_categorized_jobs.extend(categorized_jobs)

    # Print consolidated output
    json_output = json.dumps(all_categorized_jobs, default=json_util.default, indent=2)
    print(f"Categorized {len(all_categorized_jobs)} job titles across {len(docs)} profiles:")
    print(json_output)

    # Summarize categories
    category_counts = defaultdict(int)
    for job in all_categorized_jobs:
        category_counts[job["category"]] += 1

    summary = [{"category": category, "count": count} for category, count in sorted(category_counts.items())]
    json_summary = json.dumps(summary, indent=2)
    print("\nCategory Summary:")
    print(json_summary)

    # Store categorized jobs in a new collection
    categorized_collection = db['categorized_jobs']
    if all_categorized_jobs:
        categorized_collection.insert_many(all_categorized_jobs)

    counts_collection = db['category_counts']
    if summary:
        counts_collection.insert_many(summary)

    client.close()
    return all_categorized_jobs


retrieve_and_categorize_batch()
