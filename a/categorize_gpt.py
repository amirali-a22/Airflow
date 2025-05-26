import os

import openai
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()  # Set your OpenAI API key here or export it as OPENAI_API_KEY in your environment
openai.api_key = os.getenv("OPENAI_API_KEY") or "your-openai-api-key"

# Connect to MongoDB
client = MongoClient("mongodb://admin:admin@localhost:27017/")  # change connection string if needed
db = client["linkedin"]
collection = db["linkedin_person"]

# Fetch all job titles from the collection
documents = collection.find({}, {"experiences.job_title": 1, "_id": 0})

job_titles = []
for doc in documents:
    experiences = doc.get("experiences", [])
    for exp in experiences:
        job_title = exp.get("job_title")
        if job_title:
            job_titles.append(job_title)


# Prepare prompt to categorize job titles
def get_categories_for_job_titles(job_titles):
    prompt = (
        "You are a helpful assistant. Categorize the following job titles into broader categories. "
        "Return the categories as JSON in the format {\"job_title\": \"category\"}.\n\n"
        f"Job titles:\n{job_titles}\n\nCategories:"
    )

    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=500
    )

    # Extract text response
    text_response = response['choices'][0]['message']['content']
    return text_response


# Get categories
categories_json = get_categories_for_job_titles(job_titles[0:4])

print("Categorized job titles:")
print(categories_json)
