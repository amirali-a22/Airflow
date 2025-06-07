import json
import os
from datetime import datetime

import openai
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables and check for .env issues
try:
    load_dotenv()
except Exception as e:
    print(f"Error loading .env file: {e}")

openai.api_key = os.getenv("OPENAI_API_KEY") or "your-openai-api-key"
if openai.api_key == "your-openai-api-key":
    print("Warning: OPENAI_API_KEY not found in .env file, using default placeholder.")

# Connect to MongoDB
client = MongoClient("mongodb://admin:admin@localhost:27017/")
db = client["linkedin"]
collection = db["linkedin_person"]

# Fetch first_name, last_name, username, and job titles from the collection
documents = collection.find({}, {"profile_info.last_name": 1, "profile_info.first_name": 1, "username": 1,
                                 "experiences.job_title": 1, "_id": 0}).limit(10)

# Prepare data for categorization
data = []
for doc in documents:
    first_name = doc.get("profile_info", {}).get("first_name", "")
    last_name = doc.get("profile_info", {}).get("last_name", "")
    full_name = f"{first_name} {last_name}".strip()
    username = doc.get("username", "")
    experiences = doc.get("experiences", [])
    for exp in experiences:
        job_title = exp.get("job_title")
        if job_title:
            data.append({
                "full_name": full_name,
                "username": username,
                "job_title": job_title
            })


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

    # Extract and print raw response
    text_response = response['choices'][0]['message']['content']
    print("Raw GPT response:")
    print(text_response)

    # Clean response by removing markdown, extra whitespace, and newlines
    cleaned_response = text_response.strip()
    # Remove markdown code block markers
    if cleaned_response.startswith("```json"):
        cleaned_response = cleaned_response[7:].strip()
    if cleaned_response.endswith("```"):
        cleaned_response = cleaned_response[:-3].strip()
    # Remove any additional newlines or leading/trailing whitespace
    cleaned_response = ' '.join(cleaned_response.split())

    # Parse JSON response
    try:
        categories = json.loads(cleaned_response)
        print("Parsed GPT categories:")
        print(json.dumps(categories, indent=2))
        return categories
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse GPT response as JSON: {e}")
        print("Cleaned response for debugging:")
        print(cleaned_response)
        return {}


# Get unique job titles
unique_job_titles = list(set([entry["job_title"] for entry in data]))
print("Unique job titles:", unique_job_titles)

# Categorize job titles
categories = get_categories_for_job_titles(unique_job_titles)

# Add categories to data
for entry in data:
    entry["category"] = categories.get(entry["job_title"], "Uncategorized")

# Create DataFrame
df = pd.DataFrame(data, columns=["full_name", "username", "job_title", "category"])

# Generate Excel file with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f"linkedin_report_{timestamp}.xlsx"
df.to_excel(output_file, index=False, sheet_name="LinkedIn Data")

print(f"Excel report generated: {output_file}")
