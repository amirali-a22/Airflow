from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()

# Connect to MongoDB
client = MongoClient("mongodb://admin:admin@localhost:27017/")
db = client["linkedin"]
collection = db["linkedin_person"]

# Fetch all job titles from the collection
print("Fetching job titles from MongoDB collection 'linkedin_person'...")
documents = collection.find({}, {"experiences.job_title": 1, "_id": 0})

# Initialize a dictionary for job titles and their counts
job_title_counts = {}
total_job_titles = 0  # Count all job titles (including duplicates)
total_records = 0  # Count all documents processed

# Process documents to extract job titles
print("Processing documents to extract job titles...")
for doc in documents:
    total_records += 1
    # Check if 'experiences' field exists and is a list
    if "experiences" in doc and isinstance(doc["experiences"], list):
        for experience in doc["experiences"]:
            # Check if 'job_title' exists and is a non-empty string
            if "job_title" in experience and isinstance(experience["job_title"], str) and experience[
                "job_title"].strip():
                job_title = experience["job_title"].strip()
                # Update dictionary: increment count or initialize to 1
                job_title_counts[job_title] = job_title_counts.get(job_title, 0) + 1
                total_job_titles += 1
                print(f"Processed job title: {job_title} (Count: {job_title_counts[job_title]})")
    else:
        print(f"Skipping document with missing or invalid 'experiences' field: {doc}")

# Calculate duplicates
unique_job_titles = len(job_title_counts)
duplicate_count = total_job_titles - unique_job_titles

# Print results
print("\nResults:")
print(f"Total records processed: {total_records}")
print(f"Total job titles found: {total_job_titles}")
print(f"Unique job titles: {unique_job_titles}")
print(f"Duplicate job titles: {duplicate_count}")
print(f"Job titles with redundancy: {job_title_counts}")

# Close MongoDB connection
client.close()
print("MongoDB connection closed.")
