import csv

data = [
    ["job_title", "category"],
    ["Software Engineer", "Technology"],
    ["Data Analyst", "Technology"],
    ["Accountant", "Finance"],
    ["Marketing Manager", "Business"],
    ["Nurse", "Healthcare"]
]

with open("../dags/job_categories.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(data)

print("CSV file 'job_categories.csv' created with 5 rows.")