Apache Airflow with Docker
This repository sets up Apache Airflow using Docker for workflow orchestration.
Prerequisites

Docker and Docker Compose installed
Git installed

Setup Instructions

Clone the repository:
git clone <your-repo-url>
cd <repo-name>

Ensure directories exist:
mkdir -p ./dags ./logs ./plugins

Set Airflow UID:
echo -e "AIRFLOW_UID=$(id -u)" > .env

Download Airflow's Docker Compose file (if not present):
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker compose.yaml'

Start Airflow:
docker compose up -d

Access the Airflow UI at http://localhost:8080. Default login: airflow/airflow.

Stopping Airflow
docker compose down

Directory Structure

dags/: Store your Airflow DAGs here.
logs/: Airflow logs (ignored by Git).
plugins/: Custom Airflow plugins.

Notes

Do not commit .env or logs/ to Git.
Add custom DAGs to dags/ and test locally before pushing.

Troubleshooting

Check container status: docker ps
View logs: docker compose logs


