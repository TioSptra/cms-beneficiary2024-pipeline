import os
from google.cloud import bigquery
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
log = logging.getLogger()

def load_stg():
    log.info("Configuration to Google Cloud BigQuery...")

    key_path = os.path.join("/opt/airflow/keys/credentials.json")

    if not os.path.exists(key_path):
        raise FileNotFoundError(f"Credentials file not found at {key_path}")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    log.info(f"Using credentials from: {key_path}")

    project_id = "purwadika"
    dataset_id = "jcdeol005_capstone3_tio_raw"

    log.info("Instantiating BigQuery client...")
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)

    try:
        client.get_dataset(dataset_ref)
        log.info(f"DATASET '{dataset_id}' already exists in BigQuery project '{project_id}'")
        time.sleep(1)
    except Exception:
        log.info(f"Creating DATASET '{dataset_id}' in BigQuery project '{project_id}'")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-southeast2"
        client.create_dataset(dataset)
        log.info(f"[SUCCESS] Added '{dataset_id}' in BigQuery project '{project_id}'")
        time.sleep(1)

if __name__ == "__main__":
    load_stg()
