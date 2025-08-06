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
    log.info("Configuration to google cloud-bigquery...")
    key = os.path.join("/opt/airflow/keys","credentials.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key
    project_id = "purwadika"  
    dataset_id = "jcdeol005_capstone3_tio_raw"  

    log.info("insitation client Bigquery...")
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        log.info(f"DATASET '{dataset_id}' already on bigquery '{project_id}'")
        time.sleep(1)
    except Exception:
        log.info(f"CREATE DATASET '{dataset_id}' on bigquery '{project_id}'")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)
        log.info(f"[SUCCESS] add '{dataset_id}' on bigquery '{project_id}'")
        time.sleep(1)

if __name__ == "__main__":
    load_stg()
