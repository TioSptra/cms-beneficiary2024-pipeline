import os
from google.cloud import bigquery
import logging
import time
from io import StringIO


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
log = logging.getLogger()

def load_stg(year):
    log.info("Configuration to google cloud-bigquery...")
    key_path = os.path.join("/opt/airflow/keys","credentials.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    project_id = "purwadika"  
    dataset_id = "jcdeol005_capstone3_tio_raw"  
    table_id = f"raw_beneficiary_{year}" 

    log.info("initiating client Bigquery...")
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,  
        write_disposition="WRITE_TRUNCATE"  
    )

    log.info(f"Loading csv to {dataset_id}.{table_id}' ")
    try:
        import requests
        url = "https://data.cms.gov/sites/default/files/2023-04/402c0430-aab0-47f9-ab51-4274e30d2f79/beneficiary_2024.csv"
        file = url.split('/')[-1]
        log.info(f"download beneficiary csv from {url} ")
        respons = requests.get(url)
        respons.raise_for_status()
        time.sleep(1)
        load_job = client.load_table_from_file(StringIO(respons.text), table_ref, job_config=job_config)
        load_job.result() 
        log.info(f"[SUCCESS] '{file}' uploaded to {dataset_id}.{table_id} ")
        time.sleep(1)
    except Exception as e:
        log.exception(f"Unexpected error while loading to {dataset_id}.{table_id}: {e}")
        raise

if __name__ == "__main__":
    load_stg(2024)


