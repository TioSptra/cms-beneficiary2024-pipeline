import os
from google.cloud import bigquery
import logging
import time
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
log = logging.getLogger()

def load_stg(year):
    log.info("Configuration to google cloud-bigquery...")
    key = os.path.join("/opt/airflow/keys","credentials.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key
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
        csv = os.path.join("/opt/airflow/tmp",f"beneficiary_{year}.csv")
        with open(csv,"rb") as file:
            load_job = client.load_table_from_file(file, table_ref, job_config=job_config)
            load_job.result() 
        log.info(f"[SUCCESS] '{csv}' uploaded to {dataset_id}.{table_id} ")
        time.sleep(1)
    except Exception as e:
        log.exception(f"Unexpected error while loading to {dataset_id}.{table_id}: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Input the beneficiary year indicator")
    parser.add_argument("--year", type=int, help="Input the beneficiary year file that has been downloaded")
    args = parser.parse_args()
    load_stg(args.year)
