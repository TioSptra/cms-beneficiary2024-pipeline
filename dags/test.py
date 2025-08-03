import os
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

if __name__ == "__main__":
    client = bigquery.Client()
    query = "SELECT 'tio' AS name, product_name, price, from `chinook_source.source_products` order by price DESC;"

    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        print(row)
    print("Query executed successfully.")
    print("Query job ID:", query_job.job_id)