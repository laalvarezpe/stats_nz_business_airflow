from airflow import models
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.utils.dates import days_ago

from google.cloud import storage

def list_csv_files(bucket_name, prefix=''):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
    return csv_files


BUCKET_NAME = 'nz_business_data_bucket'
PROJECT_ID = 'nz-business-data-project'
DATASET = 'nz_business' 
FOLDER = ''

default_args = {
    'start_date': days_ago(1),
}

with models.DAG(
    'dag_auto_gcs_to_bq',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    
    csv_files = list_csv_files(bucket_name=BUCKET_NAME, prefix=FOLDER)

    # A single task for each file
    for csv_file in csv_files:
        task_id = f"load_{csv_file.replace('.csv', '').replace('/', '_')}"
        table_name = csv_file.split('/')[-1].replace('.csv', '')
        destination_table = f"{PROJECT_ID}.{DATASET}.{table_name}"

        load_task = GoogleCloudStorageToBigQueryOperator(
            task_id=task_id,
            bucket=BUCKET_NAME,
            source_objects=[csv_file],
            source_format='CSV',
            destination_project_dataset_table=destination_table,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            allow_jagged_rows=True,
        )
