from airflow import models
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.utils.dates import days_ago

from google.cloud import storage

BUCKET_NAME = 'nz_business_data_bucket'
PROJECT_ID = 'nz-business-data-project'
DATASET = 'nz_business' 
FOLDER = ''

default_args = {
    'start_date': days_ago(1),
}

with models.DAG(
    dag_id='gcs_to_bq_dynamic_loader',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    def get_csv_file_names(**context):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(BUCKET_NAME)
        blobs = bucket.list_blobs(prefix=FOLDER)

        csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
        context['ti'].xcom_push(key='csv_files', value=csv_files)

    list_csv_files = PythonOperator(
        task_id='list_csv_files',
        python_callable=get_csv_file_names,
        provide_context=True,
    )

    def generate_tasks(**context):
        csv_files = context['ti'].xcom_pull(key='csv_files', task_ids='list_csv_files')

        for csv_file in csv_files:
            table_name = csv_file.split('/')[-1].replace('.csv', '')
            task_id = f"load_{table_name.replace('-', '_').replace('.', '_')}"

            load_task = GoogleCloudStorageToBigQueryOperator(
                task_id=task_id,
                bucket=BUCKET_NAME,
                source_objects=[csv_file],
                destination_project_dataset_table=f"{PROJECT_ID}:{DATASET}.{table_name}",
                source_format='CSV',
                skip_leading_rows=1,
                autodetect=True,
                write_disposition='WRITE_TRUNCATE',
            )

            list_csv_files >> load_task

    create_load_tasks = PythonOperator(
        task_id='create_load_tasks',
        python_callable=generate_tasks,
        provide_context=True,
    )

    list_csv_files >> create_load_tasks
