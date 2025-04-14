from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task
import os

BUCKET_NAME = "nz_business_bucket"
LOCAL_FOLDER = "/Users/lalvarez/Documents/stats_nz_business_airflow/data"
GCP_CONN_ID = "google_cloud_default"

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id="upload_csvs_to_gcs",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["gcs", "upload"],
) as dag:

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        storage_class="STANDARD",
        location="US",
        gcp_conn_id=GCP_CONN_ID,
    )

    @task
    def list_csv_files():
        return [
            os.path.join(LOCAL_FOLDER, f)
            for f in os.listdir(LOCAL_FOLDER)
            if f.endswith(".csv")
        ]

    @task
    def upload_file(file_path: str):
        from airflow.providers.google.cloud.operators.gcs import GCSUploadFileOperator
        file_name = os.path.basename(file_path)
        upload = GCSUploadFileOperator(
            task_id=f"upload_{file_name.replace('.', '_')}",
            bucket_name=BUCKET_NAME,
            object_name=f"data/{file_name}",
            filename=file_path,
            gcp_conn_id=GCP_CONN_ID,
        )
        return upload.execute(context={})

    file_paths = list_csv_files()

    # Task mapping usando expand()
    uploaded = upload_file.expand(file_path=file_paths)

    create_bucket >> file_paths >> uploaded
