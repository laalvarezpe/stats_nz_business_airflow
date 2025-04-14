# Airflow Assessment
For the purpose of this exercise, we will be using publicly available data from the New Zealand government's website [ CSV files for down
load | Stats NZ ]. Each of the files can be considered independent; while they may be combined later in the pipeline, this task requires only
ingestion of the data into BigQuery via Airflow. You can get access to GCP with $300 of credits [ Free Trial and Free Tier Services and Pr
oducts ]. The exercise should take no longer than 2-3 hours.

## Tools
> - Python (Pandas, Beautifulsoup4)
> - Apache Airflow pip installation (Docker improvement pending)
> - BigQuery (GCP) datawarehouse



## Aproach
1. For extracting the csv files form the Stats New Zeland Business I used BeautifulSoup for automate this process
2. Data was processed and formatted validated using python
2. I use the GCP cli to move the data processed to GCS
3. Apache Airflow was installed in the local machine with SQL Lite because this a small proyect just for development approach, however it needed to move to docker



## Task
--Write an Airflow DAG to ingest each of the CSV files under the Business header of the linked datasource. The resulting data should thenm be written to BigQuery in the default project in a dataset named nz business. The DAG should be responsible for creating any tables necessary in BigQuery as well as ensuring that data has been written successfully. Note that some of the CSV files may be compressed.

