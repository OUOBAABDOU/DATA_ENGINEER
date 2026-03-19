import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add the project root to the Python path
sys.path.append('/opt/airflow')


def run_scrape_books():
    from extraction.scrape_books import scrape_books
    scrape_books()


def run_download_csv():
    from extraction.download_csv import download_csv
    download_csv()


def run_extract_sql():
    from extraction.extract_sql import extract_from_sql
    extract_from_sql()


def run_extract_insd_anpe():
    from extraction.extract_insd_anpe import extract_insd_anpe
    extract_insd_anpe()


def run_transform():
    from transformation.transform_data import transform_data
    transform_data()


def run_load():
    from storage.load_to_minio import load_to_minio
    load_to_minio(required=True)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

doc_md = """
# ETL Pipeline

This DAG orchestrates the full ETL workflow:

1. Extract data from the website, local CSV, SQL database, and INSD source
2. Transform and unify the datasets
3. Load generated files into MinIO

The extraction tasks run independently, then converge on `transform`, followed by `load`.
"""

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='ETL pipeline orchestrated with Airflow',
    schedule=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'books', 'minio'],
    doc_md=doc_md,
) as dag:
    extract_website = PythonOperator(
        task_id='extract_website',
        python_callable=run_scrape_books,
    )

    extract_csv = PythonOperator(
        task_id='extract_csv',
        python_callable=run_download_csv,
    )

    extract_sql = PythonOperator(
        task_id='extract_sql',
        python_callable=run_extract_sql,
    )

    extract_insd_anpe = PythonOperator(
        task_id='extract_insd_anpe',
        python_callable=run_extract_insd_anpe,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=run_transform,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=run_load,
    )

    [extract_website, extract_csv, extract_sql, extract_insd_anpe] >> transform >> load
