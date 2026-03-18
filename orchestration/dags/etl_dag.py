from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for data extraction, transformation, and loading',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

extract_website = BashOperator(
    task_id='extract_website',
    bash_command='python /opt/airflow/extraction/scrape_books.py',
    dag=dag,
)

extract_csv = BashOperator(
    task_id='extract_csv',
    bash_command='python /opt/airflow/extraction/download_csv.py',
    dag=dag,
)

extract_sql = BashOperator(
    task_id='extract_sql',
    bash_command='python /opt/airflow/extraction/extract_sql.py',
    dag=dag,
)

transform = BashOperator(
    task_id='transform',
    bash_command='python /opt/airflow/transformation/transform_data.py',
    dag=dag,
)

load = BashOperator(
    task_id='load',
    bash_command='python /opt/airflow/storage/load_to_minio.py',
    dag=dag,
)

# Set dependencies
extract_website >> transform
extract_csv >> transform
extract_sql >> transform
transform >> load
