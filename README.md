# ETL Pipeline Project

This repository contains an end-to-end ETL pipeline built with Python, Pandas, MinIO, Streamlit, and Apache Airflow.

## Main Features
- Extraction from a scraped website, a local CSV file, a SQL database, and an INSD open-data source
- Transformation and unification with Pandas
- Loading to MinIO as a small data lake
- Orchestration with Airflow
- Visualization with Streamlit

## Quick Start
Install dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Run MinIO only:

```bash
docker compose up -d minio
```

Run Airflow + MinIO (Docker):

```bash
docker compose up -d --build minio airflow
docker compose logs -f airflow
```

Airflow UI: `http://localhost:8080` (user/password: `admin` / `admin`)

Important: the Docker service name is `airflow` (not `apache`).

Run the full pipeline:

```bash
python main.py
```

Run the ETL with Airflow:
- Open the Airflow UI
- Trigger the DAG `etl_pipeline`

Run the dashboard:

```bash
streamlit run dashboard.py
```

## Documentation
Full project documentation is available in [docs/README.md](docs/README.md).
