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

Run the full pipeline:

```bash
python main.py
```

Run the dashboard:

```bash
streamlit run dashboard.py
```

## Documentation
Full project documentation is available in [docs/README.md](docs/README.md).
