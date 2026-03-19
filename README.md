# Data Engineer ETL Pipeline

End-to-end ETL project built with Python, Pandas, Apache Airflow, MinIO, and Streamlit.

The pipeline collects data from multiple heterogeneous sources, transforms them into a unified schema, stores processed outputs in a small data lake, and exposes the results through orchestration and visualization layers.

## Project Goals

- Extract data from several source types
- Clean and standardize the data with Pandas
- Load processed datasets into MinIO
- Orchestrate the workflow with Airflow
- Visualize the results with Streamlit

## Data Sources

The pipeline integrates four different sources:

1. A scraped website: `books.toscrape.com`
2. A local CSV source: `Books.csv`
3. A SQLite database: `storage/sample.db`
4. An INSD/ANPE open-data source

## Tech Stack

- Python 3.12+
- Pandas
- Requests
- Beautiful Soup
- Apache Airflow
- MinIO
- Streamlit
- Docker Compose

## Project Structure

```text
.
|-- extraction/         # Data extraction scripts
|-- transformation/     # Data cleaning and unification
|-- storage/            # Raw, processed, and loading logic
|-- dags/               # Airflow DAGs
|-- tests/              # Test suite
|-- config/             # Local configuration
|-- dashboard.py        # Streamlit dashboard
|-- main.py             # Local end-to-end execution
```

## ETL Workflow

1. Extract data from website, CSV, SQL, and INSD sources
2. Normalize and unify the datasets into a common schema
3. Partition processed outputs by class
4. Upload raw and processed files to the MinIO bucket `data-lake`

## Local Installation

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Run With Airflow

The orchestrated execution path for this project is Airflow.

Start the services:

```bash
docker compose up -d --build
```

Open:

- Airflow UI: `http://localhost:8080`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`

Credentials:

- Airflow: `admin` / `admin`
- MinIO: `minioadmin` / `minioadmin`

Then:

1. Open the Airflow UI
2. Activate the DAG `etl_pipeline` if needed
3. Trigger the DAG
4. Follow task logs from the Graph or Grid view

The Docker stack now runs a more robust Airflow setup for demo use:

- `postgres` for the Airflow metadata database
- `airflow-init` for database migration and admin user creation
- `airflow-webserver` for the UI
- `airflow-scheduler` for orchestration
- `minio` for object storage

## Run Locally

Run the full pipeline:

```bash
python main.py
```

If MinIO is not running locally, `python main.py` completes the ETL and skips only the final upload step with a warning.

Run the dashboard:

```bash
streamlit run dashboard.py
```

## Airflow Execution

Airflow loads the DAG from `dags/etl_pipeline.py`.

## Configuration Notes

- Local execution uses `config/.env`
- Docker execution overrides MinIO access with the Docker service hostname `minio`
- Airflow Docker execution uses `LocalExecutor` with PostgreSQL instead of SQLite
- If an external source is unavailable, some extractors fall back gracefully so the pipeline can still complete

## Tests

Run the test suite with:

```bash
pytest
```

## Known Constraints

- `Books.csv` may be stored as a Git LFS pointer instead of the full dataset
- Some external sources may block automated access or return `403 Forbidden`
- Network-restricted environments can prevent website scraping or INSD extraction

## Documentation

Detailed project documentation is available in [docs/README.md](docs/README.md).
