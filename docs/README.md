# ETL Pipeline Project

This project implements an end-to-end ETL pipeline as per the specifications.

## Overview
- **Extraction**: Data from 4 different sources: a scraped website, a local CSV file, a SQL database, and an INSD open-data source.
- **Transformation**: Data cleaning, harmonization, and enrichment with Pandas.
- **Loading**: Storage in MinIO as a data lake with partitioned processed files.
- **Orchestration**: Automated scheduling and task chaining with Apache Airflow.

## Architecture
- Extraction scripts in `extraction/`
- Pandas transformation in `transformation/`
- Airflow DAG in `dags/`
- MinIO loading in `storage/`
- Tests in `tests/`
- Config in `config/`

## Data Sources
The pipeline integrates four heterogeneous data sources:

1. `Website scraping`: book data extracted from `https://books.toscrape.com/`
2. `CSV file`: records loaded from the local `Books.csv` file
3. `SQL database`: product data extracted from the SQLite database `storage/sample.db`
4. `INSD open data`: ANPE job-offer statistics from `https://burkinafaso.opendataforafrica.org/hejwdte/evolution-des-offres-d-emploi-de-l-anpe-par-r%C3%A9gion-et-type-de-contrat`

These four sources are unified into a common schema before being stored.

## Processing Workflow
1. Extract raw data from the website, CSV file, SQL database, and INSD source
2. Clean and standardize the fields with Pandas
3. Enrich the dataset with derived columns and image download status
4. Partition processed data by class in `storage/processed/`
5. Upload processed files to the MinIO bucket `data-lake`

## Prerequisites
- Python 3.12+
- Docker
- Docker Compose

## Python Dependencies
Install the project dependencies locally with:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

The `requirements.txt` file includes the Python libraries used by the project scripts:
- `requests`
- `beautifulsoup4`
- `pandas`
- `boto3`
- `python-dotenv`
- `streamlit`
- `matplotlib`
- `pytest`

## Docker Services
Start MinIO only:

```bash
docker compose up -d minio
```

Start the full Docker stack (`minio` + `airflow`):

```bash
docker compose up -d --build
```

Access:
- Airflow UI: `http://localhost:8080` (`admin` / `admin`)
- MinIO console: `http://localhost:9001` (`minioadmin` / `minioadmin`)

## Full Execution
Run the complete ETL pipeline locally:

```bash
python main.py
```

This executes:
1. Website scraping
2. Local CSV copy
3. SQL extraction
4. INSD extraction
5. Pandas transformation
6. Upload to MinIO

## Partial Execution
Run only one step if needed:

```bash
python extraction/scrape_books.py
python extraction/download_csv.py
python extraction/extract_sql.py
python extraction/extract_insd_anpe.py
python transformation/transform_data.py
python storage/load_to_minio.py
```

## Dashboard Execution
Run the Streamlit dashboard:

```bash
streamlit run dashboard.py
```

Open `http://localhost:8501`.

## Airflow Execution
After starting Docker, trigger the `etl_pipeline` DAG from the Airflow UI or let the scheduler run it according to the DAG configuration.

## Reset Behavior for `logs/` and `db/`
The Airflow service mounts the local `logs/` and `db/` folders directly from the project.
These folders are cleared at each container start, then recreated by Airflow during initialization.

## INSD Configuration
The INSD source can be configured from `docker-compose.yml` without changing the code:

- `INSD_ANPE_CSV_URL`: direct CSV export URL if available
- `INSD_ANPE_JSON_URL`: direct JSON/API URL if available
- `INSD_ANPE_SOURCE_URL`: dataset page URL used as a fallback

The extractor tries the sources in this order: `CSV`, then `JSON`, then `SOURCE_URL`.
If the dataset portal page is not directly parseable, set a direct `CSV` or `JSON` URL.

## Monitoring
Logs are available in Airflow and application logs.

## Tests
Run tests with:
```bash
pytest
```

## Validation
- Extracts from 4 sources
- Uses Pandas as the data processing tool
- Uses Apache Airflow as the orchestrator
- Uses MinIO as the data lake storage layer
- Handles errors and retries
- Partitioned storage
- Automated orchestration
