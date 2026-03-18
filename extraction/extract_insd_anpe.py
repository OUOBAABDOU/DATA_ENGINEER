import io
import logging
import os
import re
import unicodedata

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_SOURCE_URL = (
    "https://burkinafaso.opendataforafrica.org/hejwdte/"
    "evolution-des-offres-d-emploi-de-l-anpe-par-r%C3%A9gion-et-type-de-contrat"
)


def normalize_column_name(name):
    normalized = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    return normalized


def rename_first_matching_column(df, candidates, target):
    for candidate in candidates:
        if candidate in df.columns:
            return df.rename(columns={candidate: target})
    return df


def standardize_insd_dataframe(df):
    df = df.copy()
    df.columns = [normalize_column_name(column) for column in df.columns]

    df = rename_first_matching_column(
        df,
        ["annee", "year", "time", "periode"],
        "year",
    )
    df = rename_first_matching_column(
        df,
        ["region", "regions", "localite", "zone"],
        "region",
    )
    df = rename_first_matching_column(
        df,
        ["type_de_contrat", "type_contrat", "contrat", "contract_type"],
        "contract_type",
    )
    df = rename_first_matching_column(
        df,
        ["offres", "offre", "nombre_offres", "value", "valeur"],
        "offers",
    )

    if "offers" not in df.columns:
        numeric_candidates = [column for column in df.columns if pd.api.types.is_numeric_dtype(df[column])]
        if numeric_candidates:
            df = df.rename(columns={numeric_candidates[-1]: "offers"})

    if "year" not in df.columns and len(df.columns) >= 1:
        df = df.rename(columns={df.columns[0]: "year"})
    if "region" not in df.columns and len(df.columns) >= 2:
        df = df.rename(columns={df.columns[1]: "region"})
    if "contract_type" not in df.columns and len(df.columns) >= 3:
        df = df.rename(columns={df.columns[2]: "contract_type"})

    required_columns = ["year", "region", "contract_type", "offers"]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Unable to map INSD columns: missing {missing_columns}")

    standardized = df[required_columns].copy()
    standardized["year"] = standardized["year"].astype(str).str.strip()
    standardized["region"] = standardized["region"].astype(str).str.strip()
    standardized["contract_type"] = standardized["contract_type"].astype(str).str.strip()
    standardized["offers"] = pd.to_numeric(standardized["offers"], errors="coerce").fillna(0).astype(int)
    standardized = standardized[
        (standardized["year"] != "")
        & (standardized["region"] != "")
        & (standardized["contract_type"] != "")
    ]
    standardized = standardized.drop_duplicates().reset_index(drop=True)
    return standardized


def fetch_candidate_dataframe(url, timeout=30):
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "").lower()

    if "json" in content_type:
        payload = response.json()
        if isinstance(payload, list):
            return pd.DataFrame(payload)
        if isinstance(payload, dict):
            for key in ("data", "items", "value", "values", "dataset"):
                if key in payload:
                    data = payload[key]
                    if isinstance(data, list):
                        return pd.DataFrame(data)
            return pd.json_normalize(payload)

    if "csv" in content_type or url.lower().endswith(".csv"):
        return pd.read_csv(io.StringIO(response.text))

    html_tables = pd.read_html(io.StringIO(response.text))
    if html_tables:
        return html_tables[0]

    raise ValueError(f"No supported tabular data found at {url}")


def extract_insd_anpe():
    output_dir = os.path.join(os.path.dirname(__file__), "..", "storage", "raw")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "insd_anpe.csv")

    candidate_urls = [
        os.getenv("INSD_ANPE_CSV_URL", "").strip(),
        os.getenv("INSD_ANPE_JSON_URL", "").strip(),
        os.getenv("INSD_ANPE_SOURCE_URL", "").strip(),
        DEFAULT_SOURCE_URL,
    ]
    candidate_urls = [url for url in candidate_urls if url]

    last_error = None
    for candidate_url in candidate_urls:
        try:
            logger.info("Trying INSD source: %s", candidate_url)
            df = fetch_candidate_dataframe(candidate_url)
            standardized_df = standardize_insd_dataframe(df)
            standardized_df.to_csv(output_path, index=False)
            logger.info("Extracted %s INSD ANPE records.", len(standardized_df))
            return standardized_df
        except Exception as exc:
            last_error = exc
            logger.warning("INSD extraction failed for %s: %s", candidate_url, exc)

    logger.warning("Skipping INSD source because no candidate URL returned usable tabular data.")
    if last_error:
        logger.warning("Last INSD extraction error: %s", last_error)
    return pd.DataFrame(columns=["year", "region", "contract_type", "offers"])


if __name__ == "__main__":
    extract_insd_anpe()
