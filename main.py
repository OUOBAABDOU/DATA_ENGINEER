#!/usr/bin/env python3
"""
Main script to execute the entire ETL project.
This script runs all extraction, transformation, and loading steps.
"""

import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.dirname(__file__))

from extraction.scrape_books import scrape_books
from extraction.download_csv import download_csv
from extraction.extract_insd_anpe import extract_insd_anpe
from extraction.extract_sql import extract_from_sql
from transformation.transform_data import transform_data
from storage.load_to_minio import load_to_minio

def main():
    print("Starting ETL Pipeline...")

    print("Step 1: Extracting books from website...")
    scrape_books()

    print("Step 2: Downloading CSV data...")
    download_csv()

    print("Step 3: Extracting data from SQL...")
    extract_from_sql()

    print("Step 4: Extracting INSD ANPE data...")
    extract_insd_anpe()

    print("Step 5: Transforming data...")
    transform_data()

    print("Step 6: Loading data to MinIO...")
    load_to_minio()

    print("ETL Pipeline completed successfully!")

if __name__ == "__main__":
    main()
