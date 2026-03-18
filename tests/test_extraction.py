import pytest
import os
import pandas as pd
from unittest.mock import patch, MagicMock
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from extraction.scrape_books import scrape_books
from extraction.download_csv import download_csv
from extraction.extract_insd_anpe import extract_insd_anpe
from extraction.extract_sql import extract_from_sql

def test_scrape_books():
    # Mock requests
    with patch('extraction.scrape_books.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'<html><body><article class="product_pod"><h3><a title="Test Book" href="/catalogue/test-book">Test Book</a></h3><p class="price_color">&pound;10.99</p><p class="instock availability">In stock</p><p class="star-rating One"></p></article></body></html>'
        mock_get.return_value = mock_response
        books = scrape_books()
        assert len(books) == 1
        assert books[0]['title'] == 'Test Book'

def test_download_csv():
    # Mock requests
    download_csv()
    # Check if file exists
    path = os.path.join(os.path.dirname(__file__), '..', 'storage', 'raw', 'data.csv')
    assert os.path.exists(path)
    # Clean up
    os.remove(path)

def test_extract_sql():
    extract_from_sql()
    path = os.path.join(os.path.dirname(__file__), '..', 'storage', 'raw', 'products_sql.csv')
    assert os.path.exists(path)
    df = pd.read_csv(path)
    assert len(df) > 0

def test_extract_insd_anpe():
    csv_payload = "year,region,contract_type,offers\n2023,Centre,CDD,125\n2023,Hauts-Bassins,CDI,78\n"
    with patch('extraction.extract_insd_anpe.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.headers = {'Content-Type': 'text/csv'}
        mock_response.text = csv_payload
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = extract_insd_anpe()

    assert len(df) == 2
    path = os.path.join(os.path.dirname(__file__), '..', 'storage', 'raw', 'insd_anpe.csv')
    assert os.path.exists(path)
    extracted_df = pd.read_csv(path)
    assert list(extracted_df.columns) == ['year', 'region', 'contract_type', 'offers']
