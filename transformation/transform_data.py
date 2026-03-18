import pandas as pd
import os
import logging
import hashlib
from urllib.parse import urlparse

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def normalize_image_url(url):
    if pd.isna(url) or not str(url).strip():
        return ""
    normalized = str(url).strip()
    if normalized.startswith("http://"):
        normalized = "https://" + normalized[len("http://"):]
    return normalized


def download_image(url, images_dir):
    normalized_url = normalize_image_url(url)
    if not normalized_url:
        return ""

    parsed = urlparse(normalized_url)
    _, extension = os.path.splitext(parsed.path)
    if not extension:
        extension = ".jpg"

    filename = f"{hashlib.sha256(normalized_url.encode('utf-8')).hexdigest()}{extension.lower()}"
    local_path = os.path.join(images_dir, filename)

    if os.path.exists(local_path):
        return local_path

    try:
        response = requests.get(normalized_url, timeout=15)
        response.raise_for_status()
        with open(local_path, "wb") as image_file:
            image_file.write(response.content)
        return local_path
    except requests.RequestException as exc:
        logger.warning("Failed to download image %s: %s", normalized_url, exc)
        return ""


def ensure_metric_columns(df, default_appreciation="Average"):
    metric_defaults = {
        'read_count': 0,
        'order_count': 0,
        'bought_count': 0,
        'appreciation': default_appreciation,
    }
    for column_name, default_value in metric_defaults.items():
        if column_name not in df.columns:
            df[column_name] = default_value
    return df


def build_insd_title(row):
    return f"ANPE offers - {row['region']} - {row['contract_type']} - {row['year']}"

def transform_data():
    try:
        raw_dir = os.path.join(os.path.dirname(__file__), '..', 'storage', 'raw')
        processed_dir = os.path.join(os.path.dirname(__file__), '..', 'storage', 'processed')
        images_dir = os.path.join(os.path.dirname(__file__), '..', 'storage', 'images')
        os.makedirs(processed_dir, exist_ok=True)
        os.makedirs(images_dir, exist_ok=True)

        # Unified schema columns: title, author, image_url, local_image_path, price, utility, class, source
        dfs = []

        # Process books from website
        books_path = os.path.join(raw_dir, 'books_website.csv')
        if os.path.exists(books_path):
            books_df = pd.read_csv(books_path)
            books_df = books_df[['title', 'price', 'rating', 'availability', 'image_url']].rename(columns={'availability': 'utility', 'rating': 'class'})
            books_df['author'] = None
            books_df['image_url'] = books_df['image_url'].apply(normalize_image_url)
            books_df['price'] = books_df['price'].str.replace(r'[^0-9.]', '', regex=True).astype(float)
            # books_df['local_image_path'] = books_df['image_url'].apply(lambda url: download_image(url, images_dir))
            books_df['local_image_path'] = ""
            books_df['appreciation'] = books_df['class']
            books_df = ensure_metric_columns(books_df)
            books_df['source'] = 'website'
            dfs.append(books_df)
            logger.info(f"Processed {len(books_df)} books from website.")

        # Process CSV data (Books.csv)
        csv_path = os.path.join(raw_dir, 'data.csv')
        if os.path.exists(csv_path):
            csv_df = pd.read_csv(csv_path)
            csv_df = csv_df[['Book-Title', 'Book-Author', 'Image-URL-L', 'Publisher']].rename(columns={'Book-Title': 'title', 'Book-Author': 'author', 'Image-URL-L': 'image_url', 'Publisher': 'utility'})
            csv_df['image_url'] = csv_df['image_url'].apply(normalize_image_url)
            # csv_df['local_image_path'] = csv_df['image_url'].apply(lambda url: download_image(url, images_dir))
            csv_df['local_image_path'] = ""
            csv_df['price'] = None
            csv_df['class'] = 'book'
            csv_df['appreciation'] = 'Average'
            csv_df = ensure_metric_columns(csv_df)
            csv_df['source'] = 'csv'
            dfs.append(csv_df)
            logger.info(f"Processed {len(csv_df)} records from CSV.")

        # Process SQL data
        sql_path = os.path.join(raw_dir, 'products_sql.csv')
        if os.path.exists(sql_path):
            sql_df = pd.read_csv(sql_path)
            sql_df = sql_df[['name', 'category', 'price', 'read_count', 'order_count', 'bought_count', 'appreciation']].rename(columns={'name': 'title', 'category': 'class'})
            sql_df['author'] = None
            sql_df['image_url'] = None
            sql_df['local_image_path'] = None
            sql_df['utility'] = None
            sql_df = ensure_metric_columns(sql_df)
            sql_df['source'] = 'sql'
            dfs.append(sql_df)
            logger.info(f"Processed {len(sql_df)} products from SQL.")

        # Process INSD ANPE data
        insd_path = os.path.join(raw_dir, 'insd_anpe.csv')
        if os.path.exists(insd_path):
            insd_df = pd.read_csv(insd_path)
            insd_df = insd_df[['year', 'region', 'contract_type', 'offers']].copy()
            insd_df['title'] = insd_df.apply(build_insd_title, axis=1)
            insd_df['author'] = 'INSD'
            insd_df['image_url'] = ''
            insd_df['local_image_path'] = ''
            insd_df['price'] = 0.0
            insd_df['utility'] = 'ANPE employment offers'
            insd_df['class'] = 'employment'
            insd_df['read_count'] = 0
            insd_df['order_count'] = 0
            insd_df['bought_count'] = 0
            insd_df['appreciation'] = 'Statistical'
            insd_df['source'] = 'insd_anpe'
            dfs.append(insd_df)
            logger.info(f"Processed {len(insd_df)} records from INSD ANPE.")

        # Union all DataFrames
        if dfs:
            unified_df = pd.concat(dfs, ignore_index=True)
            unified_df.fillna({
                'author': 'Unknown',
                'image_url': '',
                'local_image_path': '',
                'price': 0.0,
                'utility': 'N/A',
                'class': 'General',
                'read_count': 0,
                'order_count': 0,
                'bought_count': 0,
                'appreciation': 'Average',
                'year': '',
                'region': 'N/A',
                'contract_type': 'N/A',
                'offers': 0,
            }, inplace=True)
            # Organize: partition by class
            for cls in unified_df['class'].unique():
                cls_df = unified_df[unified_df['class'] == cls]
                cls_dir = os.path.join(processed_dir, f"class={cls}")
                os.makedirs(cls_dir, exist_ok=True)
                cls_df.to_csv(os.path.join(cls_dir, "part-00000.csv"), index=False)
            logger.info(f"Unified data saved with {len(unified_df)} records.")
        else:
            logger.warning("No data to process.")

        logger.info("Transformation completed successfully.")

    except Exception as e:
        logger.error(f"Error in transformation: {str(e)}")
        raise

if __name__ == "__main__":
    transform_data()
