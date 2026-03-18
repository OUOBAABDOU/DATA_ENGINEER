import shutil
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_csv():
    source_path = os.path.join(os.path.dirname(__file__), '..', 'Books.csv')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'storage', 'raw')
    os.makedirs(output_dir, exist_ok=True)
    dest_path = os.path.join(output_dir, 'data.csv')
    if os.path.exists(source_path):
        shutil.copy(source_path, dest_path)
        logger.info("Local CSV copied successfully.")
    else:
        logger.error("Local Books.csv not found.")
        raise FileNotFoundError("Books.csv not found")

if __name__ == "__main__":
    download_csv()
