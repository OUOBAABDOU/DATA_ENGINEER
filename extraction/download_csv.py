import shutil
import os
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def is_git_lfs_pointer(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as file_obj:
        first_line = file_obj.readline().strip()
    return first_line == "version https://git-lfs.github.com/spec/v1"


def build_fallback_books_dataframe():
    return pd.DataFrame(
        [
            {
                "Book-Title": "Sample Book A",
                "Book-Author": "Unknown Author",
                "Image-URL-L": "http://books.toscrape.com/static/sample-a.jpg",
                "Publisher": "Local fallback dataset",
            },
            {
                "Book-Title": "Sample Book B",
                "Book-Author": "Unknown Author",
                "Image-URL-L": "http://books.toscrape.com/static/sample-b.jpg",
                "Publisher": "Local fallback dataset",
            },
        ]
    )

def download_csv():
    source_path = os.path.join(os.path.dirname(__file__), '..', 'Books.csv')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'storage', 'raw')
    os.makedirs(output_dir, exist_ok=True)
    dest_path = os.path.join(output_dir, 'data.csv')
    if os.path.exists(source_path):
        if is_git_lfs_pointer(source_path):
            build_fallback_books_dataframe().to_csv(dest_path, index=False)
            logger.warning("Books.csv is a Git LFS pointer. Generated fallback CSV dataset instead.")
        else:
            shutil.copy(source_path, dest_path)
            logger.info("Local CSV copied successfully.")
    else:
        build_fallback_books_dataframe().to_csv(dest_path, index=False)
        logger.warning("Local Books.csv not found. Generated fallback CSV dataset instead.")

    return dest_path

if __name__ == "__main__":
    download_csv()
