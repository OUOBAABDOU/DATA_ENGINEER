import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
import logging
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape_books():
    base_url = "https://books.toscrape.com/"
    books = []
    page = 1
    while True:
        url = f"{base_url}catalogue/page-{page}.html" if page > 1 else base_url + "index.html"
        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Failed to fetch page {page}")
            break
        soup = BeautifulSoup(response.content, 'html.parser')
        book_containers = soup.find_all('article', class_='product_pod')
        if not book_containers:
            break
        for book in book_containers:
            title = book.h3.a['title']
            price = book.find('p', class_='price_color').text.strip()
            rating_element = book.find('p', class_='star-rating')
            rating = rating_element['class'][1] if rating_element else 'No rating'
            availability = book.find('p', class_='instock availability').text.strip()
            link = base_url + "catalogue/" + book.h3.a['href']
            image_tag = book.find('img')
            image_url = urljoin(base_url, image_tag['src']) if image_tag and image_tag.get('src') else ''
            books.append({
                'title': title,
                'price': price,
                'rating': rating,
                'availability': availability,
                'link': link,
                'image_url': image_url
            })
        page += 1
        logger.info(f"Scraped page {page-1}, total books: {len(books)}")
    logger.info(f"Total books scraped: {len(books)}")
    return books

if __name__ == "__main__":
    books_data = scrape_books()
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'storage', 'raw')
    os.makedirs(output_dir, exist_ok=True)
    df = pd.DataFrame(books_data)
    df.to_csv(os.path.join(output_dir, 'books_website.csv'), index=False)
    print(f"Scraped {len(books_data)} books.")
