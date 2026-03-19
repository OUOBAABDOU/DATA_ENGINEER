import logging
import os
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    )
}


def fetch_page(session, page):
    candidate_urls = [
        f"https://books.toscrape.com/catalogue/page-{page}.html" if page > 1 else "https://books.toscrape.com/index.html",
        f"http://books.toscrape.com/catalogue/page-{page}.html" if page > 1 else "http://books.toscrape.com/index.html",
    ]

    last_error = None
    for url in candidate_urls:
        try:
            response = session.get(url, timeout=20)
            response.raise_for_status()
            return response, url
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("Failed to fetch %s: %s", url, exc)

    raise requests.RequestException(f"Unable to fetch page {page}") from last_error


def save_books(books):
    output_dir = os.path.join(os.path.dirname(__file__), "..", "storage", "raw")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "books_website.csv")
    pd.DataFrame(
        books,
        columns=["title", "price", "rating", "availability", "link", "image_url"],
    ).to_csv(output_path, index=False)
    return output_path


def scrape_books():
    books = []
    page = 1
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    while True:
        try:
            response, current_url = fetch_page(session, page)
        except requests.RequestException as exc:
            logger.error("Stopping scrape on page %s: %s", page, exc)
            break

        soup = BeautifulSoup(response.content, "html.parser")
        book_containers = soup.find_all("article", class_="product_pod")
        if not book_containers:
            break

        for book in book_containers:
            title = book.h3.a["title"]
            price = book.find("p", class_="price_color").text.strip()
            rating_element = book.find("p", class_="star-rating")
            rating = rating_element["class"][1] if rating_element and len(rating_element.get("class", [])) > 1 else "No rating"
            availability = book.find("p", class_="instock availability").text.strip()
            link = urljoin(current_url, book.h3.a["href"])
            image_tag = book.find("img")
            image_url = urljoin(current_url, image_tag["src"]) if image_tag and image_tag.get("src") else ""
            books.append({
                "title": title,
                "price": price,
                "rating": rating,
                "availability": availability,
                "link": link,
                "image_url": image_url,
            })

        logger.info("Scraped page %s, total books: %s", page, len(books))
        next_button = soup.select_one("li.next a")
        if not next_button:
            break
        page += 1

    save_books(books)
    logger.info("Total books scraped: %s", len(books))
    return books

if __name__ == "__main__":
    books_data = scrape_books()
    print(f"Scraped {len(books_data)} books.")
