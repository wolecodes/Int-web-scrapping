import os
import time
import logging
import pandas as pd
import psycopg2
from typing import List, Dict
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


class AmazonScraper:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def _setup_webdriver(self) -> webdriver.Chrome:
        """Configure Chrome webdriver with optimal settings."""
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        # Set page load timeout
        driver.set_page_load_timeout(30)
        return driver

    def scrape_amazon_products(
        self, category_url: str, max_pages: int = 5
    ) -> List[Dict]:
        """
        Scrape Amazon product details with error handling and retry mechanism.

        Args:
            category_url (str): Amazon category search URL
            max_pages (int): Maximum number of pages to scrape

        Returns:
            List[Dict]: Scraped product information
        """
        products = []
        driver = self._setup_webdriver()

        try:
            for page in range(1, max_pages + 1):
                url = f"{category_url}&page={page}"
                driver.get(url)

                # Wait for product elements with longer timeout
                product_elements = WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "div[data-component-type='s-search-result']")
                    )
                )

                for product in product_elements:
                    try:
                        product_data = self._extract_product_details(product)
                        products.append(product_data)
                    except Exception as e:
                        self.logger.warning(f"Error extracting product details: {e}")

        except Exception as e:
            self.logger.error(f"Scraping failed: {e}")
        finally:
            driver.quit()

        return products

    def _extract_product_details(self, product_element) -> Dict:
        """Extract detailed product information with robust error handling."""

        def safe_find_text(selector, default="N/A"):
            try:
                return product_element.find_element(
                    By.CSS_SELECTOR, selector
                ).text.strip()
            except:
                return default

        return {
            "title": safe_find_text("h2 > a > span"),
            "price": safe_find_text(".a-price-whole"),
            "discount": safe_find_text(".a-price.a-price-secondary"),
            "rating": safe_find_text(".a-icon-alt"),
            "review_count": safe_find_text(".a-size-base"),
            "amount_bought": "N/A",
        }

    def save_to_database(self, products: List[Dict], category: str):
        """Save products to PostgreSQL with transaction management."""
        try:
            with psycopg2.connect(**self.db_config) as connection:
                with connection.cursor() as cursor:
                    insert_query = """
                    INSERT INTO products (name, price, discount, rating, review_count, amount_bought, category)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """

                    for product in products:
                        cursor.execute(
                            insert_query,
                            (
                                product["title"],
                                product.get("price", "N/A"),
                                product.get("discount", "N/A"),
                                product.get("rating", "N/A"),
                                product.get("review_count", "N/A"),
                                product.get("amount_bought", "N/A"),
                                category,
                            ),
                        )
                connection.commit()
                self.logger.info(
                    f"Successfully saved {len(products)} products to database"
                )
        except psycopg2.Error as e:
            self.logger.error(f"Database error: {e}")

    def save_to_csv(self, products: List[Dict], filename: str = "amazon_products.csv"):
        """Save products to CSV with error handling."""
        try:
            df = pd.DataFrame(products)
            df.to_csv(filename, index=False)
            self.logger.info(f"Saved {len(products)} products to {filename}")
        except Exception as e:
            self.logger.error(f"CSV export failed: {e}")


def main():
    # Database configuration - consider using environment variables
    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME", "amazon_scrapping"),
        "user": os.getenv("DB_USER", "amazon_scrapping_user"),
        "password": os.getenv("DB_PASSWORD", "your_password"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
    }

    category_url = "https://www.amazon.com/s?k=laptops"
    scraper = AmazonScraper(DB_CONFIG)

    products = scraper.scrape_amazon_products(category_url)
    scraper.save_to_database(products, category="adidas")
    scraper.save_to_csv(products)


if __name__ == "__main__":
    main()
