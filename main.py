import time
import pandas as pd
import os
from typing import Dict, List
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import psycopg2
import logging
import re
import uuid

class Scraper:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")
        self.logger = logging.getLogger(__name__)

    def _setup_webdriver(self) -> webdriver.Chrome:
        """Configure Chrome webdriver with optimal settings."""
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        return driver

    def scrape_jumia_products(self, category_url: str, max_pages: int = 10) -> List[Dict]:
        products = []
        driver = self._setup_webdriver()

        try:
            for page in range(1, max_pages + 1):
                url = f"{category_url}?page={page}#catalog-listing"
                self.logger.info(f"Scraping URL: {url}")
                driver.get(url)
                driver.implicitly_wait(30)  # Allow time for dynamic content to load

                # Extract page source and parse it with Beautiful Soup
                soup = BeautifulSoup(driver.page_source, "html.parser")
                product_elements = soup.select("article.prd")  # Adjust selector if necessary
                self.logger.info(f"Found {len(product_elements)} products on page {page}")

                for product_element in product_elements:
                    try:
                        product_data = self._extract_product_details_bs(product_element)
                        products.append(product_data)
                    except Exception as e:
                        self.logger.warning(f"Error extracting product details: {e}")

                # Check if there are more pages available
                if not soup.select_one(f'a.pg[aria-label="Page {page + 1}"]'):
                    self.logger.info(f"No more pages after page {page}")
                    break

        except Exception as e:
            self.logger.error(f"Scraping failed: {e}")
        finally:
            driver.quit()

        return products

    def _extract_product_details_bs(self, product_element) -> Dict:
        """Extract product details using Beautiful Soup."""
        def safe_find_text_bs(element, selector, default="N/A"):
            target = element.select_one(selector)
            return target.text.strip() if target else default
        def extract_review_count(review_string):
            match = re.search(r'\((\d+)\)', review_string)
            return int(match.group(1)) if match else 0   
        def extract_product_rating(rating_string):
            match = re.search(r'(\d+\.\d+)', rating_string)  # Match a decimal number
            return match.group(1) if match else "N/A"

        reviews_string = safe_find_text_bs(product_element, ".info .rev")  # Assuming this is where the review string is located
        review_count = extract_review_count(reviews_string)
        rating_string = safe_find_text_bs(product_element, ".info .stars._s")  # Assuming this is where the rating string is located
        product_rating = extract_product_rating(rating_string)
        
        review_to_sale_ratio = 0.1
        estimated_amount_bought = review_count * (1 / review_to_sale_ratio)


        return {
            'product_name': safe_find_text_bs(product_element, ".info h3.name"),
            'product_price': safe_find_text_bs(product_element, ".info .prc"),
            'original_price': safe_find_text_bs(product_element, ".info .old"),
            'discount_percentage': safe_find_text_bs(product_element, ".info .s-prc-w .bdg._dsct._sm"),
            'product_rating': product_rating,
            'reviews_count': review_count,
            'estimated_amount_bought': estimated_amount_bought
        }

    def save_to_database(self, products: List[Dict]):
        """Save products to PostgreSQL with transaction management."""
        try:
            with psycopg2.connect(**self.db_config) as connection:
                with connection.cursor() as cursor:
                    insert_product_query = """INSERT INTO products (product_name, product_price, original_price,
                    discount_percentage, product_rating, reviews_count) VALUES (%s, %s, %s, %s, %s, %s)"""
                    
                    

                    for product in products:
                      if (product['product_name'] == "N/A" or
                          product['product_price'] == "N/A" or
                          product['reviews_count'] == 0):
                          self.logger.warning(f"Skipping invalid product: {product}")
                          continue  # Skip this product
                      
                      cursor.execute(insert_product_query, (
                          product['product_name'],
                          product['product_price'],
                          product['original_price'],
                          product['discount_percentage'],
                          product['product_rating'],
                          product['reviews_count'],
                        ))
                connection.commit()
                self.logger.info(f"Successfully saved {len(products)} products to database")
        except psycopg2.Error as e:
            self.logger.error(f"Database error: {e}")

    def save_to_csv(self, products: List[Dict], filename: str = "jumia_products.csv"):
        """Save products to CSV with error handling."""
        try:
            products_df = pd.DataFrame(products)
            products_df.to_csv(filename, index=False)
            self.logger.info(f"Products saved to CSV file: {filename}")
        except Exception as e:
            self.logger.error(f"CSV export failed: {e}")

def main():
    # Load environment variables for database configuration
    load_dotenv()
    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME", "amazon_scrapping"),
        "user": os.getenv("DB_USER", "amazon_scrapping_user"),
        "password": os.getenv("DB_PASSWORD", "your_password"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
    }

    category_url = "https://www.jumia.com.ng/laptops/"  # Change this to your desired category URL
    scraper = Scraper(DB_CONFIG)

    products = scraper.scrape_jumia_products(category_url)
    scraper.save_to_database(products)
    scraper.save_to_csv(products)

if __name__ == "__main__":
    main()