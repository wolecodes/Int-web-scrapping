import os
import time
import logging
import pandas as pd
import psycopg2
from dotenv import load_dotenv
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
                return product_element.find_element(By.CSS_SELECTOR, selector).text.strip()
            except:
                return default

        def safe_find_rating(selector, default="N/A"):
            try:
                return WebDriverWait(product_element, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                ).get_attribute("innerText")  # Use .get_attribute("innerText")
            except:
                return default
         # Extract original price and current price
        original_price_text = safe_find_text(".a-price.a-text-price")
        current_price_text = safe_find_text(".a-price-whole")

        # Extract rating and review count
        rating_text = safe_find_rating(".a-icon-alt")  # Selector for Amazon star rating
        review_count_text = safe_find_text(".a-size-base", default="0")  # Selector for review count

        # Convert prices to float, if they exist, and then to decimal
        original_price = float(original_price_text.replace('$', '').replace(',', '')) if original_price_text != "N/A" else None
        current_price = float(current_price_text.replace('$', '').replace(',', '')) if current_price_text != "N/A" else None
         # Ensure price is not None
        if current_price is None:
            current_price = 0
      

        try:
            rating_value = float(rating_text.split()[0]) if rating_text != "N/A" else None
        except (ValueError, IndexError):
            rating_value = None
        try:
            review_count_value = int(review_count_text.replace(',', '')) if review_count_text != "N/A" else 0

        except (ValueError, TypeError):
            review_count_value = 0
        discount = None
        if original_price is not None and current_price is not None:
            discount = original_price - current_price
            discount = int(discount)

        amount_bought_raw = safe_find_text(".a-size-base.a-color-secondary")
    
        amount_bought = amount_bought_raw.split('+')[0].strip() if '+' in amount_bought_raw else amount_bought_raw
        try:
            amount_bought = int(''.join(filter(str.isdigit, amount_bought)))
        except ValueError:
            amount_bought = 0 

        return {
            "product_name": safe_find_text("h2[aria-label] > span"),
            "price": current_price,
            "rating":  rating_value,
            "discount": discount,
            "review_count": review_count_value,
            "amount_bought": amount_bought,
        }


    def save_to_database(self, products: List[Dict]):
        """Save products to PostgreSQL with transaction management."""
        try:
            with psycopg2.connect(**self.db_config) as connection:
                with connection.cursor() as cursor:
                    insert_product_query = """
                    INSERT INTO products (Product_Name, Price, Discount)
                    VALUES (%s, %s, %s) RETURNING Product_Id
                    """
                     # Insert reviews
                    insert_review_query = """
                    INSERT INTO Reviews (Product_Id, Rating, Review_Count)
                    VALUES (%s, %s, %s)
                    """
                    # Insert sales
                    insert_sales_query = """
                    INSERT INTO Sales (Product_Id, Amount_Bought)
                    VALUES (%s, %s)
                    """

                    for product in products:
                        cursor.execute(
                            insert_product_query,
                            (
                                product["product_name"],
                                product.get("price", 0),
                                product.get("discount", 0),
                            ),
                        )
                        product_id = cursor.fetchone()[0]
                        # Insert Review
                        cursor.execute(
                            insert_review_query,
                            (
                                product_id,
                                product.get("rating", None),
                                product.get("review_count", 0),
                            ),
                        )
                                                # Insert Review
                        cursor.execute(
                            insert_review_query,
                            (
                                product_id,
                                product.get("rating", None),
                                product.get("review_count", 0),
                            ),
                        )
                        # Insert Sales
                        cursor.execute(
                            insert_sales_query,
                            (
                                product_id,
                                product.get("amount_bought", 0),
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
            products_df = pd.DataFrame([
                {
                    "Product_Id": None,
                    "Product_Name": p["product_name"],
                    "Price": p.get("price", 0),
                    "Discount": p.get("discount", 0)
                } for p in products
            ])
            products_df.to_csv("products.csv", index= False)

            reviews_df = pd.DataFrame([
                {
                    "Product_Id": None,  # Will be populated after database insertion
                    "Rating": p.get("rating", None),
                    "Review_Count": p.get("review_count", 0)
                } for p in products
            ])
            reviews_df.to_csv("reviews.csv", index=False)

            sales_df = pd.DataFrame([
                {
                    "Product_Id": None,  # Will be populated after database insertion
                    "Amount_Bought": p.get("amount_bought", 0),
                } for p in products
            ])
            sales_df.to_csv("sales.csv", index=False)

        except Exception as e:
            self.logger.error(f"CSV export failed: {e}")

def main():
    # Database configuration - consider using environment variables
    load_dotenv()
    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME", "amazon_scrapping"),
        "user": os.getenv("DB_USER", "amazon_scrapping_user"),
        "password": os.getenv("DB_PASSWORD", "your_password"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
    }

    category_url = "https://www.amazon.com/s?k=asus+rog+strix+g16+2024+gaming+laptop"
    scraper = AmazonScraper(DB_CONFIG)

    products = scraper.scrape_amazon_products(category_url)
    scraper.save_to_database(products)
    scraper.save_to_csv(products)


if __name__ == "__main__":
    main()
