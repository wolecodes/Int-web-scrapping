import time
import pandas as pd
import psycopg2
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Database connection parameters
DB_NAME = "PostgreSQL 17"
DB_USER = "postgres"
DB_PASSWORD = "wolecode"
DB_HOST = "localhost"
DB_PORT = "5432"

# Function to connect to PostgreSQL
def connect_to_db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# Function to scrape Amazon product data
def scrape_amazon_products(category_url):
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=chrome_options)

    products = []

    for i in range(1, 6):  # Scraping first 5 pages
        url = f"{category_url}&page={i}"
        driver.get(url)

        # Wait for the product elements to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "h2 > a > span"))
        )

        # Extract product details
        product_elements = driver.find_elements(By.CSS_SELECTOR, "div[data-component-type='s-search-result']")
        for product in product_elements:
            title = product.h2.text.strip()
            price = product.find_element(By.CSS_SELECTOR, ".a-price-whole").text.strip() if product.find_elements(By.CSS_SELECTOR, ".a-price-whole") else "N/A"
            discount = product.find_element(By.CSS_SELECTOR, ".a-price.a-price-secondary").text.strip() if product.find_elements(By.CSS_SELECTOR, ".a-price.a-price-secondary") else "N/A"
            rating = product.find_element(By.CSS_SELECTOR, ".a-icon-alt").text.strip() if product.find_elements(By.CSS_SELECTOR, ".a-icon-alt") else "N/A"
            review_count = product.find_element(By.CSS_SELECTOR, ".a-size-base").text.strip() if product.find_elements(By.CSS_SELECTOR, ".a-size-base") else "N/A"
            amount_bought = "N/A"  # Placeholder, as this data may not be directly available

            products.append({
                'title': title,
                'price': price,
                'discount': discount,
                'rating': rating,
                'review_count': review_count,
                'amount_bought': amount_bought
            })

    driver.quit()
    return products

# Function to save products to PostgreSQL
def save_to_db(products):
    connection = connect_to_db()
    cursor = connection.cursor()

    insert_query = """
        INSERT INTO Products (name, price, discount, rating, review_count, amount_bought, category)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for product in products:
        cursor.execute(insert_query, (
            product['title'],
            product['price'],
            product['discount'],
            product['rating'],
            product['review_count'],
            product['amount_bought'],
            'adidas'  # Replace with the actual category if needed
        ))

    connection.commit()
    cursor.close()
    connection.close()

# Function to save products to CSV
def save_to_csv(products):
    df = pd.DataFrame(products)
    df.to_csv('amazon_products.csv', index=False)

# Main execution
if __name__ == "__main__":
    category_url = "https://www.amazon.com/s?k=adidas"  # Replace with your specific category URL
    products = scrape_amazon_products(category_url)
    save_to_db(products)
    save_to_csv(products)
    print("Data scraping and saving completed.")