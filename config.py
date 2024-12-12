import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database Configuration
DATABASE_CONFIG = {
    "dbname": os.getenv("DB_NAME", "amazon_scraping"),
    "user": os.getenv("DB_USER", "amazon_scraper"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

# Scraping Configuration
SCRAPING_CONFIG = {
    "max_pages": int(os.getenv("MAX_PAGES", 5)),
    "category_url": os.getenv("CATEGORY_URL", "https://www.amazon.com/s?k=asus+rog+strix+g16+2024+gaming+laptop")
}