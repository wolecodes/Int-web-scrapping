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
original_price_text = safe_find_text(".a-price.a-text-price")
        # current_price_text = safe_find_text(".a-price-whole")

        # # Extract rating and review count
        # rating_text = safe_find_rating(".a-icon-alt")  # Selector for Amazon star rating
        # review_count_text = safe_find_text(".a-size-base", default="0")  # Selector for review count

        # # Convert prices to float, if they exist, and then to decimal
        # original_price = float(original_price_text.replace('$', '').replace(',', '')) if original_price_text != "N/A" else None
        # current_price = float(current_price_text.replace('$', '').replace(',', '')) if current_price_text != "N/A" else None
        #  # Ensure price is not None
        # if current_price is None:
        #     current_price = 0
      

        # try:
        #     rating_value = float(rating_text.split()[0]) if rating_text != "N/A" else None
        # except (ValueError, IndexError):
        #     rating_value = None
        # try:
        #     review_count_value = int(review_count_text.replace(',', '')) if review_count_text != "N/A" else 0

        # except (ValueError, TypeError):
        #     review_count_value = 0
        # discount = None
        # if original_price is not None and current_price is not None:
        #     discount = original_price - current_price
        #     discount = int(discount)

        # amount_bought_raw = safe_find_text(".a-size-base.a-color-secondary")
    
        # amount_bought = amount_bought_raw.split('+')[0].strip() if '+' in amount_bought_raw else amount_bought_raw
        # try:
        #     amount_bought = int(''.join(filter(str.isdigit, amount_bought)))
        # except ValueError:
        #     amount_bought = 0 