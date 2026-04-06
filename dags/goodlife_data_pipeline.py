# dags/onlinestoregl_full_pipeline.py

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import time
import json
import re

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# =========================
# CONFIG
# =========================
BASE_URL = "https://www.goodlife.co.ke/shop/"
SF_DB = "HOSPITALS"
SF_SHARED_SCHEMA = "SHARED"
SNOWFLAKE_STAGE = f"{SF_DB}.{SF_SHARED_SCHEMA}.DB_BUCKET"
SNOWFLAKE_TABLE = "onlinestoregl_products_raw"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

# =========================
# HELPER FUNCTIONS
# =========================

def clean_price(text):
    if not text:
        return None
    nums = re.findall(r'[\d,]+\.?\d*', text)
    return float(nums[0].replace(',', '')) if nums else None


def extract_data(page_source):
    soup = BeautifulSoup(page_source, 'html.parser')
    products = []

    product_cards = soup.select('ul.products li.product')
    print(f"Found {len(product_cards)} products")

    for card in product_cards:
        try:
            # Product name
            name_el = card.select_one(".woocommerce-loop-product__title")
            name = name_el.get_text(strip=True) if name_el else None

            # Product permalink — first <a> in the card wraps the image/title
            link_el = card.select_one("a")
            product_url = link_el['href'] if link_el else None

            # Image — WP Smush serves final URL directly in src
            img_el = card.select_one("img.attachment-woocommerce_thumbnail")
            if img_el is None:
                img_el = card.select_one("img")
            img_url = img_el.get("src") if img_el else None

            # Product ID and SKU from the add-to-cart button data attributes
            cart_btn = card.select_one("a.add_to_cart_button")
            product_id = cart_btn.get("data-product_id") if cart_btn else None
            product_sku = cart_btn.get("data-product_sku") if cart_btn else None

            # Discount badge (e.g. "Sale!", "-20%")
            badge_el = card.select_one(".onsale")
            discount_badge = badge_el.get_text(strip=True) if badge_el else None

            # Prices
            current_price = None
            original_price = None

            price_container = card.select_one(".price")
            if price_container:
                ins = price_container.select_one("ins .woocommerce-Price-amount bdi")
                de = price_container.select_one("del .woocommerce-Price-amount bdi")
                if ins:
                    current_price = clean_price(ins.get_text())
                    original_price = clean_price(de.get_text()) if de else None
                else:
                    # No sale — grab the single visible price amount
                    single = price_container.select_one(".woocommerce-Price-amount bdi")
                    current_price = clean_price(single.get_text()) if single else clean_price(price_container.get_text())

            if current_price and original_price:
                discount_pct = round((original_price - current_price) / original_price * 100, 2)
            else:
                discount_pct = None

            product = {
                'scrape_date': datetime.now().isoformat(),
                'product_id': product_id,
                'product_sku': product_sku,
                'product_name': name,
                'current_price': current_price,
                'original_price': original_price,
                'discount_percentage': discount_pct,
                'discount_badge': discount_badge,
                'rating': None,
                'reviews': None,
                'url': product_url,
                'image_url': img_url,
                'full_html_snippet': str(card)[:500]
            }

            if name and current_price:
                products.append(product)

        except Exception as e:
            print(f"Error parsing product: {e}")
            continue

    return products


def has_next_page(page_source):
    """Return True if a WooCommerce 'next page' link exists in the HTML."""
    soup = BeautifulSoup(page_source, 'html.parser')
    return bool(soup.select_one("a.next.page-numbers"))


# =========================
# MAIN SCRAPER
# =========================

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
window.chrome = {runtime: {}};
const origQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (p) =>
    p.name === 'notifications'
        ? Promise.resolve({state: Notification.permission})
        : origQuery(p);
"""


def scrape_onlinestoregl():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0")
    options.binary_location = "/usr/bin/google-chrome"

    service = Service("/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 30)

    all_data = []

    def dismiss_cookie_banner():
        try:
            btn = WebDriverWait(driver, 6).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.wcc-btn-accept"))
            )
            btn.click()
            print("Cookie banner dismissed")
        except Exception:
            pass

    time.sleep(5)
    print(f"Starting scrape: {BASE_URL}")
    driver.get(BASE_URL)
    all_data = extract_data(driver.page_source)
    print(f"\nTOTAL PRODUCTS: {len(all_data)}")


    if all_data:
        df = pd.DataFrame(all_data).drop_duplicates(subset=['product_name'])

        print("\nPreview:")
        print(df[['product_name', 'current_price', 'original_price']].head())

        ts = int(time.time())
        parquet_path = f"/tmp/onlinestoregl_products_{ts}.parquet"
        df.to_parquet(parquet_path, index=False)

        return parquet_path

    return None


# =========================
# SNOWFLAKE UPLOAD
# =========================

@task
def upload_to_snowflake(file_path):
    if not file_path:
        print("No file to upload")
        return None

    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

    print(f"Uploading {file_path} to {SNOWFLAKE_STAGE}")
    hook.copy_file_to_stage(
        file_path,
        SNOWFLAKE_STAGE,
        file_name=f"onlinestoregl_{int(time.time())}.parquet"
    )

    merge_sql = f"""
    MERGE INTO {SF_DB}.{SF_SHARED_SCHEMA}.{SNOWFLAKE_TABLE} AS target
    USING (
        SELECT
            $1:scrape_date::timestamp      as scrape_date,
            $1:product_id::string          as product_id,
            $1:product_sku::string         as product_sku,
            $1:product_name::string        as product_name,
            $1:current_price::float        as current_price,
            $1:original_price::float       as original_price,
            $1:discount_percentage::float  as discount_percentage,
            $1:discount_badge::string      as discount_badge,
            $1:rating::string              as rating,
            $1:reviews::string             as reviews,
            $1:url::string                 as url,
            $1:image_url::string           as image_url
        FROM @{SNOWFLAKE_STAGE}/onlinestoregl_*.parquet
    ) AS source
    ON target.product_name = source.product_name
       AND DATE_TRUNC('day', target.scrape_date) = DATE_TRUNC('day', source.scrape_date)
    WHEN MATCHED THEN UPDATE SET
        current_price       = source.current_price,
        original_price      = source.original_price,
        discount_percentage = source.discount_percentage,
        discount_badge      = source.discount_badge,
        image_url           = source.image_url
    WHEN NOT MATCHED THEN INSERT
        (scrape_date, product_id, product_sku, product_name, current_price, original_price,
         discount_percentage, discount_badge, rating, reviews, url, image_url)
    VALUES
        (source.scrape_date, source.product_id, source.product_sku, source.product_name,
         source.current_price, source.original_price, source.discount_percentage,
         source.discount_badge, source.rating, source.reviews, source.url, source.image_url);
    """

    print("Executing MERGE...")
    hook.run(merge_sql)

    print("✅ Upload complete!")
    return "Success"


# =========================
# DAG
# =========================

with DAG(
    dag_id="onlinestoregl_full_pipeline",
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["scraper", "goodlife", "snowflake"],
) as dag:

    scrape_task = task(scrape_onlinestoregl)()
    upload_task = upload_to_snowflake(scrape_task)
