# dags/onlinestorebc_full_pipeline.py

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
BASE_URL = "https://beautyclick.co.ke/shop/"
SF_DB = "HOSPITALS"
SF_SHARED_SCHEMA = "SHARED"
SNOWFLAKE_STAGE = f"{SF_DB}.{SF_SHARED_SCHEMA}.DB_BUCKET"
SNOWFLAKE_TABLE = "onlinestorebc_products_raw"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

# =========================
# HELPERS
# =========================

def clean_price(text):
    if not text:
        return None
    nums = re.findall(r'[\d,]+\.?\d*', text)
    return float(nums[0].replace(',', '')) if nums else None


def extract_data(page_source):
    soup = BeautifulSoup(page_source, 'html.parser')
    products = []

    # 🔥 BeautyClick product cards
    product_cards = soup.select("li.product")
    print(f"Found {len(product_cards)} products")

    for card in product_cards:
        try:
            # Name
            name_el = card.select_one("h2.woocommerce-loop-product__title")
            name = name_el.get_text(strip=True) if name_el else None

            # URL
            link_el = card.select_one("a")
            product_url = link_el['href'] if link_el else None

            # Image
            img_el = card.select_one("img")
            img_url = img_el['src'] if img_el else None

            # PRICE LOGIC
            current_price = None
            original_price = None

            price_container = card.select_one(".price")

            if price_container:
                # Discounted case
                ins = price_container.select_one("ins")
                if ins:
                    current_price = clean_price(ins.get_text())

                    de = price_container.select_one("del")
                    if de:
                        original_price = clean_price(de.get_text())
                else:
                    current_price = clean_price(price_container.get_text())

            # Discount %
            if current_price and original_price:
                discount_pct = round((original_price - current_price) / original_price * 100, 2)
            else:
                discount_pct = None

            product = {
                'scrape_date': datetime.now().isoformat(),
                'product_name': name,
                'current_price': current_price,
                'original_price': original_price,
                'discount_percentage': discount_pct,
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


# =========================
# SCRAPER
# =========================

def scrape_onlinestorebc():
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
    wait = WebDriverWait(driver, 20)

    all_data = []

    def scroll_page():
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    try:
        page = 1

        while True:
            url = f"{BASE_URL}page/{page}/"
            print(f"\n{'='*50}")
            print(f"SCRAPING PAGE {page}")
            print(f"{'='*50}")

            driver.get(url)

            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.product")))
            scroll_page()

            html = driver.page_source
            page_data = extract_data(html)

            print("\nSample JSON:")
            print(json.dumps(page_data[:5], indent=2, default=str))

            if not page_data:
                break

            all_data.extend(page_data)
            page += 1

    except Exception as err:
        print(err)

    finally:
        driver.quit()

    print(f"\nTOTAL PRODUCTS: {len(all_data)}")

    if all_data:
        df = pd.DataFrame(all_data).drop_duplicates(subset=['product_name'])

        print("\nPreview:")
        print(df[['product_name', 'current_price', 'original_price']].head())

        ts = int(time.time())
        parquet_path = f"/tmp/onlinestorebc_products_{ts}.parquet"
        df.to_parquet(parquet_path, index=False)

        return parquet_path

    return None


# =========================
# SNOWFLAKE LOAD
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
        file_name=f"onlinestorebc_{int(time.time())}.parquet"
    )

    merge_sql = f"""
    MERGE INTO {SF_DB}.{SF_SHARED_SCHEMA}.{SNOWFLAKE_TABLE} AS target
    USING (
        SELECT 
            $1:scrape_date::timestamp as scrape_date,
            $1:product_name::string as product_name,
            $1:current_price::float as current_price,
            $1:original_price::float as original_price,
            $1:discount_percentage::float as discount_percentage,
            $1:rating::string as rating,
            $1:reviews::string as reviews,
            $1:url::string as url
        FROM @{SNOWFLAKE_STAGE}/onlinestorebc_*.parquet
    ) AS source
    ON target.product_name = source.product_name 
       AND DATE_TRUNC('day', target.scrape_date) = DATE_TRUNC('day', source.scrape_date)
    WHEN MATCHED THEN UPDATE SET
        current_price = source.current_price,
        original_price = source.original_price,
        discount_percentage = source.discount_percentage
    WHEN NOT MATCHED THEN INSERT 
        (scrape_date, product_name, current_price, original_price, discount_percentage, rating, reviews, url)
    VALUES 
        (source.scrape_date, source.product_name, source.current_price, source.original_price, 
         source.discount_percentage, source.rating, source.reviews, source.url);
    """

    hook.run(merge_sql)
    print("✅ Upload complete!")

    return "Success"


# =========================
# DAG
# =========================

with DAG(
    dag_id="beautyclick_full_pipeline",
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["scraper", "beautyclick", "snowflake"],
) as dag:

    scrape_task = task(scrape_onlinestorebc)()
    upload_task = upload_to_snowflake(scrape_task)
