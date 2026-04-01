# dags/mydawa_full_pipeline.py
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import pandas as pd
import time
import json
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
# =========================
# CONFIG
# =========================
BASE_URL = "https://mydawa.com/products/family-planning"
SF_DB = "HOSPITALS" 
SF_SHARED_SCHEMA = "SHARED"
SNOWFLAKE_STAGE = f"{SF_DB}.{SF_SHARED_SCHEMA}.DB_BUCKET"
SNOWFLAKE_TABLE = "mydawa_products_raw"

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
    """Extract numeric price from text like 'KSh 120' or '120.00'"""
    if not text:
        return None
    t = text.replace("KSh", "").replace(",", "").strip()
    digits = "".join(ch for ch in t if ch.isdigit() or ch == ".")
    return float(digits) if digits else None

def accept_age_modal(driver, wait):
    """Click age consent modal if present"""
    selectors = [
        "#confirmHeaderConsentModalConfirm",
        "button#confirmHeaderConsentModalConfirm", 
        "button.btn-green.btn-rounded.btn-lg",
        ".btn-green",  # Fallback
        "button[aria-label*='confirm']",
    ]
    for sel in selectors:
        try:
            btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
            print(f"Clicked age consent: {sel}")
            btn.click()
            time.sleep(2)
            return
        except Exception:
            continue
    print("No age consent modal found")

def extract_data(page_source):
    """Extract product data with multiple fallback selectors"""
    soup = BeautifulSoup(page_source, "html.parser")
    rows = []

    # Try multiple product container selectors
    product_selectors = [
        "div.info",
        "div.product-item", 
        "div.product-card",
        "div.item",
        "article.product",
        ".product",
        "[data-product-id]",
    ]
    
    products = []
    for selector in product_selectors:
        products = soup.select(selector)
        if products:
            print(f"Found {len(products)} products with selector: {selector}")
            break
    
    if not products:
        print("No products found with any selector")
        return []

    for i, product in enumerate(products):
        try:
            # Multiple name selectors
            name_el = (product.select_one("h3.name, h2, .name, .product-name, a[title], h3, h4") 
                      or product.select_one("a"))
            name = name_el.get_text(" ", strip=True)[:200] if name_el else f"Product_{i}"

            # Multiple price selectors  
            price_el = product.select_one("div.prc, .prc, .price, .current-price, .amount")
            current_price = clean_price(price_el.get_text(" ", strip=True)) if price_el else None

            # Original price
            old_el = product.select_one("div.old, .old, .old-price, .strike, del")
            original_price = clean_price(old_el.get_text(" ", strip=True)) if old_el else None

            # Discount, rating, reviews
            disc_el = product.select_one("div.bdg._dsct._sm, .discount, .badge")
            rating_el = product.select_one("div.stars._s, .stars, .rating")
            rev_el = product.select_one("div.rev, .rev, .reviews")

            row = {
                "scrape_date": datetime.now(),
                "product_name": name,
                "current_price": current_price,
                "original_price": original_price,
                "discount_percentage": disc_el.get_text(" ", strip=True) if disc_el else None,
                "rating": rating_el.get_text(" ", strip=True) if rating_el else None,
                "reviews": rev_el.get_text(" ", strip=True) if rev_el else None,
                "url": product.select_one("a")["href"] if product.select_one("a") else None,
            }
            rows.append(row)
            
        except Exception as e:
            print(f"Error parsing product {i}: {e}")
            continue
    
    return rows

# =========================
# MAIN SCRAPE FUNCTION
# =========================
def scrape_mydawa():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage") 
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    options.binary_location = "/usr/bin/google-chrome"
    
    service = Service("/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 20)
    
    all_data = []
    try:
        print(f"Starting scrape: {BASE_URL}")
        driver.get(BASE_URL)
        
        accept_age_modal(driver, wait)
        
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(3)

        page_num = 1
        while True:
            print(f"\n{'='*50}")
            print(f"SCRAPING PAGE {page_num}")
            print(f"{'='*50}")
            
            html = driver.page_source
            page_data = extract_data(html)
            
            # JSON TO LOGS
            print(f"\nRAW JSON FROM PAGE {page_num} ({len(page_data)} products):")
            print(json.dumps(page_data[:5], indent=2, default=str))  # First 5
            if len(page_data) > 5:
                print(f"... (showing first 5 of {len(page_data)})")
            
            all_data.extend(page_data)
            
            # Try next page
            try:
                next_button = wait.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "a.pg[aria-label='Next Page'], .pagination .next, .next-page")
                ))
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(1)
                ActionChains(driver).move_to_element(next_button).click().perform()
                time.sleep(5)
                accept_age_modal(driver, wait)
                page_num += 1
            except Exception as e:
                print(f"No more pages: {e}")
                break

    finally:
        driver.quit()

    # FINAL SUMMARY
    print(f"\n{'='*60}")
    print(f"SCRAPE COMPLETE: {len(all_data)} TOTAL PRODUCTS")
    print(f"{'='*60}")
    
    if all_data:
        print("\nFINAL SAMPLE (first 3):")
        print(json.dumps(all_data[:3], indent=2, default=str))
        
        df = pd.DataFrame(all_data).drop_duplicates(subset=['product_name'])
        print(f"\nDataFrame after dedup: {df.shape}")
        print("\nName + Price preview:")
        preview_cols = ['product_name', 'current_price', 'original_price']
        print(df[preview_cols].head(10).to_string(index=False))
        
        # Save files
        ts = int(time.time())
        parquet_path = f"/tmp/mydawa_products_{ts}.parquet"
        df.to_parquet(parquet_path, index=False)
        
        csv_path = parquet_path.replace('.parquet', '.csv')
        df.to_csv(csv_path, index=False)
        
        print(f"\nSaved to: {parquet_path}")
        print(f"CSV backup: {csv_path}")
        
        return parquet_path
    else:
        print("❌ NO DATA EXTRACTED - check selectors in logs above!")
        return None

# =========================
# SNOWFLAKE UPLOAD TASK
# =========================
@task
def upload_to_snowflake(file_path):
    """Upload parquet to Snowflake stage + copy to table"""
    if not file_path:
        print("No file to upload")
        return None
        
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')  # Update conn_id
    
    # Upload to stage
    print(f"Uploading {file_path} to {SNOWFLAKE_STAGE}")
    hook.copy_file_to_stage(
        file_path,
        SNOWFLAKE_STAGE,
        file_name=f"mydawa_{int(time.time())}.parquet"
    )
    
    # Copy to table (merge on product_name + date)
    merge_sql = f"""
    MERGE INTO {SF_DB}.{SF_SHARED_SCHEMA}.{SNOWFLAKE_TABLE} AS target
    USING (
        SELECT 
            $1:scrape_date::timestamp as scrape_date,
            $1:product_name::string as product_name,
            $1:current_price::float as current_price,
            $1:original_price::float as original_price,
            $1:discount_percentage::string as discount_percentage,
            $1:rating::string as rating,
            $1:reviews::string as reviews,
            $1:url::string as url
        FROM @{SNOWFLAKE_STAGE}/mydawa_*.parquet
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
    
    print("Executing MERGE:")
    print(merge_sql)
    hook.run(merge_sql)
    print("✅ Upload complete!")
    return "Success"

# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id="mydawa_full_pipeline",
    schedule=None,  # Manual trigger
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["scraper", "mydawa", "snowflake"],
) as dag:

    scrape_task = task(scrape_mydawa)()
    
    upload_task = upload_to_snowflake(scrape_task)