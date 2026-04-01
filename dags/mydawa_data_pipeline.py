# dags/onlinestore_full_pipeline.py
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import pandas as pd
import time
import json
from bs4 import BeautifulSoup
import re

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
BASE_URL = "https://mydawa.com/"
SF_DB = "HOSPITALS" 
SF_SHARED_SCHEMA = "SHARED"
SNOWFLAKE_STAGE = f"{SF_DB}.{SF_SHARED_SCHEMA}.DB_BUCKET"
SNOWFLAKE_TABLE = "onlinestore_products_raw"

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
    """Extract KES price from text"""
    if not text:
        return None
    # Find patterns like "KES 1,234.00" or "1,234"
    match = re.search(r'KES\s*([\d,]+\.?\d*)', text, re.IGNORECASE)
    if match:
        price = match.group(1).replace(',', '')
        return float(price)
    # Fallback: any number
    nums = re.findall(r'[\d,]+\.?\d*', text)
    return float(nums[0].replace(',', '')) if nums else None

def extract_data(page_source):
    soup = BeautifulSoup(page_source, 'html.parser')
    products = []
    
    # Target product cards from your HTML structure
    product_cards = soup.find_all('a', class_='card product-card')
    
    print(f"Found {len(product_cards)} product cards")
    
    for card in product_cards:
        try:
            # Product name from h5.product-title or title
            title_el = card.find('h5', class_='product-title')
            name = title_el.get_text(strip=True) if title_el else None
            
            # Image URL
            img = card.find('img', class_='product-img')
            img_url = img['src'] if img else None
            
            # Current price from data-price or p.product-price
            current_price = None
            price_el = card.find('p', class_='product-price')
            if price_el:
                current_price = clean_price(price_el.get_text())
            
            # Old price from p.product-price-old
            old_price_el = card.find('p', class_='product-price-old')
            old_price = clean_price(old_price_el.get_text()) if old_price_el else None
            
            # Sale badge
            sale_badge = card.find('span', class_='sale-badge')
            discount = sale_badge.get_text(strip=True) if sale_badge else None
            
            # Product URL
            product_url = card.get('href', '')
            
            # Data attributes with product info
            data_product_id = card.get('data-product-id', '')
            data_itemno = card.select_one('button.btn-cart')['data-itemno'] if card.select_one('button.btn-cart') else ''
            
            product = {
                'scrape_date': datetime.now().isoformat(),
                'name': name,
                'current_price': current_price,
                'original_price': old_price,
                'discount': discount,
                'image_url': img_url,
                'product_id': data_product_id,
                'item_no': data_itemno,
                'full_html_snippet': str(card)[:500]  # Debug snippet
            }
            
            # Only add if we have name + price
            if name and current_price:
                products.append(product)
                
        except Exception as e:
            print(f"Error parsing card: {e}")
            continue
    
    return products




# =========================
# MAIN SCRAPE FUNCTION
# =========================
def scrape_onlinestore():
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
        
        

        page_num = 1
        i = 0
        while i < 3:
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
            i += 1
            
           
    except Exception as err:
        print(err)
    # FINAL SUMMARY
    print(f"\n{'='*60}")
    print(f"SCRAPE COMPLETE: {len(all_data)} TOTAL PRODUCTS")
    print(f"{'='*60}")
    
    if all_data:
        print("\nFINAL SAMPLE (first 3):")
        print(json.dumps(all_data[:3], indent=2, default=str))
        
        df = pd.DataFrame(all_data).drop_duplicates(subset=['name'])
        print(f"\nDataFrame after dedup: {df.shape}")
        print("\nName + Price preview:")
        preview_cols = ['name', 'current_price', 'original_price']
        print(df[preview_cols].head(10).to_string(index=False))
        
        # Save files
        ts = int(time.time())
        parquet_path = f"/tmp/onlinestore_products_{ts}.parquet"
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
        file_name=f"onlinestore_{int(time.time())}.parquet"
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
        FROM @{SNOWFLAKE_STAGE}/onlinestore_*.parquet
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
    dag_id="onlinestore_full_pipeline",
    schedule=None,  # Manual trigger
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["scraper", "onlinestore", "snowflake"],
) as dag:

    scrape_task = task(scrape_onlinestore)()
    
    upload_task = upload_to_snowflake(scrape_task)