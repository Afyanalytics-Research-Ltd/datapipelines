# dags/jumia_full_pipeline.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

# =========================
# CONFIG
# =========================
BASE_URL = "https://www.jumia.co.ke/home-office-appliances/?tag=JMALL&sort=rating"
SF_DB = "HOSPITALS" 
SF_SHARED_SCHEMA = "SHARED"
SNOWFLAKE_STAGE = f"{SF_DB}.{SF_SHARED_SCHEMA}.DB_BUCKET"
SNOWFLAKE_TABLE = "jumia_products_raw"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2
}

# =========================
# FUNCTIONS
# =========================
def extract_data(page_source):
    soup = BeautifulSoup(page_source, "html.parser")
    products = soup.find_all('div', class_='info')
    rows = []

    for product in products:
        try:
            rows.append({
                "date": datetime.now(),
                "product_name": product.find('h3', class_='name').text.strip(),
                "current_price": product.find('div', class_='prc').text.strip(),
                "original_price": product.find('div', class_='old').text.strip() if product.find('div', class_='old') else None,
                "discount_percentage": product.find('div', class_='bdg _dsct _sm').text.strip() if product.find('div', class_='bdg _dsct _sm') else None,
                "rating": product.find('div', class_='stars _s').text.strip() if product.find('div', class_='stars _s') else None,
                "reviews": product.find('div', class_='rev').text.strip() if product.find('div', class_='rev') else None
            })
        except:
            continue
    return rows

def scrape_jumia():
    from selenium.webdriver.chrome.service import Service

# Path to the ChromeDriver you installed
    chrome_driver_path = "/usr/local/bin/chromedriver"
    service = Service(chrome_driver_path)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(service=service, options=options)
    all_data = []
    driver.get(BASE_URL)
    time.sleep(5)

    while True:
        all_data.extend(extract_data(driver.page_source))
        try:
            next_button = driver.find_element(By.CSS_SELECTOR, "a.pg[aria-label='Next Page']")
            ActionChains(driver).move_to_element(next_button).click().perform()
            time.sleep(5)
        except:
            break

    driver.quit()
    df = pd.DataFrame(all_data)
    file_path = f"/tmp/jumia_products_{int(time.time())}.parquet"
    df.to_parquet(file_path, index=False)
    return file_path

def load_to_snowflake(file_path):
    snowflake = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = snowflake.get_conn()
    cursor = conn.cursor()

    # Upload to stage
    cursor.execute(f"PUT file://{file_path} @{SNOWFLAKE_STAGE} AUTO_COMPRESS=TRUE")

    # Load into table
    cursor.execute(f"""
        COPY INTO {SNOWFLAKE_TABLE}
        FROM @{SNOWFLAKE_STAGE}
        FILE_FORMAT = (TYPE = PARQUET)
    """)

# =========================
# DAG
# =========================
with DAG(
    dag_id="jumia_full_pipeline",
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_tasks=5,
    tags=["scraper", "jumia"]
) as dag:

    @task
    def scrape_task():
        """Scrape Jumia and save to Parquet"""
        return scrape_jumia()

    @task
    def load_task(file_path: str):
        """Load Parquet into Snowflake"""
        load_to_snowflake(file_path)

    # DAG flow
    file_path = scrape_task()
    load_task(file_path)