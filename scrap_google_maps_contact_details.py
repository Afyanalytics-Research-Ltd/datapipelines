import logging
import sys
import re
import uuid
import pandas as pd
from urllib.parse import unquote

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# ------------------------------------------------------------------
# LOGGING CONFIGURATION
# ------------------------------------------------------------------
LOG_FILE = "google_maps_phone_scraper.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)
SCRAPPED_LINKS_PATH = "google_maps_places_10cdeaaa-6de2-4a27-881d-6e10e7d4b1c9.csv"
# ------------------------------------------------------------------
# HELPER FUNCTIONS
# ------------------------------------------------------------------
def extract_business_name(url: str) -> str:
    try:
        match = re.search(r"/place/([^/]+)", url)
        if match:
            raw_name = match.group(1)
            return unquote(raw_name).replace("+", " ")
    except Exception:
        logger.warning(f"Failed to extract business name from URL: {url}")
    return ""

# ------------------------------------------------------------------
# MAIN SCRIPT
# ------------------------------------------------------------------
def main(x):
    logger.info("Starting phone number extraction")

    try:
        df = pd.read_csv(f"{x}.csv")
        urls = df["url"].dropna().tolist()
        logger.info(f"Loaded {len(urls)} URLs")

    except Exception:
        logger.exception("Failed to load google_maps_places.csv")
        return

    results = []

    for idx, url in enumerate(urls, start=1):
         logger.info(f"[{idx}/{len(urls)}] Processing URL")

         options = webdriver.ChromeOptions()
         options.binary_location = "/usr/bin/google-chrome"
         options.add_argument("--headless=new")              # headless mode
         options.add_argument("--no-sandbox")               # required for Linux servers
         options.add_argument("--disable-dev-shm-usage")    # avoid memory issues
         options.add_argument("--disable-gpu")              # just in case

         driver = None
         phone_number = ""
         business_name = extract_business_name(url)

         try:
               driver = webdriver.Chrome(options=options)
               driver.get(url)

               wait = WebDriverWait(driver, 10)
               phone_elem = wait.until(
                  EC.presence_of_element_located(
                     (By.XPATH, "//button[starts-with(@data-item-id,'phone:tel:')]")
                  )
               )

               data_item = phone_elem.get_attribute("data-item-id")
               phone_number = data_item.split(":")[-1]

               logger.info(f"Phone found: {phone_number}")

         except TimeoutException:
               logger.warning("Phone number not found (timeout)")

         except WebDriverException:
               logger.exception("WebDriver error occurred")

         except Exception:
               logger.exception("Unexpected error occurred")

         finally:
               if driver:
                  driver.quit()
                  logger.info("WebDriver closed")

         results.append(
               {
                  "url": url,
                  "business_name": business_name,
                  "number": phone_number,
                  "county": x
               }
         )

    # Save results
    try:
        result_df = pd.DataFrame(results)
        result_df.to_csv(f"number_dataset_{str(uuid.uuid4())}.csv", index=False)
        logger.info(f"Saved {len(result_df)} records to number_dataset.csv")

    except Exception:
        logger.exception("Failed to save CSV")

    logger.info("Phone number scraping completed")

# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------
if __name__ == "__main__":
   KENYAN_COUNTIES = [
      # {"code": 1, "county": "Mombasa"},
      # {"code": 2, "county": "Kwale"},
      # {"code": 3, "county": "Kilifi"},
      # {"code": 4, "county": "Tana River"},
      # {"code": 5, "county": "Lamu"},
      # {"code": 6, "county": "Taita-Taveta"},
      # {"code": 7, "county": "Garissa"},
    #   {"code": 8, "county": "Wajir"},
    #   {"code": 9, "county": "Mandera"},
    #   {"code": 10, "county": "Marsabit"},
    #   {"code": 11, "county": "Isiolo"},
    #   {"code": 12, "county": "Meru"},
    #   {"code": 13, "county": "Tharaka-Nithi"},
    #   {"code": 14, "county": "Embu"},
    #   {"code": 15, "county": "Kitui"},
    #   {"code": 16, "county": "Machakos"},
    #   {"code": 17, "county": "Makueni"},
    #   {"code": 18, "county": "Nyandarua"},
    #   {"code": 19, "county": "Nyeri"},
    #   {"code": 20, "county": "Kirinyaga"},
    #   {"code": 21, "county": "Murang'a"},
    #   {"code": 22, "county": "Kiambu"},
    #   {"code": 23, "county": "Turkana"},
    #   {"code": 24, "county": "West Pokot"},
    #   {"code": 25, "county": "Samburu"},
    #   {"code": 26, "county": "Trans Nzoia"},
    #   {"code": 27, "county": "Uasin Gishu"},
      {"code": 28, "county": "Elgeyo-Marakwet"},
      {"code": 29, "county": "Nandi"},
      {"code": 30, "county": "Baringo"},
      {"code": 31, "county": "Laikipia"},
      {"code": 32, "county": "Nakuru"},
    #   {"code": 33, "county": "Narok"},
      {"code": 34, "county": "Kajiado"},
      {"code": 35, "county": "Kericho"},
      {"code": 36, "county": "Bomet"},
      {"code": 37, "county": "Kakamega"},
      {"code": 38, "county": "Vihiga"},
      {"code": 39, "county": "Bungoma"},
      {"code": 40, "county": "Busia"},
      {"code": 41, "county": "Siaya"},
      {"code": 42, "county": "Kisumu"},
      {"code": 43, "county": "Homa Bay"},
      {"code": 44, "county": "Migori"},
      {"code": 45, "county": "Kisii"},
      {"code": 46, "county": "Nyamira"},
      # {"code": 47, "county": "Nairobi"},
   ]
   counties = [x['county'] for x in KENYAN_COUNTIES]
   for county_name in counties:
      main(x = county_name)
