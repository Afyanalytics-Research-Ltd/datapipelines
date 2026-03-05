import logging
import sys
import time
import uuid
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, WebDriverException

# ------------------------------------------------------------------
# LOGGING CONFIGURATION
# ------------------------------------------------------------------
LOG_FILE = "google_maps_scraper.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# MAIN SCRIPT
# ------------------------------------------------------------------
def main(county_name=None):
    logger.info("Starting Google Maps scraping")

    try:
        # 1. Setup Chrome WebDriver
        logger.info("Initializing Chrome WebDriver")
        options = webdriver.ChromeOptions()
        options.binary_location = "/usr/bin/google-chrome"
        options.add_argument("--start-maximized")
        driver = webdriver.Chrome(options=options)

        # 2. Open Google Maps
        logger.info("Opening Google Maps")
        driver.get("https://www.google.com/maps")
        time.sleep(3)

        # 3. Search for a place or business
        search_query = f"clinics in {county_name}"
        search_box_id = "UGojuc"
        logger.info(f"Searching for: {search_query}")

        wait = WebDriverWait(driver, 10)  # wait up to 10 seconds
        search_box = wait.until(EC.presence_of_element_located((By.ID, search_box_id)))
        search_box.clear()
        search_box.send_keys(search_query)
        search_box.send_keys(Keys.ENTER)

        time.sleep(5)

        # 4. Scroll results panel
        logger.info("Locating results feed for scrolling")
        scrollable_div = driver.find_element(By.XPATH, '//div[@role="feed"]')

        scroll_count = 17
        logger.info(f"Scrolling results panel {scroll_count} times")

        for i in range(scroll_count):
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight",
                scrollable_div,
            )
            logger.info(f"Scroll iteration {i + 1}/{scroll_count}")
            time.sleep(7)

        # 5. Extract places
        logger.info("Extracting places from page")
        places = driver.find_elements(By.XPATH, '//div[@role="article"]')
        logger.info(f"Found {len(places)} places")

        data = []

        for idx, place in enumerate(places, start=1):
            try:
                link_url = place.find_element(By.TAG_NAME, "a").get_attribute("href")
            except NoSuchElementException:
                link_url = ""
                logger.warning(f"[{idx}] No URL found")

            try:
                name = place.text.strip()
            except Exception:
                name = ""
                logger.warning(f"[{idx}] No name text found")

            data.append({"name": name, "url": link_url})

            if idx % 10 == 0:
                logger.info(f"Extracted {idx}/{len(places)} places")

        # 6. Save CSV
        df = pd.DataFrame(data)
        output_file = f"{county_name}.csv"
        df.to_csv(output_file, index=False)

        logger.info(f"Saved {len(df)} records to {output_file}")

    except WebDriverException as e:
        logger.exception("WebDriver failure occurred")

    except Exception as e:
        logger.exception("Unexpected error occurred")

    finally:
        try:
            driver.quit()
            logger.info("WebDriver closed successfully")
        except Exception:
            logger.warning("WebDriver could not be closed")

        logger.info("Scraping finished")


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
        # {"code": 8, "county": "Wajir"},
        # {"code": 9, "county": "Mandera"},
        # {"code": 10, "county": "Marsabit"},
        # {"code": 11, "county": "Isiolo"},
        # {"code": 12, "county": "Meru"},
        # {"code": 13, "county": "Tharaka-Nithi"},
        # {"code": 14, "county": "Embu"},
        # {"code": 15, "county": "Kitui"},
        # {"code": 16, "county": "Machakos"},
        # {"code": 17, "county": "Makueni"},
        # {"code": 18, "county": "Nyandarua"},
        # {"code": 19, "county": "Nyeri"},
        # {"code": 20, "county": "Kirinyaga"},
        # {"code": 21, "county": "Murang'a"},
        # {"code": 22, "county": "Kiambu"},
        # {"code": 23, "county": "Turkana"},
        # {"code": 24, "county": "West Pokot"},
        # {"code": 25, "county": "Samburu"},
        # {"code": 26, "county": "Trans Nzoia"},
        # {"code": 27, "county": "Uasin Gishu"},
        # {"code": 28, "county": "Elgeyo-Marakwet"},
        # {"code": 29, "county": "Nandi"},
        # {"code": 30, "county": "Baringo"},
        {"code": 31, "county": "Laikipia"},
        {"code": 32, "county": "Nakuru"},
        {"code": 33, "county": "Narok"},
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
        main(county_name=county_name)
