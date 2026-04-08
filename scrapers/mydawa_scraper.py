import requests
import pandas as pd
import time
import os
import random

# --- CONFIG ---
OUTPUT_FILE = "mydawa_product_list.csv"
API_URL     = "https://mydawa.com/productsearch"
PAGE_LIMIT  = 100 
MAX_PAGES   = 20 

CATEGORIES = [
    "cold-and-flu",
    "health-conditions",
    "medicines-and-treatments",
    "personal-care",
    "beauty-and-skin-care",
    "vitamins-and-supplements",
    "mother-and-baby-care",
    "medical-devices",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://mydawa.com",
    "Referer": "https://mydawa.com/",
}

# --- REFRESH COOKIE FROM NETWORK TAB ---
SESSION_COOKIE = "__Host-XSRF-TOKEN=CfDJ8LqcensyVJRGhIdp4NIUYPY9wvUDrYsAg4-gNZAq-jL3_w0Uw6sxSFOFevb6skaA397VXym60S-PEcPflssVsg2WPPy2VO7aaikZVmE-hw; _gcl_gs=2.1.k1$i1775416708$u234667906; _gcl_au=1.1.1387330074.1775416742; _gid=GA1.2.1321270404.1775416751; _fbp=fb.1.1775416756691.332134824705920678; _tt_enable_cookie=1; _ttp=01KNFHBK222QKSNBTZBBJTADXA_.tt.1; __kla_id=eyJjaWQiOiJOalJtWmpFMVpXTXRNV1pqTkMwME4yTTRMV0V6WXpJdE5EWTVOamd6WkRJM05tTTEifQ==; __qca=P1-9cf3061f-b567-4a52-ac91-43eef54ad409; _gac_UA-90885718-1=1.1775416777.Cj0KCQjwkMjOBhC5ARIsADIdb3dnHHMLAirU0qoCPDI0gCk1FoBCcPFMoSVCDCcHAic_ogkhTcdFbpoaAre6EALw_wcB; moe_uuid=d2084437-f980-4592-ad83-2df8f9d8ee43; _gcl_aw=GCL.1775472694.Cj0KCQjwkMjOBhC5ARIsADIdb3dnHHMLAirU0qoCPDI0gCk1FoBCcPFMoSVCDCcHAic_ogkhTcdFbpoaAre6EALw_wcB; _clck=cvwt9n%5E2%5Eg50%5E0%5E2286; cto_bundle=e0xV1193WXpabSUyRmdlWHZiZHRxc0dZUHdianZ1aTVWR0dqU3l3dFFnSXdyTzQ5a1o4OTM2YTFGZTh4dm56NDdPTUZhMEtBdDBaamNmNWkxWmtLQXJWc3d0eDRyTVptM3djVGkzMGdRY3FaNkxhUXZyVjd1Q0s3eXB0MyUyQmxjbyUyRmFhSHowS2tmcjRMUHoxSkw4JTJGUmg3JTJCM09OOXVBJTNEJTNE; _dc_gtm_UA-90885718-1=1; _ga_E1L47YMCY5=GS2.1.s1775560311$o6$g1$t1775560321$j50$l0$h0; _ga=GA1.1.1637580469.1775416748; _ga_XY4JB9TYME=GS2.1.s1775560311$o6$g1$t1775560321$j50$l0$h0; ab.storage.deviceId.c421ab8d-0946-4b73-9ca6-9370b2bb36dd=%7B%22g%22%3A%2209903a61-5dff-1b3d-ed4d-db362b9f4879%22%2C%22c%22%3A1775416750499%2C%22l%22%3A1775560324240%7D; ttcsid_D4E5N9RC77U004J4APGG=1775560311848::3zBe4JKlZBy-xxaiiQ0Y.6.1775560325021.1; ab.storage.sessionId.c421ab8d-0946-4b73-9ca6-9370b2bb36dd=%7B%22g%22%3A%225bf740e3-418c-2a86-2d1e-616f598a015b%22%2C%22e%22%3A1775562125944%2C%22c%22%3A1775560324239%2C%22l%22%3A1775560325944%7D; _ga_6J4T5FQFXN=GS2.2.s1775560326$o5$g1$t1775560330$j56$l0$h0; _clsk=ga24vd%5E1775560338575%5E1%5E1%5Ei.clarity.ms%2Fcollect; ttcsid_D0FSG5RC77U3GUQUGH0G=1775560325996::ZAVXaINbwPqTGZpYW1xo.6.1775560343472.1; ttcsid=1775560020773::zVWhsVaLkuRt4UHQ8IgU.7.1775560343470.0::1.289095.305221::327996.32.350.15::270276.438.7182"

scraper_session = requests.Session()
scraper_session.headers.update(HEADERS)
if SESSION_COOKIE:
    scraper_session.headers.update({"Cookie": SESSION_COOKIE})

def fetch_page(category, offset=0):
    params = {"query": category, "limit": PAGE_LIMIT, "productSearch": "false", "offset": offset}
    try:
        r = scraper_session.post(API_URL, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  ! Connection error: {e}. Waiting 10s...")
        time.sleep(10)
        return []

def parse_item(item, category):
    brand = item.get("brands") or {}
    cats = item.get("categories") or [{}]
    cat0 = cats[0] if cats else {}

    return {
        "item_no":          item.get("itemNo", ""),
        "name":             item.get("name", "").strip(),
        "price_kes":        item.get("price", 0),
        "discount_price":   item.get("discountedPrice", 0),
        "discount_tag":     item.get("discountTag", ""),
        "in_stock":         item.get("inStock", False),
        "stock_qty":        item.get("stock", 0),
        "prescription":     item.get("prescriptionRequired", False),
        "brand_name":       brand.get("brandName", "Generic"),
        "division":         cat0.get("divisionDescription", ""),
        "category_name":    cat0.get("categoryDescription", ""),
        "product_group":    cat0.get("retailProductGroupDescription", ""),
        "description":      (cat0.get("description") or "").strip()[:400],
        "url":              "https://mydawa.com" + item.get("url", ""),
        "query_category":   category,
        "source":           "mydawa.com",
    }

def scrape_category(category):
    print(f"\n[STARTING] Category: {category}")
    category_results = []
    seen_ids = set()
    offset, page = 0, 0

    while True:
        raw_data = fetch_page(category, offset)
        items = raw_data if isinstance(raw_data, list) else raw_data.get("products", [])

        if not items:
            break

        new_count = 0
        for item in items:
            parsed = parse_item(item, category)
            if parsed['item_no'] not in seen_ids:
                seen_ids.add(parsed['item_no'])
                category_results.append(parsed)
                new_count += 1
        
        
        if new_count == 0:
            print("  - Detection: Duplicate data loop. Moving on.")
            break

        print(f"  - Page {page+1} (offset {offset}): Captured {new_count} items")

        if (MAX_PAGES and page + 1 >= MAX_PAGES) or len(items) < PAGE_LIMIT:
            break

        page += 1
        offset += PAGE_LIMIT
        time.sleep(random.uniform(2.0, 4.0)) 
        
    return category_results

if __name__ == "__main__":
    final_data = []
    
    for cat in CATEGORIES:
        res = scrape_category(cat)
        final_data.extend(res)
        
        if final_data:
            df = pd.DataFrame(final_data)
            df.drop_duplicates(subset=['item_no'], inplace=True)
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"  ✓ Intermediate Save: {len(df)} total unique products.")

    print(f"\nSUCCESS! Scrape complete. File: {OUTPUT_FILE}")