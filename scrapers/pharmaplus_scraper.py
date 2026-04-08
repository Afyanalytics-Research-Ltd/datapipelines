"""
Pharmaplus API Scraper 
API: https://web.pharmaplus.co.ke/datapool/read_products/fetch
"""

import requests
import pandas as pd
import time
import os

OUTPUT_FILE = "pharmaplus_sample.csv"
API_URL = "https://web.pharmaplus.co.ke/ecmws/read_products/fetch"
PAGE_SIZE   = 200      # max per request
MAX_PAGES   = 2        # pages per category → 200 x 2 = 400 products per category
                       

CATEGORIES = [
    "medicine",
    "Skin Care",
    "Vitamins & Supplements",
    "Beauty Care & Cosmetics",
    "Body Building",
    "General Hygiene Care",
    "Home Healthcare",
    "Veterinary Products",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Authorization": "Basic N05ldU1oTnY0SER1SmE1c0Y4R1k5Zz09OkxEZlNOMkpMVEF1T3RTeFd2QzRyWHNQcXJXMUp5RXNHZSsqWTJza1kwNXc9",
    "Origin": "https://shop.pharmaplus.co.ke",
    "Referer": "https://shop.pharmaplus.co.ke/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

def fetch_page(category, page=0):
    """Fetch one page of products for a category."""
    params = {
        "page":     page,
        "size":     PAGE_SIZE,
        "category": category,
    }
    r = requests.get(API_URL, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def parse_products(data, category):

    products = []
    for item in data.get("content", []):
        products.append({
            "name":               item.get("title", "").title(),
            "sku":                item.get("id", "").upper(),
            "product_code":       item.get("product_code", ""),
            "price_kes":          item.get("price", ""),
            "brand":              item.get("brand", ""),
            "category":           item.get("category", category),
            "category2":          item.get("category2", ""),
            "category3":          item.get("category3", ""),
            "category4":          item.get("category4", ""),
            "conditions":         item.get("conditions", ""),
            "description":        item.get("description", "").strip()[:300],
            "hs_code":            item.get("hs_code", ""),
            "in_stock":           item.get("in_stock", ""),
            "availability":       item.get("availability", ""),
            "requires_prescription": item.get("requires_prescription", False),
            "is_best_seller":     item.get("is_best_seller", False),
            "is_on_promotion":    item.get("is_on_promotion", False),
            "promo_discount_value":   item.get("promo_discount_value", ""),
            "promo_original_value":   item.get("promo_original_value", ""),
            "promo_discounted_value": item.get("promo_discounted_value", ""),
            "promo_start_datetime":   item.get("promo_start_datetime", ""),
            "promo_end_datetime":     item.get("promo_end_datetime", ""),
            "units_sold":             item.get("units_sold", 0.0),
            "product_group":      item.get("product_group", ""),
            "product_url":        item.get("link", ""),
            "slug":               item.get("slug", ""),
            "currency":           "KES",
            "source":             "pharmaplus.co.ke",
        })
    return products

def scrape_category(category):
    """Scrape products for one category across multiple pages."""
    print(f"\n  [{category}]")
    all_products = []
    page = 0

    while True:
        try:
            data       = fetch_page(category, page)
            pagination = data.get("page", {})
            total_pages = pagination.get("totalPages", 1)
            total_items = pagination.get("totalElements", 0)

            if page == 0:
                print(f"  Total available: {total_items} products ({total_pages} pages)")

            products = parse_products(data, category)
            if not products:
                break

            all_products.extend(products)
            print(f"  Page {page+1}/{total_pages} → {len(all_products)} collected")

            page += 1

          
            if page >= total_pages:
                break
            if MAX_PAGES and page >= MAX_PAGES:
                print(f"  Sample limit reached ({MAX_PAGES} pages)")
                break

            time.sleep(0.5)  

        except Exception as e:
            print(f"  ✗ Error on page {page}: {e}")
            break

    return all_products

def load_existing():
    if os.path.exists(OUTPUT_FILE):
        df = pd.read_csv(OUTPUT_FILE)
        print(f"Loaded {len(df)} existing products from {OUTPUT_FILE}")
        return df
    return pd.DataFrame()

def append_save(new_rows, existing_df):
    if not new_rows:
        return existing_df
    combined = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
    combined.drop_duplicates(subset=["sku"], inplace=True)
    combined.to_csv(OUTPUT_FILE, index=False)
    return combined

# ── MAIN ─────────────────────────────────────────

if __name__ == "__main__":
    existing_df = load_existing()
    done_cats   = set(existing_df["category"].unique()) if not existing_df.empty else set()

    for category in CATEGORIES:
        if category in done_cats:
            print(f"\n[SKIP] {category} already scraped")
            continue

        results     = scrape_category(category)
        existing_df = append_save(results, existing_df)
        print(f"  ✓ {len(results)} saved  |  running total: {len(existing_df)}")

    print(f"\n{'='*50}")
    print(f"Done! {len(existing_df)} products saved to {OUTPUT_FILE}")
    print(f"\nBy category:")
    print(existing_df["category"].value_counts().to_string())
    print(f"\nSample columns available:")
    print(list(existing_df.columns))