import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point
import os

# 1. Load and Prepare Data
try:
    gdf = gpd.read_file("pois.geojson")
except Exception as e:
    print(f"Error loading geojson: {e}")
    exit()

# Validate CRS
if gdf.crs is None:
    raise ValueError("GeoDataFrame has no CRS defined")

if gdf.crs.to_epsg() != 32737:
    gdf = gdf.to_crs(epsg=32737)

_ = gdf.sindex  # ensure spatial index

# 2. Define Demand Categories
TAGS = {
    "Lifestyle_Drivers": ['gym', 'fitness', 'salon', 'beauty', 'spa', 'barber'],
    "Social_Drivers": ['bar', 'pub', 'nightclub', 'restaurant', 'liquor'],
    "Competition": ['pharmacy', 'chemist', 'hospital', 'clinic']
}

TEXT_COLS = ['name', 'amenity', 'shop', 'description']

# 3. Core Function
def analyze_branch_dna(name, lat, lon, base_pop_2019):
    try:
        branch_point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326") \
            .to_crs(epsg=32737).iloc[0]

        catchment_poly = branch_point.buffer(1000)
        pressure_poly = branch_point.buffer(300)

        possible_matches = gdf[gdf.geometry.within(catchment_poly)].copy()

        stats = {
            "Branch": name,
            "Est_Pop_2026": 0,
            "Lifestyle_Drivers": 0,
            "Social_Drivers": 0,
            "Competition": 0,
            "High_Pressure_Comp": 0
        }

        if possible_matches.empty:
            return stats

        # Population projection (kept but not used for decisions)
        growth_rate = 0.042
        years = 2026 - 2019
        stats["Est_Pop_2026"] = int(base_pop_2019 * (1 + growth_rate) ** years)

        # Clean text aggregation
        available_cols = [c for c in TEXT_COLS if c in possible_matches.columns]
        possible_matches['all_text'] = (
            possible_matches[available_cols]
            .fillna('')
            .agg(' '.join, axis=1)
            .str.lower()
        )

        # Category counts
        for category, keywords in TAGS.items():
            mask = possible_matches['all_text'].str.contains('|'.join(keywords), regex=True)
            found = possible_matches[mask]

            stats[category] = len(found)

            if category == "Competition":
                stats["High_Pressure_Comp"] = len(
                    found[found.geometry.within(pressure_poly)]
                )

        return stats

    except Exception as e:
        print(f"Error analyzing {name}: {e}")
        return None

# 4. Branch Input
branch_list = [
    {"name": "Branch Yaya", "lat": -1.2921, "lon": 36.7884, "pop": 43122},
    {"name": "Branch Westlands", "lat": -1.2646, "lon": 36.8045, "pop": 38344},
    {"name": "Branch Imara Mall", "lat": -1.3242, "lon": 36.8881, "pop": 51000}
]

results = [analyze_branch_dna(b['name'], b['lat'], b['lon'], b['pop']) for b in branch_list]
df_dna = pd.DataFrame([r for r in results if r is not None])

# 5. Strategy Logic (FIXED)
def get_strategy(row):
    # --- PRIMARY: Local competition pressure ---
    if row['High_Pressure_Comp'] >= 5:
        p_logic = "HIGH LOCAL COMPETITION: aggressive pricing required"
    elif row['Competition'] >= 15:
        p_logic = "MODERATE COMPETITION: selective pricing pressure"
    else:
        p_logic = "LOW COMPETITION: margin retention viable"

# --- SECONDARY: Demand environment (refined) ---
    lifestyle = row['Lifestyle_Drivers']
    social = row['Social_Drivers']

    ratio = social / max(lifestyle, 1)

    if ratio > 3:
        m_logic = "SOCIAL-DRIVEN"
    elif ratio < 1:
        m_logic = "LIFESTYLE-DRIVEN"
    else:
        m_logic = "BALANCED"

    return f"{p_logic} | {m_logic}"

df_dna['Actionable_Strategy'] = df_dna.apply(get_strategy, axis=1)

# 6. Output
print("\n--- PHARMAPLUS GEO-DNA (CORRECTED) ---")
print(df_dna)

for _, row in df_dna.iterrows():
    print(f"\n[{row['Branch']}]\n{row['Actionable_Strategy']}")

# 7. Save
os.makedirs('../data', exist_ok=True)
OUTPUT_FILE = "../data/branch_market_dna.csv"
df_dna.to_csv(OUTPUT_FILE, index=False)

print(f"\n[SAVED] {OUTPUT_FILE}")
