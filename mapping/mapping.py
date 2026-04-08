import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point
from datetime import datetime
# 1. Load and Prepare Data
# Load the master POI file
gdf = gpd.read_file("pois.geojson")

# CRITICAL TWEAK: Project to UTM Zone 37S (Kenya) for metric accuracy (meters)
# This ensures a 1000m buffer is exactly 1000m, regardless of latitude.
if gdf.crs != "EPSG:32737":
    gdf = gdf.to_crs(epsg=32737)

# 2. Define Demand Categories
TAGS = {
    "Lifestyle_Drivers": ['gym', 'fitness', 'salon', 'beauty', 'spa', 'barber'],
    "Social_Drivers": ['bar', 'pub', 'nightclub', 'wines', 'spirits', 'restaurant', 'liquor'],
    "Competition": ['pharmacy', 'chemist', 'hospital', 'clinic', 'dawa']
}

def analyze_branch_dna(name, lat, lon, base_pop_2019):
    """
    Robust function to analyze the market DNA of a single branch.
    """
    try:
        # Create a Point and project it to the same metric CRS
        branch_point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(epsg=32737).iloc[0]
        
        # Exact 1000m and 300m Buffers (in meters)
        catchment_poly = branch_point.buffer(1000)
        pressure_poly = branch_point.buffer(300)
        
        # Spatial Filter: Only look at POIs within 1000m
        possible_matches = gdf[gdf.geometry.intersects(catchment_poly)].copy()
        
        # Error Handling: If no POIs exist in this area
        if possible_matches.empty:
            return {
                "Branch": name, "Est_Pop_2026": 0, "Lifestyle_Drivers": 0, 
                "Social_Drivers": 0, "Competition": 0, "High_Pressure_Comp": 0
            }

        # 2026 Projection Logic
        years_passed = 2026 - 2019
        growth_rate = 0.042 
        projected_pop = int(base_pop_2019 * (1 + growth_rate)**years_passed)

        stats = {"Branch": name, "Est_Pop_2026": projected_pop}
        
        # Keyword Search within the captured POIs
        possible_matches['all_text'] = possible_matches.apply(lambda x: " ".join(map(str, x.values)).lower(), axis=1)
        
        for category, keywords in TAGS.items():
            mask = possible_matches['all_text'].str.contains('|'.join(keywords))
            found_pois = possible_matches[mask]
            stats[category] = len(found_pois)
            
            # Sub-analysis for Competition Pressure (within 300m)
            if category == "Competition":
                stats["High_Pressure_Comp"] = len(found_pois[found_pois.geometry.intersects(pressure_poly)])
                
        return stats

    except Exception as e:
        print(f"Error analyzing {name}: {e}")
        return None

# 3. Execution on Branch List
branch_list = [
    {"name": "Branch Yaya", "lat": -1.2921, "lon": 36.7884, "pop": 43122},
    {"name": "Branch Westlands", "lat": -1.2646, "lon": 36.8045, "pop": 38344}
]

results = [analyze_branch_dna(b['name'], b['lat'], b['lon'], b['pop']) for b in branch_list]
df_dna = pd.DataFrame([r for r in results if r is not None])

# 4. Final Recommendation Engine
def get_strategy(row):
    if row['High_Pressure_Comp'] > 3:
        p_logic = "URGENT: Price-match competitors. Channel shift to WhatsApp Delivery."
    else:
        p_logic = "STABLE: Maintain margins. Focus on loyalty."
        
    if row['Lifestyle_Drivers'] > 10:
        m_logic = "UPSELL: Target Gyms/Salons. Grow Beauty (20% -> 25%) & Body Building (3% -> 8%)."
    elif row['Social_Drivers'] > 50:
        m_logic = "NIGHT ECONOMY: High impulse-buy potential. Stock Sexual Health & Recovery."
    else:
        m_logic = "CORE: Protect Chronic care. Stock for regular patient refills."
        
    return f"{p_logic} | {m_logic}"

df_dna['Actionable_Strategy'] = df_dna.apply(get_strategy, axis=1)

# 5. Output for Presentation
print("\n--- PHARMAPLUS STRATEGIC DNA REPORT (2026) ---")
print(df_dna[['Branch', 'Est_Pop_2026', 'Lifestyle_Drivers', 'Social_Drivers', 'High_Pressure_Comp']])
for _, row in df_dna.iterrows():
    print(f"\n[{row['Branch']} Strategy]:\n{row['Actionable_Strategy']}")


# 6. Export to CSV
OUTPUT_FILE = f"branch_market_dna_{datetime.now().strftime('%Y%m%d')}.csv"

# index=False prevents pandas from adding an extra column for row numbers
df_dna.to_csv(OUTPUT_FILE, index=False)

print(f"\n[SUCCESS] Market DNA dataset saved to: {OUTPUT_FILE}")