import asf_search as asf
from datetime import datetime, timedelta
import time

# -----------------------------
# CONFIG (edit if needed)
# -----------------------------

ROI_WKT = """POLYGON((
9.75 53.35,
10.35 53.35,
10.35 53.70,
9.75 53.70,
9.75 53.35
))"""

DAYS_BACK = 5
MAX_RETRIES = 3


# -----------------------------
# SAFE ASF SEARCH
# -----------------------------

def safe_search(query_kwargs, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            print(f"\n🔁 Attempt {attempt+1}...")
            results = list(asf.search(**query_kwargs))
            return results
        except Exception as e:
            print(f"❌ Error: {e}")
            if attempt < retries - 1:
                wait = 2 * (attempt + 1)
                print(f"⏳ Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print("🚫 Max retries reached.")
                return []


# -----------------------------
# MAIN TEST
# -----------------------------

def main():

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=DAYS_BACK)

    start = start_time.strftime('%Y-%m-%dT00:00:00Z')
    end = end_time.strftime('%Y-%m-%dT23:59:59Z')

    print("\n==============================")
    print("🛰️  ASF SENTINEL-1 TEST SEARCH")
    print("==============================")
    print(f"📅 Time Range: {start} → {end}")
    print(f"📍 ROI: {ROI_WKT}")

    # 🔥 CORRECT ASF QUERY
    query_kwargs = {
        "platform": "SENTINEL-1",
        
        "beamMode": "IW",
        "polarization": "VV",
        "intersectsWith": "POLYGON((9.75 53.35, 10.35 53.35, 10.35 53.70, 9.75 53.70, 9.75 53.35))",
        "start": start,
        "end": end,
    }

    print("\n📡 Query Parameters:")
    for k, v in query_kwargs.items():
        print(f"   {k}: {v}")

    results = safe_search(query_kwargs)

    print("\n==============================")
    print(f"✅ Total Products Found: {len(results)}")
    print("==============================\n")

    if not results:
        print("❌ No results found.")
        print("👉 Try increasing DAYS_BACK or check network.")
        return

    # Print first 5 results
    for i, product in enumerate(results[:5]):
        props = product.properties

        print(f"--- Product {i+1} ---")
        print("📄 File:", props.get("fileName"))
        print("🕒 Start:", props.get("startTime"))
        print("📡 Mode:", props.get("beamMode"))
        print("📶 Pol:", props.get("polarization"))
        print("🔗 URL:", props.get("url"))
        print()

    if len(results) > 5:
        print(f"... and {len(results)-5} more results")


# -----------------------------
if __name__ == "__main__":
    main()