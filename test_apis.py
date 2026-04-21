import os
import yfinance as yf
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

FRED_API_KEY = os.getenv("FRED_API_KEY")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# ─────────────────────────────────────────────
# 1. AAPL current price via yfinance
# ─────────────────────────────────────────────
print("=" * 50)
print("1. AAPL Current Price (yfinance)")
print("=" * 50)
try:
    ticker = yf.Ticker("AAPL")
    price = ticker.info.get("regularMarketPrice")
    if price:
        print(f"AAPL current price: ${price}")
    else:
        print("Could not retrieve AAPL price.")
except Exception as e:
    print(f"Error fetching AAPL price: {e}")

# ─────────────────────────────────────────────
# 2. US Unemployment Rate via FRED
# ─────────────────────────────────────────────
print("\n" + "=" * 50)
print("2. US Unemployment Rate (FRED)")
print("=" * 50)
try:
    fred_url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id=UNRATE&api_key={FRED_API_KEY}&sort_order=desc&limit=1&file_type=json"
    )
    response = requests.get(fred_url, timeout=10)
    response.raise_for_status()
    data = response.json()
    obs = data["observations"][0]
    print(f"US Unemployment Rate: {obs['value']}% (as of {obs['date']})")
except Exception as e:
    print(f"Error fetching unemployment rate: {e}")

# ─────────────────────────────────────────────
# 3. AAPL previous day close via Polygon
# ─────────────────────────────────────────────
print("\n" + "=" * 50)
print("3. AAPL Previous Day Close (Polygon)")
print("=" * 50)
try:
    polygon_url = (
        f"https://api.polygon.io/v2/aggs/ticker/AAPL/prev"
        f"?adjusted=true&apiKey={POLYGON_API_KEY}"
    )
    response = requests.get(polygon_url, timeout=10)
    response.raise_for_status()
    data = response.json()
    results = data.get("results", [])
    if results:
        result = results[0]
        print(f"AAPL previous close:  ${result.get('c')}")
        print(f"Open:                 ${result.get('o')}")
        print(f"High:                 ${result.get('h')}")
        print(f"Low:                  ${result.get('l')}")
        print(f"Volume:               {result.get('v'):,.0f}")
        ts = result.get('t')
        if ts:
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(ts / 1e3, tz=timezone.utc)
            print(f"Date:                 {dt.strftime('%Y-%m-%d')}")
    else:
        print("No results returned from Polygon.")
except Exception as e:
    print(f"Error fetching Polygon data: {e}")

print("\nDone.")
