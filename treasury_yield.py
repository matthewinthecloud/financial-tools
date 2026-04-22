import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

FRED_API_KEY = os.getenv("FRED_API_KEY")

# FRED series DGS10 = 10-Year Treasury Constant Maturity Rate
SERIES_ID = "DGS10"

print("=" * 50)
print("10-Year Treasury Yield (FRED)")
print("=" * 50)

try:
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={SERIES_ID}&api_key={FRED_API_KEY}"
        f"&sort_order=desc&limit=1&file_type=json"
    )
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    obs = data["observations"][0]
    value = obs["value"]
    date = obs["date"]

    if value == ".":
        print(f"No data available for {date} (market may have been closed).")
    else:
        print(f"10-Year Treasury Yield: {value}% (as of {date})")

except Exception as e:
    print(f"Error fetching Treasury yield: {e}")
