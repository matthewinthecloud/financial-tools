"""
Flask API server for the financial dashboard.
Serves current prices for Gold, WTI Crude, S&P 500, and 10-Year Treasury Yield.
"""

import os
import requests
import yfinance as yf
from typing import Optional
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

app = Flask(__name__, static_folder=".")
CORS(app)

FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def fetch_yfinance_price(ticker: str) -> Optional[float]:
    """Fetch the latest price for a given yfinance ticker symbol."""
    try:
        data = yf.Ticker(ticker)
        hist = data.history(period="1d")
        if hist.empty:
            return None
        return round(float(hist["Close"].iloc[-1]), 2)
    except Exception as e:
        app.logger.error(f"yfinance error for {ticker}: {e}")
        return None


def fetch_fred_value(series_id: str) -> Optional[float]:
    """Fetch the latest observation for a FRED series."""
    try:
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 5,
        }
        resp = requests.get(FRED_BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        observations = resp.json().get("observations", [])
        # FRED sometimes returns "." for missing values; skip those
        for obs in observations:
            if obs["value"] != ".":
                return round(float(obs["value"]), 3)
        return None
    except Exception as e:
        app.logger.error(f"FRED error for {series_id}: {e}")
        return None


@app.route("/api/prices")
def prices():
    """Return current market prices as JSON."""
    gold = fetch_yfinance_price("GC=F")
    oil = fetch_yfinance_price("CL=F")
    sp500 = fetch_yfinance_price("^GSPC")
    treasury_10y = fetch_fred_value("DGS10")

    return jsonify({
        "gold":         {"label": "Gold",               "value": gold,         "unit": "USD/oz",  "ticker": "GC=F"},
        "oil":          {"label": "WTI Crude Oil",       "value": oil,          "unit": "USD/bbl", "ticker": "CL=F"},
        "sp500":        {"label": "S&P 500",             "value": sp500,        "unit": "points",  "ticker": "^GSPC"},
        "treasury_10y": {"label": "10-Year Treasury",    "value": treasury_10y, "unit": "%",       "ticker": "DGS10"},
    })


@app.route("/")
def index():
    """Serve the dashboard HTML."""
    return send_from_directory(".", "index.html")


if __name__ == "__main__":
    app.run(port=5001, debug=False)
