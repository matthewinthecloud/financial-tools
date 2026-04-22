"""
commodities.py — Fetch current spot prices for gold and WTI crude oil via yfinance.
"""

import yfinance as yf


COMMODITIES = {
    "Gold (GC=F)": "GC=F",
    "WTI Crude Oil (CL=F)": "CL=F",
}


def fetch_price(ticker: str) -> dict:
    """Fetch the most recent price and metadata for a given futures ticker.

    Args:
        ticker: A yfinance-compatible ticker symbol (e.g. 'GC=F').

    Returns:
        A dict with keys: ticker, name, price, currency, timestamp.
        Returns None values for price/timestamp if data is unavailable.
    """
    t = yf.Ticker(ticker)
    info = t.info

    price = info.get("regularMarketPrice") or info.get("previousClose")
    currency = info.get("currency", "USD")
    name = info.get("shortName") or info.get("longName") or ticker
    timestamp = info.get("regularMarketTime")

    return {
        "ticker": ticker,
        "name": name,
        "price": price,
        "currency": currency,
        "timestamp": timestamp,
    }


def main():
    """Fetch and print current prices for gold and WTI crude oil."""
    print(f"\n{'=' * 40}")
    print("  Commodity Spot Prices")
    print(f"{'=' * 40}")

    for label, ticker in COMMODITIES.items():
        result = fetch_price(ticker)

        if result["price"] is not None:
            print(f"\n{label}")
            print(f"  Name     : {result['name']}")
            print(f"  Price    : {result['currency']} {result['price']:,.2f}")
            if result["timestamp"]:
                from datetime import datetime, timezone
                dt = datetime.fromtimestamp(result["timestamp"], tz=timezone.utc)
                print(f"  As of    : {dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        else:
            print(f"\n{label}")
            print("  Price    : unavailable")

    print(f"\n{'=' * 40}\n")


if __name__ == "__main__":
    main()
