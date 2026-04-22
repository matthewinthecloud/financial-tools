"""
stock_screener.py

Screens S&P 500 stocks by:
  - Trailing P/E ratio < 20
  - Positive 3-month price momentum

Results are saved to screener_results.csv and printed as a summary table.

Usage:
    python stock_screener.py [--pe-max 20] [--output screener_results.csv]
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf


# ─────────────────────────────────────────────────────────────
# Data fetching
# ─────────────────────────────────────────────────────────────

def get_sp500_tickers() -> List[str]:
    """Fetch current S&P 500 tickers from Wikipedia using requests to avoid 403 blocks."""
    import io
    import requests

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; financial-tools-screener/1.0)"}
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()

    tables = pd.read_html(io.StringIO(response.text))
    df = tables[0]
    # Wikipedia uses '.' in some tickers (e.g. BRK.B); yfinance wants '-'
    tickers = df["Symbol"].str.replace(".", "-", regex=False).tolist()
    return sorted(tickers)


def fetch_pe_ratio(ticker: str) -> Tuple[str, Optional[float]]:
    """
    Fetch trailing P/E ratio for a single ticker via yfinance.

    Returns (ticker, pe_ratio) where pe_ratio is None if unavailable.
    """
    try:
        info = yf.Ticker(ticker).info
        pe = info.get("trailingPE") or info.get("forwardPE")
        # Guard against nonsensical values (negative, absurdly high)
        if pe is not None and pe <= 0:
            pe = None
        return ticker, pe
    except Exception:
        return ticker, None


def get_pe_ratios(tickers: list[str], max_workers: int = 20) -> Dict[str, Optional[float]]:
    """
    Fetch trailing P/E ratios for all tickers in parallel.

    Args:
        tickers: List of ticker symbols.
        max_workers: Thread pool size. Higher = faster but more rate-limit risk.

    Returns:
        Dict mapping ticker -> P/E ratio (or None if unavailable).
    """
    pe_map: Dict[str, Optional[float]] = {}
    total = len(tickers)
    completed = 0

    print(f"Fetching P/E ratios for {total} tickers (this takes ~2–3 minutes)...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_pe_ratio, t): t for t in tickers}
        for future in as_completed(futures):
            ticker, pe = future.result()
            pe_map[ticker] = pe
            completed += 1
            if completed % 50 == 0 or completed == total:
                print(f"  {completed}/{total} done", end="\r", flush=True)

    print()  # newline after progress line
    return pe_map


def get_price_momentum(tickers: list[str], months: int = 3) -> Dict[str, Optional[float]]:
    """
    Calculate price momentum over the given number of months for all tickers.

    Momentum = (current_price - price_N_months_ago) / price_N_months_ago * 100

    Uses yf.download for a single batched request (much faster than per-ticker calls).

    Args:
        tickers: List of ticker symbols.
        months: Look-back window in months.

    Returns:
        Dict mapping ticker -> momentum % (or None if insufficient data).
    """
    end_date = datetime.today()
    # Add a small buffer so we reliably get the start price even around holidays
    start_date = end_date - timedelta(days=months * 31 + 10)

    print(f"Fetching {months}-month price history for {len(tickers)} tickers...")

    prices = yf.download(
        tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=True,
        progress=False,
    )

    # yf.download returns a MultiIndex DataFrame when >1 ticker
    if isinstance(prices.columns, pd.MultiIndex):
        close = prices["Close"]
    else:
        # Single ticker fallback (shouldn't happen in practice)
        close = prices[["Close"]]
        close.columns = tickers

    momentum: Dict[str, Optional[float]] = {}
    for ticker in tickers:
        if ticker not in close.columns:
            momentum[ticker] = None
            continue
        series = close[ticker].dropna()
        if len(series) < 2:
            momentum[ticker] = None
            continue
        # Use first available close as the "N months ago" price
        price_start = series.iloc[0]
        price_end = series.iloc[-1]
        if price_start == 0:
            momentum[ticker] = None
        else:
            momentum[ticker] = round((price_end - price_start) / price_start * 100, 2)

    return momentum


# ─────────────────────────────────────────────────────────────
# Screening logic
# ─────────────────────────────────────────────────────────────

def screen_stocks(
    tickers: list[str],
    pe_ratios: Dict[str, Optional[float]],
    momentum: Dict[str, Optional[float]],
    pe_max: float = 20.0,
) -> pd.DataFrame:
    """
    Apply screening criteria and return a sorted DataFrame of passing stocks.

    Criteria:
      1. Trailing P/E ratio is available and < pe_max
      2. 3-month momentum is available and > 0

    Args:
        tickers: Universe of tickers considered.
        pe_ratios: Dict of ticker -> P/E ratio.
        momentum: Dict of ticker -> momentum %.
        pe_max: Maximum allowed P/E ratio.

    Returns:
        DataFrame with columns [Ticker, PE_Ratio, Momentum_3M_Pct], sorted by
        momentum descending.
    """
    rows = []
    for ticker in tickers:
        pe = pe_ratios.get(ticker)
        mom = momentum.get(ticker)

        # Validate both metrics are present
        if pe is None or mom is None:
            continue
        # Apply filters
        if pe < pe_max and mom > 0:
            rows.append({"Ticker": ticker, "PE_Ratio": round(pe, 2), "Momentum_3M_Pct": mom})

    if not rows:
        return pd.DataFrame(columns=["Ticker", "PE_Ratio", "Momentum_3M_Pct"])

    df = pd.DataFrame(rows).sort_values("Momentum_3M_Pct", ascending=False).reset_index(drop=True)
    df.index += 1  # 1-based ranking
    return df


# ─────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame, pe_max: float) -> None:
    """Print a formatted summary table of screener results to stdout."""
    print("\n" + "=" * 60)
    print(f"  S&P 500 SCREENER RESULTS  |  P/E < {pe_max}  |  3M Momentum > 0%")
    print("=" * 60)

    if df.empty:
        print("  No stocks passed the screening criteria.")
        print("=" * 60)
        return

    # Format columns for display
    display = df.copy()
    display["PE_Ratio"] = display["PE_Ratio"].map(lambda x: f"{x:.2f}")
    display["Momentum_3M_Pct"] = display["Momentum_3M_Pct"].map(lambda x: f"{x:+.2f}%")
    display.index.name = "Rank"

    print(display.to_string())
    print("=" * 60)
    print(f"  {len(df)} stocks matched out of 500 screened")
    print(f"  Run date: {datetime.today().strftime('%Y-%m-%d')}")
    print("=" * 60)


def save_results(df: pd.DataFrame, output_path: str) -> None:
    """Save screener results DataFrame to a CSV file."""
    df_to_save = df.copy()
    df_to_save.insert(0, "Rank", df_to_save.index)
    df_to_save["Run_Date"] = datetime.today().strftime("%Y-%m-%d")
    df_to_save.to_csv(output_path, index=False)
    print(f"\nResults saved to: {output_path}")


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

def main() -> None:
    """Main entry point: parse args, run screener, output results."""
    parser = argparse.ArgumentParser(
        description="Screen S&P 500 stocks by P/E ratio and 3-month momentum."
    )
    parser.add_argument(
        "--pe-max",
        type=float,
        default=20.0,
        help="Maximum trailing P/E ratio (default: 20)",
    )
    parser.add_argument(
        "--output",
        default="screener_results.csv",
        help="Output CSV file path (default: screener_results.csv)",
    )
    args = parser.parse_args()

    # Step 1: Get universe
    print("Fetching S&P 500 ticker list from Wikipedia...")
    try:
        tickers = get_sp500_tickers()
    except Exception as e:
        print(f"ERROR: Could not fetch S&P 500 tickers: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"Universe: {len(tickers)} tickers\n")

    # Step 2: Fetch P/E ratios
    pe_ratios = get_pe_ratios(tickers)
    valid_pe = sum(1 for v in pe_ratios.values() if v is not None)
    print(f"P/E data available for {valid_pe}/{len(tickers)} tickers\n")

    # Step 3: Fetch price momentum
    momentum = get_price_momentum(tickers, months=3)
    valid_mom = sum(1 for v in momentum.values() if v is not None)
    print(f"Momentum data available for {valid_mom}/{len(tickers)} tickers\n")

    # Step 4: Screen
    results = screen_stocks(tickers, pe_ratios, momentum, pe_max=args.pe_max)

    # Step 5: Output
    print_summary(results, pe_max=args.pe_max)
    save_results(results, args.output)


if __name__ == "__main__":
    main()
