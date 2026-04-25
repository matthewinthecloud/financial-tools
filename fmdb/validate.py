#!/usr/bin/env python3
"""
Validation test: 5 instruments, 30-day backfill, full pipeline check.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import logging
from datetime import date, timedelta, datetime

import pandas as pd
from sqlalchemy import create_engine, select, func, text

from config import DB_URL
from db.schema import create_all, raw_prices, raw_economics, analytics_prices, analytics_economics
from adapters.yfinance_adapter import YFinanceAdapter
from adapters.fred_adapter import FredAdapter
from quality.checks import check_price_data, check_economics_data, log_issues
from analytics.compute import compute_price_analytics, compute_economics_analytics

logging.basicConfig(level=logging.WARNING)  # suppress noise for clean output

TEST_INSTRUMENTS = [
    {'instrument_id': 'SPY',      'name': 'SPDR S&P 500 ETF',     'asset_class': 'etf',      'source': 'yfinance', 'source_ticker': 'SPY'},
    {'instrument_id': '^GSPC',    'name': 'S&P 500 Index',         'asset_class': 'index',    'source': 'yfinance', 'source_ticker': '^GSPC'},
    {'instrument_id': 'EURUSD=X', 'name': 'EUR/USD',               'asset_class': 'fx',       'source': 'yfinance', 'source_ticker': 'EURUSD=X'},
    {'instrument_id': 'BTC-USD',  'name': 'Bitcoin',               'asset_class': 'crypto',   'source': 'yfinance', 'source_ticker': 'BTC-USD'},
    {'instrument_id': 'UNRATE',   'name': 'US Unemployment Rate',  'asset_class': 'economic', 'source': 'fred',     'source_ticker': 'UNRATE'},
]

START = date.today() - timedelta(days=30)
END   = date.today()

PASS = "✅"
FAIL = "❌"

def hr(): print("─" * 60)

def main():
    print("\n" + "=" * 60)
    print("  fmdb Validation Test")
    print(f"  Range: {START} → {END} (30 days)")
    print("=" * 60)

    # ── 1. Postgres connection ────────────────────────────────────
    hr()
    print("1. Postgres connection")
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"   {PASS} Connected: {DB_URL}")
    except Exception as e:
        print(f"   {FAIL} Cannot connect: {e}")
        sys.exit(1)

    # ── 2. Schema creation ────────────────────────────────────────
    hr()
    print("2. Schema creation")
    try:
        create_all(engine)
        print(f"   {PASS} All tables created/verified")
    except Exception as e:
        print(f"   {FAIL} Schema error: {e}")
        sys.exit(1)

    # ── 3. Adapter fetch + ingest ─────────────────────────────────
    hr()
    print("3. Adapter fetch & ingest")

    yf  = YFinanceAdapter()
    fred = FredAdapter()
    results = {}

    for inst in TEST_INSTRUMENTS:
        iid    = inst['instrument_id']
        ticker = inst['source_ticker']
        source = inst['source']
        ac     = inst['asset_class']

        try:
            adapter = yf if source == 'yfinance' else fred
            df = adapter.fetch_history(iid, ticker, START, END)

            if df.empty:
                print(f"   {FAIL} {iid:15s} — no data returned")
                results[iid] = {'ok': False, 'rows': 0}
                continue

            # Quality checks
            if ac == 'economic':
                issues = check_economics_data(df, iid)
                # Ingest
                from ingestion.backfill import _ingest_economics
                count = _ingest_economics(engine, iid, df, source)
            else:
                issues = check_price_data(df, iid)
                from ingestion.backfill import _ingest_prices
                count = _ingest_prices(engine, iid, df, source)

            if issues:
                log_issues(engine, issues)
                warn_count = len([i for i in issues if i['severity'] == 'warn'])
                err_count  = len([i for i in issues if i['severity'] == 'error'])
                quality_str = f"⚠️  {warn_count}w/{err_count}e quality issues"
            else:
                quality_str = "no quality issues"

            # Show sample
            sample_close = df['close'].iloc[-1] if 'close' in df.columns else df['value'].iloc[-1]
            sample_date  = df['date'].iloc[-1]

            print(f"   {PASS} {iid:15s} — {count:3d} rows | latest: {sample_close:.4f} on {sample_date} | {quality_str}")
            results[iid] = {'ok': True, 'rows': count, 'df': df, 'asset_class': ac}

        except Exception as e:
            print(f"   {FAIL} {iid:15s} — error: {e}")
            results[iid] = {'ok': False, 'rows': 0}

    # ── 4. Analytics compute ──────────────────────────────────────
    hr()
    print("4. Analytics compute")

    for inst in TEST_INSTRUMENTS:
        iid = inst['instrument_id']
        ac  = inst['asset_class']
        if not results.get(iid, {}).get('ok'):
            print(f"   ⏭️  {iid:15s} — skipped (no data)")
            continue
        try:
            if ac == 'economic':
                compute_economics_analytics(engine, iid)
                # Check result
                with engine.connect() as conn:
                    row = conn.execute(
                        select(analytics_economics).where(analytics_economics.c.series_id == iid)
                        .order_by(analytics_economics.c.date.desc()).limit(1)
                    ).fetchone()
                if row:
                    print(f"   {PASS} {iid:15s} — yoy={row.yoy_change:.2f}% mom={row.mom_change:.2f}% z1y={row.zscore_1y:.2f}" if all([row.yoy_change, row.mom_change, row.zscore_1y]) else f"   {PASS} {iid:15s} — analytics stored (limited history for z-scores)")
                else:
                    print(f"   {FAIL} {iid:15s} — analytics not found after compute")
            else:
                compute_price_analytics(engine, iid)
                with engine.connect() as conn:
                    row = conn.execute(
                        select(analytics_prices).where(analytics_prices.c.instrument_id == iid)
                        .order_by(analytics_prices.c.date.desc()).limit(1)
                    ).fetchone()
                if row:
                    rmi = row.rmi_signal or 'n/a'
                    rsi = f"{row.rsi_14:.1f}" if row.rsi_14 else 'n/a'
                    ret1d = f"{row.ret_1d:.2f}%" if row.ret_1d else 'n/a'
                    print(f"   {PASS} {iid:15s} — ret_1d={ret1d} rsi={rsi} rmi={rmi}")
                else:
                    print(f"   {FAIL} {iid:15s} — analytics not found after compute")
        except Exception as e:
            print(f"   {FAIL} {iid:15s} — analytics error: {e}")

    # ── 5. Database row counts ────────────────────────────────────
    hr()
    print("5. Database row counts")
    with engine.connect() as conn:
        rp  = conn.execute(select(func.count()).select_from(raw_prices)).scalar()
        re  = conn.execute(select(func.count()).select_from(raw_economics)).scalar()
        ap  = conn.execute(select(func.count()).select_from(analytics_prices)).scalar()
        ae  = conn.execute(select(func.count()).select_from(analytics_economics)).scalar()
    print(f"   raw_prices:          {rp:>6} rows")
    print(f"   raw_economics:       {re:>6} rows")
    print(f"   analytics_prices:    {ap:>6} rows")
    print(f"   analytics_economics: {ae:>6} rows")

    # ── Summary ───────────────────────────────────────────────────
    hr()
    ok_count = sum(1 for v in results.values() if v.get('ok'))
    print(f"Result: {ok_count}/{len(TEST_INSTRUMENTS)} instruments passed")
    if ok_count == len(TEST_INSTRUMENTS):
        print(f"{PASS} Validation passed — ready for full backfill.")
    else:
        print(f"⚠️  Some instruments failed — review before full backfill.")
    print()


if __name__ == '__main__':
    main()
