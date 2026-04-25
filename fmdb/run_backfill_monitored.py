#!/usr/bin/env python3
"""
Monitored backfill runner.
- Runs full backfill instrument by instrument
- Sends Telegram progress update every 30 minutes
- Never stops on individual failures — logs and continues
- Sends final summary when done
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import csv
import logging
import time
import requests
from datetime import date, datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, select, func

from config import DB_URL, INSTRUMENTS_CSV, FRED_API_KEY
from db.schema import create_all, raw_prices, raw_economics
from adapters.yfinance_adapter import YFinanceAdapter
from adapters.fred_adapter import FredAdapter
from adapters.worldbank_adapter import WorldBankAdapter
from adapters.imf_adapter import IMFAdapter
from ingestion.backfill import _ingest_prices, _ingest_economics, _update_instrument_status
from quality.checks import check_price_data, check_economics_data, log_issues

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('/tmp/fmdb-backfill.log'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

HISTORY_START = date(2000, 1, 1)
PROGRESS_INTERVAL = 1800  # 30 minutes
OCLAW_GATEWAY = os.getenv('OPENCLAW_GATEWAY_URL', 'http://127.0.0.1:18789')
OCLAW_TOKEN = os.getenv('OPENCLAW_GATEWAY_TOKEN', '83dd2f1bab27f2c7fbd37a57ca1a536ab29946a2191b4705')
OCLAW_SESSION = 'agent:main:telegram:direct:363349803'

ADAPTERS = {
    'yfinance': YFinanceAdapter(),
    'fred':     FredAdapter(),
    'worldbank': WorldBankAdapter(),
    'imf':      IMFAdapter(),
}


def send_telegram(msg: str):
    """Send a message via OpenClaw gateway to Matt's Telegram session."""
    try:
        # Strip markdown for plain text delivery via REST
        clean = msg.replace('*', '').replace('`', '')
        resp = requests.post(
            f"{OCLAW_GATEWAY}/api/sessions/{OCLAW_SESSION}/send",
            json={'message': clean},
            headers={'Authorization': f'Bearer {OCLAW_TOKEN}'},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"Gateway send failed: {resp.status_code} {resp.text[:100]}")
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")
    logger.info(f"[NOTIFY] {msg.replace(chr(10), ' ')[:120]}")


def main():
    engine = create_engine(DB_URL)
    create_all(engine)

    instruments = pd.read_csv(INSTRUMENTS_CSV)
    active = instruments[instruments['active'].astype(str).str.lower() == 'true']
    total = len(active)

    # Skip already-done instruments
    todo = active[~active['last_status'].isin(['ok'])]
    logger.info(f"Backfill: {len(todo)}/{total} instruments to process")

    send_telegram(
        f"🚀 *fmdb backfill started*\n"
        f"{len(todo)} instruments to process\n"
        f"Started: {datetime.now().strftime('%H:%M ET')}\n"
        f"Progress updates every 30 min."
    )

    ok_count = 0
    fail_count = 0
    failed_ids = []
    last_report = time.time()
    start_time = time.time()

    for i, (_, inst) in enumerate(todo.iterrows(), 1):
        iid        = inst['instrument_id']
        source     = inst['source']
        ticker     = inst['source_ticker']
        asset_class = inst['asset_class']

        # Send progress update every 30 min
        if time.time() - last_report >= PROGRESS_INTERVAL:
            elapsed = int((time.time() - start_time) / 60)
            pct = int(i / len(todo) * 100)
            send_telegram(
                f"📊 *fmdb backfill update* ({elapsed}m elapsed)\n"
                f"Progress: {i}/{len(todo)} ({pct}%)\n"
                f"✅ OK: {ok_count} | ❌ Failed: {fail_count}\n"
                f"Last: `{iid}`"
            )
            last_report = time.time()

        adapter = ADAPTERS.get(source)
        if not adapter:
            logger.warning(f"[{i}/{len(todo)}] {iid} — no adapter for '{source}', skipping")
            _update_instrument_status(INSTRUMENTS_CSV, iid, 'failed', f'No adapter for {source}')
            fail_count += 1
            failed_ids.append(iid)
            continue

        logger.info(f"[{i}/{len(todo)}] {iid} ({source}/{ticker})")

        try:
            df = adapter.fetch_history(iid, ticker, HISTORY_START, date.today())

            if df.empty:
                logger.warning(f"  {iid} — no data returned")
                _update_instrument_status(INSTRUMENTS_CSV, iid, 'failed', 'No data returned')
                fail_count += 1
                failed_ids.append(iid)
                continue

            if asset_class == 'economic':
                issues = check_economics_data(df, iid)
                count = _ingest_economics(engine, iid, df, source)
            else:
                issues = check_price_data(df, iid, asset_class=asset_class)
                count = _ingest_prices(engine, iid, df, source)

            if issues:
                log_issues(engine, issues)

            logger.info(f"  {iid} — {count} rows ingested")
            _update_instrument_status(INSTRUMENTS_CSV, iid, 'ok')
            ok_count += 1

        except Exception as e:
            err = str(e)[:200]
            logger.error(f"  {iid} — FAILED: {err}")
            _update_instrument_status(INSTRUMENTS_CSV, iid, 'failed', err)
            fail_count += 1
            failed_ids.append(iid)

    # Final summary
    elapsed_min = int((time.time() - start_time) / 60)

    with engine.connect() as conn:
        rp = conn.execute(select(func.count()).select_from(raw_prices)).scalar()
        re = conn.execute(select(func.count()).select_from(raw_economics)).scalar()

    fail_list = '\n'.join(f'• `{f}`' for f in failed_ids[:20])
    if len(failed_ids) > 20:
        fail_list += f'\n...and {len(failed_ids) - 20} more'

    summary = (
        f"✅ *fmdb backfill complete* ({elapsed_min}m)\n\n"
        f"✅ OK: {ok_count} | ❌ Failed: {fail_count}\n"
        f"📦 raw_prices: {rp:,} rows\n"
        f"📦 raw_economics: {re:,} rows\n"
    )
    if failed_ids:
        summary += f"\n*Failed instruments:*\n{fail_list}"

    logger.info(summary.replace('*', '').replace('`', ''))
    send_telegram(summary)


if __name__ == '__main__':
    main()
