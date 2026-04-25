import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import DB_URL, INSTRUMENTS_CSV
from db.schema import raw_prices, raw_economics
from adapters.yfinance_adapter import YFinanceAdapter
from adapters.fred_adapter import FredAdapter
from adapters.worldbank_adapter import WorldBankAdapter
from adapters.imf_adapter import IMFAdapter
from ingestion.backfill import _update_instrument_status, _ingest_prices, _ingest_economics
from analytics.compute import compute_price_analytics, compute_economics_analytics, compute_yield_spreads
from quality.checks import check_price_data, check_economics_data, log_issues

logger = logging.getLogger(__name__)

ADAPTERS = {
    'yfinance': YFinanceAdapter(),
    'fred': FredAdapter(),
    'worldbank': WorldBankAdapter(),
    'imf': IMFAdapter(),
}


def run_daily_update():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    engine = create_engine(DB_URL)

    instruments = pd.read_csv(INSTRUMENTS_CSV)
    active = instruments[instruments['active'].astype(str).str.lower() == 'true']

    total = len(active)
    logger.info(f"Starting daily update for {total} instruments...")

    for i, (_, inst) in enumerate(active.iterrows(), 1):
        iid = inst['instrument_id']
        source = inst['source']
        ticker = inst['source_ticker']
        asset_class = inst['asset_class']

        logger.info(f"[{i}/{total}] {iid}")

        adapter = ADAPTERS.get(source)
        if not adapter:
            logger.warning(f"  No adapter for '{source}', skipping.")
            continue

        try:
            df = adapter.fetch_latest(iid, ticker)
            if df.empty:
                logger.warning(f"  No data for {iid}")
                _update_instrument_status(INSTRUMENTS_CSV, iid, 'failed', 'No data on daily update')
                continue

            if asset_class == 'economic':
                issues = check_economics_data(df, iid)
                _ingest_economics(engine, iid, df, source)
            else:
                issues = check_price_data(df, iid, asset_class=asset_class)
                _ingest_prices(engine, iid, df, source)

            if issues:
                log_issues(engine, issues)

            _update_instrument_status(INSTRUMENTS_CSV, iid, 'ok')

        except Exception as e:
            logger.error(f"  FAILED: {e}")
            _update_instrument_status(INSTRUMENTS_CSV, iid, 'failed', str(e)[:200])

    # Compute analytics after all ingestion
    logger.info("Running analytics compute...")
    price_instruments = active[active['asset_class'] != 'economic']
    for _, inst in price_instruments.iterrows():
        try:
            compute_price_analytics(engine, inst['instrument_id'])
        except Exception as e:
            logger.error(f"Analytics failed for {inst['instrument_id']}: {e}")

    econ_instruments = active[active['asset_class'] == 'economic']
    for _, inst in econ_instruments.iterrows():
        try:
            compute_economics_analytics(engine, inst['instrument_id'])
        except Exception as e:
            logger.error(f"Econ analytics failed for {inst['instrument_id']}: {e}")

    try:
        compute_yield_spreads(engine)
    except Exception as e:
        logger.error(f"Yield spreads failed: {e}")

    logger.info("Daily update complete.")
