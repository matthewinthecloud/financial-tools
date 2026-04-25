import logging
import csv
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import DB_URL, INSTRUMENTS_CSV, BATCH_SIZE
from db.schema import create_all, raw_prices, raw_economics
from adapters.yfinance_adapter import YFinanceAdapter
from adapters.fred_adapter import FredAdapter
from adapters.worldbank_adapter import WorldBankAdapter
from adapters.imf_adapter import IMFAdapter
from quality.checks import check_price_data, check_economics_data, log_issues

logger = logging.getLogger(__name__)

ADAPTERS = {
    'yfinance': YFinanceAdapter(),
    'fred': FredAdapter(),
    'worldbank': WorldBankAdapter(),
    'imf': IMFAdapter(),
}

HISTORY_START = date(2000, 1, 1)


def _update_instrument_status(csv_path, instrument_id, status, error=''):
    rows = []
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            if row['instrument_id'] == instrument_id:
                row['last_status'] = status
                row['last_error'] = error
                if status == 'ok':
                    row['last_updated'] = datetime.utcnow().isoformat()
            rows.append(row)

    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _has_data(engine, instrument_id, asset_class):
    with engine.connect() as conn:
        if asset_class == 'economic':
            count = conn.execute(
                select(func.count()).select_from(raw_economics)
                .where(raw_economics.c.series_id == instrument_id)
            ).scalar()
        else:
            count = conn.execute(
                select(func.count()).select_from(raw_prices)
                .where(raw_prices.c.instrument_id == instrument_id)
            ).scalar()
    return count > 0


def _ingest_prices(engine, instrument_id, df, source):
    if df.empty:
        return 0
    df = df.dropna(subset=['close'])
    records = []
    for _, row in df.iterrows():
        records.append({
            'instrument_id': instrument_id,
            'date': row['date'],
            'open': row.get('open'),
            'high': row.get('high'),
            'low': row.get('low'),
            'close': row['close'],
            'adj_close': row.get('adj_close'),
            'volume': row.get('volume'),
            'source': source,
            'ingested_at': datetime.utcnow(),
        })
    if not records:
        return 0
    with engine.connect() as conn:
        stmt = pg_insert(raw_prices).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['instrument_id', 'date'])
        conn.execute(stmt)
        conn.commit()
    return len(records)


def _ingest_economics(engine, series_id, df, source):
    if df.empty:
        return 0
    records = []
    for _, row in df.iterrows():
        records.append({
            'series_id': series_id,
            'date': row['date'],
            'value': row['value'],
            'source': source,
            'frequency': row.get('frequency', 'monthly'),
            'ingested_at': datetime.utcnow(),
        })
    if not records:
        return 0
    with engine.connect() as conn:
        stmt = pg_insert(raw_economics).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['series_id', 'date'])
        conn.execute(stmt)
        conn.commit()
    return len(records)


def run_backfill():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    engine = create_engine(DB_URL)
    create_all(engine)

    instruments = pd.read_csv(INSTRUMENTS_CSV)
    active = instruments[instruments['active'].astype(str).str.lower() == 'true']

    total = len(active)
    logger.info(f"Starting backfill for {total} instruments...")

    for i, (_, inst) in enumerate(active.iterrows(), 1):
        iid = inst['instrument_id']
        source = inst['source']
        ticker = inst['source_ticker']
        asset_class = inst['asset_class']

        logger.info(f"[{i}/{total}] {iid} ({source})")

        if _has_data(engine, iid, asset_class):
            logger.info(f"  Already has data, skipping.")
            continue

        adapter = ADAPTERS.get(source)
        if not adapter:
            logger.warning(f"  No adapter for source '{source}', skipping.")
            _update_instrument_status(INSTRUMENTS_CSV, iid, 'failed', f'No adapter for {source}')
            continue

        try:
            df = adapter.fetch_history(iid, ticker, HISTORY_START, date.today())

            if df.empty:
                logger.warning(f"  No data returned for {iid}")
                _update_instrument_status(INSTRUMENTS_CSV, iid, 'failed', 'No data returned')
                continue

            if asset_class == 'economic':
                issues = check_economics_data(df, iid)
                count = _ingest_economics(engine, iid, df, source)
            else:
                issues = check_price_data(df, iid)
                count = _ingest_prices(engine, iid, df, source)

            if issues:
                log_issues(engine, issues)

            logger.info(f"  Ingested {count} rows.")
            _update_instrument_status(INSTRUMENTS_CSV, iid, 'ok')

        except Exception as e:
            logger.error(f"  FAILED: {e}")
            _update_instrument_status(INSTRUMENTS_CSV, iid, 'failed', str(e)[:200])

    logger.info("Backfill complete.")
