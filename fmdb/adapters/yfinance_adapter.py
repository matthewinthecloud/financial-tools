import time
import logging
from datetime import date, timedelta
import pandas as pd
import yfinance as yf

from adapters.base import BaseAdapter
from config import RATE_LIMIT_SLEEP

logger = logging.getLogger(__name__)

SUPPORTED_ASSET_CLASSES = {'etf', 'index', 'fx', 'crypto', 'commodity'}


class YFinanceAdapter(BaseAdapter):

    @property
    def name(self) -> str:
        return 'yfinance'

    def supports(self, asset_class: str) -> bool:
        return asset_class.lower() in SUPPORTED_ASSET_CLASSES

    def fetch_history(self, instrument_id, source_ticker, start_date, end_date) -> pd.DataFrame:
        try:
            df = yf.download(
                source_ticker,
                start=start_date,
                end=end_date,
                auto_adjust=True,
                progress=False,
                threads=False,
            )
            if df.empty:
                logger.warning(f"[yfinance] No data for {instrument_id} ({source_ticker})")
                return pd.DataFrame()

            # Flatten multi-level columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
            df = df.rename(columns={'date': 'date', 'open': 'open', 'high': 'high',
                                     'low': 'low', 'close': 'close', 'volume': 'volume'})
            df['adj_close'] = df['close']  # auto_adjust=True means close IS adj_close
            df['date'] = pd.to_datetime(df['date']).dt.date

            time.sleep(RATE_LIMIT_SLEEP)
            return df[['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]

        except Exception as e:
            logger.error(f"[yfinance] fetch_history failed for {instrument_id}: {e}")
            return pd.DataFrame()

    def fetch_latest(self, instrument_id, source_ticker) -> pd.DataFrame:
        try:
            end = date.today() + timedelta(days=1)
            start = date.today() - timedelta(days=7)
            df = self.fetch_history(instrument_id, source_ticker, start, end)
            if df.empty:
                return pd.DataFrame()
            return df.tail(1).reset_index(drop=True)
        except Exception as e:
            logger.error(f"[yfinance] fetch_latest failed for {instrument_id}: {e}")
            return pd.DataFrame()
