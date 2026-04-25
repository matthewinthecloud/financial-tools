import logging
from datetime import date
import pandas as pd
import requests

from adapters.base import BaseAdapter

logger = logging.getLogger(__name__)

WB_BASE = "https://api.worldbank.org/v2"


class WorldBankAdapter(BaseAdapter):

    @property
    def name(self) -> str:
        return 'worldbank'

    def supports(self, asset_class: str) -> bool:
        return asset_class.lower() == 'economic'

    def _fetch(self, country: str, indicator: str) -> pd.DataFrame:
        """
        source_ticker format: "NY.GDP.MKTP.CD" (indicator only)
        country comes from instruments.csv country column
        """
        url = f"{WB_BASE}/country/{country}/indicator/{indicator}"
        params = {'format': 'json', 'per_page': 1000, 'mrv': 50}
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if not data or len(data) < 2 or not data[1]:
            return pd.DataFrame()

        rows = []
        for entry in data[1]:
            if entry.get('value') is None:
                continue
            try:
                rows.append({
                    'date': date(int(entry['date']), 12, 31),
                    'value': float(entry['value']),
                    'frequency': 'annual',
                })
            except (ValueError, KeyError, TypeError):
                continue

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).sort_values('date').reset_index(drop=True)

    def fetch_history(self, instrument_id, source_ticker, start_date, end_date) -> pd.DataFrame:
        try:
            # instrument_id format: "GB_NY.GDP.MKTP.CD" — country_indicator
            # source_ticker is the indicator code; country from instruments.csv
            parts = instrument_id.split('_', 1)
            country = parts[0] if len(parts) == 2 else 'US'
            indicator = source_ticker
            df = self._fetch(country, indicator)
            if df.empty:
                return df
            if start_date:
                df = df[df['date'] >= start_date]
            if end_date:
                df = df[df['date'] <= end_date]
            return df
        except Exception as e:
            logger.error(f"[worldbank] fetch_history failed for {instrument_id}: {e}")
            return pd.DataFrame()

    def fetch_latest(self, instrument_id, source_ticker) -> pd.DataFrame:
        try:
            df = self.fetch_history(instrument_id, source_ticker, None, None)
            if df.empty:
                return df
            return df.tail(1).reset_index(drop=True)
        except Exception as e:
            logger.error(f"[worldbank] fetch_latest failed for {instrument_id}: {e}")
            return pd.DataFrame()
