import logging
from datetime import date
import pandas as pd
import requests

from adapters.base import BaseAdapter

logger = logging.getLogger(__name__)

IMF_BASE = "https://www.imf.org/external/datamapper/api/v1"


class IMFAdapter(BaseAdapter):

    @property
    def name(self) -> str:
        return 'imf'

    def supports(self, asset_class: str) -> bool:
        return asset_class.lower() == 'economic'

    def _fetch(self, indicator: str, country: str) -> pd.DataFrame:
        url = f"{IMF_BASE}/{indicator}/{country}"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        values = (
            data.get('values', {})
                .get(indicator, {})
                .get(country, {})
        )
        if not values:
            return pd.DataFrame()

        rows = []
        for year_str, val in values.items():
            if val is None:
                continue
            try:
                rows.append({
                    'date': date(int(year_str), 12, 31),
                    'value': float(val),
                    'frequency': 'annual',
                })
            except (ValueError, TypeError):
                continue

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).sort_values('date').reset_index(drop=True)

    def fetch_history(self, instrument_id, source_ticker, start_date, end_date) -> pd.DataFrame:
        try:
            # instrument_id format: "CN_NGDP_RPCH" — country_indicator
            parts = instrument_id.split('_', 1)
            country = parts[0] if len(parts) == 2 else 'USA'
            indicator = source_ticker
            df = self._fetch(indicator, country)
            if df.empty:
                return df
            if start_date:
                df = df[df['date'] >= start_date]
            if end_date:
                df = df[df['date'] <= end_date]
            return df
        except Exception as e:
            logger.error(f"[imf] fetch_history failed for {instrument_id}: {e}")
            return pd.DataFrame()

    def fetch_latest(self, instrument_id, source_ticker) -> pd.DataFrame:
        try:
            df = self.fetch_history(instrument_id, source_ticker, None, None)
            if df.empty:
                return df
            return df.tail(1).reset_index(drop=True)
        except Exception as e:
            logger.error(f"[imf] fetch_latest failed for {instrument_id}: {e}")
            return pd.DataFrame()
