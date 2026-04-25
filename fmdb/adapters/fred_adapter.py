import logging
from datetime import date
import pandas as pd
import requests

from adapters.base import BaseAdapter
from config import FRED_API_KEY

logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred"


class FredAdapter(BaseAdapter):

    @property
    def name(self) -> str:
        return 'fred'

    def supports(self, asset_class: str) -> bool:
        return asset_class.lower() == 'economic'

    def _get_frequency(self, series_id: str) -> str:
        try:
            resp = requests.get(
                f"{FRED_BASE}/series",
                params={'series_id': series_id, 'api_key': FRED_API_KEY, 'file_type': 'json'},
                timeout=10,
            )
            resp.raise_for_status()
            freq = resp.json().get('seriess', [{}])[0].get('frequency_short', 'M')
            mapping = {'D': 'daily', 'W': 'weekly', 'M': 'monthly', 'Q': 'quarterly', 'A': 'annual'}
            return mapping.get(freq, 'monthly')
        except Exception:
            return 'monthly'

    def _fetch(self, series_id: str, start_date=None, end_date=None) -> pd.DataFrame:
        params = {
            'series_id': series_id,
            'api_key': FRED_API_KEY,
            'file_type': 'json',
            'sort_order': 'asc',
        }
        if start_date:
            params['observation_start'] = str(start_date)
        if end_date:
            params['observation_end'] = str(end_date)

        resp = requests.get(f"{FRED_BASE}/series/observations", params=params, timeout=15)
        resp.raise_for_status()
        obs = resp.json().get('observations', [])

        rows = []
        for o in obs:
            if o['value'] == '.':
                continue
            try:
                rows.append({'date': pd.to_datetime(o['date']).date(), 'value': float(o['value'])})
            except (ValueError, KeyError):
                continue

        if not rows:
            return pd.DataFrame()

        freq = self._get_frequency(series_id)
        df = pd.DataFrame(rows)
        df['frequency'] = freq
        return df

    def fetch_history(self, instrument_id, source_ticker, start_date, end_date) -> pd.DataFrame:
        try:
            return self._fetch(source_ticker, start_date, end_date)
        except Exception as e:
            logger.error(f"[fred] fetch_history failed for {instrument_id}: {e}")
            return pd.DataFrame()

    def fetch_latest(self, instrument_id, source_ticker) -> pd.DataFrame:
        try:
            params = {
                'series_id': source_ticker,
                'api_key': FRED_API_KEY,
                'file_type': 'json',
                'sort_order': 'desc',
                'limit': 1,
            }
            resp = requests.get(f"{FRED_BASE}/series/observations", params=params, timeout=10)
            resp.raise_for_status()
            obs = resp.json().get('observations', [])
            if not obs or obs[0]['value'] == '.':
                return pd.DataFrame()
            freq = self._get_frequency(source_ticker)
            return pd.DataFrame([{
                'date': pd.to_datetime(obs[0]['date']).date(),
                'value': float(obs[0]['value']),
                'frequency': freq,
            }])
        except Exception as e:
            logger.error(f"[fred] fetch_latest failed for {instrument_id}: {e}")
            return pd.DataFrame()
