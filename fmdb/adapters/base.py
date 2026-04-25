from abc import ABC, abstractmethod
from datetime import date
import pandas as pd


class BaseAdapter(ABC):
    """
    Abstract base class for all data source adapters.

    Market data adapters return DataFrames with columns:
        date, open, high, low, close, adj_close, volume

    Economics adapters return DataFrames with columns:
        date, value, frequency
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Identifier for this adapter, e.g. 'yfinance', 'fred'"""
        pass

    @abstractmethod
    def fetch_history(
        self,
        instrument_id: str,
        source_ticker: str,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        Fetch full historical data for an instrument.
        Returns empty DataFrame on failure — never raises.
        """
        pass

    @abstractmethod
    def fetch_latest(
        self,
        instrument_id: str,
        source_ticker: str,
    ) -> pd.DataFrame:
        """
        Fetch the most recent data point for an instrument.
        Returns empty DataFrame on failure — never raises.
        """
        pass

    @abstractmethod
    def supports(self, asset_class: str) -> bool:
        """Returns True if this adapter handles the given asset_class."""
        pass
