from adapters.base import BaseAdapter


class BloombergCSVAdapter(BaseAdapter):
    """
    Stub adapter for Bloomberg CSV drop folder.
    Drop CSV files in data/bloomberg_drop/ — not yet implemented.
    """

    @property
    def name(self) -> str:
        return 'bloomberg_csv'

    def supports(self, asset_class: str) -> bool:
        return False

    def fetch_history(self, instrument_id, source_ticker, start_date, end_date):
        raise NotImplementedError(
            "Bloomberg CSV adapter not yet implemented. "
            "Drop CSV files in data/bloomberg_drop/"
        )

    def fetch_latest(self, instrument_id, source_ticker):
        raise NotImplementedError(
            "Bloomberg CSV adapter not yet implemented. "
            "Drop CSV files in data/bloomberg_drop/"
        )
