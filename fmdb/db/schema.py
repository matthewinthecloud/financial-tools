from sqlalchemy import (
    MetaData, Table, Column, Integer, Text, Date, Float,
    DateTime, UniqueConstraint, Index, create_engine
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime

metadata = MetaData()

# ─────────────────────────────────────────────
# RAW TABLES — append only, never modified
# ─────────────────────────────────────────────

raw_prices = Table('raw_prices', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('instrument_id', Text, nullable=False),
    Column('date', Date, nullable=False),
    Column('open', Float),
    Column('high', Float),
    Column('low', Float),
    Column('close', Float, nullable=False),
    Column('adj_close', Float),
    Column('volume', Float),
    Column('source', Text, nullable=False),
    Column('ingested_at', DateTime, default=datetime.utcnow),
    UniqueConstraint('instrument_id', 'date', name='uq_raw_prices_instrument_date'),
)
Index('ix_raw_prices_instrument_date', raw_prices.c.instrument_id, raw_prices.c.date.desc())

raw_economics = Table('raw_economics', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('series_id', Text, nullable=False),
    Column('date', Date, nullable=False),
    Column('value', Float, nullable=False),
    Column('source', Text, nullable=False),
    Column('frequency', Text),  # 'daily', 'monthly', 'quarterly', 'annual'
    Column('ingested_at', DateTime, default=datetime.utcnow),
    UniqueConstraint('series_id', 'date', name='uq_raw_economics_series_date'),
)
Index('ix_raw_economics_series_date', raw_economics.c.series_id, raw_economics.c.date.desc())

raw_quality_log = Table('raw_quality_log', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('instrument_id', Text),
    Column('date', Date),
    Column('check_name', Text),
    Column('severity', Text),   # 'warn', 'error'
    Column('message', Text),
    Column('logged_at', DateTime, default=datetime.utcnow),
)

# ─────────────────────────────────────────────
# ANALYTICS TABLES — fully recomputed daily
# ─────────────────────────────────────────────

analytics_prices = Table('analytics_prices', metadata,
    Column('instrument_id', Text, primary_key=True),
    Column('date', Date, primary_key=True),
    # Returns
    Column('ret_1d', Float),
    Column('ret_1w', Float),
    Column('ret_1m', Float),
    Column('ret_3m', Float),
    Column('ret_6m', Float),
    Column('ret_12m', Float),
    Column('ret_ytd', Float),
    # Volatility
    Column('vol_20d', Float),
    Column('vol_60d', Float),
    Column('vol_252d', Float),
    # Moving averages
    Column('ma_50d', Float),
    Column('ma_200d', Float),
    Column('dist_ma50', Float),
    Column('dist_ma200', Float),
    Column('golden_cross', Integer),  # 1=golden, -1=death, 0=neither
    # Risk
    Column('beta_spy_252d', Float),
    Column('sharpe_252d', Float),
    Column('drawdown_52w', Float),
    # Oscillators
    Column('rsi_14', Float),
    Column('mfi_14', Float),
    Column('macd_line', Float),
    Column('macd_signal', Float),
    Column('macd_hist', Float),
    Column('atr_14', Float),
    Column('atr_100', Float),
    Column('bb_width', Float),
    Column('adx', Float),
    # RMI Trend Sniper
    Column('rmi_value', Float),
    Column('rmi_signal', Text),  # 'bullish', 'bearish', 'neutral'
    Column('computed_at', DateTime, default=datetime.utcnow),
)
Index('ix_analytics_prices_instrument_date', analytics_prices.c.instrument_id, analytics_prices.c.date.desc())

analytics_economics = Table('analytics_economics', metadata,
    Column('series_id', Text, primary_key=True),
    Column('date', Date, primary_key=True),
    Column('value', Float),
    Column('yoy_change', Float),
    Column('mom_change', Float),
    Column('zscore_1y', Float),
    Column('zscore_5y', Float),
    Column('computed_at', DateTime, default=datetime.utcnow),
)

analytics_yield_spreads = Table('analytics_yield_spreads', metadata,
    Column('date', Date, primary_key=True),
    Column('spread_2s10s', Float),
    Column('spread_5s30s', Float),
    Column('computed_at', DateTime, default=datetime.utcnow),
)


def create_all(engine):
    metadata.create_all(engine)
    print("All tables created.")


def drop_all(engine):
    metadata.drop_all(engine)
    print("All tables dropped.")
