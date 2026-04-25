import logging
from datetime import datetime, date
import numpy as np
import pandas as pd
from sqlalchemy import select, delete, insert
from db.schema import raw_prices, raw_economics, analytics_prices, analytics_economics, analytics_yield_spreads

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _load_prices(engine, instrument_id: str) -> pd.DataFrame:
    with engine.connect() as conn:
        rows = conn.execute(
            select(raw_prices).where(raw_prices.c.instrument_id == instrument_id)
            .order_by(raw_prices.c.date)
        ).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=raw_prices.c.keys())
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()
    return df


def _load_spy(engine) -> pd.Series:
    df = _load_prices(engine, 'SPY')
    if df.empty:
        return pd.Series(dtype=float)
    return df['adj_close'].pct_change().dropna()


def _rmi(close: pd.Series, length: int = 8) -> pd.Series:
    """Range Momentum Index (base for RMI Trend Sniper)"""
    delta = close.diff(length)
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rmi = 100 - (100 / (1 + rs))
    return rmi


def _rwma(series: pd.Series, length: int = 8) -> pd.Series:
    """Range Weighted Moving Average"""
    weights = np.arange(1, length + 1)
    return series.rolling(length).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)


def _rmi_trend_sniper(df: pd.DataFrame, length: int = 8, pos_thresh: float = 66,
                       neg_thresh: float = 30, signal_period: int = 5):
    """
    RMI Trend Sniper — TZack88 (TradingView: rtD9lc5D)
    Parameters: length=8, positive threshold=66, negative threshold=30, EMA signal=5
    """
    close = df['adj_close']
    high = df['high']
    low = df['low']

    rmi = _rmi(close, length)
    rwma = _rwma(close, length)

    # ATR bands
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(length).mean()
    upper_band = rwma + atr
    lower_band = rwma - atr

    # 5-period EMA for signal confirmation
    ema_signal = rmi.ewm(span=signal_period, adjust=False).mean()

    signals = []
    for i in range(len(rmi)):
        r = rmi.iloc[i]
        e = ema_signal.iloc[i]
        if pd.isna(r) or pd.isna(e):
            signals.append('neutral')
        elif r > pos_thresh and e > pos_thresh:
            signals.append('bullish')
        elif r < neg_thresh and e < neg_thresh:
            signals.append('bearish')
        else:
            signals.append('neutral')

    return pd.Series(rmi.values, index=df.index), pd.Series(signals, index=df.index)


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta).clip(lower=0).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _mfi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    tp = (df['high'] + df['low'] + df['adj_close']) / 3
    mf = tp * df['volume'].fillna(0)
    pos_mf = mf.where(tp > tp.shift(), 0).rolling(period).sum()
    neg_mf = mf.where(tp <= tp.shift(), 0).rolling(period).sum()
    mfr = pos_mf / neg_mf.replace(0, np.nan)
    return 100 - (100 / (1 + mfr))


def _macd(close: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def _atr(df: pd.DataFrame, period: int) -> pd.Series:
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['adj_close'].shift()).abs(),
        (df['low'] - df['adj_close'].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def _adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df['high'], df['low'], df['adj_close']
    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1/period, adjust=False).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1/period, adjust=False).mean() / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1/period, adjust=False).mean()


# ─────────────────────────────────────────────
# Main compute functions
# ─────────────────────────────────────────────

def compute_price_analytics(engine, instrument_id: str, date_range=None):
    df = _load_prices(engine, instrument_id)
    if df.empty or len(df) < 20:
        logger.warning(f"[analytics] Not enough data for {instrument_id}, skipping.")
        return

    spy_returns = _load_spy(engine)
    close = df['adj_close']
    today = df.index[-1]

    def pct_ret(days):
        if len(df) <= days:
            return None
        return (close.iloc[-1] / close.iloc[-1 - days] - 1) * 100

    def ytd_ret():
        ytd_start = df[df.index.year == today.year]
        if ytd_start.empty:
            return None
        return (close.iloc[-1] / ytd_start['adj_close'].iloc[0] - 1) * 100

    def rolling_vol(days):
        if len(df) < days:
            return None
        return close.pct_change().rolling(days).std().iloc[-1] * np.sqrt(252) * 100

    def beta(days=252):
        if len(df) < days:
            return None
        instr_ret = close.pct_change().dropna()
        common = instr_ret.index.intersection(spy_returns.index)
        if len(common) < days // 2:
            return None
        x = spy_returns.loc[common].tail(days)
        y = instr_ret.loc[common].tail(days)
        cov = np.cov(y, x)
        return cov[0, 1] / cov[1, 1] if cov[1, 1] != 0 else None

    def sharpe(days=252):
        if len(df) < days:
            return None
        ret = close.pct_change().dropna().tail(days)
        return (ret.mean() / ret.std()) * np.sqrt(252) if ret.std() != 0 else None

    def drawdown_52w():
        window = df.tail(252)
        peak = window['adj_close'].max()
        current = close.iloc[-1]
        return ((current - peak) / peak) * 100 if peak != 0 else None

    ma50 = close.rolling(50).mean().iloc[-1] if len(df) >= 50 else None
    ma200 = close.rolling(200).mean().iloc[-1] if len(df) >= 200 else None
    last_close = close.iloc[-1]

    dist_ma50 = ((last_close - ma50) / ma50 * 100) if ma50 else None
    dist_ma200 = ((last_close - ma200) / ma200 * 100) if ma200 else None

    golden_cross = 0
    if ma50 and ma200:
        prev_ma50 = close.rolling(50).mean().iloc[-2] if len(df) >= 51 else None
        prev_ma200 = close.rolling(200).mean().iloc[-2] if len(df) >= 201 else None
        if prev_ma50 and prev_ma200:
            if prev_ma50 <= prev_ma200 and ma50 > ma200:
                golden_cross = 1
            elif prev_ma50 >= prev_ma200 and ma50 < ma200:
                golden_cross = -1

    rsi = _rsi(close).iloc[-1] if len(df) >= 15 else None
    mfi = _mfi(df).iloc[-1] if len(df) >= 15 and 'volume' in df.columns else None
    macd_line, macd_sig, macd_hist = _macd(close)
    atr14 = _atr(df, 14).iloc[-1] if len(df) >= 14 else None
    atr100 = _atr(df, 100).iloc[-1] if len(df) >= 100 else None

    # Bollinger Band width
    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    bb_width = ((bb_mid + 2 * bb_std) - (bb_mid - 2 * bb_std)) / bb_mid
    bb_w = bb_width.iloc[-1] if len(df) >= 20 else None

    adx = _adx(df).iloc[-1] if len(df) >= 28 else None

    rmi_vals, rmi_sigs = _rmi_trend_sniper(df) if len(df) >= 20 else (pd.Series(), pd.Series())
    rmi_val = rmi_vals.iloc[-1] if not rmi_vals.empty else None
    rmi_sig = rmi_sigs.iloc[-1] if not rmi_sigs.empty else 'neutral'

    row = {
        'instrument_id': instrument_id,
        'date': today.date(),
        'ret_1d': pct_ret(1),
        'ret_1w': pct_ret(5),
        'ret_1m': pct_ret(21),
        'ret_3m': pct_ret(63),
        'ret_6m': pct_ret(126),
        'ret_12m': pct_ret(252),
        'ret_ytd': ytd_ret(),
        'vol_20d': rolling_vol(20),
        'vol_60d': rolling_vol(60),
        'vol_252d': rolling_vol(252),
        'ma_50d': float(ma50) if ma50 else None,
        'ma_200d': float(ma200) if ma200 else None,
        'dist_ma50': dist_ma50,
        'dist_ma200': dist_ma200,
        'golden_cross': golden_cross,
        'beta_spy_252d': beta(),
        'sharpe_252d': sharpe(),
        'drawdown_52w': drawdown_52w(),
        'rsi_14': float(rsi) if rsi and not np.isnan(rsi) else None,
        'mfi_14': float(mfi) if mfi and not np.isnan(mfi) else None,
        'macd_line': float(macd_line.iloc[-1]) if not macd_line.empty else None,
        'macd_signal': float(macd_sig.iloc[-1]) if not macd_sig.empty else None,
        'macd_hist': float(macd_hist.iloc[-1]) if not macd_hist.empty else None,
        'atr_14': float(atr14) if atr14 and not np.isnan(atr14) else None,
        'atr_100': float(atr100) if atr100 and not np.isnan(atr100) else None,
        'bb_width': float(bb_w) if bb_w and not np.isnan(bb_w) else None,
        'adx': float(adx) if adx and not np.isnan(adx) else None,
        'rmi_value': float(rmi_val) if rmi_val and not np.isnan(rmi_val) else None,
        'rmi_signal': rmi_sig,
        'computed_at': datetime.utcnow(),
    }

    with engine.connect() as conn:
        conn.execute(
            delete(analytics_prices).where(
                analytics_prices.c.instrument_id == instrument_id,
                analytics_prices.c.date == row['date']
            )
        )
        conn.execute(insert(analytics_prices).values(**row))
        conn.commit()

    logger.info(f"[analytics] Computed price analytics for {instrument_id}")


def compute_economics_analytics(engine, series_id: str):
    with engine.connect() as conn:
        rows = conn.execute(
            select(raw_economics).where(raw_economics.c.series_id == series_id)
            .order_by(raw_economics.c.date)
        ).fetchall()

    if not rows:
        return

    df = pd.DataFrame(rows, columns=raw_economics.c.keys())
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()
    values = df['value']

    yoy = values.pct_change(periods=12) * 100  # assumes monthly; annual divides by 1
    mom = values.pct_change(periods=1) * 100

    def zscore(window_days):
        roll = values.rolling(window_days)
        return (values - roll.mean()) / roll.std().replace(0, np.nan)

    z1y = zscore(252)
    z5y = zscore(252 * 5)

    records = []
    for dt, val in values.items():
        records.append({
            'series_id': series_id,
            'date': dt.date(),
            'value': float(val) if not np.isnan(val) else None,
            'yoy_change': float(yoy.loc[dt]) if dt in yoy.index and not np.isnan(yoy.loc[dt]) else None,
            'mom_change': float(mom.loc[dt]) if dt in mom.index and not np.isnan(mom.loc[dt]) else None,
            'zscore_1y': float(z1y.loc[dt]) if dt in z1y.index and not np.isnan(z1y.loc[dt]) else None,
            'zscore_5y': float(z5y.loc[dt]) if dt in z5y.index and not np.isnan(z5y.loc[dt]) else None,
            'computed_at': datetime.utcnow(),
        })

    with engine.connect() as conn:
        conn.execute(delete(analytics_economics).where(analytics_economics.c.series_id == series_id))
        conn.execute(insert(analytics_economics), records)
        conn.commit()

    logger.info(f"[analytics] Computed economics analytics for {series_id}")


def compute_yield_spreads(engine):
    series_needed = {'DGS2': None, 'DGS5': None, 'DGS10': None, 'DGS30': None}

    with engine.connect() as conn:
        for sid in series_needed:
            rows = conn.execute(
                select(raw_economics).where(raw_economics.c.series_id == sid)
                .order_by(raw_economics.c.date)
            ).fetchall()
            if rows:
                df = pd.DataFrame(rows, columns=raw_economics.c.keys())
                df['date'] = pd.to_datetime(df['date'])
                series_needed[sid] = df.set_index('date')['value']

    dfs = {k: v for k, v in series_needed.items() if v is not None}
    if len(dfs) < 2:
        logger.warning("[analytics] Not enough yield series for spread computation.")
        return

    combined = pd.DataFrame(dfs)
    combined = combined.dropna(how='all')

    records = []
    for dt, row in combined.iterrows():
        s2 = row.get('DGS2')
        s5 = row.get('DGS5')
        s10 = row.get('DGS10')
        s30 = row.get('DGS30')
        records.append({
            'date': dt.date(),
            'spread_2s10s': float(s10 - s2) if pd.notna(s10) and pd.notna(s2) else None,
            'spread_5s30s': float(s30 - s5) if pd.notna(s30) and pd.notna(s5) else None,
            'computed_at': datetime.utcnow(),
        })

    if not records:
        return

    with engine.connect() as conn:
        conn.execute(delete(analytics_yield_spreads))
        conn.execute(insert(analytics_yield_spreads), records)
        conn.commit()

    logger.info(f"[analytics] Computed yield spreads ({len(records)} rows)")
