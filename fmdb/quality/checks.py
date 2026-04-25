import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import insert
from db.schema import raw_quality_log

logger = logging.getLogger(__name__)


NO_VOLUME_ASSET_CLASSES = {'fx', 'index', 'precious_metal'}


def check_price_data(df: pd.DataFrame, instrument_id: str, asset_class: str = '', **kwargs) -> list:
    kwargs['skip_volume'] = asset_class.lower() in NO_VOLUME_ASSET_CLASSES or kwargs.get('skip_volume', False)
    issues = []

    if df.empty:
        return issues

    # Null close prices
    null_closes = df[df['close'].isnull()]
    for _, row in null_closes.iterrows():
        issues.append({
            'instrument_id': instrument_id,
            'date': row.get('date'),
            'check_name': 'null_close',
            'severity': 'error',
            'message': 'Close price is null',
        })

    # Negative prices
    neg = df[(df['close'] < 0) | (df['open'] < 0) | (df['high'] < 0) | (df['low'] < 0)]
    for _, row in neg.iterrows():
        issues.append({
            'instrument_id': instrument_id,
            'date': row.get('date'),
            'check_name': 'negative_price',
            'severity': 'error',
            'message': f"Negative price detected: close={row.get('close')}",
        })

    # Zero volume — skip for asset classes where volume is not applicable
    # (FX, indices, precious metals spot — handled by caller passing skip_volume=True)
    if not kwargs.get('skip_volume', False):
        zero_vol = df[df['volume'] == 0]
        for _, row in zero_vol.iterrows():
            issues.append({
                'instrument_id': instrument_id,
                'date': row.get('date'),
                'check_name': 'zero_volume',
                'severity': 'warn',
                'message': 'Volume is zero',
            })

    # Large day-over-day price gaps (>20%)
    if 'close' in df.columns and len(df) > 1:
        df = df.sort_values('date')
        pct_change = df['close'].pct_change().abs()
        gaps = df[pct_change > 0.20]
        for _, row in gaps.iterrows():
            issues.append({
                'instrument_id': instrument_id,
                'date': row.get('date'),
                'check_name': 'large_price_gap',
                'severity': 'warn',
                'message': f"Price gap >20% detected on {row.get('date')}",
            })

    # Duplicate dates
    dupes = df[df.duplicated('date', keep=False)]
    if not dupes.empty:
        issues.append({
            'instrument_id': instrument_id,
            'date': None,
            'check_name': 'duplicate_dates',
            'severity': 'error',
            'message': f"{len(dupes)} duplicate date rows detected",
        })

    return issues


def check_economics_data(df: pd.DataFrame, series_id: str) -> list:
    issues = []

    if df.empty:
        return issues

    # Null values
    nulls = df[df['value'].isnull()]
    for _, row in nulls.iterrows():
        issues.append({
            'instrument_id': series_id,
            'date': row.get('date'),
            'check_name': 'null_value',
            'severity': 'error',
            'message': 'Economic value is null',
        })

    # Extreme outliers (>5 std devs)
    if len(df) > 10:
        mean = df['value'].mean()
        std = df['value'].std()
        if std > 0:
            outliers = df[(df['value'] - mean).abs() > 5 * std]
            for _, row in outliers.iterrows():
                issues.append({
                    'instrument_id': series_id,
                    'date': row.get('date'),
                    'check_name': 'extreme_outlier',
                    'severity': 'warn',
                    'message': f"Value {row.get('value')} is >5 std devs from mean ({mean:.2f})",
                })

    # Duplicate dates
    dupes = df[df.duplicated('date', keep=False)]
    if not dupes.empty:
        issues.append({
            'instrument_id': series_id,
            'date': None,
            'check_name': 'duplicate_dates',
            'severity': 'error',
            'message': f"{len(dupes)} duplicate date rows detected",
        })

    return issues


def log_issues(engine, issues: list):
    if not issues:
        return
    rows = [{**issue, 'logged_at': datetime.utcnow()} for issue in issues]
    with engine.connect() as conn:
        conn.execute(insert(raw_quality_log), rows)
        conn.commit()
    logger.info(f"Logged {len(rows)} quality issue(s).")
