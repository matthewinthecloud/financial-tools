#!/usr/bin/env python3
"""
fmdb entry point.

Usage:
  python run.py backfill         Full historical backfill (first run)
  python run.py update           Daily update + analytics
  python run.py status           Summary of all instruments
  python run.py status --failed  Show only failed instruments
"""
import sys
import pandas as pd
from config import INSTRUMENTS_CSV


def cmd_status(failed_only=False):
    df = pd.read_csv(INSTRUMENTS_CSV)
    total = len(df)
    ok = len(df[df['last_status'] == 'ok'])
    failed = len(df[df['last_status'] == 'failed'])
    never = len(df[df['last_status'].isin(['never', '']) | df['last_status'].isna()])

    print(f"\n{'─'*50}")
    print(f"  fmdb Instrument Status")
    print(f"{'─'*50}")
    print(f"  Total:   {total}")
    print(f"  ✅ OK:    {ok}")
    print(f"  ❌ Failed: {failed}")
    print(f"  ⏳ Never:  {never}")
    print(f"{'─'*50}\n")

    if failed_only:
        subset = df[df['last_status'] == 'failed'][['instrument_id', 'last_updated', 'last_error']]
    else:
        subset = df[['instrument_id', 'last_status', 'last_updated', 'last_error']]

    print(subset.to_string(index=False))
    print()


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(1)

    cmd = args[0]

    if cmd == 'backfill':
        from ingestion.backfill import run_backfill
        run_backfill()

    elif cmd == 'update':
        from ingestion.daily_update import run_daily_update
        run_daily_update()

    elif cmd == 'status':
        failed_only = '--failed' in args
        cmd_status(failed_only)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == '__main__':
    main()
