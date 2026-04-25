# fmdb — Financial Markets Database

A financial markets database covering ETFs, indices, FX, crypto, commodities, and global economic data — with a full analytics layer computed daily.

## Prerequisites

- Docker
- Python 3.9+
- pip

## Setup

### 1. Start Postgres
```bash
cd ~/projects/financial-tools/fmdb
docker-compose up -d
```

### 2. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 3. Run historical backfill (first run)
```bash
python run.py backfill
```
This will take several hours on first run — fetching max available history for all instruments.

### 4. Daily update
```bash
python run.py update
```

### 5. Check status
```bash
python run.py status           # all instruments
python run.py status --failed  # failed only
```

## Cron (daily update at 6am ET)
```
0 6 * * * cd ~/projects/financial-tools/fmdb && /usr/bin/python3 run.py update >> /tmp/fmdb-update.log 2>&1
```

## Architecture

```
fmdb/
├── instruments.csv        # Master instrument list — add a row to add an instrument
├── config.py
├── db/schema.py           # SQLAlchemy table definitions
├── adapters/              # One file per data source
│   ├── base.py
│   ├── yfinance_adapter.py
│   ├── fred_adapter.py
│   ├── worldbank_adapter.py
│   ├── imf_adapter.py
│   └── bloomberg_csv_adapter.py  (stub)
├── ingestion/
│   ├── backfill.py
│   └── daily_update.py
├── analytics/compute.py   # All indicator calculations
├── quality/checks.py      # Data quality validation
└── run.py                 # Entry point
```

## Adding a new instrument

Add one row to `instruments.csv` with `last_status=never`. The next `python run.py update` will automatically backfill it.

## Environment variables

- `FMDB_DB_URL` — override default Postgres URL (default: `postgresql://fmdb:fmdb@localhost:5432/fmdb`)
- `FRED_API_KEY` — loaded from `~/projects/financial-tools/.env`
