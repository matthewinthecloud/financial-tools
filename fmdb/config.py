import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from financial-tools root
load_dotenv(Path(__file__).parent.parent / '.env')

# Database
DB_URL = os.getenv('FMDB_DB_URL', 'postgresql://fmdb:fmdb@localhost:5432/fmdb')

# Paths
BASE_DIR = Path(__file__).parent
INSTRUMENTS_CSV = BASE_DIR / 'instruments.csv'
BLOOMBERG_DROP_DIR = BASE_DIR / 'data' / 'bloomberg_drop'

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# API Keys
FRED_API_KEY = os.getenv('FRED_API_KEY')

# Ingestion settings
MAX_RETRIES = 3
BATCH_SIZE = 10          # instruments per yfinance batch
RATE_LIMIT_SLEEP = 1.0   # seconds between batches
