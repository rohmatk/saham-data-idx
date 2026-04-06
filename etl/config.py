from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / 'data'
RAW_PDF_DIR = DATA_DIR / 'raw' / 'pdfs'
BRONZE_DIR = DATA_DIR / 'bronze' / 'raw_csv'
SILVER_DIR = DATA_DIR / 'silver' / 'clean_csv'
LOG_DIR = DATA_DIR / 'logs'
SAMPLE_DIR = DATA_DIR / 'sample'

for path in [RAW_PDF_DIR, BRONZE_DIR, SILVER_DIR, LOG_DIR, SAMPLE_DIR]:
    path.mkdir(parents=True, exist_ok=True)


def get_database_url() -> str:
    return os.getenv('DATABASE_URL', f"sqlite:///{(DATA_DIR / 'local_dev.db').as_posix()}")


def get_engine():
    return create_engine(get_database_url(), future=True)
