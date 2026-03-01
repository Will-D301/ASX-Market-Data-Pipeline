ASX_50_ETF_EXCEL_LINK = "https://www.ssga.com/au/en_gb/intermediary/library-content/products/fund-data/etfs/apac/holdings-daily-au-en-sfy.xlsx"

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

OHLCV_PATH = BASE_DIR / "data/ohlcv_data.parquet"
FEATURE_PATH = BASE_DIR / "data/feature_data.parquet"
DUCKDB_PATH = BASE_DIR / "data/market.db"
