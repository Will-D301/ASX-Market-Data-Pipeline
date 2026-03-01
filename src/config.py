ASX_50_ETF_EXCEL_LINK = "https://www.ssga.com/au/en_gb/intermediary/library-content/products/fund-data/etfs/apac/holdings-daily-au-en-sfy.xlsx"

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

OHLCV_PATH = BASE_DIR / "data/ohlcv_data.parquet"
FEATURE_PATH = BASE_DIR / "data/feature_data.parquet"
DUCKDB_PATH = BASE_DIR / "data/market.db"

OHLCV_WITH_ADJ_PATH = BASE_DIR / "data/ohlcv_with_adj_data.parquet"
PROB_DATA_PATH = BASE_DIR / "data/prob_data.parquet"
PREDICTION_PROB_DATA_PATH = BASE_DIR / "data/prediction_prob_data.parquet"
BACK_TEST_DATA_PATH = BASE_DIR / "data/back_test_data.parquet"