import duckdb
import pandas as pd

OHLCV_PARQUET_PATH = "ohlcv_data.parquet"
FEATURE_PARQUET_PATH = "feature_data.parquet"

def connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect("market.db")

def init_all_views(con: duckdb.DuckDBPyConnection, ohlcv_parquet=OHLCV_PARQUET_PATH, feature_parquet=FEATURE_PARQUET_PATH):
    # Creates ohlcv data view
    con.execute(f""" 
            CREATE OR REPLACE VIEW ohlcv AS
            SELECT * FROM read_parquet('{ohlcv_parquet}');
        """)

    # Creates feature data view
    con.execute(f"""
            CREATE OR REPLACE VIEW features AS
            SELECT * 
            FROM read_parquet('{feature_parquet}'); 
        """)

    # Joins both views so each company on a date has all information
    con.execute(f"""
        CREATE OR REPLACE VIEW market AS
        SELECT o.*, f.* EXCLUDE (Date, Ticker)
        FROM ohlcv o
        JOIN features f USING (Date, Ticker); 
        """)

