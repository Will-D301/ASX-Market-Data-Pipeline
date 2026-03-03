import duckdb
import pandas as pd
from src.config import OHLCV_PATH, FEATURE_PATH, DUCKDB_PATH

def connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DUCKDB_PATH)

def init_all_views(con: duckdb.DuckDBPyConnection, ohlcv_path=OHLCV_PATH, feature_path=FEATURE_PATH):
    # Creates ohlcv data view
    con.execute(f""" 
            CREATE OR REPLACE VIEW ohlcv AS
            SELECT * FROM read_parquet('{ohlcv_path}');
        """)

    # Creates feature data view
    con.execute(f"""
            CREATE OR REPLACE VIEW features AS
            SELECT * 
            FROM read_parquet('{feature_path}'); 
        """)

    # Joins both views so each company on a date has all information
    con.execute(f"""
        CREATE OR REPLACE VIEW market AS
        SELECT o.*, f.* EXCLUDE (Date, Ticker)
        FROM ohlcv o
        JOIN features f USING (Date, Ticker); 
        """)

def join_feature_prob_data(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(f"""
        SELECT f.*, p.gap_up_on_open, p.gap_ret_next_open
        FROM features f
        JOIN pred_prob p USING (Date, Ticker)
    """).df()