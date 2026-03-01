import pandas as pd
import duckdb
from src.data_collection.duckdb_collection import connect
from src.config import OHLCV_WITH_ADJ_PATH, BACK_TEST_DATA_PATH

def compute_adj_open(con: duckdb.DuckDBPyConnection, start_date: str, end_date: str) -> None:
    con.execute(f"""
    COPY(
        SELECT
          Date,
          Ticker,
          "Open",
          "Close",
          "Adj Close" / NULLIF("Close", 0) AS adj_factor,
          "Open" * ("Adj Close" / NULLIF("Close", 0)) AS adj_open_est,
          "Adj Close" as adj_close
        FROM ohlcv 
        WHERE Date >= '{start_date}' AND Date <= '{end_date}'
        ORDER BY Ticker, Date
    ) TO '{OHLCV_WITH_ADJ_PATH}'
    (FORMAT parquet);
    """)

def compute_prob_gapup(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
    COPY(
        WITH adj_open_next_data  AS (
            SELECT
                Date,
                Ticker,
                adj_open_est,
                adj_close,
                LAG(adj_close) OVER (PARTITION BY Ticker ORDER BY Date) AS adj_close_prev,
                    CASE
                        WHEN adj_close > LAG(adj_close) 
                        OVER (PARTITION BY Ticker ORDER BY Date) 
                        THEN 1 
                        ELSE 0
                END AS ret_up_t,
                LEAD(adj_open_est) OVER (
                    PARTITION BY Ticker
                    ORDER BY Date
                ) AS adj_open_est_next
            FROM read_parquet('{OHLCV_WITH_ADJ_PATH}')
        )
        SELECT
            Date,
            Ticker,
            adj_close,
            adj_open_est,
            adj_open_est_next,
            CASE 
                WHEN adj_open_est_next > adj_close
                THEN 1 
                ELSE 0 
            END AS gap_up_on_open,
            ret_up_t,
            (adj_open_est_next / adj_close) - 1 AS gap_ret_next_open
        FROM adj_open_next_data
        ORDER BY Ticker, Date
        ) TO '{BACK_TEST_DATA_PATH}'
        (FORMAT parquet);
    """)
con = connect()