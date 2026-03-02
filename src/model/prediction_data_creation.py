import pandas as pd
import duckdb

def compute_adj_open(con: duckdb.DuckDBPyConnection, start_date: str, end_date: str,) -> None:
    con.execute(f"""
        CREATE OR REPLACE VIEW ohlcv_with_adj AS
        SELECT
            Date,
            Ticker,
            "Open",
            "Close",
            "Adj Close" / "Close" AS adj_factor,
            "Open" * ("Adj Close" / "Close") AS adj_open_est,
            "Adj Close" AS adj_close
        FROM ohlcv
        WHERE Date >= '{start_date}' AND Date <= '{end_date}';
    """)


def compute_prob_gapup(con: duckdb.DuckDBPyConnection, adj_view_name="ohlcv_with_adj",) -> None:
    con.execute(f"""
        CREATE OR REPLACE VIEW prob_data AS
        WITH adj_open_next_data AS (
            SELECT
                Date,
                Ticker,
                adj_open_est,
                adj_close,
                LAG(adj_close) OVER w AS adj_close_prev,
                LEAD(adj_open_est) OVER w AS adj_open_est_next
            FROM {adj_view_name}
            WINDOW w AS (PARTITION BY Ticker ORDER BY Date)
        )
        SELECT
            Date,
            Ticker,
            adj_close,
            adj_open_est,
            adj_open_est_next,
            CASE
                WHEN adj_open_est_next > adj_close THEN 1
                ELSE 0
            END AS gap_up_on_open,
            CASE
                WHEN adj_close > adj_close_prev THEN 1
                ELSE 0
            END AS ret_up_t,
            (adj_open_est_next / adj_close) - 1 AS gap_ret_next_open
        FROM adj_open_next_data;
    """)

def open_prob_data(con) -> pd.DataFrame:
    return con.execute(f"""
                       SELECT *
                       FROM prob_data
                       """).df()