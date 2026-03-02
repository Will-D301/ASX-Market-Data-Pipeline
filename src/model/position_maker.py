import pandas as pd
import numpy as np
import duckdb
from src.config import BACK_TEST_DATA_PATH

def make_positions(prob_data: pd.DataFrame, short_value: float, long_value: float) -> np.ndarray:
    prob_data_cp = prob_data.copy().reset_index(drop=True)

    positions = np.zeros(len(prob_data_cp), dtype=int)

    for index, row in prob_data_cp.iterrows():

        if row["p_hat"] > long_value:
            positions[index] = 1

        elif row["p_hat"] < short_value:
            positions[index] = -1

    return positions

def back_test(prob_data: pd.DataFrame, short_value: float, long_value: float, start_equity=100, bps_cost=5)-> pd.DataFrame:
    prob_data_cp = prob_data.copy().sort_values(["Ticker", "Date"]).reset_index(drop=True)

    prob_data_cp["pos"] = make_positions(prob_data_cp, short_value, long_value)

    cost = bps_cost / 10000

    prob_data_cp["cost"] = np.abs(prob_data_cp["pos"]) * 2 * cost
    prob_data_cp["strategy_ret"] = prob_data_cp["pos"] * prob_data_cp["gap_ret_next_open"] - prob_data_cp["cost"]
    prob_data_cp["equity"] = start_equity * (1 + prob_data_cp["strategy_ret"]).groupby(prob_data_cp["Ticker"]).cumprod()
    prob_data_cp["pct_return"] = ((prob_data_cp["equity"] / start_equity) - 1) * 100

    return prob_data_cp

def save_back_test(prob_data: pd.DataFrame, con: duckdb.DuckDBPyConnection, name: str,
                   path=BACK_TEST_DATA_PATH):
    file_path = path / f"{name}.parquet"
    prob_data.to_parquet(file_path, engine='pyarrow', index=False)
    con.execute(f"""
    CREATE OR REPLACE VIEW v_{name} AS
    SELECT *
    FROM read_parquet('{file_path}')
    """)

def open_back_test(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(f"""
                    SELECT *
                    FROM back_test  
                    """).df()
