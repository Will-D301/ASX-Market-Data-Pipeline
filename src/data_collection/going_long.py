import pandas as pd
import numpy as np
from src.config import GOING_LONG_DATA_PATH

def filter_tickers(df, start_date, end_date):
    df = df.copy()

    df["Date"] = pd.to_datetime(df["Date"])
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    window_start = max(df["Date"].min(), start)
    window_end = min(df["Date"].max(), end)

    span = df.groupby("Ticker")["Date"].agg(min_date="min", max_date="max")
    tickers_ok = span[(span["min_date"] <= window_start) & (span["max_date"] >= window_end)].index

    df = df[df["Ticker"].isin(tickers_ok) & (df["Date"].between(window_start, window_end))]

    return df

def compute_going_long(feature_data: pd.DataFrame, start_equity=100, path=GOING_LONG_DATA_PATH) -> None:
    prob_df = feature_data.copy()
    prob_df = prob_df.sort_values(["Ticker", "Date"])

    prob_df["vol_scale"] = prob_df["vol_20d"] * np.sqrt(252) * 100

    equity = start_equity * (1 + prob_df["ret_1d"]).groupby(prob_df["Ticker"]).cumprod()
    prob_df["equity"] = equity
    prob_df = prob_df[['Ticker', 'Date', 'ret_1d', 'vol_scale', 'equity']]

    prob_df.to_parquet(path, engine="pyarrow", index=False)


def open_going_long(path=GOING_LONG_DATA_PATH) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["Date"] = pd.to_datetime(df["Date"])
    return df
