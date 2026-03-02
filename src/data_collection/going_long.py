import pandas as pd
from src.data_collection.asx_feature_OHLCV_data import open_feature_data
from src.config import GOING_LONG_DATA_PATH

def filter_tickers(df, start_date, end_date):

    span = df.groupby("Ticker")["Date"].agg(min_date="min", max_date="max")

    tickers_ok = span[(span["min_date"] <= start_date) & (span["max_date"] >= end_date)].index

    df = df[df["Ticker"].isin(tickers_ok) & (df["Date"].between(start_date, end_date))]

    return df

def compute_going_long(feature_data: pd.DataFrame, start_equity=100, path=GOING_LONG_DATA_PATH) -> None:
    prob_df = feature_data.copy()
    prob_df = prob_df.sort_values(["Ticker", "Date"])

    equity = start_equity * (1 + prob_df["ret_1d"]).groupby(prob_df["Ticker"]).cumprod()
    prob_df["equity"] = equity

    prob_df.to_parquet(path, engine="pyarrow", index=False)

compute_going_long(filter_tickers(open_feature_data(), "2010-01-01", "2020-12-31"))
