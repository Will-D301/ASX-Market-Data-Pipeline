import pandas as pd
import numpy as np
from src.data_collection.asx_OHLCV_collection import open_ohlcv_data
from src.config import OHLCV_PATH, FEATURE_PATH

def open_feature_data(file_name=FEATURE_PATH) -> pd.DataFrame:
    try:
        df = pd.read_parquet(path=file_name, engine="pyarrow")
        df["Date"] = pd.to_datetime(df["Date"])
        return df

    except FileNotFoundError:
        return create_feature_data_df(open_ohlcv_data(OHLCV_PATH))


def create_feature_data_df(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    px = "Adj Close"

    companies_ohlcv = df.groupby("Ticker", sort=False)

    feature_df = pd.DataFrame({
        "Date": df["Date"],
        "Ticker": df["Ticker"],

        "ret_1d": pd.Series(index=df.index, dtype="float64"),
        "ret_5d": pd.Series(index=df.index, dtype="float64"),
        "ret_20d": pd.Series(index=df.index, dtype="float64"),
        "ret_60d": pd.Series(index=df.index, dtype="float64"),
        "ret_252d": pd.Series(index=df.index, dtype="float64"),

        "px_sma20": pd.Series(index=df.index, dtype="float64"),
        "px_sma50": pd.Series(index=df.index, dtype="float64"),
        "px_sma200": pd.Series(index=df.index, dtype="float64"),

        "vol_20d": pd.Series(index=df.index, dtype="float64"),
        "vol_60d": pd.Series(index=df.index, dtype="float64"),
        "atr_14_pct": pd.Series(index=df.index, dtype="float64"),

        "hl_range": pd.Series(index=df.index, dtype="float64"),
        "gap": pd.Series(index=df.index, dtype="float64"),
        "ret_oc": pd.Series(index=df.index, dtype="float64"),

        "dollar_vol": pd.Series(index=df.index, dtype="float64"),
        "dollar_vol_20d": pd.Series(index=df.index, dtype="float64"),
        "vol_z_20d": pd.Series(index=df.index, dtype="float64"),

        "dow": pd.Series(index=df.index, dtype="Int8"),

        "tradeable": pd.Series(index=df.index, dtype="bool"),
    })

    # returns (Adj Close)
    feature_df["ret_1d"] = companies_ohlcv[px].pct_change(1)
    feature_df["ret_5d"] = companies_ohlcv[px].pct_change(5)
    feature_df["ret_20d"] = companies_ohlcv[px].pct_change(20)
    feature_df["ret_60d"] = companies_ohlcv[px].pct_change(60)
    feature_df["ret_252d"] = companies_ohlcv[px].pct_change(252)

    # log returns + realised vol
    logret_1d = np.log(df[px]).groupby(df["Ticker"]).diff()
    feature_df["vol_20d"] = logret_1d.groupby(df["Ticker"]).rolling(20).std().reset_index(level=0, drop=True)
    feature_df["vol_60d"] = logret_1d.groupby(df["Ticker"]).rolling(60).std().reset_index(level=0, drop=True)

    # moving-average ratios (Adj Close)
    sma20 = df[px].groupby(df["Ticker"]).rolling(20).mean().reset_index(level=0, drop=True)
    sma50 = df[px].groupby(df["Ticker"]).rolling(50).mean().reset_index(level=0, drop=True)
    sma200 = df[px].groupby(df["Ticker"]).rolling(200).mean().reset_index(level=0, drop=True)

    feature_df["px_sma20"] = df[px] / sma20  - 1
    feature_df["px_sma50"] = df[px] / sma50  - 1
    feature_df["px_sma200"] = df[px] / sma200 - 1

    # ATR(14) as % of Adj Close
    prev_close = companies_ohlcv["Close"].shift(1)
    tr = np.maximum.reduce([
        (df["High"] - df["Low"]).to_numpy(),
        (df["High"] - prev_close).abs().to_numpy(),
        (df["Low"] - prev_close).abs().to_numpy(),
    ])
    tr = pd.Series(tr, index=df.index)

    atr14 = tr.groupby(df["Ticker"]).rolling(14).mean().reset_index(level=0, drop=True)
    feature_df["atr_14_pct"] = atr14 / df[px]

    # range / execution
    feature_df["hl_range"] = df["High"] / df["Low"] - 1
    feature_df["gap"] = df["Open"] / companies_ohlcv["Close"].shift(1) - 1
    feature_df["ret_oc"] = df["Close"] / df["Open"] - 1

    # liquidity / volume
    feature_df["dollar_vol"] = df[px] * df["Volume"]

    feature_df["dollar_vol_20d"] = feature_df["dollar_vol"].groupby(df["Ticker"]).rolling(20).mean().reset_index(level=0, drop=True)


    vol_mean = df["Volume"].groupby(df["Ticker"]).rolling(20).mean().reset_index(level=0, drop=True)
    vol_std = df["Volume"].groupby(df["Ticker"]).rolling(20).std().reset_index(level=0, drop=True)
    feature_df["vol_z_20d"] = (df["Volume"] - vol_mean) / vol_std.replace(0, np.nan)

    # calendar
    feature_df["dow"] = df["Date"].dt.dayofweek.astype("Int8")

    # Tradeable if volume > 0 (At least one trade was made)
    feature_df["tradeable"] = df["Volume"].gt(0)

    return feature_df

def save_feature_data(ohlcv_data: pd.DataFrame, file_name=FEATURE_PATH) -> None:
    features = create_feature_data_df(ohlcv_data)
    features.dropna(subset=["ret_252d"], inplace=True, ignore_index=True)
    features.to_parquet(file_name, engine="pyarrow", index=False)
