import yfinance as yf
import pandas as pd
from asx_data_name_collection import get_names_from_etf
from datetime import date

raw_names = get_names_from_etf()
PATH = "asx_50_OHLCV_data.parquet"


def reformat_OHLCV_df(df: pd.DataFrame) -> pd.DataFrame:
    stacked_df = df.stack(level=0, future_stack=True).reset_index()
    stacked_df.columns.name = None
    stacked_df.columns = ["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    stacked_df["Date"] = pd.to_datetime(stacked_df["Date"])

    return stacked_df


def create_asx50_df(start_date = date.today().isoformat(), end_date = None) -> pd.DataFrame:
    raw_asx50_OHLCV_df = yf.download(
        tickers=raw_names,
        start=start_date,
        end=end_date,
        group_by="ticker",
        auto_adjust=False
        )

    if raw_asx50_OHLCV_df is None or raw_asx50_OHLCV_df.empty:
        print("No new data returned. Most likely rate limited by yfinance")
        quit()


    return raw_asx50_OHLCV_df

def save_df_to_file(file_name: str) -> None:
    df = open_OHLCV_data(file_name)

    new_data = create_asx50_df(df["Date"].max().date())
    new_data = reformat_OHLCV_df(new_data)

    joined_data = pd.concat([df, new_data], ignore_index=True)

    joined_data.drop_duplicates(subset=["Ticker", "Date"], keep="last", inplace=True, ignore_index=True)
    joined_data.sort_values(["Ticker","Date"], inplace=True, ignore_index=True)
    joined_data.dropna(subset=["Open","High","Low","Close"], inplace=True, ignore_index=True)


    joined_data.to_parquet(path=file_name, engine="pyarrow", index=False)



def open_OHLCV_data(file_name: str) -> pd.DataFrame:
    try:
        df = pd.read_parquet(path=file_name, engine="pyarrow")
        df["Date"] = pd.to_datetime(df["Date"])
        return df

    except FileNotFoundError:
        df = create_asx50_df(start_date = "2000-01-01")
        return reformat_OHLCV_df(df)


