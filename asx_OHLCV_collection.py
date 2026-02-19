import yfinance as yf
import pandas as pd
from asx_data_name_collection import get_names_from_etf
from datetime import date

raw_names = get_names_from_etf()
PATH = "asx_50_OHLCV_data.parquet"

def create_asx50_df(start_date = date.today().isoformat(), end_date = None) -> pd.DataFrame:
    raw_asx50_OHLCV_df = yf.download(
        tickers=raw_names,
        start = start_date,
        end=end_date,
        group_by="ticker"
        )

    return raw_asx50_OHLCV_df

def save_df_to_file(file_name: str, df: pd.DataFrame) -> None:

    # Manipulate DF so values are vertically not horizontally for more effective storage
    stacked_df = df.stack(level= [0, 1], future_stack=True).reset_index()
    stacked_df.columns = ['Date', 'Ticker', 'Price', 'Value']
    file_exists = True

    try:
        curr_df = pd.read_parquet(path=file_name, engine="pyarrow")

    except FileNotFoundError:
        file_exists = False

    if file_exists:
        stacked_df = pd.concat([curr_df, stacked_df], ignore_index=True)


    stacked_df.drop_duplicates(inplace=True)
    stacked_df.sort_values(['Date'], inplace=True, ignore_index=True)
    stacked_df.dropna(inplace=True, ignore_index=True)

    stacked_df.to_parquet(path=file_name, engine="pyarrow", index=False)

