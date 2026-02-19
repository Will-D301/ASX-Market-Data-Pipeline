import yfinance as yf
import pandas as pd
from asx_data_name_collection import get_names_from_etf
from datetime import date

raw_names = get_names_from_etf()

def create_asx50_df(start_date = date.today().isoformat(), end_date = None) -> pd.DataFrame:
    df = yf.download(
        tickers=raw_names,
        end=end_date,
        group_by="ticker"
    )
