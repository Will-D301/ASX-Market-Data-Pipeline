import requests
import pandas as pd
import io
from src.config import ASX_50_ETF_EXCEL_LINK

def get_names_from_etf() -> list[str]:
    raw_content = requests.get(ASX_50_ETF_EXCEL_LINK, timeout = 100).content
    content = pd.read_excel(io.BytesIO(raw_content), skiprows = 4, skipfooter = 4)
    return [name.removesuffix("-AU") + ".AX" for name in content["Ticker"]]
