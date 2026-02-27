import pandas as pd
from asx_OHLCV_collection import save_ohlcv_data, open_OHLCV_data
from asx_feature_OHLCV_data import save_feature_data
import duckdb_collection

def main() -> None:

    save_ohlcv_data()
    ohlcv_data = open_OHLCV_data()
    save_feature_data(ohlcv_data)
    con = duckdb_collection.connect()
    duckdb_collection.init_all_views(con)

if __name__ == '__main__':
    main()