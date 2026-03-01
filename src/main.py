from data_collection.asx_data_name_collection import get_names_from_etf
from data_collection.asx_OHLCV_collection import save_ohlcv_data, open_ohlcv_data
from data_collection.asx_feature_OHLCV_data import save_feature_data
from data_collection import duckdb_collection


def main() -> None:

    raw_names = get_names_from_etf()
    save_ohlcv_data(ticker_names=raw_names)
    ohlcv_data = open_ohlcv_data(ticker_names=raw_names)
    save_feature_data(ohlcv_data)
    con = duckdb_collection.connect()
    duckdb_collection.init_all_views(con)

if __name__ == '__main__':
    main()
