from src.model.position_maker import back_test, save_back_test, open_back_test_data
from src.model.predict_probabilities import compute_probabilities, save_prob_data, open_prob_data
from src.model.prediction_data_creation import compute_adj_open, compute_prob_gapup, open_prob_data
from src.data_collection.asx_data_name_collection import get_names_from_etf
from src.data_collection.asx_OHLCV_collection import save_ohlcv_data, open_ohlcv_data
from src.data_collection.asx_feature_OHLCV_data import save_feature_data
from src.data_collection import duckdb_collection


def main() -> None:

    raw_names = get_names_from_etf()
    save_ohlcv_data(ticker_names=raw_names)
    ohlcv_data = open_ohlcv_data(ticker_names=raw_names)
    save_feature_data(ohlcv_data)
    con = duckdb_collection.connect()
    duckdb_collection.init_all_views(con)
    compute_adj_open(con, "2010-01-01", "2020-12-31")
    compute_prob_gapup(con)
    prob_data_df = open_prob_data()
    predicted_prob_df = compute_probabilities(prob_data_df)
    save_prob_data(predicted_prob_df)
    positions = back_test(predicted_prob_df, 0.45, 0.55)
    save_back_test(positions)
    print(open_back_test_data())

if __name__ == '__main__':
    main()
