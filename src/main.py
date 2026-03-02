from src.bayesian_alpha_beta_model.position_maker import back_test, save_back_test, open_back_test
from src.bayesian_alpha_beta_model.predict_probabilities import compute_probabilities, open_pred_prob_data
from src.bayesian_alpha_beta_model.prediction_data_creation import compute_adj_open, compute_prob_gapup, open_prob_data
from src.data_collection.asx_data_name_collection import get_names_from_etf
from src.data_collection.asx_OHLCV_collection import save_ohlcv_data, open_ohlcv_data
from src.data_collection.asx_feature_OHLCV_data import save_feature_data, open_feature_data
from src.data_collection import duckdb_collection
from src.data_collection.going_long import filter_tickers, compute_going_long
from src.comparison.comparison import save_comparison


start_date = '2010-01-01'
end_date = '2020-12-31'

def main() -> None:
    raw_names = get_names_from_etf()
    save_ohlcv_data(raw_names)
    ohlcv_data = open_ohlcv_data(raw_names)
    save_feature_data(ohlcv_data)
    con = duckdb_collection.connect()
    duckdb_collection.init_all_views(con)
    compute_adj_open(con, start_date, end_date)
    compute_prob_gapup(con)
    prob_data = open_prob_data(con)
    prob_data = filter_tickers(prob_data, start_date, end_date)
    compute_probabilities(prob_data, con)
    pred_prob_data = filter_tickers(open_pred_prob_data(con), start_date, end_date)
    positions = back_test(pred_prob_data, 0.45, 0.55)
    save_back_test(positions, con, "bt_4555split")
    compute_going_long(pred_prob_data)
    save_comparison("bt_4555split")



if __name__ == '__main__':
    main()
