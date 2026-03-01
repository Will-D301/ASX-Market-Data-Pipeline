import pandas as pd
from src.config import PREDICTION_PROB_DATA_PATH


def compute_probabilities(back_test_data: pd.DataFrame, alpha0=1, beta0=1) -> pd.DataFrame:
    back_test_data_cp = back_test_data.copy()
    back_test_data_cp.dropna(inplace=True, how='any')
    back_test_data_cp = back_test_data_cp.sort_values(by=['Date', 'Ticker']).reset_index(drop=True)

    prob_df = back_test_data_cp.copy()

    p_hat = [None] * len(back_test_data_cp)

    grouped = back_test_data_cp.groupby('Ticker', sort=False).groups.items()
    for ticker, index in grouped:
        alpha = {0: alpha0, 1: alpha0}
        beta = {0: beta0, 1: beta0}

        for i in index:
            ret_up_t = int(back_test_data_cp.at[i, 'ret_up_t'])
            gap_up_on_open = int(back_test_data_cp.at[i, 'gap_up_on_open'])

            p_hat[i] = alpha[ret_up_t] / (alpha[ret_up_t] + beta[ret_up_t])

            if gap_up_on_open == 1:
                alpha[ret_up_t] += 1.0
            else:
                beta[ret_up_t] += 1.0

    prob_df['p_hat'] = p_hat
    return prob_df

def save_prob_data(prob_df: pd.DataFrame, path=PREDICTION_PROB_DATA_PATH) -> None:
    prob_df.to_parquet(path, engine='pyarrow', index=False)

def open_prob_data(path=PREDICTION_PROB_DATA_PATH) -> pd.DataFrame:
    return pd.read_parquet(path, engine='pyarrow')