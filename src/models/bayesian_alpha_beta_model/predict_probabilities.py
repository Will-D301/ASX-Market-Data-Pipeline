import pandas as pd
import duckdb


def compute_probabilities(back_test_data: pd.DataFrame, con: duckdb.DuckDBPyConnection,
                          alpha0=1, beta0=1) -> pd.DataFrame:
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

    con.register("p_hat_tmp", prob_df)
    con.execute("""
        CREATE OR REPLACE TABLE pred_prob AS
        SELECT *
        FROM p_hat_tmp
    """)

def open_pred_prob_data(con: duckdb.DuckDBPyConnection):
    return con.execute("""
                       SELECT * 
                       FROM pred_prob
                       """).df()