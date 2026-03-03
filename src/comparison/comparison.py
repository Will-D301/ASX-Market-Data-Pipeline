import pandas as pd
import numpy as np

from src.config import GOING_LONG_DATA_PATH, BAYESIAN_DATA_PATH
from src.models.position_maker import open_back_test
from src.data_collection.going_long import open_going_long

def merge_equity(long_df: pd.DataFrame, strat_df: pd.DataFrame) -> pd.DataFrame:
    long_cols = long_df[["Ticker", "Date", "equity", "ret_1d"]].rename(
        columns={"equity": "equity_long"}
    )
    strat_cols = strat_df[["Ticker", "Date", "equity", "strategy_ret"]].rename(
        columns={"equity": "equity_strategy"}
    )
    merged = long_cols.merge(strat_cols, on=["Ticker", "Date"], how="inner")
    merged.sort_values(["Ticker", "Date"], inplace=True, ignore_index=True)
    merged["excess_ret"] = merged["strategy_ret"] - merged["ret_1d"]
    return merged


def max_drawdown(equity: pd.Series) -> float:
    running_max = equity.cummax()
    drawdown = equity / running_max - 1
    return drawdown.min()


def cagr(equity: pd.Series) -> float:
    if equity.empty:
        return np.nan
    total_years = (equity.index[-1] - equity.index[0]).days / 365
    if total_years <= 0:
        return np.nan
    start_val = equity.iloc[0]
    end_val = equity.iloc[-1]
    if start_val == 0:
        return np.nan
    return (end_val / start_val) ** (1 / total_years) - 1


def _summarise_ticker(df: pd.DataFrame) -> pd.Series:
    df = df.sort_values("Date").set_index("Date")
    long_eq = df["equity_long"]
    strat_eq = df["equity_strategy"]
    return pd.Series({
        "long_final_equity": long_eq.iloc[-1],
        "strategy_final_equity": strat_eq.iloc[-1],
        "long_cagr": cagr(long_eq),
        "strategy_cagr": cagr(strat_eq),
        "long_max_drawdown": max_drawdown(long_eq),
        "strategy_max_drawdown": max_drawdown(strat_eq),
        "avg_daily_excess_ret": df["excess_ret"].mean(),
    })


def summarise_comparison(merged: pd.DataFrame):
    merged["Date"] = pd.to_datetime(merged["Date"])
    per_ticker = merged.groupby("Ticker", group_keys=False).apply(_summarise_ticker)

    portfolio = merged.groupby("Date").agg(
        equity_long=("equity_long", "mean"),
        equity_strategy=("equity_strategy", "mean"),
        avg_excess_ret=("excess_ret", "mean"),
    )

    summary = {
        "portfolio_long_final_equity": portfolio["equity_long"].iloc[-1],
        "portfolio_strategy_final_equity": portfolio["equity_strategy"].iloc[-1],
        "portfolio_long_cagr": cagr(portfolio["equity_long"]),
        "portfolio_strategy_cagr": cagr(portfolio["equity_strategy"]),
        "portfolio_long_max_drawdown": max_drawdown(portfolio["equity_long"]),
        "portfolio_strategy_max_drawdown": max_drawdown(portfolio["equity_strategy"]),
        "portfolio_avg_daily_excess_ret": portfolio["avg_excess_ret"].mean(),
    }
    return per_ticker, portfolio, summary


def compare_backtests(backtest_name: str, backtest_dir, going_long_path=GOING_LONG_DATA_PATH, ):
    long_df = open_going_long(going_long_path)
    strat_df = open_back_test(backtest_dir, backtest_name)
    merged = merge_equity(long_df, strat_df)
    return summarise_comparison(merged)


def save_comparison(
    backtest_name: str,
    data_path = BAYESIAN_DATA_PATH,
    going_long_path = GOING_LONG_DATA_PATH,
):
    per_ticker, portfolio, summary = compare_backtests(backtest_name, data_path, going_long_path)
    per_ticker_path = data_path / f"{backtest_name}_per_ticker.parquet"
    portfolio_path = data_path / f"{backtest_name}_portfolio.parquet"

    per_ticker.to_parquet(per_ticker_path, engine="pyarrow", index=True)
    portfolio.to_parquet(portfolio_path, engine="pyarrow", index=True)

    summary_path = data_path / f"{backtest_name}_summary.parquet"
    pd.Series(summary).to_frame(name="value").to_parquet(summary_path, engine="pyarrow")
