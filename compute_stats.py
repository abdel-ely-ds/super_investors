import pandas as pd
import numpy as np
import warnings

from metrics import compute_irr, quarter_diff_years
from yahoo import fetch_current_price

warnings.filterwarnings("ignore")


def split_by_sells(df, activity_col="activity"):
    split_dfs = []
    current_chunk = []
    for _, row in df.iloc[::-1].iterrows():
        current_chunk.append(row)
        if row[activity_col].startswith("Sell 100.00%"):
            chunk_df = pd.DataFrame(current_chunk)
            split_dfs.append(chunk_df.iloc[::-1].reset_index(drop=True))
            current_chunk = []
    if current_chunk:
        chunk_df = pd.DataFrame(current_chunk)
        split_dfs.append(chunk_df.iloc[::-1].reset_index(drop=True))
    return split_dfs


def ticker_stats_to_df(ticker_stats):
    rows = []
    for ticker, stats_list in ticker_stats.items():
        irr_best, cost_best = stats_list[0]
        irr_worst, cost_worst = stats_list[1]
        irr_avg, cost_avg = stats_list[2]
        holding_period = stats_list[-3]
        holding = stats_list[-2]
        period = stats_list[-1]
        rows.append(
            {
                "period": period,
                "ticker": ticker,
                "irr_best": irr_best,
                "cost_best": cost_best,
                "irr_worst": irr_worst,
                "cost_worst": cost_worst,
                "irr_avg": irr_avg,
                "cost_avg": cost_avg,
                "holding_period": holding_period,
                "holding": holding,
            }
        )
    return pd.DataFrame(rows)


def compute_stats(df):
    ticker_stats = {}
    for ticker in df.stock.unique():
        stock_hist = df[df.stock == ticker]
        stock_splits = split_by_sells(stock_hist)
        for idx, split in enumerate(stock_splits):
            buys = split[split["activity"].str.startswith(("Add", "Buy"))]
            sells = split[~split["activity"].str.startswith(("Add", "Buy"))]
            if buys.empty or buys.iloc[-1].activity != "Buy":
                continue
            buys["cost_p10"] = buys.shares * buys.price_p10 / 1e6
            buys["cost_p90"] = buys.shares * buys.price_p90 / 1e6
            buys["cost_p50"] = buys.shares * buys.price_p50 / 1e6
            sells["exit_p10"] = sells.shares * sells.price_p10 / 1e6
            sells["exit_p90"] = sells.shares * sells.price_p90 / 1e6
            sells["exit_p50"] = sells.shares * sells.price_p50 / 1e6
            qs = [10, 90, 50]
            cost_cols = {q: f"cost_p{q}" for q in qs}
            exit_cols = {q: f"exit_p{q}" for q in qs}
            worst = avg = best = 0
            worst_cost = avg_cost = best_cost = 0
            min_q = split.iloc[-1].quarter
            max_q = sells.iloc[0].quarter if not sells.empty else None
            shares_still_holding = buys.shares.sum() - sells.shares.sum()
            unrealized_profit = 0
            holding = False
            if shares_still_holding > 0:
                price = fetch_current_price(ticker)
                if price is not None:
                    unrealized_profit = price * shares_still_holding / 1e6
                current_q = pd.Period(pd.Timestamp.now(), freq="Q")
                max_q = f"Q{current_q.quarter} {current_q.year}"
                holding = True
            for q in qs:
                ccol = cost_cols[q]
                ecol = exit_cols[100 - q]
                cost = buys[ccol].sum()
                profit = sells[ecol].sum() - cost
                if q == 10:
                    best = profit + unrealized_profit
                    best_cost = cost
                if q == 90:
                    worst = profit + unrealized_profit
                    worst_cost = cost
                else:
                    avg = profit + unrealized_profit
                    avg_cost = cost
            holding_period = quarter_diff_years(min_q, max_q)
            use_ticker = ticker + str(idx + 1) if len(stock_splits) > 1 else ticker
            ticker_stats[use_ticker] = [
                [compute_irr(best, best_cost, holding_period), best_cost],
                [compute_irr(worst, worst_cost, holding_period), worst_cost],
                [compute_irr(avg, avg_cost, holding_period), avg_cost],
                holding_period,
                holding,
                min_q,
            ]
    stats = ticker_stats_to_df(ticker_stats)
    stats.replace([np.inf, -np.inf], np.nan, inplace=True)
    return stats.dropna()
