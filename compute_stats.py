import pandas as pd
import numpy as np
import warnings
from datetime import datetime

from metrics import compute_xirr, quarter_to_date, quarter_diff_years
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
        irr_best, cost_best, ret_best = stats_list[0]
        irr_worst, cost_worst, ret_worst = stats_list[1]
        irr_avg, cost_avg, ret_avg = stats_list[2]
        holding_period = stats_list[-3]
        holding = stats_list[-2]
        period = stats_list[-1]
        rows.append(
            {
                "period": period,
                "ticker": ticker,
                "irr_best": irr_best,
                "cost_best": cost_best,
                "ret_best": ret_best,
                "irr_worst": irr_worst,
                "cost_worst": cost_worst,
                "ret_worst": ret_worst,
                "irr_avg": irr_avg,
                "cost_avg": cost_avg,
                "ret_avg": ret_avg,
                "holding_period": holding_period,
                "holding": holding,
            }
        )
    return pd.DataFrame(rows)


def _build_cash_flows(buys, sells, ccol, ecol, shares_holding, unrealized_price, now):
    """Build sorted (date, amount) list for XIRR. Negative = buy, positive = sell."""
    flows = []
    for _, row in buys.iterrows():
        flows.append((quarter_to_date(row.quarter), -row[ccol]))
    for _, row in sells.iterrows():
        flows.append((quarter_to_date(row.quarter), row[ecol]))
    if shares_holding > 0 and unrealized_price is not None:
        flows.append((now, unrealized_price * shares_holding / 1e6))
    flows.sort(key=lambda x: x[0])
    return flows


def compute_stats(df):
    ticker_stats = {}
    now = datetime.now()

    for ticker in df.stock.unique():
        stock_hist = df[df.stock == ticker]
        stock_splits = split_by_sells(stock_hist)

        for idx, split in enumerate(stock_splits):
            buys = split[split["activity"].str.startswith(("Add", "Buy"))]
            sells = split[~split["activity"].str.startswith(("Add", "Buy"))]
            if buys.empty or buys.iloc[-1].activity != "Buy":
                continue

            buys = buys.copy()
            sells = sells.copy()

            for q in [10, 50, 90]:
                buys[f"cost_p{q}"] = buys.shares * buys[f"price_p{q}"] / 1e6
                sells[f"exit_p{q}"] = sells.shares * sells[f"price_p{q}"] / 1e6

            cost_cols = {q: f"cost_p{q}" for q in [10, 90, 50]}
            exit_cols = {q: f"exit_p{q}" for q in [10, 90, 50]}

            min_q = split.iloc[-1].quarter
            shares_still_holding = buys.shares.sum() - sells.shares.sum()
            unrealized_price = None
            holding = False

            if shares_still_holding > 0:
                unrealized_price = fetch_current_price(ticker)
                current_q = pd.Period(pd.Timestamp.now(), freq="Q")
                max_q = f"Q{current_q.quarter} {current_q.year}"
                holding = True
            else:
                max_q = sells.iloc[0].quarter if not sells.empty else None

            holding_period = quarter_diff_years(min_q, max_q)

            # best (q=10 cost, q=90 exit), worst (q=90 cost, q=10 exit), avg (q=50, q=50)
            mode_map = {"best": (10, 90), "worst": (90, 10), "avg": (50, 50)}
            results = {}

            for label, (cq, eq) in mode_map.items():
                ccol = cost_cols[cq]
                ecol = exit_cols[eq]
                cost = buys[ccol].sum()

                flows = _build_cash_flows(
                    buys, sells, ccol, ecol,
                    shares_still_holding, unrealized_price, now,
                )

                irr = compute_xirr(flows) if len(flows) >= 2 else 0.0

                total_inflows = sum(a for _, a in flows if a > 0)
                ret = (total_inflows / cost - 1) if cost > 0 else 0.0

                results[label] = (irr, cost, ret)

            use_ticker = ticker + str(idx + 1) if len(stock_splits) > 1 else ticker
            ticker_stats[use_ticker] = [
                list(results["best"]),
                list(results["worst"]),
                list(results["avg"]),
                holding_period,
                holding,
                min_q,
            ]

    stats = ticker_stats_to_df(ticker_stats)
    stats.replace([np.inf, -np.inf], np.nan, inplace=True)
    return stats.dropna()
